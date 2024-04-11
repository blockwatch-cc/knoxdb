# KnoxDB

Blockwatch KnoxDB is a columnar database for fast and efficient blockchain analytics. KnoxDB is stable and ready to use, but has a few limitations which will be addressed in future versions.

## Concepts

KnoxDB is an in-process hybrid transactional and analytics database for building blockchain indexes and derived datasets written in Go. It provides high-level APIs for working with relational data models, key-value data and time-series. Internal data structures and algorithms are designed for maximum speed, storage efficiency and flexibility.

KnoxDB is a great choice for storing immutable event feeds like blocks and transactions, it supports rapid updates of fast-changing objects like account balances, and it enables on-the-fly generation of aggregate statistics and time-series.

### Highlights

- flexible table engines: packed column vectors, key-value (LSM-tree)
- vector engine with in-memory journal for fast updates on hot rows
- multiple indexes types: hash, foreign-key, composite
- index-free tables using zone-maps and bloom filters
- on disc compression: zigzag, delta, RLE, simple8, gorilla, snappy, lz4
- SIMD accelerated algorithms

### Data types

- timestamps (w/o time-zone)
- signed|unsigned integers 8|16|32|64|128|256 bit
- float 32|64
- decimals 32|64|128|256 bit
- variable length binary with auto-deduplication
- variable length string with auto-deduplication
- bool
- bitmaps

### Engines

KnoxDB supports two kinds of storage engines at the moment:

- `pack` a vector based (columnar) engine with configurable segment sizes and SIMD support
- `kv` a key-value engine based on Log-Structured Merge trees

The Pack engine is well suited for append only data models, statistics and data that has a moderate amount of row updates. Columnar vectors allow for stunning compression (in particular of integer vectors). Queries make use of zone maps and bloom filters defined for each column vector segment which makes then fast, especially for analytical and time-series workloads that need to visit many rows. Use this engine as default whenever you can.

The Key-value engine has an extremely high read and write throughput. Use it for data models that update very often, like accounts, balances or other state tracking objects. The downside of the KV engine is that compression is much less effective and that it requires explicit indexes on every field or combination of fields which appear in typical queries.


### Limitations and Caveats

KnoxDB is still an early-stage database system. Users should be aware of a few limitations when constructing data models and when integrating KnoxDB into an application. Some concepts and interfaces may change in the future.

- KnoxDB is a Golang library, so access is limited to a single OS process
- each table must have a primary key
- primary keys are uint64, value zero is an invalid key
- tables are limited to 256 columns
- renaming or altering columns is not possible
- transactions are limited to tables in the same database file
- data durability is only guaranteed after calling a table's `Sync()` method
- packed vector segment and journal length must be between 2^8 (256) and 2^22 (4M)
- key-value LSM engine is a singleton, all tables & indexes share the same namespace

At Blockwatch we successfully use KnoxDB in production for our market and blockchain indexers. However, several features found in traditional databases are not available yet.

- Write-ahead log, redo-log and checkpointing
- Cross-table transactions
- Native enum and bigint types
- Advanced joins
- Group by queries
- Top-k queries
- Materialized views
- Continuous queries
- Triggers and change data capture into event streams

## Usage

### Defining Table Schemas

Table schemas in KnoxDB are static at the moment. You cannot change them after you have created a table. To insert data you must pass a Go struct type fully implements the schema, i.e. it defines all fields with compatible types. To read/decode records from queries you may pass a shorter Go struct type with less fields, but names and types must still match.

The canonical way to define a schema is by configuring a list of fields

```go
schema := pack.FieldList{
  {
    Index: 0,
    Name: "id",
    Type:  pack.FieldTypeUint64,
    Flags: pack.FlagPrimary,
  },
  {
    Index: 1,
    Name: "address",
    Type:  pack.FieldTypeBytes,
    Flags: pack.FlagBloom,
    Scale: 3,
  },
  {
    Index: 2,
    Name: "balance",
    Type:  pack.FieldTypeUint64,
  },
}
```

A more convenient way for Go developers is to add Go struct tags under the key `knox`. The first argument in a Knox tag is the column name, followed by optional arguments. To ignore fields, use dash as name (`knox="-"`). By default, column vector segments are compressed based on their type and contents using advanced integer compression methods and de-duplication.

Optional arguments let you configure the handling of column vectors in greater detail:

- type modifier: changes the stored bit-depth of integers and decimals but may lose precision
- type argument: configures type-specific traits
- index argument: defines which kind of index to create for this column
- compression argument: defines how packed vectors are compressed on disk

```
// type modifiers
u8, u16, u32, u64    - cast from/to any unsigned Go int type to this storage type
i8, i16, i32, i64    - cast from/to any signed Go int type to this storage type
d32, d64, d128, d256 - cast from float or int type to fixed point decimal

// type arguments
pk        - field is primary key
scale=num - fixed decimals for decimal types

// index arguments
bloom=num       - generate bloom filters of precision num (1: 2%, 2: 0.2%, 3: 0.02%, 4: 0.002%)
index=kind      - generate sorted index of kind for this field (hash, int, composite)

// compression arguments
snappy  - compress vector using snappy
lz4     - compress vector using lz4
```

### Defining Indexes

Indexes differ between `pack` and `kv` engines. Thanks to automatic zone maps and bloom filters, pack vector tables almost never require an index. KV engine tables always require indexes for every field or combination of fields that are often queried together.

For pack vector tables, KnoxDB automatically generates zone maps for every vector segment so that even if no index exists, a full table scan would still not have to visit all data. This works best on sorted fields like primary keys and timestamps (depending on the data set) and sufficiently well on low cardinality selective fields like enums.

For unsorted columns with low cardinality (enums) or sparse distributions (sender/receiver addresses or account ids) it is often sufficient to enable bloom filters. Bloom filters are probabilistic data structures with a configurable false positive rate. When dimensioned well, they can successfully skip most non-matching vector segments before they are loaded from disk.

The pack engine also supports two kinds of indexes on single fields: `hash` for arbitrary data (strings, bytes) which is hashed to 64 bit and `int` for referencing foreign uint64 keys.

The KV engine on the other hand requires explicit indexes. It supports single field and composite indexes on any type of column.


### Creating a Database

```go
import "blockwatch.cc/knoxdb/pack"

// db options
engine := "bolt"        // b-tree storage best for packed vector tables
path := "./db"          // directory to store database file
name := "mydb"          // filename without suffix, KnoxDB automatically adds '.db'
label := "custom-label" // must match on open, use to distinguish many similar dbs

// create new or open existing db
db, err := pack.CreateDatabaseIfNotExists(engine, path, name, label)

// define table schema: we use Go struct tags here, but you can also define an
// explicit field list; make sure column names are unique
type Account struct {
    Id      uint64 `knox:"id,pk"`
    Address []byte `knox:"address,bloom=3"`
    Balance uint64 `knox:"balance"`
}

// extract schema from knox struct tags
schema, err := pack.MakeSchema(Account{})

// table options
opts := pack.Options{
    PackSizeLog2:    16,  // use vector segments of 32k rows
    JournalSizeLog2: 17,  // keep 64k rows in memory cache before merge
    CacheSize:       128, // use at most 128MB memory for caching packs to speed up queries
    FillLevel:       100, // fill bolt file pages, we won't rewrite packs (append-only)
}

// create new or open existing table
table, err := db.CreateTableIfNotExists(
    pack.TableEnginePack,
    "accounts",
    schema,
    opts,
)

// close table after use to sync pending data and metadata to disk
defer table.Close()
```

If we were to use a KV table instead, we would define a slightly different schema:

```go
// define a KV table schema
type AccountKV struct {
    Id      uint64 `knox:"id,pk"`
    Address []byte `knox:"address,index"`
    Balance uint64 `knox:"balance"`
}

// extract schema from knox struct tags
schema, err := pack.MakeSchema(Account{})

// create new or open existing table
table, err := db.CreateTableIfNotExists(
    pack.TableEngineKV,
    "accounts",
    schema,
    pack.NoOptions,
)

// create address index (for simplicity we use all schema fields tagged as index)
err = table.CreateIndexIfNotExists(pack.IndexKindComposite, schema.Indexed(), pack.NoOptions)
```


### Writing Data

```go
var (
  single Account
  batch []Account
)

// write a single record, note we pass a pointer so that Insert can
// write the new primary key back into the struct; if the struct already
// contains a valid primary key (>0), it is used
err := table.Insert(ctx, &single)

// write multiple records at once, note the slice may or may not
// contains pointers, primary keys are written in-place if missing
err := table.Insert(ctx, batch)

// update a record, note: the primary key must be valid (>0), but
// in case a record with this key does not exist, Update is semantically
// similar to Insert
err := table.Update(ctx, &single)

// update multiple records
err := table.Update(ctx, batch)

// delete data that matches a query (we explain query creation below)
num, err := table.Delete(ctx, query)

// delete records by primary key
num, err := table.DeletePks(ctx, []uint64{1, 2})
```

### Query Data

There are multiple ways to query data from KnoxDB. All have in common that they require a `Query` object. Queries can be constructed step-wise by chaining member function calls. Each function call returns a new Query struct (with all previous fields copied), so that any build stage can be cached and re-used later.

```go
import "blockwatch.cc/knoxdb/pack"

// Query API
type IQuery interface {
  // bind query to table (REQUIRED)
  WithTable(Table) Query

  // result columns SQL SELECT
  WithColumns(names ...string) Query

  // limit/offset
  WithLimit(l int) Query
  WithOffset(o int) Query

  // ordering, only primary key (SQL ORDER BY id)
  WithOrder(o OrderType) Query
  WithDesc() Query
  WithAsc() Query

  // filter conditions (SQL WHERE)
  AndCondition(conds ...UnboundCondition) Query
  OrCondition(conds ...UnboundCondition) Query
  And(field string, mode FilterMode, value any) Query
  Or(field string, mode FilterMode, value any) Query
  AndEqual(field string, value any) Query
  AndNotEqual(field string, value any) Query
  AndIn(field string, value any) Query
  AndNotIn(field string, value any) Query
  AndLt(field string, value any) Query
  AndLte(field string, value any) Query
  AndGt(field string, value any) Query
  AndGte(field string, value any) Query
  AndRegexp(field string, value any) Query
  AndRange(field string, from, to any) Query

  // query execution
  Execute(ctx context.Context, val any) error
  Stream(ctx context.Context, fn func(r Row) error)
  Delete(ctx context.Context) (int64, error)
  Count(ctx context.Context) (int64, error)
  Run(ctx context.Context) (*Result, error)

  // debugging
  WithIndex(enable bool) Query
  WithoutIndex() Query
  WithCache(enable bool) Query
  WithoutCache() Query
  WithStats() Query
  WithoutStats() Query
  WithStatsAfter(d time.Duration) Query
  WithDebug() Query
}
```

#### Lets run a simple query to find an account by its (binary) address.

```go
var (
  acc Account
  addr []byte
)

// construct and execute a query, because our target is a single struct
// at most one record is returned
err := pack.NewQuery("request-id-or-identifier").
  WithTable(table).
  AndEqual("address", addr).
  Execute(ctx, &acc)

// check we have actually found a match, no match means the original
// account struct remains untouched, so we can check for Id == 0
if acc.Id == 0 {
  // no record found
}
```

#### List the first 1000 accounts with a minimum balance.

```go
list := make([]Account, 0, 1000)

// construct and execute a query, because our target is a slice
// up to limit values are returned
err := pack.NewQuery("request-id-or-identifier").
  WithTable(table).
  WithLimit(1000).
  AndGte("balance", 100_000_000).
  Execute(ctx, &list)

// check we have actually found a match
if len(list) == 0 {
  // no record found
}
```

#### Streaming Data

In cases where raw results are not required for long we can use streaming queries to save memory allocations. Lets say we want to add all balances greater than a minimum and at the same time count how many accounts hold that much.

```go
// define a single struct into which we will decode
var acc Account

// define our results
var (
  count int
  sum uint64
)

// run a stream query and only request the balance field
err := pack.NewQuery("request-id-or-identifier").
  WithTable(table).
  AndGte("balance", 100_000_000).
  WithColumns("balance").
  Stream(ctx, func(r pack.Row) error {
    // decode the next match into our struct, this will only decode
    // the balance field since the result row does not contain anything else
    if err := r.Decode(&acc); err != nil {
      return err
    }

    // aggregate
    sum += acc.Balance
    count++

    // if we wanted to stop early we can return pack.EndStream
    // as special error
    if sum > 100_000_000_000 {
      return pack.EndStream
    }

    return nil
  })
```

### Generating Time-series

Time-series are special kinds of streaming queries that aggregate data across pre-defined time windows. Because this use-case is so common, KnoxDB offers a dedicated API for it.

First build a `Request` with the minimum of a table to query from, a list of column expressions, a time range and an interval.

- Aggregation function: sum, mean, var, std, first, last, min, max, count
- Fill function: none, null, last, linear, zero
- Interval: flexible, many pre-defines like TimeUnitDay for 1 day
- Range: flexible, either absolute or relative to now

```go
import "blockwatch.cc/knoxdb/series"

// type Request struct {
//     Select   ExprList  `form:"select"`
//     Range    TimeRange `form:"range,default=M"`
//     Interval TimeUnit  `form:"interval,default=d"`
//     Fill     FillMode  `form:"fill,default=none"`
//     Limit    int       `form:"limit,default=100"`
//     GroupBy  string    `form:"group_by"`
//     Table    string    `form:"table"`
//     TypeMap  TypeMap
// }

// construct and run a time-series request
res, err := series.NewRequest().
    WithTable(table).
    WithExpr("volume", series.ReducerFuncSum).
    WithRange(series.NewTimeRangeSince(series.TimeUnitMonth)).
    WithInterval(series.TimeUnitDay).
    Run(ctx, "my-request")


// time series results are currently only meant to be sent as JSON back to a user
buf, err := res.MarshalJSON()

// which produces this structure
//  {"series": [{
//     "name": "table_name",
//     "tags":{"group_by_field":"group_name"}
//     "columns": ["col_1", "col_2"],
//     "values": [[...],[...]]
//  }]

```

## Architecture

- Layers:
  - Query Layer: query construction, analysis, scheduling
  - Table Layer: relational tables, indexes, transactions, caches
  - Storage Layer: low-level KV store and filesystem access
- Data Flow:
  - pack engine: user -> journal -> packs
  - KV engine: user -> LSM tree
- Transaction semantics:
  - ACI(D) for Insert, Update, Delete calls
  - (D)urability requires explicit Sync() calls currently
  - concurrent read transactions
  - pessimistic concurrency control (per-table locks) for write transactions

## Code Structure

* **cache** in-memory cache interface and LRU implementations
* **encoding** data compression and encoding libraries
* **encoding/bitmap** roaring bitmap wrapper
* **encoding/block** block-level encoding for compressed column vector segments
* **encoding/compress** vector compression for int, float, bool, timestamp, string, bytes
* **encoding/csv** CSV format reader/writer
* **encoding/decimal** fixed point decimal types
* **encoding/dedup** byte/string de-duplication helpers
* **encoding/s8b** Simple8b integer compression lib (adapted from https://github.com/jwilder/encoding)
* **encoding/xroar** optimized Roaring bitmap library (adapted from https://github.com/outcaste-io/sroar)
* **filter** bloom and cuckoo filter implementations
* **filter/bloom** efficient bloom filter implementations using xxhash32 and pow-2 sizes
* **filter/cuckoo** cuckoo filter implementations
* **filter/loglogbeta** fast cardinality esitmation
* **hash** hash utilities
* **hash/blake256** blacke256 hash from Dmitry Chestnykh
* **hash/metro** metro hash from Damian Gryski
* **hash/murmur3** murmur3 hash from Sébastien Paolacci
* **hash/xxhash** xxhash64 version from Caleb Spare
* **hash/xxhash32** xxhash32 version from Pierre Curto
* **hash/xxhashvec** vectorized versions of xxhash32, xxhash64 and XXH3
* **pack** columnar database engine and query processing
* **series** time-series handling on top of KnoxDB stream queries
* **store** database wrapper for key-value stores
* **store/bolt** BoltDB (B+ tree) key-value store
* **store/badger** BadgerDB (LSM tree) key-value store
* **tools** utility programs for managing KnoxDB database files
* **util** helpers for ints, strings, times, durations, etc.
* **vec** SIMD vector library

## License

KnoxDB is licensed under the [Apache License, Version 2.0](LICENSE). © 2018-2024 Blockwatch Data Inc.
