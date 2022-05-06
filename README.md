There be dragons.

© 2018-2020 Blockwatch Data Inc, All rights reserved.

Blockwatch PackDB columnar database for fast blockchain analytics.

* **cache** in-memory cache interface and LRU impl
* **encoding** data compression and encoding libraries
* **encoding/block** block-level encoding for compressed column vector groups
* **encoding/compress** vector compression for int, float, bool, timestamp, string, bytes
* **encoding/csv** CSV format reader/writer
* **encoding/simple8b** Simple8b integer compression lib (from https://github.com/jwilder/encoding)
* **filter** bloom and cuckoo filter implementations
* **hash** hash utilities for different blockchains (blacke256, metro, murmur3, xxhash)
* **pack** columnar database engine and query processing
* **store** database wrapper for key-value stores
* **store/bolt** BoltDB (B+ tree) key-value store
* **util** utility helpers for ints, strings, times, durations
* **vec** vector library

## Links / Competitors / Inspirations

LibMDBX (faster lmdb, used by Erigon eth nodes)
https://gitflic.ru/project/erthink/libmdbx

OmniSciDB (MapD Core)
https://github.com/omnisci/omniscidb

TiDB
- https://www.crunchbase.com/organization/pingcap
- https://github.com/pingcap/tidb
- https://pingcap.com/docs/dev/overview/

Clickhouse
- https://clickhouse.yandex/docs/en/data_types/decimal/
- https://www.altinity.com/blog/2020/1/1/clickhouse-cost-efficiency-in-action-analyzing-500-billion-rows-on-an-intel-nuc
- https://presentations.clickhouse.tech/original_website/benchmark.html

Brytlyt (GPU database, closed source)
- https://www.brytlyt.com/blog/gpu-databases-today-big-challenge-joins/#

Interesting DB design read with loads of refs
- https://en.wikipedia.org/wiki/Slowly_changing_dimension
- https://news.ycombinator.com/item?id=19818899
- https://news.ycombinator.com/item?id=21970952 // clickhouse perf
- https://tech.marksblogg.com/benchmarks.html // top DB benchmarks

ScyllaDB
- https://github.com/scylladb/scylla

M3DB (Uber)
- https://m3db.github.io/m3/m3db/architecture/storage/

QuestDB (raised 2.1M in 2019/20, London)
- https://github.com/questdb/questdb // embedded, Postgres protocol, zero-GC, low latency

Tarantool (in-memory + on-disk DB with SQL layer, WAL)
https://github.com/tarantool/tarantool

Apache Arrow (Columnar in-memory data processing library)
- https://github.com/apache/arrow
- https://arrow.apache.org/docs/index.html

Benchmarks
- https://tech.marksblogg.com/benchmarks.html
- https://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html
- https://blog.timescale.com/blog/building-columnar-compression-in-a-row-oriented-database/


### Performance
- [x] scalable pack header index
- [x] binary marhslaing for pack headers
- [x] numeric index (foreign key relations, uint64)
- [x] faster range scans for inbetween pack selection
- [x] process joins in packs, loop to pull more data until limit is reached
- [x] ASM impl for between functions
- [x] change query/stream to always output pk sorted results
  - merge journal/tombstone as last step
  - keep journal/tombstone id maps for fast lookups
- [x] improved compression
  - [x] lz4 instead of snappy (test benefits first: int, time, byte cols!)
  - [x] compress columns separately (not full pack) to allow mmap skip
  - [x] zigzag encode timestamp cols (see go-play/zigzag.go) - as new enc type

- [ ] LZ4 1.9 is pretty fast https://github.com/lz4/lz4

```
Using LZ4 can easily improve performance even if all data reside in memory.

This is the case in ClickHouse: if data is compressed, we decompress it in blocks that fit in CPU cache and then perform data processing inside cache; if data is uncompressed, larger amount of data is read from memory.

Strictly speaking, LZ4 data decompression (typically 3 GB/sec) is slower than memcpy (typically 12 GB/sec). But when using e.g. 128 CPU cores, LZ4 decompression will scale up to memory bandwidth (typically 150 GB/sec) as well as memcpy. And memcpy is wasting more memory bandwidth by reading uncompressed data while LZ4 decompression reads compressed data.
```

- [ ] Filter Discussions: https://news.ycombinator.com/item?id=21840821
  https://lemire.me/blog/2019/12/19/xor-filters-faster-and-smaller-than-bloom-filters/
- [ ] faster int64->int64 maps
  https://github.com/brentp/intintmap/blob/master/intintmap.go
- [ ] Integer Compression / SIMD integer packing
  - https://michael.stapelberg.ch/posts/2019-02-05-turbopfor-analysis/
  - https://github.com/powturbo/TurboPFor  !!!
  - https://github.com/lemire/FastPFor
  - https://github.com/lemire/SIMDCompressionAndIntersection
  - https://github.com/zentures/encoding
- [ ] roaring bitmap compression and checks (may be faster than AVX2 sometimes, but check!)
  - https://github.com/RoaringBitmap/roaring
  - http://roaringbitmap.org/
- [ ] condition tree (AND/OR) with SSE filters for pack scans (output bitmap as result)
  OrCondition {A, B}, AndCondition {A, B}, ConditionList{}
  - then change query/stream/lookup loops to scan full pack first and loop through bitmap
  - support math expressions as condition
  - SIMD filter https://github.com/yandex/ClickHouse/blob/master/dbms/src/Columns/ColumnVector.cpp
- [ ] reference pack list + bitmap in query results to avoid accumulating very large packs
- [ ] caching layer for block columns instead of packs
- [ ] index improvements ideas?
  - what's wrong with hash indexes
    - slow insert: re-sorted and re-written at every insert (hash randomness spreads
      writes to all index packs)
    - slow lookup: sparse numbers and binary search on lookup
    - 16 byte per entry
  - try b-tree index, inserts and lookups are O(log n) (hash index is O(1+c) with large c)
- [ ] faster binary search https://github.com/scandum/binary_search
- [ ] POPCOUNT false dependency bug https://github.com/tmthrgd/go-popcount/blob/master/popcount_amd64.s
- [ ] ARM64 assembly in Go https://barakmich.dev/posts/popcnt-arm64-go-asm/
- [ ] Min/max in AVX2 https://stackoverflow.com/questions/31623383/calculating-min-of-8-long-ints-using-avx2

### Pack DB features
- [x] Count/CountTx
- [x] streaming query QueryStream(q, func(r Row)) to avoid building huge result sets
  - required for addr, utxo analytics
  - good for addr rollback
  - can build streaming versions of selectors and aggregators (e.g. TopN/Sum)
- [x] table/index statistics
  - nAdd/nUpd/nDel tuples
  - nAdd/nUpd/nDel calls
  - nFlush/nLoad/nStore/nRead/nWrite internal calls and timings (pack performance)
  - pack cache hits/misses/evictions
- [x] configurable cache size
- [x] joins (inner, left)
- [x] pack Field.Alias string
  - extra field in Field struct (maybe init from long name, maybe alias==longname)
  - design check if mapping in pack.Push/.. is easy to extend
  - replace JoinTable.FieldsAs with Alias
- [x] use context in query and join processing
- [ ] table triggers (watch conditions on insert/update & fire notification)
- [ ] materialized views (cond/watchers, build view, update view on table change)
- [ ] time-series API
  - support aggregations and grouping (sum, mean, median, std, top-k)
  - support math expressions (rate, mean-of-means, median-of-medians, std-of-stds)
  - group by time with correct week, month, quarter
  - limit float decimals on delivery
- [ ] storage engines (KV store backends for table/pack storage on remote servers)
  - see for inspiration https://github.com/janelia-flyem/dvid/tree/master/storage
  - groupcache by Brad Fitz https://github.com/golang/groupcache

### ACID
- [ ] make database a main db server object with cache manager, table manager, query engine
- [ ] concurrent table access
  - db.Table() should return cached ptr
  - replace table lock with fine-granular locks per pack
    - single writer thread only!
    - protect: table pack header list
    - protect: individual packs when flushing
- [ ] persist journal after insert/update/delete
- [ ] background flush of journal/index entries to avoid extremely long flush times

### Backup
- difficult with mmap thorugh boltdb on a hot database
- LVM can do snapshots, see https://github.com/benschweizer/dsnapshot

### Pack low-level features
- [ ] SIMD aggregate functions (int64, uint64, float64)
  - sum
  - mean
  - median (?)
  - std
  - top-k
- [ ] decimal type (e.g. https://github.com/shopspring/decimal, https://clickhouse.yandex/docs/en/data_types/decimal/)
- [ ] more pack funcs (filter, index, permute, compare, replicate) - maybe SIMD impls, see https://github.com/yandex/ClickHouse/blob/master/dbms/src/Columns/IColumn.h
- [ ] pack checksums (also keep in header) to detect updates
- [ ] cuckoo/bloom filter for sparse pk's in pack headers
- [ ] per-pack dictionary for low cardinality columns (strings/bytes maybe)
- [ ] tries for fast matching of addr/tx hashes (think: insert/update table triggers)
- [ ] more column stats (sum, mean) in pack headers
- [ ] pack push/pull/subscribe
- [ ] hash performance: maybe replace FNV with xxHash64, HighwayHash, Murmur3, SpookyV2
- [ ] radix sort for uint64 https://github.com/influxdata/influxdb/blob/master/pkg/radix/sort.go
- [ ] HyperLogLog++ https://github.com/axiomhq/hyperloglog
- [ ] Filtered Space-Saving for TopK streaming analysis: http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf with reduce-and-combine algorithm from Parallel Space Saving like https://arxiv.org/pdf/1401.0702.pdf; Clickouse impl at https://github.com/yandex/ClickHouse/blob/master/dbms/src/Common/SpaceSaving.h
- [ ] substring search using Volnitsky Algo http://volnitsky.com/project/str_search/
- [ ] grep uses Boyer-Moore https://lists.freebsd.org/pipermail/freebsd-current/2010-August/019310.html

### Query features
- [ ] joins: as-of, window joins; see https://en.wikipedia.org/wiki/Sql_join, https://en.wikipedia.org/wiki/Hash_join, https://code.kx.com/q4m3/9_Queries_q-sql/#998-as-of-joins
- [ ] ordering (ORDER BY) with multi-column stable sort
- [ ] aggregation (SUM, MEAN, COUNT, DISTINCT, ...)
- [ ] grouping (GROUP BY, HAVING)
- [ ] grouping with aggregation on computed columns (non-sql, kdb+/q supported)
- [ ] stddev aggregations
- [ ] mean-of-means
- [ ] median-of-medians (approximation), see https://stackoverflow.com/questions/52461306/something-i-dont-understand-about-median-of-medians-algorithm, https://en.wikipedia.org/wiki/Median_of_medians, https://www.reddit.com/r/statistics/comments/2s7icg/proper_way_to_aggregate_medians/
- [ ] query functions
```
$sum(field), $mean(field), $median(field), $first(), $last(), $change(), $diff(), $std(), $top(field,n), $quartile(field,n), $min(), $max(), $count()

q := Query{
  Name: "",
  Fields: table.Fields().Select("x"),  // columns to aggregate
  Conditions: ConditionList{},         // pre-filter
}

// don't change table impl that does filtering (WHERE clause)
// have PackWriter interface as output to a results Package or downstream Aggregator
// have PackReader interface as input for use in Result and Stream
type PackWriter interface{

}

type PackReader interface{

}

type Aggregator struct {
  fields FieldList
  fn AggregationFunc // sum, mean, ...
  out Package
}
```

### DSL

Eventually we need a DSL (domain specific language) for users. SQL might now be the right choice for the domain. kdb+ uses Q which is a version of K and similar to J https://en.wikipedia.org/wiki/J_(programming_language)


### Hash functions
- https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
- https://burtleburtle.net/bob/hash/doobs.html
- https://github.com/postgres/postgres/blob/master/src/backend/access/hash/hashfunc.c
