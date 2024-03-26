There be dragons.

© 2018-2024 Blockwatch Data Inc, All rights reserved.

Blockwatch KnoxDB columnar database for fast blockchain analytics.

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
- [x] time-series API
  - support aggregations and grouping (sum, mean, median, std, top-k)
  - support math expressions (rate, mean-of-means, median-of-medians, std-of-stds)
  - group by time with correct week, month, quarter
  - limit float decimals on delivery
- [x] storage engines (KV store backends for table/pack storage on remote servers)

