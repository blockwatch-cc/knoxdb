# String Pools

Memory efficient string pools for use in vectorized query engines and wire protocols. The common goal is to limit the overhead of Golang slice headers which require 24 bytes for `[]byte` and 16 bytes for `string`.

- StringPool - basic and fast, not concurrency safe, realloc/memmove on growth
- SlabPool - concurrency safe, no memove, slower due to atomics
- DedupPool - basic StringPool with deduplication on append

### Benchmarks

```
native ([][]byte reference)

BenchmarkByteSliceAppend/1k-10       22369 ns/op     1464.88 MB/s    0.046 vals/ns
BenchmarkByteSliceAppend/16k-10     348575 ns/op     1504.09 MB/s    0.047 vals/ns
BenchmarkByteSliceAppend/64k-10    1394556 ns/op     1503.81 MB/s    0.047 vals/ns
BenchmarkByteSliceGet/1k-10            329 ns/op    99558.12 MB/s    3.111 vals/ns  
BenchmarkByteSliceGet/16k-10          5119 ns/op   102413.62 MB/s    3.200 vals/ns  
BenchmarkByteSliceGet/64k-10         20444 ns/op   102579.16 MB/s    3.206 vals/ns  

StringPool baseline (not concurrency safe)

BenchmarkStringPoolAppend/1k-10       4548 ns/op     7205.37 MB/s    0.2252 vals/ns
BenchmarkStringPoolAppend/16k-10     77468 ns/op     6767.84 MB/s    0.2115 vals/ns
BenchmarkStringPoolAppend/64k-10    292639 ns/op     7166.34 MB/s    0.2239 vals/ns
BenchmarkStringPoolGet/1k-10          2250 ns/op    14563.30 MB/s    0.4551 vals/ns
BenchmarkStringPoolGet/16k-10        35515 ns/op    14762.56 MB/s    0.4613 vals/ns
BenchmarkStringPoolGet/64k-10       142840 ns/op    14681.79 MB/s    0.4588 vals/ns

StringPool with atomics

BenchmarkSlabPoolAppend/1k-10         7920 ns/op     4137.63 MB/s    0.1293 vals/ns
BenchmarkSlabPoolAppend/16k-10      123733 ns/op     4237.26 MB/s    0.1324 vals/ns
BenchmarkSlabPoolAppend/64k-10      500394 ns/op     4191.00 MB/s    0.1310 vals/ns
BenchmarkSlabPoolGet/1k-10            2671 ns/op    12269.20 MB/s    0.3834 vals/ns
BenchmarkSlabPoolGet/16k-10          43023 ns/op    12186.34 MB/s    0.3808 vals/ns
BenchmarkSlabPoolGet/64k-10         172506 ns/op    12157.00 MB/s    0.3799 vals/ns

StringPool with mutex (benchmark experiment on baseline StringPool)

BenchmarkStringPoolAppend/1k-10      19755 ns/op     1658.69 MB/s   0.05183 vals/ns
BenchmarkStringPoolAppend/16k-10    312867 ns/op     1675.76 MB/s   0.05237 vals/ns
BenchmarkStringPoolAppend/64k-10   1298087 ns/op     1615.57 MB/s   0.05049 vals/ns
BenchmarkStringPoolGet/1k-10         14637 ns/op     2238.70 MB/s   0.06996 vals/ns
BenchmarkStringPoolGet/16k-10       234894 ns/op     2232.02 MB/s   0.06975 vals/ns
BenchmarkStringPoolGet/64k-10       938328 ns/op     2234.99 MB/s   0.06984 vals/ns
```