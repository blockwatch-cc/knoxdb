# Bloom Filter Pack Statistcis

Calculator
https://hur.st/bloomfilter

**Goal**

- limit number of scanned packs for queries against non-indexed columns
- for tables with 10k-100k packs
- at lowest possible runtime (priority 1)
- at lowest possible storage cost (priority 2)
- example: list incoming transactions on address use match equal on `receiver_id`

**Design Decision**

After empirical evaluation of the design space the following variables look best.

- filter type `bloom` (no other filteres considered)
- hash function `xxhash` (other impl exist, but xxhash is fastest)
- number of hash functions `k=4` (linear relation to runtime)
- size of filter `16k/32k/64k/128k` (for optimal runtime, use pow2 filter sizes only)
- size of packs `16k/32k/64k` (typical app specific sizes)


Efficiency at 4 hash functions (k=4)
- spend 1 byte per entry in bloom filter for 2% false positive rate
- spend 2 bytes per entry in bloom filter for 0.2% false positive rate


## Bloom Parameter Optimization for Packs
       
N      Bits     Size   K   Prob (1 in N)         Optimal       Selected     Params
------------------------------------------------------------------------------------
16384  524288   64kB   22  0.00000021  1/4752501    *                        
16384  524288   64kB   4   0.00019 1/5246
16384  262144   32kB   11  0.00046 1/2180           *                 
16384  262144   32kB   4   0.0024 1/418                            =       16k/32kB/4 0.2%
16384  131072   16kB   6   0.0216 1/46              *              
16384  131072   16kB   4   0.0239 1/42                             =       16k/16kB/4 2%
16384  65536    8kB    3   0.1469 1/7               *             
-----------------------------------------------------------------------------------
32768  524288   64kB   11  0.0005 1/2180            *                
32768  524288   64kB   6   0.0009 1/1069
32768  524288   64kB   4   0.0024 1/418                            =       32k/64kB/4 0.2%
32768  262144   32kB   6   0.0215 1/46              *              
32768  262144   32kB   4   0.0239 1/42                             =       32k/32kB/4 2%
32768  131072   16kB   3   0.1469 1/7               *             
-----------------------------------------------------------------------------------
65536  1048576 128kB   11  0.0005 1/2180            *                
65536  1048576 128kB   4   0.0024 1/418                            =       64k/128kB/4 0.2%
65536  524288   64kB   6   0.0216 1/46              *              
65536  524288   64kB   4   0.0239 1/42                             =       64k/64kB/4  2%
65536  262144   32kB   3   0.1469 1/7               *              
65536  131072   16kB   1   0.3935 1/3               *              


## Bloom implementation Benchmark

Precise filter size must use `%` division ops, but this is too expensive:

```sh
$ go test -bench=. ./filter/bloom/...
goos: darwin
goarch: amd64
pkg: blockwatch.cc/knoxdb/filter/bloom
cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
BenchmarkFilter_Insert/m=310585_k=7_n=32768-8                403       2862107 ns/op           0 B/op          0 allocs/op
BenchmarkFilter_Insert/m=621170_k=7_n=65536-8                196       5878924 ns/op           0 B/op          0 allocs/op
BenchmarkFilter_Contains/IN_m=310585_k=7_n=32768-8      13110668            94.48 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/NI_m=310585_k=7_n=32768-8      31689499            36.28 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/IN_m=621170_k=7_n=65536-8      13420514            83.83 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/NI_m=621170_k=7_n=65536-8      32240941            36.06 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Merge/m=310585_k=7_n=32768-8               28065         42438 ns/op           0 B/op          0 allocs/op
BenchmarkFilter_Merge/m=621170_k=7_n=65536-8               10000        117161 ns/op           0 B/op          0 allocs/op
PASS
ok      blockwatch.cc/knoxdb/filter/bloom   33.150s
```

Pow-2 Size Only can use fast mask/shift ops (>2x faster):

```sh
$ go test -bench=. ./filter/bloom/...
goos: darwin
goarch: amd64
pkg: blockwatch.cc/knoxdb/filter/bloom
cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
BenchmarkFilter_Insert/m=310585_k=7_n=32768-8                825       1423532 ns/op           0 B/op          0 allocs/op
BenchmarkFilter_Insert/m=621170_k=7_n=65536-8                411       2855042 ns/op           0 B/op          0 allocs/op
BenchmarkFilter_Contains/IN_m=310585_k=7_n=32768-8      27856870            41.32 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/NI_m=310585_k=7_n=32768-8      47250686            32.73 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/IN_m=621170_k=7_n=65536-8      28906359            39.68 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/NI_m=621170_k=7_n=65536-8      39278245            27.31 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Merge/m=310585_k=7_n=32768-8               16801         69332 ns/op          48 B/op          1 allocs/op
BenchmarkFilter_Merge/m=621170_k=7_n=65536-8                8028        139899 ns/op          48 B/op          1 allocs/op
PASS
ok      blockwatch.cc/knoxdb/filter/bloom   29.735s
```

Different k with pow-2 sized filter: 4=32.5ns 6=37ns 11=46ns

```
BenchmarkFilter_Contains/IN_m=524288_k=11_n=32768-8     23774496            49.55 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/NI_m=524288_k=11_n=32768-8     47331392            26.71 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/IN_m=524288_k=4_n=32768-8      36534030            32.81 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/NI_m=524288_k=4_n=32768-8      43786842            26.20 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/IN_m=262144_k=6_n=32768-8      27557376            42.46 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/NI_m=262144_k=6_n=32768-8      42004832            27.81 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/IN_m=262144_k=4_n=32768-8      35399962            44.75 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/NI_m=262144_k=4_n=32768-8      42938756            28.41 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/IN_m=1048576_k=11_n=65536-8    24150866            47.61 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/NI_m=1048576_k=11_n=65536-8    42707007            27.48 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/IN_m=1048576_k=4_n=65536-8     35101596            33.62 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/NI_m=1048576_k=4_n=65536-8     42046716            28.00 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/IN_m=524288_k=6_n=65536-8      29715216            38.30 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/NI_m=524288_k=6_n=65536-8      42108170            29.53 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/IN_m=524288_k=4_n=65536-8      33537842            36.32 ns/op        0 B/op          0 allocs/op
BenchmarkFilter_Contains/NI_m=524288_k=4_n=65536-8      42733791            27.88 ns/op        0 B/op          0 allocs/op
```

