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

### Cardinality Estimation

Goal is to have a rough estimate in the order of pow-2 steps on how many unique values exist per pack/block so we can dimension our bloom filters more properly.

- using loglog-beta https://github.com/seiflotfy/loglogbeta
- using xxhash (empirically the fastest)
- precision 12 (=4096 byte data buffer)
- avg error probability for sender/receiver id field is 2-4% (max abs error 394, resp 843)

loglog-beta estimation is faster and more memory efficient on 32k uint64 values:

| Method              | Build Time | Count Time | Overall |
|---------------------|------------|------------|---------|
|loglog-beta          |    0.094ms |   0.016ms  |  0.11ms |
|`map[uint64]struct{}`|    1.272ms |   0.288ns  | 1.272ms |
|`vec.Uint64.Unique()`|    0.232ms |   5.185ms  | 5.417ms |


Impact of hash functions on loglog-beta performance

```sh
go test -bench=Add ./filter/loglogbeta/

# xxhash
goos: darwin
goarch: amd64
pkg: blockwatch.cc/knoxdb/filter/loglogbeta
cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
Benchmark_FilterAdd/n=32768_p=10-8     12688         94217 ns/op        1024 B/op          1 allocs/op
Benchmark_FilterAdd/n=32768_p=12-8     12676         94602 ns/op        4096 B/op          1 allocs/op
Benchmark_FilterAdd/n=32768_p=14-8     12165         97842 ns/op       16384 B/op          1 allocs/op

# murmur3
goos: darwin
goarch: amd64
pkg: blockwatch.cc/knoxdb/filter/loglogbeta
cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
Benchmark_FilterAdd/n=32768_p=10-8     12607         94196 ns/op        1024 B/op          1 allocs/op
Benchmark_FilterAdd/n=32768_p=12-8     12607         94275 ns/op        4096 B/op          1 allocs/op
Benchmark_FilterAdd/n=32768_p=14-8     12200         97338 ns/op       16384 B/op          1 allocs/op

# metro
goos: darwin
goarch: amd64
pkg: blockwatch.cc/knoxdb/filter/loglogbeta
cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
Benchmark_FilterAdd/n=32768_p=10-8     12589         93841 ns/op        1024 B/op          1 allocs/op
Benchmark_FilterAdd/n=32768_p=12-8     12493         96264 ns/op        4096 B/op          1 allocs/op
Benchmark_FilterAdd/n=32768_p=14-8     12266         97215 ns/op       16384 B/op          1 allocs/op
```

Comparison against exact methods (using xxhash):

```sh
$ go test -bench=. ./filter/loglogbeta/
goos: darwin
goarch: amd64
pkg: blockwatch.cc/knoxdb/filter/loglogbeta
cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
Benchmark_FilterAdd/n=32768_p=10-8             12555         97300 ns/op        1024 B/op          1 allocs/op
Benchmark_FilterAdd/n=32768_p=12-8             12529         94961 ns/op        4096 B/op          1 allocs/op
Benchmark_FilterAdd/n=32768_p=14-8             12265         97371 ns/op       16384 B/op          1 allocs/op
BenchmarkFilter_Cardinality/n=32768_p=10-8    268563          4319 ns/op           0 B/op          0 allocs/op
BenchmarkFilter_Cardinality/n=32768_p=12-8     70489         16615 ns/op           0 B/op          0 allocs/op
BenchmarkFilter_Cardinality/n=32768_p=14-8     18303         65975 ns/op           0 B/op          0 allocs/op
BenchmarkFilter_Merge/n=32768_p=10-8         1630194           768.5 ns/op        64 B/op          1 allocs/op
BenchmarkFilter_Merge/n=32768_p=12-8          435492          2774 ns/op          64 B/op          1 allocs/op
BenchmarkFilter_Merge/n=32768_p=14-8          107492         10785 ns/op          64 B/op          1 allocs/op
Benchmark_FilterAddExact/n=32768-8               883       1272323 ns/op      700458 B/op         12 allocs/op
Benchmark_FilterCardinalityExact/n=32768-8  1000000000      0.2882 ns/op          0 B/op          0 allocs/op
Benchmark_FilterAddExactHashed/n=32768-8        4948        232259 ns/op      262243 B/op          2 allocs/op
Benchmark_FilterCardinalityExactHashed/n=32768-8 228       5184787 ns/op      262192 B/op          3 allocs/op
```

```sh
$ go run ./examples/cardinality.go -db ./db/op.db -precision 12
...
Processed 2113 packs at loglog precision 12 in 5m33.232972951s
Col            Name        Type    Min    Max    Avg              Var  Err-Min  Err-Max  Err-Avg  Err-Var
00           row_id      uint64   3741  32768  32551      478543.1562        0     1787      202       85347.5439
01             time    datetime     21   9484    736      237417.5845        2      215       36         493.2587
02           height       int32     21   9306    735      236491.3917        2      236       36         498.1199
03            cycle       int16      0      3      0           0.1550        1        2        1           0.0005
04             hash       bytes   1971  32768  21969    58797632.8878        0     1508      261       48493.8722
05          counter       int32    421  29266  10659    47496829.7244        1      921      159       16701.1092
06             op_n       int16     26  29400    145      438419.1623        2      133        7          68.9556
07             op_c       int16      1    477    191        8278.7500        1       23       11          27.8229
08             op_i       int16      0    137      4          43.7714        1        8        1           0.0689
09             op_l       int16      2      5      3           0.4045        1        1        1           0.0000
10             op_p       int16     21  29400    125      437832.6295        2      145        6          72.8576
11             type       uint8      4     11      6           1.5010        1        1        1           0.0000
12           status       uint8      0      3      2           0.3694        1        1        1           0.0000
13       is_success     boolean      0      1      0           0.0158        1        2        1           0.0181
14      is_contract     boolean      0      1      0           0.1680        1        2        1           0.1302
15        gas_limit       int32      3   3167    486      498346.5498        1      165       23         988.2248
16         gas_used       int32      2   2587    428      390853.9217        1      147       21         919.3620
17        gas_price   decimal32      7   2890    609      405428.6112        1      154       30         928.0398
18    storage_limit       int32      0    302     76        7731.9730        1       17        4          14.9335
19     storage_size       int32      0  12806   2066     8478433.7565        0      557       55        6561.3469
20     storage_paid       int32      0    227     54        4434.0825        1       13        3           9.4311
21           volume       int64    181  25255   6045    27564699.1963        0      795      126        8109.5392
22              fee       int64     12   3153    917      403992.7487        1      176       41         776.9939
23           reward       int64      0    389     66        6560.5857        1       22        3          18.8631
24          deposit       int64      0    223     42        4041.6053        1       16        2          12.0895
25           burned       int64      1    221     53        4188.8371        1       17        3          15.0760
26        sender_id      uint64    140  14482   1646      959273.9403        5      394       68        1039.6012
27      receiver_id      uint64    369  29511   6707    39113577.2384        0      843      133        9211.2400
28       creator_id      uint64      0   1910    409      270912.7010        1      127       21         711.0163
29      delegate_id      uint64      3   1165     34        1727.1840        1       89        2           6.9350
30      is_internal     boolean      0      1      0           0.1521        1        2        1           0.0506
31         has_data     boolean      0      1      0           0.2189        1        2        1           0.2189
32             data      string    467  21069   3142     6316312.5997        0      479       99        2637.4281
33       parameters       bytes      0  25157   3764    25835386.0054        0      582       65        8561.0973
34          storage       bytes      0  27799   2261     4725545.8611        1      481       79        5002.8052
35     big_map_diff       bytes      0  25321   4072    33446826.5845        0      633       64       10486.1459
36           errors      string      0    140     10          63.1745        1        7        1           0.1715
37   days_destroyed   decimal32    149  18812   5013    11878404.8436        0      591      121        5672.3863
38        branch_id      uint64     33   6494    750      199905.6901        4      190       37         481.1446
39    branch_height       int32     35   6438    750      200319.7847        2      177       37         462.3833
40     branch_depth        int8      3     59     15          50.9384        1        3        1           0.2276
41      is_implicit     boolean      0      1      0           0.0047        1        2        1           0.0047
42    entrypoint_id        int8      0     23      9          69.1615        1        2        1           0.1693
43        is_orphan     boolean      0      0      0           0.0000        1        1        1           0.0000
44         is_batch     boolean      0      1      0           0.1980        1        2        1           0.1980
45       is_sapling     boolean      0      1      0           0.0019        1        1        1           0.0000
```

## Bloom (re)-dimensioning

Measured on real data to count effects of false positives of table scan performance.

Result:
- change LLB to 16
- change bloom factor to 2

```sh
## Mainnet 12/1 (current setting)

>> 43s build time for 4341 packs

Bloom Filter Accuracy Statistics
--------------------------------
Total Ids          2149563
LLB precision      12
Bloom Err Rate     2.000000 (scale=1)
Total Bloom Bytes  12116224
No Matches         1015185
Optimal Matches    8407591
Bloom Matches      109553618 (+1203.03%)
--------------------------------
Bloom Min Abs Err  0
Bloom Max Abs Err  4202
Bloom Avg Abs Err  46.95
Bloom Med Abs Err  29.00
Bloom Abs Err Std  60.87
--------------------------------
Bloom Min Pct Err  0.00
Bloom Max Pct Err  4202.00
Bloom Avg Pct Err  34.37
Bloom Pct Err Std  52.49

## Mainnet 14/1 (fair estimate)

>> 43sec build time for 4341 packs

Bloom Filter Accuracy Statistics
--------------------------------
Total Ids          2149563
LLB precision      14
Bloom Err Rate     2.000000 (scale=1)
Total Bloom Bytes  12745472
No Matches         1015185
Optimal Matches    8407591
Bloom Matches      92676057 (+1002.29%)
--------------------------------
Bloom Min Abs Err  0
Bloom Max Abs Err  4202
Bloom Avg Abs Err  39.11
Bloom Med Abs Err  24.00
Bloom Abs Err Std  53.14
--------------------------------
Bloom Min Pct Err  0.00
Bloom Max Pct Err  4202.00
Bloom Avg Pct Err  28.63
Bloom Pct Err Std  45.66

## Mainnet 16/1 (better estimate)

>> 37sec build time for 4341 packs

Bloom Filter Accuracy Statistics
--------------------------------
Total Ids          2149563
LLB precision      16
Bloom Err Rate     2.000000 (scale=1)
Total Bloom Bytes  13410048
No Matches         1015185
Optimal Matches    8407591
Bloom Matches      78019577 (+827.97%)
--------------------------------
Bloom Min Abs Err  0
Bloom Max Abs Err  4199
Bloom Avg Abs Err  32.31
Bloom Med Abs Err  19.00
Bloom Abs Err Std  46.22
--------------------------------
Bloom Min Pct Err  0.00
Bloom Max Pct Err  4199.00
Bloom Avg Pct Err  23.66
Bloom Pct Err Std  39.58

## Mainnet 16/2 (0.2% error, better estimate)

>> 38sec build time for 4341 packs

Bloom Filter Accuracy Statistics
--------------------------------
Total Ids          2149563
LLB precision      16
Bloom Err Rate     0.200000 (scale=2)
Total Bloom Bytes  26820096
No Matches         1015185
Optimal Matches    8407591
Bloom Matches      14726250 (+75.15%)
--------------------------------
Bloom Min Abs Err  0
Bloom Max Abs Err  2035
Bloom Avg Abs Err  2.92
Bloom Med Abs Err  1.00
Bloom Abs Err Std  10.03
--------------------------------
Bloom Min Pct Err  0.00
Bloom Max Pct Err  2035.00
Bloom Avg Pct Err  2.14
Bloom Pct Err Std  8.20

## Mainnet 16/3 (0.02% error, better estimate, long build time)

>> 5m17s build time for 4341 packs

Bloom Filter Accuracy Statistics
--------------------------------
Total Ids          2149563
LLB precision      16
Bloom Err Rate     0.020000 (scale=3)
Total Bloom Bytes  35955200
No Matches         1015185
Optimal Matches    8407591
Bloom Matches      10255709 (+21.98%)
--------------------------------
Bloom Min Abs Err  0
Bloom Max Abs Err  1729
Bloom Avg Abs Err  0.85
Bloom Med Abs Err  0.00
Bloom Abs Err Std  5.53
--------------------------------
Bloom Min Pct Err  0.00
Bloom Max Pct Err  1729.00
Bloom Avg Pct Err  0.62
Bloom Pct Err Std  4.32
```