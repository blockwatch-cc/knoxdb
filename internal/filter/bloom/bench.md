# Bloom benchmarks

## Apple M1 Max

```
cpu: Apple M1 Max
BenchmarkAddManyBytesGo/1k/32kB-10             6634 ns/op     617.42 MB/s
BenchmarkAddManyBytesGo/1k/128kB-10            6764 ns/op     605.54 MB/s
BenchmarkAddManyBytesGo/1k/512kB-10            6558 ns/op     624.53 MB/s
BenchmarkAddManyBytesGo/16k/32kB-10          107050 ns/op     612.20 MB/s
BenchmarkAddManyBytesGo/16k/128kB-10         105752 ns/op     619.71 MB/s
BenchmarkAddManyBytesGo/16k/512kB-10         106885 ns/op     613.14 MB/s
BenchmarkAddManyBytesGo/64k/32kB-10          432389 ns/op     606.27 MB/s
BenchmarkAddManyBytesGo/64k/128kB-10         423123 ns/op     619.55 MB/s
BenchmarkAddManyBytesGo/64k/512kB-10         421010 ns/op     622.66 MB/s
BenchmarkAddManyUint32Go/1k/32kB-10            5189 ns/op     789.32 MB/s
BenchmarkAddManyUint32Go/1k/128kB-10           5126 ns/op     799.11 MB/s
BenchmarkAddManyUint32Go/1k/512kB-10           5123 ns/op     799.47 MB/s
BenchmarkAddManyUint32Go/16k/32kB-10          81839 ns/op     800.79 MB/s
BenchmarkAddManyUint32Go/16k/128kB-10         82563 ns/op     793.77 MB/s
BenchmarkAddManyUint32Go/16k/512kB-10         82877 ns/op     790.76 MB/s
BenchmarkAddManyUint32Go/64k/32kB-10         338682 ns/op     774.01 MB/s
BenchmarkAddManyUint32Go/64k/128kB-10        342152 ns/op     766.16 MB/s
BenchmarkAddManyUint32Go/64k/512kB-10        338296 ns/op     774.89 MB/s
BenchmarkAddManyUint64Go/1k/32kB-10            5788 ns/op    1415.42 MB/s
BenchmarkAddManyUint64Go/1k/128kB-10           5839 ns/op    1403.05 MB/s
BenchmarkAddManyUint64Go/1k/512kB-10           5863 ns/op    1397.12 MB/s
BenchmarkAddManyUint64Go/16k/32kB-10          92936 ns/op    1410.35 MB/s
BenchmarkAddManyUint64Go/16k/128kB-10         92152 ns/op    1422.34 MB/s
BenchmarkAddManyUint64Go/16k/512kB-10         92530 ns/op    1416.54 MB/s
BenchmarkAddManyUint64Go/64k/32kB-10         374140 ns/op    1401.31 MB/s
BenchmarkAddManyUint64Go/64k/128kB-10        372634 ns/op    1406.98 MB/s
BenchmarkAddManyUint64Go/64k/512kB-10        372337 ns/op    1408.10 MB/s
BenchmarkMergeGo/32kB-10                       1502 ns/op    2726.69 MB/s
BenchmarkMergeGo/128kB-10                      6018 ns/op    2722.28 MB/s
BenchmarkMergeGo/512kB-10                     23915 ns/op    2740.43 MB/s

BenchmarkContainsGo/1k/32kB/IN-10             21.01 ns/op
BenchmarkContainsGo/1k/32kB/NI-10             17.26 ns/op
BenchmarkContainsGo/1k/128kB/IN-10            21.38 ns/op
BenchmarkContainsGo/1k/128kB/NI-10            14.76 ns/op
BenchmarkContainsGo/1k/512kB/IN-10            21.89 ns/op
BenchmarkContainsGo/1k/512kB/NI-10            14.05 ns/op
BenchmarkContainsGo/16k/32kB/IN-10            26.67 ns/op
BenchmarkContainsGo/16k/32kB/NI-10            30.53 ns/op
BenchmarkContainsGo/16k/128kB/IN-10           25.42 ns/op
BenchmarkContainsGo/16k/128kB/NI-10           33.33 ns/op
BenchmarkContainsGo/16k/512kB/IN-10           26.40 ns/op
BenchmarkContainsGo/16k/512kB/NI-10           22.78 ns/op
BenchmarkContainsGo/64k/32kB/IN-10            31.07 ns/op
BenchmarkContainsGo/64k/32kB/NI-10            35.14 ns/op
BenchmarkContainsGo/64k/128kB/IN-10           32.26 ns/op
BenchmarkContainsGo/64k/128kB/NI-10           44.53 ns/op
BenchmarkContainsGo/64k/512kB/IN-10           32.45 ns/op
BenchmarkContainsGo/64k/512kB/NI-10           43.29 ns/op
```

## Intel (Go)

```
cpu: 12th Gen Intel(R) Core(TM) i9-12900K
BenchmarkAddManyBytesGo/1k/32kB-24             4401 ns/op     930.70 MB/s
BenchmarkAddManyBytesGo/1k/128kB-24            4547 ns/op     900.77 MB/s
BenchmarkAddManyBytesGo/1k/512kB-24            4468 ns/op     916.72 MB/s
BenchmarkAddManyBytesGo/16k/32kB-24           72312 ns/op     906.30 MB/s
BenchmarkAddManyBytesGo/16k/128kB-24          72069 ns/op     909.34 MB/s
BenchmarkAddManyBytesGo/16k/512kB-24          71440 ns/op     917.36 MB/s
BenchmarkAddManyBytesGo/64k/32kB-24          288786 ns/op     907.74 MB/s
BenchmarkAddManyBytesGo/64k/128kB-24         283847 ns/op     923.54 MB/s
BenchmarkAddManyBytesGo/64k/512kB-24         286972 ns/op     913.48 MB/s
BenchmarkAddManyUint32Go/1k/32kB-24            3376 ns/op    1213.39 MB/s
BenchmarkAddManyUint32Go/1k/128kB-24           3385 ns/op    1210.04 MB/s
BenchmarkAddManyUint32Go/1k/512kB-24           3999 ns/op    1024.32 MB/s
BenchmarkAddManyUint32Go/16k/32kB-24          55528 ns/op    1180.24 MB/s
BenchmarkAddManyUint32Go/16k/128kB-24         60941 ns/op    1075.40 MB/s
BenchmarkAddManyUint32Go/16k/512kB-24         65364 ns/op    1002.64 MB/s
BenchmarkAddManyUint32Go/64k/32kB-24         291232 ns/op     900.12 MB/s
BenchmarkAddManyUint32Go/64k/128kB-24        232645 ns/op    1126.80 MB/s
BenchmarkAddManyUint32Go/64k/512kB-24        258851 ns/op    1012.72 MB/s
BenchmarkAddManyUint64Go/1k/32kB-24            4253 ns/op    1926.25 MB/s
BenchmarkAddManyUint64Go/1k/128kB-24           4024 ns/op    2035.65 MB/s
BenchmarkAddManyUint64Go/1k/512kB-24           4670 ns/op    1754.25 MB/s
BenchmarkAddManyUint64Go/16k/32kB-24          65169 ns/op    2011.27 MB/s
BenchmarkAddManyUint64Go/16k/128kB-24         72993 ns/op    1795.68 MB/s
BenchmarkAddManyUint64Go/16k/512kB-24         74858 ns/op    1750.94 MB/s
BenchmarkAddManyUint64Go/64k/32kB-24         330586 ns/op    1585.94 MB/s
BenchmarkAddManyUint64Go/64k/128kB-24        276256 ns/op    1897.83 MB/s
BenchmarkAddManyUint64Go/64k/512kB-24        310009 ns/op    1691.20 MB/s
BenchmarkMergeGo/32kB-24                        982.4 ns/op  4169.28 MB/s
BenchmarkMergeGo/128kB-24                      3900 ns/op    4201.54 MB/s
BenchmarkMergeGo/512kB-24                     15577 ns/op    4207.26 MB/s

BenchmarkContainsGo/1k/32kB/IN-24             15.22 ns/op   
BenchmarkContainsGo/1k/32kB/NI-24             12.03 ns/op   
BenchmarkContainsGo/1k/128kB/IN-24            15.72 ns/op   
BenchmarkContainsGo/1k/128kB/NI-24            10.94 ns/op   
BenchmarkContainsGo/1k/512kB/IN-24            16.38 ns/op   
BenchmarkContainsGo/1k/512kB/NI-24            11.14 ns/op   
BenchmarkContainsGo/16k/32kB/IN-24            18.73 ns/op   
BenchmarkContainsGo/16k/32kB/NI-24            21.27 ns/op   
BenchmarkContainsGo/16k/128kB/IN-24           19.20 ns/op   
BenchmarkContainsGo/16k/128kB/NI-24           22.16 ns/op   
BenchmarkContainsGo/16k/512kB/IN-24           19.35 ns/op   
BenchmarkContainsGo/16k/512kB/NI-24           16.69 ns/op   
BenchmarkContainsGo/64k/32kB/IN-24            22.18 ns/op   
BenchmarkContainsGo/64k/32kB/NI-24            25.61 ns/op   
BenchmarkContainsGo/64k/128kB/IN-24           22.04 ns/op   
BenchmarkContainsGo/64k/128kB/NI-24           29.68 ns/op   
BenchmarkContainsGo/64k/512kB/IN-24           22.46 ns/op   
BenchmarkContainsGo/64k/512kB/NI-24           30.80 ns/op   
```

## Intel AVX2

```
cpu: 12th Gen Intel(R) Core(TM) i9-12900K
BenchmarkAddManyUint32AVX2/1k/32kB-24          1464 ns/op    2798.20 MB/s
BenchmarkAddManyUint32AVX2/1k/128kB-24         1588 ns/op    2580.02 MB/s
BenchmarkAddManyUint32AVX2/1k/512kB-24         1531 ns/op    2675.23 MB/s
BenchmarkAddManyUint32AVX2/16k/32kB-24        23712 ns/op    2763.87 MB/s
BenchmarkAddManyUint32AVX2/16k/128kB-24       23982 ns/op    2732.66 MB/s
BenchmarkAddManyUint32AVX2/16k/512kB-24       24259 ns/op    2701.57 MB/s
BenchmarkAddManyUint32AVX2/64k/32kB-24        96809 ns/op    2707.84 MB/s
BenchmarkAddManyUint32AVX2/64k/128kB-24       96074 ns/op    2728.56 MB/s
BenchmarkAddManyUint32AVX2/64k/512kB-24      101089 ns/op    2593.20 MB/s
BenchmarkAddManyUint64AVX2/1k/32kB-24          1820 ns/op    4501.82 MB/s
BenchmarkAddManyUint64AVX2/1k/128kB-24         1805 ns/op    4537.95 MB/s
BenchmarkAddManyUint64AVX2/1k/512kB-24         1877 ns/op    4364.04 MB/s
BenchmarkAddManyUint64AVX2/16k/32kB-24        29776 ns/op    4401.98 MB/s
BenchmarkAddManyUint64AVX2/16k/128kB-24       30941 ns/op    4236.19 MB/s
BenchmarkAddManyUint64AVX2/16k/512kB-24       31902 ns/op    4108.63 MB/s
BenchmarkAddManyUint64AVX2/64k/32kB-24       120534 ns/op    4349.71 MB/s
BenchmarkAddManyUint64AVX2/64k/128kB-24      126830 ns/op    4133.78 MB/s
BenchmarkAddManyUint64AVX2/64k/512kB-24      129394 ns/op    4051.87 MB/s
BenchmarkMergeAVX2/32kB-24                       53.47 ns/op 76608.40 MB/s
BenchmarkMergeAVX2/128kB-24                     212.2 ns/op  77220.24 MB/s
BenchmarkMergeAVX2/512kB-24                    1349 ns/op    48578.84 MB/s
```