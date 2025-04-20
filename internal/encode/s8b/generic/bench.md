# Simple8b Encode Benchmarks

## M1 Max

> legacy encode
```
BenchmarkEncodeLegacy/dups_64K-10       1091777 ns/op     480.22 MB/s
BenchmarkEncodeLegacy/runs_64K-10       1082648 ns/op     484.26 MB/s
BenchmarkEncodeLegacy/seq_64K-10         310506 ns/op    1688.50 MB/s
```

> legacy decode
```
BenchmarkDecodeLegacy/dups_64K-10        101767 ns/op    5151.86 MB/s
BenchmarkDecodeLegacy/runs_64K-10        101013 ns/op    5190.32 MB/s
BenchmarkDecodeLegacy/seq_64K-10          40282 ns/op    13015.30 MB/s
```

> NEW early search break, pack kernels, zero/ones
```
BenchmarkEncode/uint64/dups_64k          286404 ns/op    1830.59 MB/s
BenchmarkEncode/uint64/runs_64k          170765 ns/op    3070.22 MB/s
BenchmarkEncode/uint64/seq_64k           121291 ns/op    4322.56 MB/s
BenchmarkEncode/uint32/dups_64k          291887 ns/op     898.10 MB/s
BenchmarkEncode/uint32/runs_64k          180737 ns/op    1450.42 MB/s
BenchmarkEncode/uint32/seq_64k           126888 ns/op    2065.95 MB/s
BenchmarkEncode/uint16/dups_64k          302652 ns/op     433.08 MB/s
BenchmarkEncode/uint16/runs_64k          183167 ns/op     715.59 MB/s
BenchmarkEncode/uint16/seq_64k           133248 ns/op     983.67 MB/s
BenchmarkEncode/uint8/dups_64k           114617 ns/op     571.78 MB/s
BenchmarkEncode/uint8/runs_64k           103907 ns/op     630.72 MB/s
BenchmarkEncode/uint8/seq_64k             92294 ns/op     710.08 MB/s
```

> NEW decode
```
BenchmarkDecode/uint64/dups_64k           63433 ns/op    8265.17 MB/s
BenchmarkDecode/uint64/runs_64k           66392 ns/op    7896.84 MB/s
BenchmarkDecode/uint64/seq_64k            46097 ns/op    11373.52 MB/s
BenchmarkDecode/uint32/dups_64k           60323 ns/op    4345.70 MB/s
BenchmarkDecode/uint32/runs_64k           64751 ns/op    4048.52 MB/s
BenchmarkDecode/uint32/seq_64k            39265 ns/op    6676.36 MB/s
BenchmarkDecode/uint16/dups_64k           53527 ns/op    2448.73 MB/s
BenchmarkDecode/uint16/runs_64k           69222 ns/op    1893.49 MB/s
BenchmarkDecode/uint16/seq_64k            38191 ns/op    3431.97 MB/s
BenchmarkDecode/uint8/dups_64k            18963 ns/op    3456.05 MB/s
BenchmarkDecode/uint8/runs_64k            36988 ns/op    1771.80 MB/s
BenchmarkDecode/uint8/seq_64k             21848 ns/op    2999.65 MB/s
```

> AVX2 decode
```
BenchmarkDecode/uint64/dups_64k           13102 ns/op    40017.04 MB/s
BenchmarkDecode/uint64/runs_64k           17445 ns/op    30053.51 MB/s
BenchmarkDecode/uint64/seq_64k             8702 ns/op    60252.28 MB/s
BenchmarkDecode/uint32/dups_64k           12795 ns/op    20488.38 MB/s
BenchmarkDecode/uint32/runs_64k           28753 ns/op    9117.25 MB/s
BenchmarkDecode/uint32/seq_64k             8926 ns/op    29369.64 MB/s
BenchmarkDecode/uint16/dups_64k           19577 ns/op    6695.19 MB/s
BenchmarkDecode/uint16/runs_64k           33473 ns/op    3915.73 MB/s
BenchmarkDecode/uint16/seq_64k             8988 ns/op    14582.47 MB/s
BenchmarkDecode/uint8/dups_64k             3319 ns/op    19748.61 MB/s
BenchmarkDecode/uint8/runs_64k            16068 ns/op    4078.55 MB/s
BenchmarkDecode/uint8/seq_64k              4491 ns/op    14591.62 MB/s
```

# Simpl8 Cmp Fusion Benchmarks

### Serial Execution - ARM64 M1

> EQ

```
BenchmarkCmpEqualUnpacked/uint64/dups_64k-10          110113 ns/op    4761.35 MB/s
BenchmarkCmpEqualUnpacked/uint64/runs_64k-10          119334 ns/op    4393.45 MB/s
BenchmarkCmpEqualUnpacked/uint64/seq_64k-10            93493 ns/op    5607.75 MB/s
BenchmarkCmpEqualUnpacked/uint32/dups_64k-10          110868 ns/op    2364.46 MB/s
BenchmarkCmpEqualUnpacked/uint32/runs_64k-10          102559 ns/op    2556.02 MB/s
BenchmarkCmpEqualUnpacked/uint32/seq_64k-10            80614 ns/op    3251.86 MB/s
BenchmarkCmpEqualUnpacked/uint16/dups_64k-10           94140 ns/op    1392.31 MB/s
BenchmarkCmpEqualUnpacked/uint16/runs_64k-10          110358 ns/op    1187.70 MB/s
BenchmarkCmpEqualUnpacked/uint16/seq_64k-10            76117 ns/op    1721.99 MB/s
BenchmarkCmpEqualUnpacked/uint8/dups_64k-10            50930 ns/op    1286.78 MB/s
BenchmarkCmpEqualUnpacked/uint8/runs_64k-10            67964 ns/op     964.28 MB/s
BenchmarkCmpEqualUnpacked/uint8/seq_64k-10             52182 ns/op    1255.90 MB/s
```

## Serial Execution - 12th Gen Intel(R) Core(TM) i9-12900K

> EQ

```
BenchmarkCmpEqualUnpacked/uint64/dups_64k-24          349774 ns/op    1498.93 MB/s
BenchmarkCmpEqualUnpacked/uint64/runs_64k-24          350202 ns/op    1497.10 MB/s
BenchmarkCmpEqualUnpacked/uint64/seq_64k-24           305284 ns/op    1717.38 MB/s
BenchmarkCmpEqualUnpacked/uint32/dups_64k-24          252298 ns/op    1039.03 MB/s
BenchmarkCmpEqualUnpacked/uint32/runs_64k-24          336570 ns/op     778.87 MB/s
BenchmarkCmpEqualUnpacked/uint32/seq_64k-24           217307 ns/op    1206.33 MB/s
BenchmarkCmpEqualUnpacked/uint16/dups_64k-24          191792 ns/op     683.41 MB/s
BenchmarkCmpEqualUnpacked/uint16/runs_64k-24          270467 ns/op     484.61 MB/s
BenchmarkCmpEqualUnpacked/uint16/seq_64k-24           133813 ns/op     979.52 MB/s
BenchmarkCmpEqualUnpacked/uint8/dups_64k-24            58440 ns/op    1121.42 MB/s
BenchmarkCmpEqualUnpacked/uint8/runs_64k-24           141735 ns/op     462.38 MB/s
BenchmarkCmpEqualUnpacked/uint8/seq_64k-24             76077 ns/op     861.44 MB/s
```

### Fusion Kernel - ARM64 M1

> EQ

```
BenchmarkFusionCmpEqual/uint64/dups_64k-10             90266 ns/op    5808.23 MB/s
BenchmarkFusionCmpEqual/uint64/runs_64k-10             94290 ns/op    5560.37 MB/s
BenchmarkFusionCmpEqual/uint64/seq_64k-10              55643 ns/op    9422.44 MB/s
BenchmarkFusionCmpEqual/uint32/dups_64k-10             91840 ns/op    2854.37 MB/s
BenchmarkFusionCmpEqual/uint32/runs_64k-10             75056 ns/op    3492.65 MB/s
BenchmarkFusionCmpEqual/uint32/seq_64k-10              56589 ns/op    4632.39 MB/s
BenchmarkFusionCmpEqual/uint16/dups_64k-10             77365 ns/op    1694.19 MB/s
BenchmarkFusionCmpEqual/uint16/runs_64k-10            102560 ns/op    1278.00 MB/s
BenchmarkFusionCmpEqual/uint16/seq_64k-10              56677 ns/op    2312.63 MB/s
BenchmarkFusionCmpEqual/uint8/dups_64k-10              35380 ns/op    1852.33 MB/s
BenchmarkFusionCmpEqual/uint8/runs_64k-10              55633 ns/op    1178.00 MB/s
BenchmarkFusionCmpEqual/uint8/seq_64k-10               34442 ns/op    1902.80 MB/s
```

> GT

```
BenchmarkFusionCmpGreater/uint64/dups_64k-10           97945 ns/op    5352.86 MB/s
BenchmarkFusionCmpGreater/uint64/runs_64k-10          100103 ns/op    5237.47 MB/s
BenchmarkFusionCmpGreater/uint64/seq_64k-10            59557 ns/op    8803.10 MB/s
BenchmarkFusionCmpGreater/uint32/dups_64k-10           97511 ns/op    2688.35 MB/s
BenchmarkFusionCmpGreater/uint32/runs_64k-10           83892 ns/op    3124.80 MB/s
BenchmarkFusionCmpGreater/uint32/seq_64k-10            59793 ns/op    4384.20 MB/s
BenchmarkFusionCmpGreater/uint16/dups_64k-10           84124 ns/op    1558.09 MB/s
BenchmarkFusionCmpGreater/uint16/runs_64k-10          108097 ns/op    1212.54 MB/s
BenchmarkFusionCmpGreater/uint16/seq_64k-10            58366 ns/op    2245.71 MB/s
BenchmarkFusionCmpGreater/uint8/dups_64k-10            35314 ns/op    1855.83 MB/s
BenchmarkFusionCmpGreater/uint8/runs_64k-10            56305 ns/op    1163.95 MB/s
BenchmarkFusionCmpGreater/uint8/seq_64k-10             38630 ns/op    1696.49 MB/s
```


### Fusion Kernel - 12th Gen Intel(R) Core(TM) i9-12900K

> EQ

```
BenchmarkFusionCmpEqual/uint64/dups_64k-24             65311 ns/op    8027.52 MB/s
BenchmarkFusionCmpEqual/uint64/runs_64k-24             69209 ns/op    7575.39 MB/s
BenchmarkFusionCmpEqual/uint64/seq_64k-24              38441 ns/op   13638.61 MB/s
BenchmarkFusionCmpEqual/uint32/dups_64k-24             65467 ns/op    4004.19 MB/s
BenchmarkFusionCmpEqual/uint32/runs_64k-24             63495 ns/op    4128.56 MB/s
BenchmarkFusionCmpEqual/uint32/seq_64k-24              37951 ns/op    6907.38 MB/s
BenchmarkFusionCmpEqual/uint16/dups_64k-24             64377 ns/op    2036.02 MB/s
BenchmarkFusionCmpEqual/uint16/runs_64k-24             76678 ns/op    1709.38 MB/s
BenchmarkFusionCmpEqual/uint16/seq_64k-24              38186 ns/op    3432.47 MB/s
BenchmarkFusionCmpEqual/uint8/dups_64k-24              32876 ns/op    1993.45 MB/s
BenchmarkFusionCmpEqual/uint8/runs_64k-24              43367 ns/op    1511.20 MB/s
BenchmarkFusionCmpEqual/uint8/seq_64k-24               30542 ns/op    2145.76 MB/s
```

## Microbenchmarks AVX2

### AVX2 Decode - 12th Gen Intel(R) Core(TM) i9-12900K (AVX2)

```
BenchmarkDecodeUint64/dups_64K                         23264 ns/op    22536.07 MB/s
BenchmarkDecodeUint64/runs_64K                         22992 ns/op    22802.71 MB/s
BenchmarkDecodeUint64/seq_64K                           8486 ns/op    61786.19 MB/s
BenchmarkDecodeUint32/dups_64K                         12738 ns/op    20580.29 MB/s
BenchmarkDecodeUint32/runs_64K                         12894 ns/op    20331.44 MB/s
BenchmarkDecodeUint32/seq_64K                           8862 ns/op    29581.24 MB/s
BenchmarkDecodeUint16/dups_64K                         19152 ns/op     6843.86 MB/s
BenchmarkDecodeUint16/runs_64K                         33546 ns/op     3907.18 MB/s
BenchmarkDecodeUint16/seq_64K                           9002 ns/op    14560.13 MB/s
BenchmarkDecodeUint8/dups_64K                           3481 ns/op    18826.63 MB/s
BenchmarkDecodeUint8/runs_64K                          17931 ns/op     3654.92 MB/s
BenchmarkDecodeUint8/seq_64K                            6118 ns/op    10711.95 MB/s
```

### AVX2 Compare - 12th Gen Intel(R) Core(TM) i9-12900K (AVX2)

```
BenchmarkMatchEqualAVX2/uint64/64k                      4399 ns/op   119170.49 MB/s
BenchmarkMatchEqualAVX2/uint32/64k                      2333 ns/op   112385.59 MB/s
BenchmarkMatchEqualAVX2/uint16/64k                      1348 ns/op    97204.62 MB/s
BenchmarkMatchEqualAVX2/uint8/64k                        619 ns/op   105789.67 MB/s
```