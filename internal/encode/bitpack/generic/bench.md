# Bitpack Cmp Fusion Benchmarks

## Apple M1 Max

### Fused kernels

```
BenchmarkCmpEqualFused/u32/64k/90%/1_bits                8844 ns/op    29639.27 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/2_bits               25985 ns/op    10088.35 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/3_bits               34090 ns/op    7689.68 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/4_bits               41488 ns/op    6318.59 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/5_bits               44322 ns/op    5914.51 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/6_bits               42783 ns/op    6127.25 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/7_bits               47692 ns/op    5496.59 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/8_bits               29462 ns/op    8897.65 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/9_bits               57946 ns/op    4523.92 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/10_bits              55027 ns/op    4763.89 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/11_bits              52131 ns/op    5028.57 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/12_bits              56244 ns/op    4660.84 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/13_bits              52113 ns/op    5030.25 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/14_bits              51054 ns/op    5134.61 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/15_bits              50317 ns/op    5209.89 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/16_bits              55498 ns/op    4723.47 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/17_bits              54824 ns/op    4781.57 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/18_bits              56557 ns/op    4635.02 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/19_bits              55686 ns/op    4707.51 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/20_bits              55413 ns/op    4730.74 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/21_bits              55548 ns/op    4719.23 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/22_bits              54538 ns/op    4806.67 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/23_bits              54980 ns/op    4767.95 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/24_bits              54299 ns/op    4827.75 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/25_bits              54799 ns/op    4783.74 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/26_bits              54762 ns/op    4786.98 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/27_bits              51910 ns/op    5049.95 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/28_bits              55844 ns/op    4694.24 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/29_bits              55586 ns/op    4716.04 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/30_bits              54886 ns/op    4776.15 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/31_bits              54324 ns/op    4825.57 MB/s
```

### Sequential kernels

```
BenchmarkCmpEqualUnpacked/u32/64k/90%/1_bits           159606 ns/op    1642.45 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/2_bits           155642 ns/op    1684.27 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/3_bits           161631 ns/op    1621.87 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/4_bits           152911 ns/op    1714.35 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/5_bits           184828 ns/op    1418.31 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/6_bits           185338 ns/op    1414.41 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/7_bits           165638 ns/op    1582.63 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/8_bits           129903 ns/op    2018.00 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/9_bits           145580 ns/op    1800.68 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/10_bits          156493 ns/op    1675.12 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/11_bits          166258 ns/op    1576.73 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/12_bits          153280 ns/op    1710.22 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/13_bits          182120 ns/op    1439.40 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/14_bits          183007 ns/op    1432.42 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/15_bits          166886 ns/op    1570.80 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/16_bits          144013 ns/op    1820.28 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/17_bits          171654 ns/op    1527.16 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/18_bits          172380 ns/op    1520.73 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/19_bits          179047 ns/op    1464.11 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/20_bits          167860 ns/op    1561.68 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/21_bits          187785 ns/op    1395.98 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/22_bits          201590 ns/op    1300.38 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/23_bits          172296 ns/op    1521.48 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/24_bits          162334 ns/op    1614.84 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/25_bits          175854 ns/op    1490.69 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/26_bits          168799 ns/op    1552.99 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/27_bits          176061 ns/op    1488.94 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/28_bits          171627 ns/op    1527.41 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/29_bits          204975 ns/op    1278.91 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/30_bits          205422 ns/op    1276.12 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/31_bits          191186 ns/op    1371.15 MB/s
```

### Unpack Only

```
BenchmarkDecode/u32/dups_64K                           152569 ns/op    1718.20 MB/s
BenchmarkDecode/u32/runs_64K                           149165 ns/op    1757.41 MB/s
BenchmarkDecode/u32/seq_64K                            104458 ns/op    2509.57 MB/s
```

### Compare Only

```
BenchmarkMatchInt16Equal/64K                            28476 ns/op    4602.93 MB/s
BenchmarkMatchUint32Equal/64K                           26295 ns/op    9969.45 MB/s
```

## cpu: 12th Gen Intel(R) Core(TM) i9-12900K

### Fused kernels

```
BenchmarkCmpEqualFused/u32/64k/90%/1_bits                3828 ns/op     68481.14 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/2_bits               23794 ns/op     11017.28 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/3_bits               30323 ns/op      8645.06 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/4_bits               21201 ns/op     12364.56 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/5_bits               33788 ns/op      7758.43 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/6_bits               32470 ns/op      8073.41 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/7_bits               39002 ns/op      6721.26 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/8_bits                 521 ns/op    502529.10 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/9_bits               44987 ns/op      5827.16 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/10_bits              44313 ns/op      5915.78 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/11_bits              37076 ns/op      7070.40 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/12_bits              42394 ns/op      6183.57 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/13_bits              38106 ns/op      6879.34 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/14_bits              37605 ns/op      6971.01 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/15_bits              37413 ns/op      7006.84 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/16_bits              39265 ns/op      6676.24 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/17_bits              39957 ns/op      6560.69 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/18_bits              39841 ns/op      6579.77 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/19_bits              40018 ns/op      6550.69 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/20_bits              39114 ns/op      6702.13 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/21_bits              39656 ns/op      6610.48 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/22_bits              38843 ns/op      6748.80 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/23_bits              39902 ns/op      6569.76 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/24_bits              38321 ns/op      6840.80 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/25_bits              39342 ns/op      6663.24 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/26_bits              39133 ns/op      6698.79 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/27_bits              42652 ns/op      6146.18 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/28_bits              38250 ns/op      6853.43 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/29_bits              43107 ns/op      6081.21 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/30_bits              42412 ns/op      6180.90 MB/s
BenchmarkCmpEqualFused/u32/64k/90%/31_bits              44239 ns/op      5925.62 MB/s
```

### Sequential kernels

```
BenchmarkCmpEqualUnpacked/u32/64k/90%/1_bits           102734 ns/op      2551.66 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/2_bits           100242 ns/op      2615.11 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/3_bits           108038 ns/op      2426.41 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/4_bits            99831 ns/op      2625.88 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/5_bits           111705 ns/op      2346.74 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/6_bits           107874 ns/op      2430.09 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/7_bits           105003 ns/op      2496.55 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/8_bits            98865 ns/op      2651.55 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/9_bits           108627 ns/op      2413.25 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/10_bits          107935 ns/op      2428.73 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/11_bits          112308 ns/op      2334.14 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/12_bits          110988 ns/op      2361.91 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/13_bits          120079 ns/op      2183.10 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/14_bits          120957 ns/op      2167.24 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/15_bits          116210 ns/op      2255.78 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/16_bits          104514 ns/op      2508.21 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/17_bits          119011 ns/op      2202.69 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/18_bits          117447 ns/op      2232.03 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/19_bits          119058 ns/op      2201.81 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/20_bits          117915 ns/op      2223.16 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/21_bits          121007 ns/op      2166.36 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/22_bits          121678 ns/op      2154.40 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/23_bits          120847 ns/op      2169.22 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/24_bits          111568 ns/op      2349.63 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/25_bits          123718 ns/op      2118.88 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/26_bits          124378 ns/op      2107.64 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/27_bits          125081 ns/op      2095.79 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/28_bits          119306 ns/op      2197.24 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/29_bits          126194 ns/op      2077.31 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/30_bits          127293 ns/op      2059.38 MB/s
BenchmarkCmpEqualUnpacked/u32/64k/90%/31_bits          129432 ns/op      2025.34 MB/s
```

### Unpack Only

```
BenchmarkDecode/u32/dups_64K                           109500 ns/op     2394.01 MB/s
BenchmarkDecode/u32/runs_64K                           108441 ns/op     2417.39 MB/s
BenchmarkDecode/u32/seq_64K                             88599 ns/op     2958.76 MB/s
```

### Compare Only

```
BenchmarkMatchUint32Equal/64K                           17524 ns/op    14958.81 MB/s
```

# Bitpack Encode Benchmarks

### Apple M1 Max

```
BenchmarkEncode/u64/dups_64K                7166        170041 ns/op    3083.30 MB/s
BenchmarkEncode/u64/runs_64K                7071        171569 ns/op    3055.85 MB/s
BenchmarkEncode/u64/seq_64K                12039        102582 ns/op    5110.92 MB/s
BenchmarkEncode/u32/dups_64K                7449        167163 ns/op    1568.20 MB/s
BenchmarkEncode/u32/runs_64K                7214        166751 ns/op    1572.07 MB/s
BenchmarkEncode/u32/seq_64K                10000        108408 ns/op    2418.12 MB/s
BenchmarkEncode/u16/dups_64K               10000        109306 ns/op    1199.13 MB/s
BenchmarkEncode/u16/runs_64K               10000        108104 ns/op    1212.46 MB/s
BenchmarkEncode/u16/seq_64K                10000        106760 ns/op    1227.72 MB/s
BenchmarkEncode/u8/dups_64K                13140         92284 ns/op     710.15 MB/s
BenchmarkEncode/u8/runs_64K                12355         92611 ns/op     707.65 MB/s
BenchmarkEncode/u8/seq_64K                 12907         94463 ns/op     693.78 MB/s
```

### Intel 12th Gen Intel(R) Core(TM) i9-12900K

```
BenchmarkEncode/u64/dups_64K                7712        152564 ns/op    3436.52 MB/s
BenchmarkEncode/u64/runs_64K                7632        153896 ns/op    3406.77 MB/s
BenchmarkEncode/u64/seq_64K                12585         95632 ns/op    5482.33 MB/s
BenchmarkEncode/u32/dups_64K                9835        120944 ns/op    2167.48 MB/s
BenchmarkEncode/u32/runs_64K                9667        121392 ns/op    2159.48 MB/s
BenchmarkEncode/u32/seq_64K                12564         95395 ns/op    2747.98 MB/s
BenchmarkEncode/u16/dups_64K               12598         94798 ns/op    1382.64 MB/s
BenchmarkEncode/u16/runs_64K               12574         94865 ns/op    1381.67 MB/s
BenchmarkEncode/u16/seq_64K                12630         95045 ns/op    1379.06 MB/s
BenchmarkEncode/u8/dups_64K                14324         82834 ns/op     791.17 MB/s
BenchmarkEncode/u8/runs_64K                14379         83264 ns/op     787.08 MB/s
BenchmarkEncode/u8/seq_64K                 14472         82981 ns/op     789.78 MB/s
```

# Bitpack Decode Benchmarks

### Apple M1 Max

```
BenchmarkDecode/u64/dups_64K                8415        142163 ns/op    3687.94 MB/s
BenchmarkDecode/u64/runs_64K                8542        138844 ns/op    3776.10 MB/s
BenchmarkDecode/u64/seq_64K                10000        105875 ns/op    4951.94 MB/s
BenchmarkDecode/u32/dups_64K                7849        152569 ns/op    1718.20 MB/s
BenchmarkDecode/u32/runs_64K                8037        149165 ns/op    1757.41 MB/s
BenchmarkDecode/u32/seq_64K                10000        104458 ns/op    2509.57 MB/s
BenchmarkDecode/u16/dups_64K               10000        102289 ns/op    1281.39 MB/s
BenchmarkDecode/u16/runs_64K               10000        104530 ns/op    1253.92 MB/s
BenchmarkDecode/u16/seq_64K                10000        102810 ns/op    1274.89 MB/s
BenchmarkDecode/u8/dups_64K                13459         89145 ns/op     735.16 MB/s
BenchmarkDecode/u8/runs_64K                13840         90265 ns/op     726.04 MB/s
BenchmarkDecode/u8/seq_64K                 13387         86188 ns/op     760.38 MB/s
```

### Intel 12th Gen Intel(R) Core(TM) i9-12900K

```
BenchmarkDecode/u64/dups_64K               10000        114251 ns/op    4588.91 MB/s
BenchmarkDecode/u64/runs_64K               10000        114686 ns/op    4571.49 MB/s
BenchmarkDecode/u64/seq_64K                13484         89169 ns/op    5879.73 MB/s
BenchmarkDecode/u32/dups_64K               10000        109500 ns/op    2394.01 MB/s
BenchmarkDecode/u32/runs_64K               10000        108441 ns/op    2417.39 MB/s
BenchmarkDecode/u32/seq_64K                13508         88599 ns/op    2958.76 MB/s
BenchmarkDecode/u16/dups_64K               13442         89009 ns/op    1472.57 MB/s
BenchmarkDecode/u16/runs_64K               13494         88792 ns/op    1476.18 MB/s
BenchmarkDecode/u16/seq_64K                13390         88895 ns/op    1474.46 MB/s
BenchmarkDecode/u8/dups_64K                14536         82386 ns/op     795.47 MB/s
BenchmarkDecode/u8/runs_64K                14262         83087 ns/op     788.76 MB/s
BenchmarkDecode/u8/seq_64K                 14454         82571 ns/op     793.70 MB/s
```