# Bitpack Cmp Fusion Benchmarks

## Apple M1 Max

### Fused kernels

```
BenchmarkCmpEqual/64k/90%/0_bits             9445678           125.5 ns/op  1044572.11 MB/s
BenchmarkCmpEqual/64k/90%/1_bits              180838          6591 ns/op    19887.38 MB/s
BenchmarkCmpEqual/64k/90%/2_bits               46404         25903 ns/op    5060.03 MB/s
BenchmarkCmpEqual/64k/90%/3_bits               36104         33040 ns/op    3967.11 MB/s
BenchmarkCmpEqual/64k/90%/4_bits               43503         27545 ns/op    4758.47 MB/s
BenchmarkCmpEqual/64k/90%/5_bits               28977         41603 ns/op    3150.58 MB/s
BenchmarkCmpEqual/64k/90%/6_bits               28592         42334 ns/op    3096.12 MB/s
BenchmarkCmpEqual/64k/90%/7_bits               24445         49361 ns/op    2655.40 MB/s
BenchmarkCmpEqual/64k/90%/8_bits               46315         25688 ns/op    5102.54 MB/s
BenchmarkCmpEqual/64k/90%/9_bits               20785         56775 ns/op    2308.64 MB/s
BenchmarkCmpEqual/64k/90%/10_bits              21078         57591 ns/op    2275.90 MB/s
BenchmarkCmpEqual/64k/90%/11_bits              22776         52666 ns/op    2488.75 MB/s
BenchmarkCmpEqual/64k/90%/12_bits              21072         57031 ns/op    2298.25 MB/s
BenchmarkCmpEqual/64k/90%/13_bits              22843         52587 ns/op    2492.49 MB/s
BenchmarkCmpEqual/64k/90%/14_bits              22686         52957 ns/op    2475.08 MB/s
BenchmarkCmpEqual/64k/90%/15_bits              22724         52636 ns/op    2490.18 MB/s
BenchmarkCmpEqual/64k/90%/16_bits              20773         56818 ns/op    2306.86 MB/s
BenchmarkCmpEqual/64k/90%/17_bits              20487         56859 ns/op    2305.23 MB/s
BenchmarkCmpEqual/64k/90%/18_bits              20964         57008 ns/op    2299.17 MB/s
BenchmarkCmpEqual/64k/90%/19_bits              21152         57011 ns/op    2299.07 MB/s
BenchmarkCmpEqual/64k/90%/20_bits              21193         56992 ns/op    2299.84 MB/s
BenchmarkCmpEqual/64k/90%/21_bits              21220         57476 ns/op    2280.48 MB/s
BenchmarkCmpEqual/64k/90%/22_bits              21168         57329 ns/op    2286.30 MB/s
BenchmarkCmpEqual/64k/90%/23_bits              20443         56676 ns/op    2312.64 MB/s
BenchmarkCmpEqual/64k/90%/24_bits              21016         56554 ns/op    2317.63 MB/s
BenchmarkCmpEqual/64k/90%/25_bits              20912         56604 ns/op    2315.59 MB/s
BenchmarkCmpEqual/64k/90%/26_bits              21081         56966 ns/op    2300.90 MB/s
BenchmarkCmpEqual/64k/90%/27_bits              22585         53461 ns/op    2451.75 MB/s
BenchmarkCmpEqual/64k/90%/28_bits              21220         57247 ns/op    2289.57 MB/s
BenchmarkCmpEqual/64k/90%/29_bits              21190         56856 ns/op    2305.33 MB/s
BenchmarkCmpEqual/64k/90%/30_bits              21088         56849 ns/op    2305.61 MB/s
BenchmarkCmpEqual/64k/90%/31_bits              21174         56926 ns/op    2302.49 MB/s
BenchmarkCmpEqual/64k/90%/32_bits              22640         52972 ns/op    2474.37 MB/s
BenchmarkCmpEqual/64k/90%/33_bits              20925         56878 ns/op    2304.45 MB/s
BenchmarkCmpEqual/64k/90%/34_bits              20689         56866 ns/op    2304.92 MB/s
BenchmarkCmpEqual/64k/90%/35_bits              20371         56845 ns/op    2305.78 MB/s
BenchmarkCmpEqual/64k/90%/36_bits              21085         57172 ns/op    2292.59 MB/s
BenchmarkCmpEqual/64k/90%/37_bits              21136         57565 ns/op    2276.93 MB/s
BenchmarkCmpEqual/64k/90%/38_bits              21110         57203 ns/op    2291.35 MB/s
BenchmarkCmpEqual/64k/90%/39_bits              21091         57006 ns/op    2299.27 MB/s
BenchmarkCmpEqual/64k/90%/40_bits              20979         57533 ns/op    2278.19 MB/s
BenchmarkCmpEqual/64k/90%/41_bits              20931         56851 ns/op    2305.55 MB/s
BenchmarkCmpEqual/64k/90%/42_bits              20973         56765 ns/op    2309.03 MB/s
BenchmarkCmpEqual/64k/90%/43_bits              20503         56809 ns/op    2307.25 MB/s
BenchmarkCmpEqual/64k/90%/44_bits              20959         57040 ns/op    2297.89 MB/s
BenchmarkCmpEqual/64k/90%/45_bits              21040         57165 ns/op    2292.89 MB/s
BenchmarkCmpEqual/64k/90%/46_bits              21088         58235 ns/op    2250.76 MB/s
BenchmarkCmpEqual/64k/90%/47_bits              20390         57489 ns/op    2279.93 MB/s
BenchmarkCmpEqual/64k/90%/48_bits              20995         57554 ns/op    2277.39 MB/s
BenchmarkCmpEqual/64k/90%/49_bits              21016         57478 ns/op    2280.37 MB/s
BenchmarkCmpEqual/64k/90%/50_bits              20918         57189 ns/op    2291.92 MB/s
BenchmarkCmpEqual/64k/90%/51_bits              20880         56971 ns/op    2300.70 MB/s
BenchmarkCmpEqual/64k/90%/52_bits              20749         57127 ns/op    2294.38 MB/s
BenchmarkCmpEqual/64k/90%/53_bits              20730         56898 ns/op    2303.63 MB/s
BenchmarkCmpEqual/64k/90%/54_bits              21025         57163 ns/op    2292.95 MB/s
BenchmarkCmpEqual/64k/90%/55_bits              21094         57487 ns/op    2280.03 MB/s
BenchmarkCmpEqual/64k/90%/56_bits              21135         57756 ns/op    2269.42 MB/s
BenchmarkCmpEqual/64k/90%/57_bits              21082         57318 ns/op    2286.76 MB/s
BenchmarkCmpEqual/64k/90%/58_bits              20616         58116 ns/op    2255.36 MB/s
BenchmarkCmpEqual/64k/90%/59_bits              20098         59658 ns/op    2197.06 MB/s
BenchmarkCmpEqual/64k/90%/60_bits              20190         58415 ns/op    2243.82 MB/s
BenchmarkCmpEqual/64k/90%/61_bits              18866         61891 ns/op    2117.78 MB/s
BenchmarkCmpEqual/64k/90%/62_bits              18886         62993 ns/op    2080.72 MB/s
BenchmarkCmpEqual/64k/90%/63_bits              17671         64124 ns/op    2044.03 MB/s
```

### Sequential kernels

```
BenchmarkCmpEqualUnpacked/64k/90%/0_bits       13173         90640 ns/op    1446.07 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/1_bits        9746        118451 ns/op    1106.55 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/2_bits       10000        117344 ns/op    1116.99 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/3_bits        9500        126530 ns/op    1035.89 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/4_bits        9855        118500 ns/op    1106.09 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/5_bits        8557        137628 ns/op     952.37 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/6_bits        9004        135792 ns/op     965.24 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/7_bits        8152        144753 ns/op     905.49 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/8_bits       10000        117877 ns/op    1111.94 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/9_bits        7765        152807 ns/op     857.76 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/10_bits       7894        151030 ns/op     867.86 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/11_bits       7492        163496 ns/op     801.69 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/12_bits       8034        151138 ns/op     867.24 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/13_bits       7011        169007 ns/op     775.54 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/14_bits       7027        170670 ns/op     767.98 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/15_bits       6692        177085 ns/op     740.16 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/16_bits       7803        153939 ns/op     851.45 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/17_bits       6318        187968 ns/op     697.31 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/18_bits       6271        188075 ns/op     696.91 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/19_bits       5881        197245 ns/op     664.51 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/20_bits       6259        186879 ns/op     701.37 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/21_bits       5743        205003 ns/op     639.37 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/22_bits       5671        204844 ns/op     639.86 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/23_bits       5510        214676 ns/op     610.56 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/24_bits       6260        187544 ns/op     698.89 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/25_bits       5275        221798 ns/op     590.95 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/26_bits       5383        222425 ns/op     589.29 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/27_bits       5121        232648 ns/op     563.39 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/28_bits       5301        222502 ns/op     589.08 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/29_bits       4826        242055 ns/op     541.50 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/30_bits       4987        245276 ns/op     534.38 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/31_bits       4771        269613 ns/op     486.15 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/32_bits       5342        223236 ns/op     587.14 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/33_bits       4563        259758 ns/op     504.59 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/34_bits       4588        260470 ns/op     503.21 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/35_bits       4362        273254 ns/op     479.67 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/36_bits       4629        261088 ns/op     502.02 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/37_bits       4237        279635 ns/op     468.73 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/38_bits       4161        279253 ns/op     469.37 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/39_bits       4084        289993 ns/op     451.98 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/40_bits       4496        261070 ns/op     502.06 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/41_bits       3898        298676 ns/op     438.84 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/42_bits       3543        298445 ns/op     439.18 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/43_bits       3858        308504 ns/op     424.86 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/44_bits       3982        297687 ns/op     440.30 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/45_bits       3774        318281 ns/op     411.81 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/46_bits       3687        321163 ns/op     408.12 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/47_bits       3554        331906 ns/op     394.91 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/48_bits       4032        296115 ns/op     442.64 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/49_bits       3458        336911 ns/op     389.04 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/50_bits       3522        335331 ns/op     390.87 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/51_bits       3381        350468 ns/op     373.99 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/52_bits       3450        337301 ns/op     388.59 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/53_bits       3343        353922 ns/op     370.34 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/54_bits       3345        362812 ns/op     361.27 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/55_bits       3255        363733 ns/op     360.35 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/56_bits       3469        340801 ns/op     384.60 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/57_bits       3124        374998 ns/op     349.53 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/58_bits       3122        373814 ns/op     350.63 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/59_bits       3055        385849 ns/op     339.70 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/60_bits       3153        382467 ns/op     342.70 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/61_bits       3088        394494 ns/op     332.25 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/62_bits       3003        394526 ns/op     332.23 MB/s
BenchmarkCmpEqualUnpacked/64k/90%/63_bits       2941        406329 ns/op     322.58 MB/s
```

### Unpack Only

```
BenchmarkCmpEqualUnpacked/64k/10%/0_bits-10                19177         62499 ns/op    2097.17 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/1_bits-10                12969         90614 ns/op    1446.49 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/2_bits-10                12716         90731 ns/op    1444.62 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/3_bits-10                12188         98609 ns/op    1329.21 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/4_bits-10                13285         90827 ns/op    1443.10 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/5_bits-10                10000        106638 ns/op    1229.13 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/6_bits-10                10000        109381 ns/op    1198.31 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/7_bits-10                10000        114748 ns/op    1142.26 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/8_bits-10                13257         90294 ns/op    1451.61 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/9_bits-10                 9708        124009 ns/op    1056.95 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/10_bits-10                9488        123784 ns/op    1058.88 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/11_bits-10                9021        133102 ns/op     984.75 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/12_bits-10                9505        125120 ns/op    1047.57 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/13_bits-10                8234        141642 ns/op     925.38 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/14_bits-10                8503        141418 ns/op     926.84 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/15_bits-10                8010        149788 ns/op     875.05 MB/s
```

### Compare Only

```
BenchmarkMatchInt16Equal/64K-10                41971         28476 ns/op    4602.93 MB/s
BenchmarkMatchUint32Equal/64K-10               45056         26295 ns/op    9969.45 MB/s
```

## cpu: 12th Gen Intel(R) Core(TM) i9-12900K

### Fused kernels

```
BenchmarkCmpEqual/64k/90%/0_bits                22174220                55.12 ns/op     2378118.14 MB/s
BenchmarkCmpEqual/64k/90%/1_bits                  321148              3770 ns/op        34770.88 MB/s
BenchmarkCmpEqual/64k/90%/2_bits                   50605             23929 ns/op        5477.57 MB/s
BenchmarkCmpEqual/64k/90%/3_bits                   39722             30355 ns/op        4317.93 MB/s
BenchmarkCmpEqual/64k/90%/4_bits                   50203             23802 ns/op        5506.82 MB/s
BenchmarkCmpEqual/64k/90%/5_bits                   32653             36772 ns/op        3564.42 MB/s
BenchmarkCmpEqual/64k/90%/6_bits                   32726             36192 ns/op        3621.56 MB/s
BenchmarkCmpEqual/64k/90%/7_bits                   28041             42568 ns/op        3079.11 MB/s
BenchmarkCmpEqual/64k/90%/8_bits                 2326280               516.8 ns/op      253630.73 MB/s
BenchmarkCmpEqual/64k/90%/9_bits                   24364             48645 ns/op        2694.45 MB/s
BenchmarkCmpEqual/64k/90%/10_bits                  25074             47939 ns/op        2734.16 MB/s
BenchmarkCmpEqual/64k/90%/11_bits                  29704             40640 ns/op        3225.17 MB/s
BenchmarkCmpEqual/64k/90%/12_bits                  25598             46527 ns/op        2817.15 MB/s
BenchmarkCmpEqual/64k/90%/13_bits                  29461             40676 ns/op        3222.33 MB/s
BenchmarkCmpEqual/64k/90%/14_bits                  29720             40500 ns/op        3236.35 MB/s
BenchmarkCmpEqual/64k/90%/15_bits                  29709             40677 ns/op        3222.26 MB/s
BenchmarkCmpEqual/64k/90%/16_bits                  27902             43128 ns/op        3039.11 MB/s
BenchmarkCmpEqual/64k/90%/17_bits                  28160             42803 ns/op        3062.21 MB/s
BenchmarkCmpEqual/64k/90%/18_bits                  28134             43131 ns/op        3038.90 MB/s
BenchmarkCmpEqual/64k/90%/19_bits                  28107             43045 ns/op        3045.00 MB/s
BenchmarkCmpEqual/64k/90%/20_bits                  28399             42244 ns/op        3102.77 MB/s
BenchmarkCmpEqual/64k/90%/21_bits                  27818             42991 ns/op        3048.82 MB/s
BenchmarkCmpEqual/64k/90%/22_bits                  28414             42214 ns/op        3104.96 MB/s
BenchmarkCmpEqual/64k/90%/23_bits                  28016             42600 ns/op        3076.80 MB/s
BenchmarkCmpEqual/64k/90%/24_bits                  29271             41485 ns/op        3159.54 MB/s
BenchmarkCmpEqual/64k/90%/25_bits                  28135             42896 ns/op        3055.58 MB/s
BenchmarkCmpEqual/64k/90%/26_bits                  28104             42265 ns/op        3101.21 MB/s
BenchmarkCmpEqual/64k/90%/27_bits                  25567             46911 ns/op        2794.06 MB/s
BenchmarkCmpEqual/64k/90%/28_bits                  29085             41014 ns/op        3195.75 MB/s
BenchmarkCmpEqual/64k/90%/29_bits                  25705             46361 ns/op        2827.22 MB/s
BenchmarkCmpEqual/64k/90%/30_bits                  26161             46166 ns/op        2839.12 MB/s
BenchmarkCmpEqual/64k/90%/31_bits                  24792             48287 ns/op        2714.44 MB/s
BenchmarkCmpEqual/64k/90%/32_bits                  31048             38122 ns/op        3438.22 MB/s
BenchmarkCmpEqual/64k/90%/33_bits                  23943             50191 ns/op        2611.47 MB/s
BenchmarkCmpEqual/64k/90%/34_bits                  24050             49840 ns/op        2629.84 MB/s
BenchmarkCmpEqual/64k/90%/35_bits                  23916             49906 ns/op        2626.37 MB/s
BenchmarkCmpEqual/64k/90%/36_bits                  24139             49684 ns/op        2638.11 MB/s
BenchmarkCmpEqual/64k/90%/37_bits                  24001             50191 ns/op        2611.48 MB/s
BenchmarkCmpEqual/64k/90%/38_bits                  23908             49714 ns/op        2636.54 MB/s
BenchmarkCmpEqual/64k/90%/39_bits                  23841             50211 ns/op        2610.44 MB/s
BenchmarkCmpEqual/64k/90%/40_bits                  26761             44771 ns/op        2927.58 MB/s
BenchmarkCmpEqual/64k/90%/41_bits                  23952             49935 ns/op        2624.86 MB/s
BenchmarkCmpEqual/64k/90%/42_bits                  23906             50432 ns/op        2599.01 MB/s
BenchmarkCmpEqual/64k/90%/43_bits                  23990             50379 ns/op        2601.71 MB/s
BenchmarkCmpEqual/64k/90%/44_bits                  24238             49041 ns/op        2672.71 MB/s
BenchmarkCmpEqual/64k/90%/45_bits                  23780             50300 ns/op        2605.78 MB/s
BenchmarkCmpEqual/64k/90%/46_bits                  24374             49203 ns/op        2663.90 MB/s
BenchmarkCmpEqual/64k/90%/47_bits                  23996             49899 ns/op        2626.77 MB/s
BenchmarkCmpEqual/64k/90%/48_bits                  26799             44930 ns/op        2917.27 MB/s
BenchmarkCmpEqual/64k/90%/49_bits                  23852             50048 ns/op        2618.93 MB/s
BenchmarkCmpEqual/64k/90%/50_bits                  24097             49647 ns/op        2640.10 MB/s
BenchmarkCmpEqual/64k/90%/51_bits                  24004             50102 ns/op        2616.11 MB/s
BenchmarkCmpEqual/64k/90%/52_bits                  24954             48230 ns/op        2717.63 MB/s
BenchmarkCmpEqual/64k/90%/53_bits                  24090             49952 ns/op        2623.96 MB/s
BenchmarkCmpEqual/64k/90%/54_bits                  24354             49569 ns/op        2644.25 MB/s
BenchmarkCmpEqual/64k/90%/55_bits                  23912             50190 ns/op        2611.50 MB/s
BenchmarkCmpEqual/64k/90%/56_bits                  26739             44821 ns/op        2924.34 MB/s
BenchmarkCmpEqual/64k/90%/57_bits                  24052             50226 ns/op        2609.63 MB/s
BenchmarkCmpEqual/64k/90%/58_bits                  24445             49182 ns/op        2665.01 MB/s
BenchmarkCmpEqual/64k/90%/59_bits                  23359             51616 ns/op        2539.35 MB/s
BenchmarkCmpEqual/64k/90%/60_bits                  25316             48109 ns/op        2724.48 MB/s
BenchmarkCmpEqual/64k/90%/61_bits                  21924             53438 ns/op        2452.78 MB/s
BenchmarkCmpEqual/64k/90%/62_bits                  22946             52343 ns/op        2504.10 MB/s
BenchmarkCmpEqual/64k/90%/63_bits                  21747             54845 ns/op        2389.88 MB/s
```

### Sequential kernels

```
BenchmarkCmpEqualUnpacked/64k/10%/0_bits-24                20866         57225 ns/op    2290.48 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/1_bits-24                15510         77558 ns/op    1689.98 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/2_bits-24                15334         78297 ns/op    1674.05 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/3_bits-24                13126         90888 ns/op    1442.13 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/4_bits-24                15490         77393 ns/op    1693.58 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/5_bits-24                12309         97442 ns/op    1345.13 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/6_bits-24                12080         99304 ns/op    1319.90 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/7_bits-24                 9958        115597 ns/op    1133.87 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/8_bits-24                15328         77505 ns/op    1691.14 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/9_bits-24                10000        103131 ns/op    1270.93 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/10_bits-24               10000        103224 ns/op    1269.78 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/11_bits-24                8852        138016 ns/op     949.69 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/12_bits-24               10000        103308 ns/op    1268.75 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/13_bits-24                9267        130030 ns/op    1008.01 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/14_bits-24                9045        129871 ns/op    1009.25 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/15_bits-24                6730        174892 ns/op     749.45 MB/s
```

### Unpack Only

```
BenchmarkCmpEqualUnpacked/64k/10%/0_bits-24                21070         56291 ns/op    2328.47 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/1_bits-24                15636         76193 ns/op    1720.26 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/2_bits-24                15625         76744 ns/op    1707.90 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/3_bits-24                13560         88629 ns/op    1478.89 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/4_bits-24                15703         76511 ns/op    1713.11 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/5_bits-24                12196         96171 ns/op    1362.90 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/6_bits-24                12333         96444 ns/op    1359.04 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/7_bits-24                10000        113132 ns/op    1158.57 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/8_bits-24                15645         76447 ns/op    1714.55 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/9_bits-24                10000        102285 ns/op    1281.44 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/10_bits-24               10000        101258 ns/op    1294.43 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/11_bits-24                8653        136952 ns/op     957.07 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/12_bits-24               10000        101727 ns/op    1288.47 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/13_bits-24                9482        127105 ns/op    1031.21 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/14_bits-24                8498        127536 ns/op    1027.73 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/15_bits-24                7164        164751 ns/op     795.58 MB/s
```

### Compare Only

```
BenchmarkMatchInt16Equal/64K-24            68745         17562 ns/op    7463.53 MB/s
```