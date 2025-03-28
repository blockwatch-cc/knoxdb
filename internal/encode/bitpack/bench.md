# Bitpack Cmp Fusion Benchmarks

## Apple M1 Max

### Fused kernels

```
BenchmarkCmpEqual/64k/90%/0_bits-10              9539395           126.2 ns/op  1038992.09 MB/s
BenchmarkCmpEqual/64k/90%/1_bits-10               136724          8592 ns/op    15254.98 MB/s
BenchmarkCmpEqual/64k/90%/2_bits-10                46648         25860 ns/op    5068.45 MB/s
BenchmarkCmpEqual/64k/90%/3_bits-10                36421         33150 ns/op    3953.88 MB/s
BenchmarkCmpEqual/64k/90%/4_bits-10                43296         27460 ns/op    4773.23 MB/s
BenchmarkCmpEqual/64k/90%/5_bits-10                28617         41424 ns/op    3164.13 MB/s
BenchmarkCmpEqual/64k/90%/6_bits-10                28544         42324 ns/op    3096.86 MB/s
BenchmarkCmpEqual/64k/90%/7_bits-10                23930         49929 ns/op    2625.19 MB/s
BenchmarkCmpEqual/64k/90%/8_bits-10                46771         25813 ns/op    5077.80 MB/s
BenchmarkCmpEqual/64k/90%/9_bits-10                21092         56714 ns/op    2311.12 MB/s
BenchmarkCmpEqual/64k/90%/10_bits-10               20907         56834 ns/op    2306.23 MB/s
BenchmarkCmpEqual/64k/90%/11_bits-10               22574         52436 ns/op    2499.66 MB/s
BenchmarkCmpEqual/64k/90%/12_bits-10               20926         56665 ns/op    2313.10 MB/s
BenchmarkCmpEqual/64k/90%/13_bits-10               22755         52655 ns/op    2489.28 MB/s
BenchmarkCmpEqual/64k/90%/14_bits-10               22882         52784 ns/op    2483.16 MB/s
BenchmarkCmpEqual/64k/90%/15_bits-10               22869         52484 ns/op    2497.38 MB/s
BenchmarkCmpEqual/64k/90%/16_bits-10               20892         56987 ns/op    2300.04 MB/s
BenchmarkCmpEqual/64k/90%/17_bits-10               20836         56959 ns/op    2301.17 MB/s
BenchmarkCmpEqual/64k/90%/18_bits-10               20366         56671 ns/op    2312.84 MB/s
BenchmarkCmpEqual/64k/90%/19_bits-10               20818         56527 ns/op    2318.76 MB/s
BenchmarkCmpEqual/64k/90%/20_bits-10               20805         57612 ns/op    2275.06 MB/s
BenchmarkCmpEqual/64k/90%/21_bits-10               21200         57004 ns/op    2299.36 MB/s
BenchmarkCmpEqual/64k/90%/22_bits-10               21224         56749 ns/op    2309.68 MB/s
BenchmarkCmpEqual/64k/90%/23_bits-10               21152         56965 ns/op    2300.91 MB/s
BenchmarkCmpEqual/64k/90%/24_bits-10               21230         56705 ns/op    2311.48 MB/s
BenchmarkCmpEqual/64k/90%/25_bits-10               21126         56748 ns/op    2309.72 MB/s
BenchmarkCmpEqual/64k/90%/26_bits-10               21170         56667 ns/op    2313.01 MB/s
BenchmarkCmpEqual/64k/90%/27_bits-10               22287         52993 ns/op    2473.40 MB/s
BenchmarkCmpEqual/64k/90%/28_bits-10               20847         56540 ns/op    2318.22 MB/s
BenchmarkCmpEqual/64k/90%/29_bits-10               20988         56713 ns/op    2311.14 MB/s
BenchmarkCmpEqual/64k/90%/30_bits-10               21205         57198 ns/op    2291.55 MB/s
BenchmarkCmpEqual/64k/90%/31_bits-10               21171         57240 ns/op    2289.87 MB/s
```

### Sequential kernels

```
BenchmarkCmpEqualUnpacked/64k/10%/0_bits-10        13159         91242 ns/op    1436.53 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/1_bits-10        10000        117312 ns/op    1117.29 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/2_bits-10        10000        119477 ns/op    1097.04 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/3_bits-10         9280        125326 ns/op    1045.85 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/4_bits-10        10000        126702 ns/op    1034.49 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/5_bits-10         8682        146021 ns/op     897.63 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/6_bits-10         7491        142824 ns/op     917.72 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/7_bits-10         8397        143050 ns/op     916.26 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/8_bits-10        10000        116873 ns/op    1121.49 MB/s
BenchmarkCmpEqualUnpacked/64k/10%/9_bits-10         7851        151147 ns/op     867.18 MB/s
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
BenchmarkCmpEqual/64k/90%/0_bits-24             20933392                55.22 ns/op     2373467.42 MB/s
BenchmarkCmpEqual/64k/90%/1_bits-24               307945              3737 ns/op        35072.31 MB/s
BenchmarkCmpEqual/64k/90%/2_bits-24                49112             23967 ns/op        5468.89 MB/s
BenchmarkCmpEqual/64k/90%/3_bits-24                39706             30423 ns/op        4308.39 MB/s
BenchmarkCmpEqual/64k/90%/4_bits-24                50109             23960 ns/op        5470.40 MB/s
BenchmarkCmpEqual/64k/90%/5_bits-24                32679             36719 ns/op        3569.57 MB/s
BenchmarkCmpEqual/64k/90%/6_bits-24                33162             35744 ns/op        3666.95 MB/s
BenchmarkCmpEqual/64k/90%/7_bits-24                28347             42854 ns/op        3058.56 MB/s
BenchmarkCmpEqual/64k/90%/8_bits-24              2284618               527.1 ns/op      248660.06 MB/s
BenchmarkCmpEqual/64k/90%/9_bits-24                23756             48854 ns/op        2682.96 MB/s
BenchmarkCmpEqual/64k/90%/10_bits-24               24381             49475 ns/op        2649.25 MB/s
BenchmarkCmpEqual/64k/90%/11_bits-24               28557             41418 ns/op        3164.62 MB/s
BenchmarkCmpEqual/64k/90%/12_bits-24               25058             47611 ns/op        2753.01 MB/s
BenchmarkCmpEqual/64k/90%/13_bits-24               28693             42180 ns/op        3107.46 MB/s
BenchmarkCmpEqual/64k/90%/14_bits-24               29038             41440 ns/op        3162.91 MB/s
BenchmarkCmpEqual/64k/90%/15_bits-24               28676             41770 ns/op        3137.93 MB/s
BenchmarkCmpEqual/64k/90%/16_bits-24               26731             44065 ns/op        2974.49 MB/s
BenchmarkCmpEqual/64k/90%/17_bits-24               27892             43772 ns/op        2994.44 MB/s
BenchmarkCmpEqual/64k/90%/18_bits-24               26638             44552 ns/op        2942.02 MB/s
BenchmarkCmpEqual/64k/90%/19_bits-24               27786             44629 ns/op        2936.95 MB/s
BenchmarkCmpEqual/64k/90%/20_bits-24               27282             43238 ns/op        3031.44 MB/s
BenchmarkCmpEqual/64k/90%/21_bits-24               26467             43452 ns/op        3016.45 MB/s
BenchmarkCmpEqual/64k/90%/22_bits-24               27174             44102 ns/op        2972.00 MB/s
BenchmarkCmpEqual/64k/90%/23_bits-24               26936             44173 ns/op        2967.25 MB/s
BenchmarkCmpEqual/64k/90%/24_bits-24               28705             42645 ns/op        3073.57 MB/s
BenchmarkCmpEqual/64k/90%/25_bits-24               28158             44939 ns/op        2916.65 MB/s
BenchmarkCmpEqual/64k/90%/26_bits-24               26409             42646 ns/op        3073.45 MB/s
BenchmarkCmpEqual/64k/90%/27_bits-24               24002             47257 ns/op        2773.59 MB/s
BenchmarkCmpEqual/64k/90%/28_bits-24               27672             41374 ns/op        3167.94 MB/s
BenchmarkCmpEqual/64k/90%/29_bits-24               24680             48109 ns/op        2724.49 MB/s
BenchmarkCmpEqual/64k/90%/30_bits-24               26296             48638 ns/op        2694.83 MB/s
BenchmarkCmpEqual/64k/90%/31_bits-24               24205             51370 ns/op        2551.52 MB/s
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