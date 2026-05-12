## LLB Benchmarks

### cpu: Apple M1 Max

Method                   p=8             p=12           p=14            p=16
------------------------------------------------------------------------------------
AddUint32PureGo/1k        1820.74 MB/s   1427.71 MB/s   1086.36 MB/s   715.04 MB/s
AddUint32PureGo/16k       1722.05 MB/s    960.48 MB/s   1069.00 MB/s  1378.49 MB/s
AddUint32PureGo/64k       1786.20 MB/s   1243.70 MB/s    856.94 MB/s   957.02 MB/s
AddUint64PureGo/1k        3499.84 MB/s   2790.49 MB/s   2038.32 MB/s  1327.71 MB/s
AddUint64PureGo/16k       3358.71 MB/s   1953.36 MB/s   2172.83 MB/s  2735.13 MB/s
AddUint64PureGo/64k       3635.25 MB/s   2454.31 MB/s   1731.22 MB/s  1874.75 MB/s

HashAndAddPrealloc/1k     3560.29 MB/s   2857.24 MB/s   2305.61 MB/s  1400.13 MB/s
HashAndAddPrealloc/16k    3470.02 MB/s   1961.99 MB/s   2213.16 MB/s  2640.85 MB/s
HashAndAddPrealloc/64k    3630.27 MB/s   2561.46 MB/s   1743.75 MB/s  1932.55 MB/s

CardinalityPureGo/1k       948.79 MB/s   1057.71 MB/s   1068.76 MB/s  1061.67 MB/s
CardinalityPureGo/16k     1096.23 MB/s   1053.67 MB/s    480.56 MB/s   527.93 MB/s
CardinalityPureGo/64k     1088.76 MB/s   1039.18 MB/s   1048.45 MB/s   382.12 MB/s

CardinalityPureGo/1k       897.77 MB/s   1014.77 MB/s   1026.58 MB/s   995.30 MB/s
CardinalityPureGo/16k     1045.33 MB/s   1024.86 MB/s    455.07 MB/s   518.11 MB/s
CardinalityPureGo/64k     1054.15 MB/s   1035.41 MB/s   1028.97 MB/s   364.06 MB/s

MergePureGo/1k            1705.87 MB/s   2080.22 MB/s   2113.74 MB/s  2120.13 MB/s
MergePureGo/16k           1700.26 MB/s   2074.55 MB/s   2095.69 MB/s  2069.40 MB/s
MergePureGo/64k           1702.35 MB/s   2063.33 MB/s   2101.95 MB/s  2115.53 MB/s

UniqueMap/1k                    741.00 MB/s
UniqueMap/16k                   667.24 MB/s
UniqueMap/64k                   637.15 MB/s
UniqueSort/1k                   287.29 MB/s
UniqueSort/16k                  422.87 MB/s
UniqueSort/64k                  346.74 MB/s

### cpu: 12th Gen Intel(R) Core(TM) i9-12900K

Method                       p=8             p=12           p=14            p=16
------------------------------------------------------------------------------------
AddUint32AVX2/1k         2451.53 MB/s     531.78 MB/s     300.31 MB/s    110.89 MB/s
AddUint32AVX2/16k        4736.93 MB/s    3184.75 MB/s    1540.55 MB/s    610.74 MB/s
AddUint32AVX2/64k        4814.53 MB/s    4608.89 MB/s    3494.47 MB/s   1575.38 MB/s
AddUint64AVX2/1k         4579.20 MB/s     988.74 MB/s     527.33 MB/s    215.67 MB/s
AddUint64AVX2/16k        7604.70 MB/s    5373.85 MB/s    2853.93 MB/s   1036.99 MB/s
AddUint64AVX2/64k        8126.54 MB/s    7358.06 MB/s    5544.15 MB/s   2630.49 MB/s

AddUint32PureGo/1k       1845.19 MB/s     355.85 MB/s     191.11 MB/s     96.64 MB/s
AddUint32PureGo/16k      2171.62 MB/s    1183.96 MB/s     940.55 MB/s    319.13 MB/s
AddUint32PureGo/64k      2182.25 MB/s    1467.76 MB/s     965.39 MB/s    521.65 MB/s
AddUint64PureGo/1k       3853.31 MB/s     727.32 MB/s     397.76 MB/s    194.35 MB/s
AddUint64PureGo/16k      4488.65 MB/s    2435.10 MB/s    1570.54 MB/s    641.71 MB/s
AddUint64PureGo/64k      4502.98 MB/s    3051.25 MB/s    1953.78 MB/s   1252.73 MB/s

HashAndAddPrealloc/1k    3941.33 MB/s     738.52 MB/s     409.70 MB/s    202.91 MB/s
HashAndAddPrealloc/16k   4733.75 MB/s    2604.74 MB/s    1553.88 MB/s    721.44 MB/s
HashAndAddPrealloc/64k   4756.05 MB/s    3171.46 MB/s    2003.90 MB/s   1243.69 MB/s

CardinalityAVX2/1k       3530.93 MB/s   16083.60 MB/s   19787.75 MB/s  20124.56 MB/s
CardinalityAVX2/16k      6795.77 MB/s   16112.82 MB/s   19814.15 MB/s  20658.48 MB/s
CardinalityAVX2/64k      6768.96 MB/s   18173.57 MB/s   19638.55 MB/s  20155.82 MB/s

CardinalityPureGo/1k     1024.36 MB/s    1365.55 MB/s    1315.26 MB/s   1234.44 MB/s
CardinalityPureGo/16k    1231.77 MB/s    1260.37 MB/s     519.42 MB/s    564.73 MB/s
CardinalityPureGo/64k    1232.16 MB/s    1387.03 MB/s    1257.73 MB/s    393.57 MB/s

MergeAVX2/1k             2279.30 MB/s   24470.29 MB/s   53075.88 MB/s  60066.90 MB/s
MergeAVX2/16k            2194.99 MB/s   19425.13 MB/s   53619.45 MB/s  61361.83 MB/s
MergeAVX2/64k            2202.12 MB/s   22854.07 MB/s   57661.90 MB/s  60902.40 MB/s

MergePureGo/1k           1056.35 MB/s    2515.47 MB/s    2711.42 MB/s   2838.42 MB/s
MergePureGo/16k          1120.71 MB/s    2538.14 MB/s    2721.88 MB/s   2856.65 MB/s
MergePureGo/64k          1107.01 MB/s    2563.05 MB/s    2788.25 MB/s   2954.74 MB/s

UniqueMap/1k           158.54 MB/s
UniqueMap/16k          114.75 MB/s
UniqueMap/64k          109.66 MB/s

UniqueSort/1k          230.84 MB/s
UniqueSort/16k         180.99 MB/s
UniqueSort/64k         232.50 MB/s

