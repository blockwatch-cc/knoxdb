## Hash benchmarks

cpu: Apple M1 Max
BenchmarkHash/fnv/4-10                 3.776 ns/op       1059.45 MB/s
BenchmarkHash/fnv/8-10                 5.095 ns/op       1570.23 MB/s
BenchmarkHash/fnv/16-10                7.603 ns/op       2104.43 MB/s
BenchmarkHash/fnv/64-10               46.88 ns/op        1365.14 MB/s
BenchmarkHash/fnv/128-10             111.3 ns/op         1150.21 MB/s
BenchmarkHash/fnv/1024-10           1250 ns/op            819.03 MB/s
BenchmarkHash/xxhash64/4-10            2.952 ns/op       1354.83 MB/s
BenchmarkHash/xxhash64/8-10            3.787 ns/op       2112.46 MB/s
BenchmarkHash/xxhash64/16-10           5.079 ns/op       3150.27 MB/s
BenchmarkHash/xxhash64/64-10           9.038 ns/op       7081.26 MB/s
BenchmarkHash/xxhash64/128-10         12.97 ns/op        9872.37 MB/s
BenchmarkHash/xxhash64/1024-10        72.77 ns/op       14072.55 MB/s
BenchmarkHash/xxhash32/4-10            3.479 ns/op       1149.68 MB/s
BenchmarkHash/xxhash32/8-10            4.712 ns/op       1697.87 MB/s
BenchmarkHash/xxhash32/16-10           5.135 ns/op       3116.05 MB/s
BenchmarkHash/xxhash32/64-10          10.32 ns/op        6198.96 MB/s
BenchmarkHash/xxhash32/128-10         17.75 ns/op        7209.46 MB/s
BenchmarkHash/xxhash32/1024-10       141.5 ns/op         7238.98 MB/s
BenchmarkHash/wyhash/4-10              2.264 ns/op       1767.13 MB/s
BenchmarkHash/wyhash/8-10              2.413 ns/op       3315.32 MB/s
BenchmarkHash/wyhash/16-10             2.596 ns/op       6162.98 MB/s
BenchmarkHash/wyhash/64-10             4.440 ns/op      14413.97 MB/s
BenchmarkHash/wyhash/128-10            6.294 ns/op      20338.43 MB/s
BenchmarkHash/wyhash/1024-10          36.05 ns/op       28402.09 MB/s
BenchmarkHash/aeshash/4-10             4.088 ns/op        978.38 MB/s
BenchmarkHash/aeshash/8-10             4.159 ns/op       1923.32 MB/s
BenchmarkHash/aeshash/16-10            2.520 ns/op       6350.10 MB/s
BenchmarkHash/aeshash/64-10            3.541 ns/op      18072.79 MB/s
BenchmarkHash/aeshash/128-10           5.663 ns/op      22603.27 MB/s
BenchmarkHash/aeshash/1024-10         18.99 ns/op       53936.82 MB/s
BenchmarkXxhashVec64-10                0.3124 ns/op     25605.33 MB/s
BenchmarkXxhashVec32-10                0.3225 ns/op     24809.62 MB/s
BenchmarkXxh3Vec64-10                  0.3151 ns/op     25387.42 MB/s
BenchmarkWyhash64-10                   0.3175 ns/op     25196.02 MB/s
BenchmarkWyhash32-10                   0.3136 ns/op     12756.01 MB/s

cpu: 12th Gen Intel(R) Core(TM) i9-12900K
BenchmarkHash/fnv/4-24              3.662 ns/op       1092.35 MB/s
BenchmarkHash/fnv/8-24              3.943 ns/op       2028.91 MB/s
BenchmarkHash/fnv/16-24             5.443 ns/op       2939.62 MB/s
BenchmarkHash/fnv/64-24             31.51 ns/op       2031.04 MB/s
BenchmarkHash/fnv/128-24            77.09 ns/op       1660.36 MB/s
BenchmarkHash/fnv/1024-24           781.2 ns/op       1310.81 MB/s
BenchmarkHash/xxhash64/4-24         2.288 ns/op       1748.21 MB/s
BenchmarkHash/xxhash64/8-24         2.365 ns/op       3382.06 MB/s
BenchmarkHash/xxhash64/16-24        3.025 ns/op       5289.23 MB/s
BenchmarkHash/xxhash64/64-24        5.916 ns/op      10817.54 MB/s
BenchmarkHash/xxhash64/128-24       9.017 ns/op      14195.29 MB/s
BenchmarkHash/xxhash64/1024-24      52.28 ns/op      19585.03 MB/s
BenchmarkHash/xxhash32/4-24         2.472 ns/op       1618.27 MB/s
BenchmarkHash/xxhash32/8-24         3.512 ns/op       2277.63 MB/s
BenchmarkHash/xxhash32/16-24        3.474 ns/op       4606.01 MB/s
BenchmarkHash/xxhash32/64-24        6.867 ns/op       9319.27 MB/s
BenchmarkHash/xxhash32/128-24       12.96 ns/op       9876.53 MB/s
BenchmarkHash/xxhash32/1024-24      101.2 ns/op      10115.79 MB/s
BenchmarkHash/wyhash/4-24           1.831 ns/op       2184.07 MB/s
BenchmarkHash/wyhash/8-24           1.939 ns/op       4126.09 MB/s
BenchmarkHash/wyhash/16-24          2.014 ns/op       7945.99 MB/s
BenchmarkHash/wyhash/64-24          3.498 ns/op      18297.18 MB/s
BenchmarkHash/wyhash/128-24         5.421 ns/op      23613.69 MB/s
BenchmarkHash/wyhash/1024-24        30.83 ns/op      33217.91 MB/s
BenchmarkHash/aeshash/4-24          1.754 ns/op       2279.95 MB/s
BenchmarkHash/aeshash/8-24          1.738 ns/op       4603.19 MB/s
BenchmarkHash/aeshash/16-24         1.649 ns/op       9704.18 MB/s
BenchmarkHash/aeshash/64-24         2.518 ns/op      25421.94 MB/s
BenchmarkHash/aeshash/128-24        4.236 ns/op      30215.66 MB/s
BenchmarkHash/aeshash/1024-24       15.40 ns/op      66505.87 MB/s
BenchmarkXxhashVec64-24             0.1072 ns/op     74593.41 MB/s
BenchmarkXxhashVec32-24             0.1052 ns/op     76068.58 MB/s
BenchmarkXxh3Vec64-24               0.1021 ns/op     78347.97 MB/s
BenchmarkWyhash64-24                0.1138 ns/op     70313.69 MB/s
BenchmarkWyhash32-24                0.1057 ns/op     37829.79 MB/s

## Multi Hashes

M1

BenchmarkMultiHash/xxhash64/1k-10         937075          1238 ns/op    6615.20 MB/s
BenchmarkMultiHash/xxhash64/16k-10         61320         19966 ns/op    6564.85 MB/s
BenchmarkMultiHash/xxhash64/64k-10         14569         81092 ns/op    6465.36 MB/s
BenchmarkMultiHash/xxhash32/1k-10         995358          1215 ns/op    6740.76 MB/s
BenchmarkMultiHash/xxhash32/16k-10         62817         19808 ns/op    6617.11 MB/s
BenchmarkMultiHash/xxhash32/64k-10         14750         78964 ns/op    6639.60 MB/s
BenchmarkMultiHash/xxh3/1k-10            1000000          1136 ns/op    7212.55 MB/s
BenchmarkMultiHash/xxh3/16k-10             66799         17804 ns/op    7361.92 MB/s
BenchmarkMultiHash/xxh3/64k-10             16794         71275 ns/op    7355.87 MB/s
BenchmarkMultiHash/wyhash/1k-10          1682613           738.9 ns/op  11087.25 MB/s
BenchmarkMultiHash/wyhash/16k-10          102748         11426 ns/op    11471.75 MB/s
BenchmarkMultiHash/wyhash/64k-10           26558         45274 ns/op    11580.34 MB/s

Intel/AVX2

BenchmarkMultiHash/xxhash64/1k            849090          1396 ns/op    5870.05 MB/s
BenchmarkMultiHash/xxhash64/16k            51524         22511 ns/op    5822.65 MB/s
BenchmarkMultiHash/xxhash64/64k            13459         89269 ns/op    5873.15 MB/s
BenchmarkMultiHash/xxhash32/1k           3411392           353.8 ns/op  23152.83 MB/s
BenchmarkMultiHash/xxhash32/16k           212116          5665 ns/op    23136.45 MB/s
BenchmarkMultiHash/xxhash32/64k            52680         22736 ns/op    23060.29 MB/s
BenchmarkMultiHash/xxh3/1k               1976161           607.8 ns/op  13479.15 MB/s
BenchmarkMultiHash/xxh3/16k               124760          9662 ns/op    13566.39 MB/s
BenchmarkMultiHash/xxh3/64k                30790         38792 ns/op    13515.51 MB/s
BenchmarkMultiHash/wyhash/1k             2460909           490.9 ns/op  16687.38 MB/s
BenchmarkMultiHash/wyhash/16k             154038          7894 ns/op    16604.59 MB/s
BenchmarkMultiHash/wyhash/64k              38485         31180 ns/op    16814.76 MB/s