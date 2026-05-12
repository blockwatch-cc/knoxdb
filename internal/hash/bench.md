## Hash benchmarks `[]byte[:N]`

cpu: Apple M1 Max
BenchmarkHash/fnv/4-10              3.818 ns/op       1047.81 MB/s
BenchmarkHash/fnv/8-10              5.031 ns/op       1589.99 MB/s
BenchmarkHash/fnv/16-10             7.563 ns/op       2115.43 MB/s
BenchmarkHash/fnv/64-10            46.45 ns/op        1377.93 MB/s
BenchmarkHash/fnv/128-10          113.8 ns/op         1124.42 MB/s
BenchmarkHash/fnv/1024-10        1235 ns/op	           829.00 MB/s

BenchmarkHash/xxh3/4-10             2.776 ns/op       1440.93 MB/s
BenchmarkHash/xxh3/8-10             2.797 ns/op       2860.30 MB/s
BenchmarkHash/xxh3/16-10            2.838 ns/op       5636.84 MB/s
BenchmarkHash/xxh3/64-10            5.303 ns/op      12069.09 MB/s
BenchmarkHash/xxh3/128-10           8.299 ns/op      15424.07 MB/s
BenchmarkHash/xxh3/1024-10         41.48 ns/op       24686.83 MB/s

BenchmarkHash/wyhash/4-10           2.364 ns/op       1691.81 MB/s
BenchmarkHash/wyhash/8-10           2.496 ns/op       3205.46 MB/s
BenchmarkHash/wyhash/16-10          2.644 ns/op       6052.18 MB/s
BenchmarkHash/wyhash/64-10          4.523 ns/op      14149.54 MB/s
BenchmarkHash/wyhash/128-10         6.308 ns/op      20291.66 MB/s
BenchmarkHash/wyhash/1024-10       35.90 ns/op       28525.34 MB/s

BenchmarkHash/aeshash/4-10          4.385 ns/op        912.18 MB/s
BenchmarkHash/aeshash/8-10          4.067 ns/op       1967.26 MB/s
BenchmarkHash/aeshash/16-10         2.519 ns/op       6352.64 MB/s
BenchmarkHash/aeshash/64-10         3.781 ns/op      16926.72 MB/s
BenchmarkHash/aeshash/128-10        5.639 ns/o       22697.96 MB/s
BenchmarkHash/aeshash/1024-10      19.12 ns/op       53562.51 MB/s

cpu: 12th Gen Intel(R) Core(TM) i9-12900K
BenchmarkHash/fnv/4-24             3.264 ns/op    1225.40 MB/s
BenchmarkHash/fnv/8-24             3.545 ns/op    2256.87 MB/s
BenchmarkHash/fnv/16-24            4.995 ns/op    3203.50 MB/s
BenchmarkHash/fnv/64-24            29.90 ns/op    2140.43 MB/s
BenchmarkHash/fnv/128-24           73.13 ns/op    1750.33 MB/s
BenchmarkHash/fnv/1024-24         772.3 ns/op     1325.99 MB/s

BenchmarkHash/xxh3/4-24            2.192 ns/op    1824.45 MB/s
BenchmarkHash/xxh3/8-24            2.185 ns/op    3661.48 MB/s
BenchmarkHash/xxh3/16-24           1.953 ns/op    8193.51 MB/s
BenchmarkHash/xxh3/64-24           3.293 ns/op   19434.06 MB/s
BenchmarkHash/xxh3/128-24          5.150 ns/op   24853.27 MB/s
BenchmarkHash/xxh3/1024-24         19.55 ns/op   52371.51 MB/s

BenchmarkHash/wyhash/4-24          1.771 ns/op    2258.38 MB/s
BenchmarkHash/wyhash/8-24          1.974 ns/op    4051.89 MB/s
BenchmarkHash/wyhash/16-24         2.016 ns/op    7935.63 MB/s
BenchmarkHash/wyhash/64-24         3.702 ns/op   17286.07 MB/s
BenchmarkHash/wyhash/128-24        5.631 ns/op   22731.41 MB/s
BenchmarkHash/wyhash/1024-24       30.93 ns/op   33107.82 MB/s

BenchmarkHash/aeshash/4-24         2.160 ns/op    1852.02 MB/s
BenchmarkHash/aeshash/8-24         2.146 ns/op    3727.86 MB/s
BenchmarkHash/aeshash/16-24        2.060 ns/op    7765.93 MB/s
BenchmarkHash/aeshash/64-24        2.606 ns/op   24559.99 MB/s
BenchmarkHash/aeshash/128-24       4.280 ns/op   29908.42 MB/s
BenchmarkHash/aeshash/1024-24      15.68 ns/op   65320.06 MB/s


## Multi Hashes `[]uint64[:N]`

M1

BenchmarkMultiHash/xxh3_64/1k-10        1096 ns/op           7476.40 MB/s
BenchmarkMultiHash/xxh3_64/16k-10       17623 ns/op          7437.62 MB/s
BenchmarkMultiHash/xxh3_64/64k-10       71039 ns/op          7380.25 MB/s

BenchmarkMultiHash/wyhash64/1k-10         704.5 ns/o        11628.51 MB/s
BenchmarkMultiHash/wyhash64/16k-10       11193 ns/op        11709.82 MB/s
BenchmarkMultiHash/wyhash64/64k-10       44339 ns/op        11824.57 MB/s

Intel/AVX2

BenchmarkMultiHash/xxh3_64/1k-24           610.5 ns/op      13418.34 MB/s
BenchmarkMultiHash/xxh3_64/16k-24        10380 ns/op        12627.89 MB/s
BenchmarkMultiHash/xxh3_64/64k-24        39172 ns/op        13384.33 MB/s

BenchmarkMultiHash/wyhash64/1k-24          503.0 ns/op      16284.84 MB/s
BenchmarkMultiHash/wyhash64/16k-24        8379 ns/op        15642.78 MB/s
BenchmarkMultiHash/wyhash64/64k-24       32059 ns/op        16353.84 MB/s
