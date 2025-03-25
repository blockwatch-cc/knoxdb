# Performance Benchmarks

### Table 1: seq_64K 

| Data Type | Generic Go (MB/s) | AVX2 Go (MB/s) | AVX2 GCC (MB/s) | AVX2 Go vs. Generic (x) | AVX2 GCC vs. Generic (x) | AVX2 Go vs. AVX2 GCC (x) |
|-----------|-------------------|----------------|-----------------|-------------------------|--------------------------|---------------------------|
| int8      | 1452.78           | 33595.36       | 33273.57        | 23.13x                  | 22.91x                   | 1.01x                     |
| uint8     | 1528.84           | 33774.83       | 32823.45        | 22.09x                  | 21.47x                   | 1.03x                     |
| int16     | 3039.08           | 26383.19       | 23070.29        | 8.68x                   | 7.59x                    | 1.14x                     |
| uint16    | 3043.59           | 27016.32       | 22058.85        | 8.87x                   | 7.25x                    | 1.22x                     |
| int32     | 6101.20           | 33595.36       | 25095.70        | 5.51x                   | 4.11x                    | 1.34x                     |
| uint32    | 6052.62           | 33774.83       | 24861.82        | 5.58x                   | 4.11x                    | 1.36x                     |
| int64     | 11979.61          | 21154.63       | 20049.92        | 1.77x                   | 1.67x                    | 1.06x                     |
| uint64    | 12160.81          | 21790.51       | 18747.80        | 1.79x                   | 1.54x                    | 1.16x                     |


### Table 2: dups_64K

| Data Type | Generic Go (MB/s) | AVX2 Go (MB/s) | AVX2 GCC (MB/s) | AVX2 Go vs. Generic (x) | AVX2 GCC vs. Generic (x) | AVX2 Go vs. AVX2 GCC (x) |
|-----------|-------------------|----------------|-----------------|-------------------------|--------------------------|---------------------------|
| int8      | 1579.76           | 45573.70       | 45051.22        | 28.85x                  | 28.52x                   | 1.01x                     |
| uint8     | 1613.16           | 45270.39       | 44786.91        | 28.06x                  | 27.76x                   | 1.01x                     |
| int16     | 3833.28           | 41102.22       | 38187.31        | 10.72x                  | 9.96x                    | 1.08x                     |
| uint16    | 3837.52           | 40675.74       | 37308.65        | 10.60x                  | 9.72x                    | 1.09x                     |
| int32     | 6799.37           | 45573.70       | 42507.28        | 6.70x                   | 6.25x                    | 1.07x                     |
| uint32    | 6763.88           | 45270.39       | 40740.12        | 6.69x                   | 6.02x                    | 1.11x                     |
| int64     | 15423.55          | 23159.29       | 22908.15        | 1.50x                   | 1.49x                    | 1.01x                     |
| uint64    | 15353.99          | 23188.87       | 22564.15        | 1.51x                   | 1.47x                    | 1.03x                     |

### Table 3: runs_64K

| Data Type | Generic Go (MB/s) | AVX2 Go (MB/s) | AVX2 GCC (MB/s) | AVX2 Go vs. Generic (x) | AVX2 GCC vs. Generic (x) | AVX2 Go vs. AVX2 GCC (x) |
|-----------|-------------------|----------------|-----------------|-------------------------|--------------------------|---------------------------|
| int8      | 981.95            | 44929.57       | 44137.31        | 45.76x                  | 44.95x                   | 1.02x                     |
| uint8     | 1342.30           | 45381.53       | 45190.09        | 33.81x                  | 33.66x                   | 1.00x                     |
| int16     | 4893.84           | 38781.68       | 38922.79        | 7.92x                   | 7.95x                    | 1.00x                     |
| uint16    | 4827.26           | 40879.90       | 36709.08        | 8.47x                   | 7.60x                    | 1.11x                     |
| int32     | 8901.00           | 44929.57       | 43198.30        | 5.05x                   | 4.85x                    | 1.04x                     |
| uint32    | 8842.62           | 45381.53       | 41687.44        | 5.13x                   | 4.71x                    | 1.09x                     |
| int64     | 19167.35          | 23224.99       | 22346.16        | 1.21x                   | 1.17x                    | 1.04x                     |
| uint64    | 19583.62          | 23356.20       | 22547.15        | 1.19x                   | 1.15x                    | 1.04x                     |

## Raw benchmarks

```
M1 Max

BenchmarkAnalyzeInt64/dups_64K-10              19516         61658 ns/op    8503.11 MB/s
BenchmarkAnalyzeInt64/runs_64K-10              25422         47821 ns/op    10963.64 MB/s
BenchmarkAnalyzeInt64/seq_64K-10               19518         61561 ns/op    8516.54 MB/s
BenchmarkAnalyzeUint64/dups_64K-10             19486         61783 ns/op    8485.91 MB/s
BenchmarkAnalyzeUint64/runs_64K-10             25468         47310 ns/op    11081.99 MB/s
BenchmarkAnalyzeUint64/seq_64K-10              19293         61539 ns/op    8519.58 MB/s
BenchmarkAnalyzeInt32/dups_64K-10              19358         61561 ns/op    4258.25 MB/s
BenchmarkAnalyzeInt32/runs_64K-10              25501         47299 ns/op    5542.30 MB/s
BenchmarkAnalyzeInt32/seq_64K-10               19500         61524 ns/op    4260.87 MB/s
BenchmarkAnalyzeUint32/dups_64K-10             19492         61751 ns/op    4245.18 MB/s
BenchmarkAnalyzeUint32/runs_64K-10             25424         47167 ns/op    5557.80 MB/s
BenchmarkAnalyzeUint32/seq_64K-10              19483         61377 ns/op    4271.06 MB/s
BenchmarkAnalyzeInt16/dups_64K-10              18687         64127 ns/op    2043.93 MB/s
BenchmarkAnalyzeInt16/runs_64K-10              19491         61530 ns/op    2130.23 MB/s
BenchmarkAnalyzeInt16/seq_64K-10               14649         82852 ns/op    1582.01 MB/s
BenchmarkAnalyzeUint16/dups_64K-10             19537         61573 ns/op    2128.72 MB/s
BenchmarkAnalyzeUint16/runs_64K-10             19489         61449 ns/op    2133.03 MB/s
BenchmarkAnalyzeUint16/seq_64K-10              14616         82125 ns/op    1596.01 MB/s
BenchmarkAnalyzeInt8/dups_64K-10               18072         66025 ns/op     992.59 MB/s
BenchmarkAnalyzeInt8/runs_64K-10               19542         61610 ns/op    1063.72 MB/s
BenchmarkAnalyzeInt8/seq_64K-10                14673         81848 ns/op     800.70 MB/s
BenchmarkAnalyzeUint8/dups_64K-10              19040         62812 ns/op    1043.37 MB/s
BenchmarkAnalyzeUint8/runs_64K-10              19570         61368 ns/op    1067.92 MB/s
BenchmarkAnalyzeUint8/seq_64K-10               14626         82048 ns/op     798.75 MB/s


M4 Pro

BenchmarkAnalyzeInt64/dups_64K-14              26109         46079 ns/op    11378.08 MB/s
BenchmarkAnalyzeInt64/runs_64K-14              29037         41209 ns/op    12722.67 MB/s
BenchmarkAnalyzeInt64/seq_64K-14               32239         37235 ns/op    14080.66 MB/s
BenchmarkAnalyzeUint64/dups_64K-14             25776         46238 ns/op    11338.89 MB/s
BenchmarkAnalyzeUint64/runs_64K-14             33477         35752 ns/op    14664.67 MB/s
BenchmarkAnalyzeUint64/seq_64K-14              32192         37226 ns/op    14084.07 MB/s
BenchmarkAnalyzeInt32/dups_64K-14              26124         45950 ns/op    5704.96 MB/s
BenchmarkAnalyzeInt32/runs_64K-14              33949         38398 ns/op    6826.95 MB/s
BenchmarkAnalyzeInt32/seq_64K-14               25754         46580 ns/op    5627.83 MB/s
BenchmarkAnalyzeUint32/dups_64K-14             26080         46191 ns/op    5675.27 MB/s
BenchmarkAnalyzeUint32/runs_64K-14             30490         40261 ns/op    6511.18 MB/s
BenchmarkAnalyzeUint32/seq_64K-14              32208         37256 ns/op    7036.31 MB/s
BenchmarkAnalyzeInt16/dups_64K-14              24966         48093 ns/op    2725.36 MB/s
BenchmarkAnalyzeInt16/runs_64K-14              25348         47389 ns/op    2765.88 MB/s
BenchmarkAnalyzeInt16/seq_64K-14               24622         48884 ns/op    2681.29 MB/s
BenchmarkAnalyzeUint16/dups_64K-14             24997         48034 ns/op    2728.76 MB/s
BenchmarkAnalyzeUint16/runs_64K-14             25278         47607 ns/op    2753.23 MB/s
BenchmarkAnalyzeUint16/seq_64K-14              24602         48810 ns/op    2685.36 MB/s
BenchmarkAnalyzeInt8/dups_64K-14               24751         48455 ns/op    1352.52 MB/s
BenchmarkAnalyzeInt8/runs_64K-14               25446         47195 ns/op    1388.62 MB/s
BenchmarkAnalyzeInt8/seq_64K-14                24645         48588 ns/op    1348.82 MB/s
BenchmarkAnalyzeUint8/dups_64K-14              24806         48430 ns/op    1353.21 MB/s
BenchmarkAnalyzeUint8/runs_64K-14              25352         47250 ns/op    1387.01 MB/s
BenchmarkAnalyzeUint8/seq_64K-14               24684         48511 ns/op    1350.95 MB/s

AVX2

cpu: 12th Gen Intel(R) Core(TM) i9-12900K
BenchmarkAnalyzeInt64/dups_64K-24              57552         20664 ns/op    25372.03 MB/s
BenchmarkAnalyzeInt64/runs_64K-24              57555         20719 ns/op    25304.39 MB/s
BenchmarkAnalyzeInt64/seq_64K-24               53737         21724 ns/op    24133.54 MB/s
BenchmarkAnalyzeUint64/dups_64K-24             56452         20897 ns/op    25089.16 MB/s
BenchmarkAnalyzeUint64/runs_64K-24             57426         20619 ns/op    25427.10 MB/s
BenchmarkAnalyzeUint64/seq_64K-24              55227         21906 ns/op    23933.72 MB/s
BenchmarkAnalyzeInt32/dups_64K-24             198307          6020 ns/op    43545.89 MB/s
BenchmarkAnalyzeInt32/runs_64K-24             198518          5793 ns/op    45250.04 MB/s
BenchmarkAnalyzeInt32/seq_64K-24              137401          7796 ns/op    33627.01 MB/s
BenchmarkAnalyzeUint32/dups_64K-24            200101          5930 ns/op    44208.55 MB/s
BenchmarkAnalyzeUint32/runs_64K-24            198756          5945 ns/op    44091.97 MB/s
BenchmarkAnalyzeUint32/seq_64K-24             134548          8513 ns/op    30795.15 MB/s
BenchmarkAnalyzeInt16/dups_64K-24             452751          2543 ns/op    51547.64 MB/s
BenchmarkAnalyzeInt16/runs_64K-24             445284          2388 ns/op    54888.01 MB/s
BenchmarkAnalyzeInt16/seq_64K-24              296536          3548 ns/op    36943.74 MB/s
BenchmarkAnalyzeUint16/dups_64K-24            445879          2320 ns/op    56504.48 MB/s
BenchmarkAnalyzeUint16/runs_64K-24            448903          2298 ns/op    57041.32 MB/s
BenchmarkAnalyzeUint16/seq_64K-24             297784          3977 ns/op    32960.40 MB/s
BenchmarkAnalyzeInt8/dups_64K-24              889600          1305 ns/op    50212.72 MB/s
BenchmarkAnalyzeInt8/runs_64K-24              905098          1290 ns/op    50816.15 MB/s
BenchmarkAnalyzeInt8/seq_64K-24               707973          1600 ns/op    40959.46 MB/s
BenchmarkAnalyzeUint8/dups_64K-24             909447          1170 ns/op    56019.71 MB/s
BenchmarkAnalyzeUint8/runs_64K-24             978973          1305 ns/op    50213.14 MB/s
BenchmarkAnalyzeUint8/seq_64K-24              641990          1843 ns/op    35563.72 MB/s
```