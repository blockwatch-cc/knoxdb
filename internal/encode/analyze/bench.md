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
BenchmarkAnalyze/uint64/dups_64k-10        53062 ns/op    9880.62 MB/s          1.235 vals/ns
BenchmarkAnalyze/uint64/runs_64k-10        49225 ns/op    10650.77 MB/s         1.331 vals/ns
BenchmarkAnalyze/uint64/seq_64k-10         66425 ns/op    7892.94 MB/s          0.9866 vals/ns
BenchmarkAnalyze/uint32/dups_64k-10        53062 ns/op    4940.29 MB/s          1.235 vals/ns
BenchmarkAnalyze/uint32/runs_64k-10        49198 ns/op    5328.39 MB/s          1.332 vals/ns
BenchmarkAnalyze/uint32/seq_64k-10         68164 ns/op    3845.78 MB/s          0.9614 vals/ns
BenchmarkAnalyze/uint16/dups_64k-10        61851 ns/op    2119.15 MB/s          1.060 vals/ns
BenchmarkAnalyze/uint16/runs_64k-10        65797 ns/op    1992.05 MB/s          0.9960 vals/ns
BenchmarkAnalyze/uint16/seq_64k-10         67271 ns/op    1948.41 MB/s          0.9742 vals/ns
BenchmarkAnalyze/uint8/dups_64k-10         64453 ns/op    1016.80 MB/s          1.017 vals/ns
BenchmarkAnalyze/uint8/runs_64k-10         66710 ns/op     982.39 MB/s          0.9824 vals/ns
BenchmarkAnalyze/uint8/seq_64k-10          64707 ns/op    1012.81 MB/s          1.013 vals/ns

BenchmarkAnalyze/int64/dups_64k-10         53294 ns/op    9837.73 MB/s          1.230 vals/ns
BenchmarkAnalyze/int64/runs_64k-10         49077 ns/op    10682.99 MB/s         1.335 vals/ns
BenchmarkAnalyze/int64/seq_64k-10          66919 ns/op    7834.67 MB/s          0.9793 vals/ns
BenchmarkAnalyze/int32/dups_64k-10         53453 ns/op    4904.24 MB/s          1.226 vals/ns
BenchmarkAnalyze/int32/runs_64k-10         49128 ns/op    5335.93 MB/s          1.334 vals/ns
BenchmarkAnalyze/int32/seq_64k-10          68476 ns/op    3828.25 MB/s          0.9571 vals/ns
BenchmarkAnalyze/int16/dups_64k-10         61760 ns/op    2122.26 MB/s          1.061 vals/ns
BenchmarkAnalyze/int16/runs_64k-10         65566 ns/op    1999.09 MB/s          0.9995 vals/ns
BenchmarkAnalyze/int16/seq_64k-10          68317 ns/op    1918.58 MB/s          0.9593 vals/ns
BenchmarkAnalyze/int8/dups_64k-10          65746 ns/op     996.80 MB/s          0.9968 vals/ns
BenchmarkAnalyze/int8/runs_64k-10          65847 ns/op     995.28 MB/s          0.9953 vals/ns
BenchmarkAnalyze/int8/seq_64k-10           68419 ns/op     957.86 MB/s          0.9579 vals/ns

BenchmarkAnalyze/float64/dups_64k-10       41259 ns/op    12707.20 MB/s         1.588 vals/ns
BenchmarkAnalyze/float64/runs_64k-10       41228 ns/op    12716.91 MB/s         1.590 vals/ns
BenchmarkAnalyze/float64/seq_64k-10        41049 ns/op    12772.17 MB/s         1.597 vals/ns
BenchmarkAnalyze/float32/dups_64k-10       41063 ns/op    6383.89 MB/s          1.596 vals/ns
BenchmarkAnalyze/float32/runs_64k-10       41170 ns/op    6367.36 MB/s          1.592 vals/ns
BenchmarkAnalyze/float32/seq_64k-10        41246 ns/op    6355.65 MB/s          1.589 vals/ns
```

AVX2
```
cpu: 12th Gen Intel(R) Core(TM) i9-12900K
BenchmarkAnalyze/uint64/dups_64k-24        21303 ns/op    24611.07 MB/s         3.076 vals/ns
BenchmarkAnalyze/uint64/runs_64k-24        20715 ns/op    25309.46 MB/s         3.164 vals/ns
BenchmarkAnalyze/uint64/seq_64k-24         21590 ns/op    24283.88 MB/s         3.035 vals/ns
BenchmarkAnalyze/uint32/dups_64k-24         5953 ns/op    44035.66 MB/s        11.01 vals/ns
BenchmarkAnalyze/uint32/runs_64k-24         5803 ns/op    45169.99 MB/s        11.29 vals/ns
BenchmarkAnalyze/uint32/seq_64k-24          7726 ns/op    33927.93 MB/s         8.482 vals/ns
BenchmarkAnalyze/uint16/dups_64k-24         2253 ns/op    58183.07 MB/s        29.09 vals/ns
BenchmarkAnalyze/uint16/runs_64k-24         2461 ns/op    53266.84 MB/s        26.63 vals/ns
BenchmarkAnalyze/uint16/seq_64k-24          3499 ns/op    37464.71 MB/s        18.73 vals/ns
BenchmarkAnalyze/uint8/dups_64k-24          1163 ns/op    56367.76 MB/s        56.37 vals/ns
BenchmarkAnalyze/uint8/runs_64k-24          1193 ns/op    54941.30 MB/s        54.94 vals/ns
BenchmarkAnalyze/uint8/seq_64k-24           1594 ns/op    41120.14 MB/s        41.12 vals/ns

BenchmarkAnalyze/int64/dups_64k-24         20902 ns/op    25083.59 MB/s         3.135 vals/ns
BenchmarkAnalyze/int64/runs_64k-24         20960 ns/op    25013.53 MB/s         3.127 vals/ns
BenchmarkAnalyze/int64/seq_64k-24          22176 ns/op    23642.26 MB/s         2.955 vals/ns
BenchmarkAnalyze/int32/dups_64k-24          6027 ns/op    43492.22 MB/s        10.87 vals/ns
BenchmarkAnalyze/int32/runs_64k-24          5781 ns/op    45343.84 MB/s        11.34 vals/ns
BenchmarkAnalyze/int32/seq_64k-24           8695 ns/op    30148.07 MB/s         7.537 vals/ns
BenchmarkAnalyze/int16/dups_64k-24          2317 ns/op    56562.65 MB/s        28.28 vals/ns
BenchmarkAnalyze/int16/runs_64k-24          2382 ns/op    55031.70 MB/s        27.52 vals/ns
BenchmarkAnalyze/int16/seq_64k-24           3918 ns/op    33454.39 MB/s        16.73 vals/ns
BenchmarkAnalyze/int8/dups_64k-24           1144 ns/op    57278.91 MB/s        57.28 vals/ns
BenchmarkAnalyze/int8/runs_64k-24           1154 ns/op    56789.06 MB/s        56.79 vals/ns
BenchmarkAnalyze/int8/seq_64k-24            1633 ns/op    40124.12 MB/s        40.12 vals/ns

BenchmarkAnalyze/float64/dups_64k-24       12732 ns/op    41178.09 MB/s         5.147 vals/ns
BenchmarkAnalyze/float64/runs_64k-24       12701 ns/op    41279.58 MB/s         5.160 vals/ns
BenchmarkAnalyze/float64/seq_64k-24        12658 ns/op    41418.05 MB/s         5.177 vals/ns
BenchmarkAnalyze/float32/dups_64k-24        6395 ns/op    40994.77 MB/s        10.25 vals/ns
BenchmarkAnalyze/float32/runs_64k-24        6378 ns/op    41104.12 MB/s        10.28 vals/ns
BenchmarkAnalyze/float32/seq_64k-24         6326 ns/op    41440.76 MB/s        10.36 vals/ns
```