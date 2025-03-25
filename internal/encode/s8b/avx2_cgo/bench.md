# Simple8b Benchmarks

```
goos: linux
goarch: amd64
pkg: blockwatch.cc/simple8
cpu: 12th Gen Intel(R) Core(TM) i9-12900K

C SIMD (bug?)

BenchmarkEncodeUint32/dups_1K-24              386160          3123 ns/op    1311.41 MB/s          4120 mean_bytes
BenchmarkEncodeUint32/dups_16K-24              23893         50846 ns/op    1288.91 MB/s         65560 mean_bytes
BenchmarkEncodeUint32/dups_64K-24               5833        204957 ns/op    1279.02 MB/s        262160 mean_bytes
BenchmarkEncodeUint32/runs_1K-24              372568          3175 ns/op    1290.02 MB/s          4120 mean_bytes
BenchmarkEncodeUint32/runs_16K-24              23215         51575 ns/op    1270.69 MB/s         65552 mean_bytes
BenchmarkEncodeUint32/runs_64K-24               5712        206257 ns/op    1270.96 MB/s        262136 mean_bytes
BenchmarkEncodeUint32/seq_1K-24              1000000          1063 ns/op    3853.84 MB/s          1296 mean_bytes
BenchmarkEncodeUint32/seq_16K-24               49522         24419 ns/op    2683.78 MB/s         30800 mean_bytes
BenchmarkEncodeUint32/seq_64K-24               10000        115144 ns/op    2276.66 MB/s        150928 mean_bytes

C Scalar Simple

BenchmarkEncodeUint32/dups_1K-24              374432          3227 ns/op    1269.23 MB/s          4096 mean_bytes
BenchmarkEncodeUint32/dups_16K-24              18904         63202 ns/op    1036.94 MB/s         65536 mean_bytes
BenchmarkEncodeUint32/dups_64K-24               4528        254356 ns/op    1030.62 MB/s        262144 mean_bytes
BenchmarkEncodeUint32/runs_1K-24              367923          3147 ns/op    1301.59 MB/s          4088 mean_bytes
BenchmarkEncodeUint32/runs_16K-24              23437         50754 ns/op    1291.25 MB/s         65512 mean_bytes
BenchmarkEncodeUint32/runs_64K-24               5834        201719 ns/op    1299.55 MB/s        261880 mean_bytes
BenchmarkEncodeUint32/seq_1K-24               955027          1247 ns/op    3284.00 MB/s          1288 mean_bytes
BenchmarkEncodeUint32/seq_16K-24               47760         25045 ns/op    2616.76 MB/s         30776 mean_bytes
BenchmarkEncodeUint32/seq_64K-24                9865        120052 ns/op    2183.59 MB/s        150928 mean_bytes

C SIMD Work Preserving

BenchmarkEncodeUint32/dups_1K-24              492492          2490 ns/op    1645.28 MB/s          4096 mean_bytes
BenchmarkEncodeUint32/dups_16K-24              22478         53826 ns/op    1217.56 MB/s         65536 mean_bytes
BenchmarkEncodeUint32/dups_64K-24               5209        217854 ns/op    1203.30 MB/s        262144 mean_bytes
BenchmarkEncodeUint32/runs_1K-24              465319          2527 ns/op    1620.72 MB/s          4096 mean_bytes
BenchmarkEncodeUint32/runs_16K-24              28468         41634 ns/op    1574.10 MB/s         65512 mean_bytes
BenchmarkEncodeUint32/runs_64K-24               7266        165658 ns/op    1582.44 MB/s        261976 mean_bytes
BenchmarkEncodeUint32/seq_1K-24               839254          1407 ns/op    2911.37 MB/s          1288 mean_bytes
BenchmarkEncodeUint32/seq_16K-24               49045         24709 ns/op    2652.31 MB/s         30776 mean_bytes
BenchmarkEncodeUint32/seq_64K-24               10000        107144 ns/op    2446.65 MB/s        150928 mean_bytes

C Scalar Work Preserving

BenchmarkEncodeUint32/dups_1K-24              447272          2682 ns/op    1527.21 MB/s          4096 mean_bytes
BenchmarkEncodeUint32/dups_16K-24              21531         56260 ns/op    1164.89 MB/s         65536 mean_bytes
BenchmarkEncodeUint32/dups_64K-24               5220        226817 ns/op    1155.75 MB/s        262144 mean_bytes
BenchmarkEncodeUint32/runs_1K-24              482428          2459 ns/op    1665.91 MB/s          4096 mean_bytes
BenchmarkEncodeUint32/runs_16K-24              31047         38799 ns/op    1689.11 MB/s         65512 mean_bytes
BenchmarkEncodeUint32/runs_64K-24               7622        158502 ns/op    1653.88 MB/s        262024 mean_bytes
BenchmarkEncodeUint32/seq_1K-24               757318          1596 ns/op    2565.62 MB/s          1288 mean_bytes
BenchmarkEncodeUint32/seq_16K-24               44721         26883 ns/op    2437.78 MB/s         30776 mean_bytes
BenchmarkEncodeUint32/seq_64K-24               10000        117911 ns/op    2223.24 MB/s        150928 mean_bytes

BenchmarkEncodeUint64/dups_1K-24              248612          4335 ns/op    1889.70 MB/s          8192 mean_bytes
BenchmarkEncodeUint64/dups_16K-24              18060         66039 ns/op    1984.77 MB/s        131072 mean_bytes
BenchmarkEncodeUint64/dups_64K-24               3982        270387 ns/op    1939.03 MB/s        524288 mean_bytes
BenchmarkEncodeUint64/runs_1K-24              276679          4374 ns/op    1872.80 MB/s          8192 mean_bytes
BenchmarkEncodeUint64/runs_16K-24              17581         67882 ns/op    1930.87 MB/s        131072 mean_bytes
BenchmarkEncodeUint64/runs_64K-24               4500        270990 ns/op    1934.72 MB/s        524288 mean_bytes
BenchmarkEncodeUint64/seq_1K-24               715282          1639 ns/op    4996.87 MB/s          1288 mean_bytes
BenchmarkEncodeUint64/seq_16K-24               39654         30203 ns/op    4339.70 MB/s         30776 mean_bytes
BenchmarkEncodeUint64/seq_64K-24                9188        131045 ns/op    4000.81 MB/s        150928 mean_bytes

BenchmarkEncodeUint16/dups_1K-24              526850          2307 ns/op     887.86 MB/s          2688 mean_bytes
BenchmarkEncodeUint16/dups_16K-24              22267         53434 ns/op     613.25 MB/s         42824 mean_bytes
BenchmarkEncodeUint16/dups_64K-24               5106        231615 ns/op     565.90 MB/s        171536 mean_bytes
BenchmarkEncodeUint16/runs_1K-24              552080          2081 ns/op     983.91 MB/s          2408 mean_bytes
BenchmarkEncodeUint16/runs_16K-24              33154         37007 ns/op     885.45 MB/s         38536 mean_bytes
BenchmarkEncodeUint16/runs_64K-24               7893        149808 ns/op     874.94 MB/s        153912 mean_bytes
BenchmarkEncodeUint16/seq_1K-24               732212          1609 ns/op    1273.22 MB/s          1288 mean_bytes
BenchmarkEncodeUint16/seq_16K-24               43686         27053 ns/op    1211.24 MB/s         30776 mean_bytes
BenchmarkEncodeUint16/seq_64K-24               10000        117213 ns/op    1118.24 MB/s        150928 mean_bytes

GO Scalar

BenchmarkEncodeUint32/dups_1K-24              439110          2706 ns/op    1513.87 MB/s
BenchmarkEncodeUint32/dups_16K-24              18584         64415 ns/op    1017.40 MB/s
BenchmarkEncodeUint32/dups_64K-24               4524        265373 ns/op     987.83 MB/s
BenchmarkEncodeUint32/runs_1K-24              438584          2624 ns/op    1560.83 MB/s
BenchmarkEncodeUint32/runs_16K-24              29209         40778 ns/op    1607.13 MB/s
BenchmarkEncodeUint32/runs_64K-24               6874        168741 ns/op    1553.53 MB/s
BenchmarkEncodeUint32/seq_1K-24               580780          2063 ns/op    1985.09 MB/s
BenchmarkEncodeUint32/seq_16K-24               34653         34501 ns/op    1899.54 MB/s
BenchmarkEncodeUint32/seq_64K-24                8073        145111 ns/op    1806.51 MB/s
```