# Simple8b Generic Go Benchmarks

```
M1 Max

BenchmarkEncodeLegacy/dups_64K-10               1096       1091777 ns/op     480.22 MB/s
BenchmarkEncodeLegacy/runs_64K-10               1072       1082648 ns/op     484.26 MB/s
BenchmarkEncodeLegacy/seq_64K-10                3734        310506 ns/op    1688.50 MB/s

BenchmarkDecodeLegacy/dups_64K-10              10000        101767 ns/op    5151.86 MB/s
BenchmarkDecodeLegacy/runs_64K-10              10000        101013 ns/op    5190.32 MB/s
BenchmarkDecodeLegacy/seq_64K-10               29841         40282 ns/op    13015.30 MB/s

BenchmarkEncodeUint64/dups_64K-10               5224        225337 ns/op    2326.68 MB/s
BenchmarkEncodeUint64/runs_64K-10               5191        226527 ns/op    2314.46 MB/s
BenchmarkEncodeUint64/seq_64K-10                7436        147453 ns/op    3555.63 MB/s
BenchmarkEncodeUint32/dups_64K-10               4022        293342 ns/op     893.64 MB/s
BenchmarkEncodeUint32/runs_64K-10               6802        175323 ns/op    1495.21 MB/s
BenchmarkEncodeUint32/seq_64K-10                7942        148692 ns/op    1763.01 MB/s
BenchmarkEncodeUint16/dups_64K-10               3918        307202 ns/op     426.66 MB/s
BenchmarkEncodeUint16/runs_64K-10               6274        187961 ns/op     697.34 MB/s
BenchmarkEncodeUint16/seq_64K-10                7989        147700 ns/op     887.42 MB/s
BenchmarkEncodeUint8/dups_64K-10                5761        204442 ns/op     320.56 MB/s
BenchmarkEncodeUint8/runs_64K-10                6375        186024 ns/op     352.30 MB/s
BenchmarkEncodeUint8/seq_64K-10                 8973        131892 ns/op     496.89 MB/s

BenchmarkDecodeUint64/dups_64K-10              10000        106042 ns/op    4944.17 MB/s
BenchmarkDecodeUint64/runs_64K-10              10000        106639 ns/op    4916.46 MB/s
BenchmarkDecodeUint64/seq_64K-10               28647         42587 ns/op    12311.01 MB/s
BenchmarkDecodeUint32/dups_64K-10              20996         57373 ns/op    4569.11 MB/s
BenchmarkDecodeUint32/runs_64K-10              21033         56969 ns/op    4601.54 MB/s
BenchmarkDecodeUint32/seq_64K-10               32682         36391 ns/op    7203.61 MB/s
BenchmarkDecodeUint16/dups_64K-10              21561         56019 ns/op    2339.76 MB/s
BenchmarkDecodeUint16/runs_64K-10              15776         76744 ns/op    1707.91 MB/s
BenchmarkDecodeUint16/seq_64K-10               34216         34868 ns/op    3759.06 MB/s
BenchmarkDecodeUint8/dups_64K-10               67405         17763 ns/op    3689.42 MB/s
BenchmarkDecodeUint8/runs_64K-10               31630         37803 ns/op    1733.62 MB/s
BenchmarkDecodeUint8/seq_64K-10                65364         18268 ns/op    3587.50 MB/s

M4 Pro

BenchmarkEncodeLegacy/dups_64K-14               1418        854938 ns/op     613.25 MB/s
BenchmarkEncodeLegacy/runs_64K-14               1400        851518 ns/op     615.71 MB/s
BenchmarkEncodeLegacy/seq_64K-14                4664        257434 ns/op    2036.59 MB/s

BenchmarkDecodeLegacy/dups_64K-14              17689         68076 ns/op    7701.46 MB/s
BenchmarkDecodeLegacy/runs_64K-14              17602         68112 ns/op    7697.43 MB/s
BenchmarkDecodeLegacy/seq_64K-14               39577         30403 ns/op    17244.89 MB/s

BenchmarkEncodeUint64/dups_64K-14               7000        171325 ns/op    3060.20 MB/s
BenchmarkEncodeUint64/runs_64K-14               7065        171760 ns/op    3052.44 MB/s
BenchmarkEncodeUint64/seq_64K-14               10000        103750 ns/op    5053.40 MB/s
BenchmarkEncodeUint32/dups_64K-14               5598        213921 ns/op    1225.42 MB/s
BenchmarkEncodeUint32/runs_64K-14               9876        119933 ns/op    2185.75 MB/s
BenchmarkEncodeUint32/seq_64K-14               10000        101613 ns/op    2579.82 MB/s
BenchmarkEncodeUint16/dups_64K-14               6382        199761 ns/op     656.14 MB/s
BenchmarkEncodeUint16/runs_64K-14               8304        145392 ns/op     901.51 MB/s
BenchmarkEncodeUint16/seq_64K-14               10000        104087 ns/op    1259.25 MB/s
BenchmarkEncodeUint8/dups_64K-14                8458        135797 ns/op     482.60 MB/s
BenchmarkEncodeUint8/runs_64K-14                6520        182219 ns/op     359.66 MB/s
BenchmarkEncodeUint8/seq_64K-14                10000        100985 ns/op     648.97 MB/s

BenchmarkDecodeUint64/dups_64K-14              10000        110909 ns/op    4727.21 MB/s
BenchmarkDecodeUint64/runs_64K-14              10000        112481 ns/op    4661.11 MB/s
BenchmarkDecodeUint64/seq_64K-14               37435         31994 ns/op    16387.08 MB/s
BenchmarkDecodeUint32/dups_64K-14              29976         40045 ns/op    6546.23 MB/s
BenchmarkDecodeUint32/runs_64K-14              29743         40432 ns/op    6483.53 MB/s
BenchmarkDecodeUint32/seq_64K-14               36201         33177 ns/op    7901.45 MB/s
BenchmarkDecodeUint16/dups_64K-14              28321         42425 ns/op    3089.47 MB/s
BenchmarkDecodeUint16/runs_64K-14              21092         56947 ns/op    2301.66 MB/s
BenchmarkDecodeUint16/seq_64K-14               47035         25517 ns/op    5136.73 MB/s
BenchmarkDecodeUint8/dups_64K-14               94258         12750 ns/op    5140.23 MB/s
BenchmarkDecodeUint8/runs_64K-14               41568         28875 ns/op    2269.66 MB/s
BenchmarkDecodeUint8/seq_64K-14                87141         13765 ns/op    4761.16 MB/s

AVX2

BenchmarkDecodeUint64/dups_64K-24              51543         23635 ns/op    22182.74 MB/s
BenchmarkDecodeUint64/runs_64K-24              50010         23555 ns/op    22257.58 MB/s
BenchmarkDecodeUint64/seq_64K-24              130790          8470 ns/op    61901.96 MB/s
BenchmarkDecodeUint32/dups_64K-24              92600         12721 ns/op    20606.72 MB/s
BenchmarkDecodeUint32/runs_64K-24              93481         12920 ns/op    20289.07 MB/s
BenchmarkDecodeUint32/seq_64K-24              113628         10688 ns/op    24525.88 MB/s
BenchmarkDecodeUint16/dups_64K-24              65584         18339 ns/op    7147.13 MB/s
BenchmarkDecodeUint16/runs_64K-24              36504         34025 ns/op    3852.21 MB/s
BenchmarkDecodeUint16/seq_64K-24              133980          8951 ns/op    14643.56 MB/s
BenchmarkDecodeUint8/dups_64K-24              317013          3464 ns/op    18920.89 MB/s
BenchmarkDecodeUint8/runs_64K-24               64576         18038 ns/op    3633.13 MB/s
BenchmarkDecodeUint8/seq_64K-24               180457          6258 ns/op    10472.59 MB/s
```