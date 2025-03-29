# Simple8b Encode Benchmarks

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

# Simpl8 Cmp Fusion Benchmarks

### Serial Execution - ARM64 M1

> EQ

```
BenchmarkCmpSerial64/dups_64K-10        7429        149506 ns/op    3506.81 MB/s
BenchmarkCmpSerial64/runs_64K-10        7578        149856 ns/op    3498.62 MB/s
BenchmarkCmpSerial64/seq_64K-10        13657         90528 ns/op    5791.45 MB/s
```

> GT

```
BenchmarkCmpSerial64/dups_64K-10        7294        149883 ns/op    3497.99 MB/s
BenchmarkCmpSerial64/runs_64K-10        7125        151845 ns/op    3452.78 MB/s
BenchmarkCmpSerial64/seq_64K-10        13256         87773 ns/op    5973.21 MB/s
```

### Fusion Kernel - ARM64 M1

> EQ

```
BenchmarkCmpEQ64/dups_64K               7461        157320 ns/op    3332.63 MB/s
BenchmarkCmpEQ64/runs_64K               7388        157311 ns/op    3332.81 MB/s
BenchmarkCmpEQ64/seq_64K               23127         52495 ns/op    9987.46 MB/s
```

> GT

```
BenchmarkCmpGT64/dups_64K               7173        165769 ns/op    3162.76 MB/s
BenchmarkCmpGT64/runs_64K               7416        165585 ns/op    3166.28 MB/s
BenchmarkCmpGT64/seq_64K               21355         56303 ns/op    9311.98 MB/s
```

## Serial Execution - 12th Gen Intel(R) Core(TM) i9-12900K

> EQ

```
BenchmarkCmpSerial64/dups_64K-24        1570        693107 ns/op     756.43 MB/s
BenchmarkCmpSerial64/runs_64K-24        1575        679090 ns/op     772.05 MB/s
BenchmarkCmpSerial64/seq_64K-24         1983        558952 ns/op     937.98 MB/s
```

### Fusion Kernel - 12th Gen Intel(R) Core(TM) i9-12900K

> EQ

```
BenchmarkCmpEQ64/dups_64K-24            9537        122632 ns/op    4275.28 MB/s
BenchmarkCmpEQ64/runs_64K-24            9709        123244 ns/op    4254.05 MB/s
BenchmarkCmpEQ64/seq_64K-24            29298         41050 ns/op   12772.03 MB/s
```


### Simple8 Decode Time - ARM64 M1

```
BenchmarkDecodeUint64/dups_64K              9732        107095 ns/op    4895.52 MB/s
BenchmarkDecodeUint64/runs_64K             10000        106211 ns/op    4936.30 MB/s
BenchmarkDecodeUint64/seq_64K              28140         41868 ns/op    12522.28 MB/s
BenchmarkDecodeUint32/dups_64K             20815         56811 ns/op    4614.35 MB/s
BenchmarkDecodeUint32/runs_64K             21175         57197 ns/op    4583.19 MB/s
BenchmarkDecodeUint32/seq_64K              33072         36873 ns/op    7109.33 MB/s
BenchmarkDecodeUint16/dups_64K             20617         58908 ns/op    2225.02 MB/s
BenchmarkDecodeUint16/runs_64K             15421         79196 ns/op    1655.03 MB/s
BenchmarkDecodeUint16/seq_64K              34454         35046 ns/op    3740.02 MB/s
BenchmarkDecodeUint8/dups_64K              66428         17637 ns/op    3715.76 MB/s
BenchmarkDecodeUint8/runs_64K              31567         38347 ns/op    1709.03 MB/s
BenchmarkDecodeUint8/seq_64K               66066         18327 ns/op    3575.91 MB/s
```

### Simple8 Decode Time - 12th Gen Intel(R) Core(TM) i9-12900K (Generic)

```
BenchmarkDecodeUint64/dups_64K             14946             80052 ns/op        6549.31 MB/s
BenchmarkDecodeUint64/runs_64K             15097             79589 ns/op        6587.47 MB/s
BenchmarkDecodeUint64/seq_64K              41668             28938 ns/op        18117.65 MB/s
BenchmarkDecodeUint32/dups_64K             29150             41382 ns/op        6334.75 MB/s
BenchmarkDecodeUint32/runs_64K             28348             42271 ns/op        6201.47 MB/s
BenchmarkDecodeUint32/seq_64K              43662             27548 ns/op        9515.89 MB/s
BenchmarkDecodeUint16/dups_64K             28148             42608 ns/op        3076.21 MB/s
BenchmarkDecodeUint16/runs_64K             20505             58627 ns/op        2235.69 MB/s
BenchmarkDecodeUint16/seq_64K              42286             28431 ns/op        4610.20 MB/s
BenchmarkDecodeUint8/dups_64K              83570             14540 ns/op        4507.20 MB/s
BenchmarkDecodeUint8/runs_64K              38394             31093 ns/op        2107.72 MB/s
BenchmarkDecodeUint8/seq_64K               82243             14489 ns/op        4523.03 MB/s
```

### Simple8 Decode Time - 12th Gen Intel(R) Core(TM) i9-12900K (AVX2)

```
BenchmarkDecodeUint64/dups_64K             50029             23264 ns/op        22536.07 MB/s
BenchmarkDecodeUint64/runs_64K             51486             22992 ns/op        22802.71 MB/s
BenchmarkDecodeUint64/seq_64K             142801              8486 ns/op        61786.19 MB/s
BenchmarkDecodeUint32/dups_64K             92458             12738 ns/op        20580.29 MB/s
BenchmarkDecodeUint32/runs_64K             93169             12894 ns/op        20331.44 MB/s
BenchmarkDecodeUint32/seq_64K             136070              8862 ns/op        29581.24 MB/s
BenchmarkDecodeUint16/dups_64K             63378             19152 ns/op        6843.86 MB/s
BenchmarkDecodeUint16/runs_64K             35978             33546 ns/op        3907.18 MB/s
BenchmarkDecodeUint16/seq_64K             131856              9002 ns/op        14560.13 MB/s
BenchmarkDecodeUint8/dups_64K             342310              3481 ns/op        18826.63 MB/s
BenchmarkDecodeUint8/runs_64K              66674             17931 ns/op        3654.92 MB/s
BenchmarkDecodeUint8/seq_64K              197192              6118 ns/op        10711.95 MB/s
```

### Compare Time - 12th Gen Intel(R) Core(TM) i9-12900K (AVX2)

```
BenchmarkMatchUint64EqualAVX2/64K         271599              4421 ns/op        118603.46 MB/s
BenchmarkMatchUint32EqualAVX2/64K         505689              2333 ns/op        112371.35 MB/s
BenchmarkMatchUint16EqualAVX2/64K         707709              1602 ns/op        81839.03 MB/s
BenchmarkMatchUint8EqualAVX2/64K         1908936               621.2 ns/op      105503.69 MB/s
```