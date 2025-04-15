# Hashtable Dictionary Benchmarks

## Generic (pure-Go) Version

```
cpu: Apple M1 Max
BenchmarkDictGeneric/uint64/1k/D1              36453         31942 ns/op     256.46 MB/s        2144 B/op          5 allocs/op
BenchmarkDictGeneric/uint64/16k/D1             18954         64031 ns/op    2047.01 MB/s        2144 B/op          5 allocs/op
BenchmarkDictGeneric/uint64/64k/D1              7104        167399 ns/op    3131.97 MB/s        2144 B/op          5 allocs/op
BenchmarkDictGeneric/uint64/1k/D16             22489         52165 ns/op     157.04 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/16k/D16             2732        447448 ns/op     292.93 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/64k/D16             1209       1010081 ns/op     519.06 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/1k/D48             23786         50292 ns/op     162.89 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/16k/D48             2785        420047 ns/op     312.04 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/64k/D48              579       2093503 ns/op     250.44 MB/s          96 B/op          4 allocs/op

BenchmarkDictGeneric/uint32/1k/D1              37065         32077 ns/op     127.69 MB/s        1120 B/op          5 allocs/op
BenchmarkDictGeneric/uint32/16k/D1             19038         63798 ns/op    1027.25 MB/s        1120 B/op          5 allocs/op
BenchmarkDictGeneric/uint32/64k/D1              7144        163920 ns/op    1599.22 MB/s        1120 B/op          5 allocs/op
BenchmarkDictGeneric/uint32/1k/D16             28188         42425 ns/op      96.55 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/16k/D16             2401        494437 ns/op     132.55 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/64k/D16             1046       1151233 ns/op     227.71 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/1k/D48             28066         42664 ns/op      96.01 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/16k/D48             2438        486101 ns/op     134.82 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/64k/D48              518       2351323 ns/op     111.49 MB/s          96 B/op          4 allocs/op


BenchmarkDictGeneric/float64/1k/D1             34944         34338 ns/op     238.57 MB/s        2144 B/op          5 allocs/op
BenchmarkDictGeneric/float64/16k/D1            18106         66114 ns/op    1982.52 MB/s        2144 B/op          5 allocs/op
BenchmarkDictGeneric/float64/64k/D1             7108        166634 ns/op    3146.34 MB/s        2144 B/op          5 allocs/op
BenchmarkDictGeneric/float64/1k/D16            23962         50945 ns/op     160.80 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/float64/16k/D16            2132        557653 ns/op     235.04 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/float64/64k/D16            1014       1140889 ns/op     459.54 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/float64/1k/D48            24760         51161 ns/op     160.12 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/float64/16k/D48            2152        568922 ns/op     230.39 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/float64/64k/D48             478       2418275 ns/op     216.80 MB/s          96 B/op          4 allocs/op

BenchmarkDictGeneric/float32/1k/D1             35098         33947 ns/op     120.66 MB/s        1120 B/op          5 allocs/op
BenchmarkDictGeneric/float32/16k/D1            18163         66367 ns/op     987.47 MB/s        1120 B/op          5 allocs/op
BenchmarkDictGeneric/float32/64k/D1             6907        172701 ns/op    1517.91 MB/s        1120 B/op          5 allocs/op
BenchmarkDictGeneric/float32/1k/D16            24362         48109 ns/op      85.14 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/float32/16k/D16            6198        190254 ns/op     344.47 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/float32/64k/D16            3896        305711 ns/op     857.49 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/float32/1k/D48            24718         48724 ns/op      84.06 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/float32/16k/D48            6220        194226 ns/op     337.42 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/float32/64k/D48            3862        313398 ns/op     836.46 MB/s          96 B/op          4 allocs/op
```


## AVX2 Version

```
cpu: 12th Gen Intel(R) Core(TM) i9-12900K
BenchmarkDictAVX2/uint64/1k/D1            174753          6691 ns/op    1224.37 MB/s        2144 B/op          5 allocs/op
BenchmarkDictAVX2/uint64/16k/D1            51355         22854 ns/op    5735.16 MB/s        2144 B/op          5 allocs/op
BenchmarkDictAVX2/uint64/64k/D1            17187         70028 ns/op    7486.86 MB/s        2144 B/op          5 allocs/op
BenchmarkDictAVX2/uint64/1k/D16            77365         15539 ns/op     527.20 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/16k/D16            4072        292257 ns/op     448.48 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/64k/D16            1808        675442 ns/op     776.21 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/1k/D48            80241         14868 ns/op     550.98 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/16k/D48            4030        289086 ns/op     453.40 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/64k/D48             766       1573409 ns/op     333.22 MB/s         107 B/op          4 allocs/op

BenchmarkDictAVX2/float64/1k/D1               151064          7626 ns/op    1074.24 MB/s        2144 B/op          5 allocs/op
BenchmarkDictAVX2/float64/16k/D1               48938         24650 ns/op    5317.28 MB/s        2144 B/op          5 allocs/op
BenchmarkDictAVX2/float64/64k/D1               17040         70764 ns/op    7408.97 MB/s        2144 B/op          5 allocs/op
BenchmarkDictAVX2/float64/1k/D16               87550         13852 ns/op     591.41 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/float64/16k/D16               3019        388055 ns/op     337.77 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/float64/64k/D16               1490        811060 ns/op     646.42 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/float64/1k/D48               86985         13796 ns/op     593.82 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/float64/16k/D48               3003        388390 ns/op     337.48 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/float64/64k/D48                662       1800629 ns/op     291.17 MB/s          96 B/op          4 allocs/op

BenchmarkDictAVX2/uint32/1k/D1            185787          6266 ns/op     653.70 MB/s        1120 B/op          5 allocs/op
BenchmarkDictAVX2/uint32/16k/D1            60964         19697 ns/op    3327.18 MB/s        1120 B/op          5 allocs/op
BenchmarkDictAVX2/uint32/64k/D1            19131         63106 ns/op    4154.04 MB/s        1120 B/op          5 allocs/op
BenchmarkDictAVX2/uint32/1k/D16           118008         10116 ns/op     404.92 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/16k/D16            3592        334225 ns/op     196.08 MB/s          98 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/64k/D16            1524        793409 ns/op     330.40 MB/s         101 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/1k/D48           116743         10334 ns/op     396.35 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/16k/D48            3555        337945 ns/op     193.93 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/64k/D48             745       1615901 ns/op     162.23 MB/s          96 B/op          4 allocs/op

BenchmarkDictAVX2/float32/1k/D1               164422          7322 ns/op     559.42 MB/s        1120 B/op          5 allocs/op
BenchmarkDictAVX2/float32/16k/D1               57884         20907 ns/op    3134.62 MB/s        1120 B/op          5 allocs/op
BenchmarkDictAVX2/float32/64k/D1               18808         63703 ns/op    4115.11 MB/s        1120 B/op          5 allocs/op
BenchmarkDictAVX2/float32/1k/D16               85557         13846 ns/op     295.83 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/float32/16k/D16              10000        103263 ns/op     634.65 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/float32/64k/D16               6855        166097 ns/op    1578.26 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/float32/1k/D48               86659         13917 ns/op     294.32 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/float32/16k/D48              10000        102038 ns/op     642.27 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/float32/64k/D48               7066        167591 ns/op    1564.19 MB/s          96 B/op          4 allocs/op
```
