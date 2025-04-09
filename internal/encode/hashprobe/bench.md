# Hashtable Dictionary Benchmarks

## Generic (pure-Go) Version

```
cpu: Apple M1 Max
BenchmarkDictGeneric/uint64/1k/D1-10               36535         31900 ns/op     256.81 MB/s        1140 B/op          5 allocs/op
BenchmarkDictGeneric/uint64/16k/D1-10              19576         61728 ns/op    2123.37 MB/s        1206 B/op          5 allocs/op
BenchmarkDictGeneric/uint64/64k/D1-10               8208        146151 ns/op    3587.29 MB/s        1217 B/op          5 allocs/op
BenchmarkDictGeneric/uint64/1k/D2-10               23702         50349 ns/op     162.71 MB/s         124 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/16k/D2-10               8400        135564 ns/op     966.87 MB/s         180 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/64k/D2-10               4719        250481 ns/op    2093.12 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/1k/D8-10               24324         49075 ns/op     166.93 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/16k/D8-10               3813        310614 ns/op     421.98 MB/s         296 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/64k/D8-10               2296        519292 ns/op    1009.62 MB/s          97 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/1k/D16-10              23296         52215 ns/op     156.89 MB/s         125 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/16k/D16-10              2906        411024 ns/op     318.89 MB/s         379 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/64k/D16-10              1353        896927 ns/op     584.54 MB/s         777 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/1k/D32-10              23680         50591 ns/op     161.93 MB/s         152 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/16k/D32-10              2870        416445 ns/op     314.74 MB/s          97 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/64k/D32-10               764       1570022 ns/op     333.94 MB/s         101 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/1k/D48-10              24116         49963 ns/op     163.96 MB/s         124 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/16k/D48-10              2774        410939 ns/op     318.96 MB/s         404 B/op          4 allocs/op
BenchmarkDictGeneric/uint64/64k/D48-10               606       1993550 ns/op     262.99 MB/s         103 B/op          4 allocs/op

BenchmarkDictGeneric/uint32/1k/D1-10               37964         31998 ns/op     256.02 MB/s         630 B/op          5 allocs/op
BenchmarkDictGeneric/uint32/16k/D1-10              18769         64397 ns/op    2035.37 MB/s         640 B/op          5 allocs/op
BenchmarkDictGeneric/uint32/64k/D1-10               7170        165516 ns/op    3167.60 MB/s         700 B/op          5 allocs/op
BenchmarkDictGeneric/uint32/1k/D2-10               28032         42127 ns/op     194.46 MB/s         110 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/16k/D2-10              10000        105583 ns/op    1241.42 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/64k/D2-10               5122        227681 ns/op    2302.73 MB/s         202 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/1k/D8-10               28830         41636 ns/op     196.75 MB/s          96 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/16k/D8-10               3812        319196 ns/op     410.63 MB/s          97 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/64k/D8-10               2178        550520 ns/op     952.35 MB/s         353 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/1k/D16-10              28402         41890 ns/op     195.56 MB/s         110 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/16k/D16-10              2478        475897 ns/op     275.42 MB/s          97 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/64k/D16-10              1065       1096897 ns/op     477.97 MB/s         100 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/1k/D32-10              28318         41776 ns/op     196.09 MB/s         110 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/16k/D32-10              2433        472607 ns/op     277.34 MB/s         299 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/64k/D32-10               669       1793931 ns/op     292.26 MB/s        1082 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/1k/D48-10              28213         41582 ns/op     197.01 MB/s         110 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/16k/D48-10              2236        478707 ns/op     273.80 MB/s          97 B/op          4 allocs/op
BenchmarkDictGeneric/uint32/64k/D48-10               500       2205874 ns/op     237.68 MB/s        1956 B/op          4 allocs/op
```


## AVX2 Version

```
cpu: 12th Gen Intel(R) Core(TM) i9-12900K
BenchmarkDictAVX2/uint64/1k/D1            184426          6399 ns/op    1280.23 MB/s        1376 B/op          5 allocs/op
BenchmarkDictAVX2/uint64/16k/D1            53194         20865 ns/op    6281.88 MB/s        1376 B/op          5 allocs/op
BenchmarkDictAVX2/uint64/64k/D1            17896         66931 ns/op    7833.21 MB/s        1376 B/op          5 allocs/op
BenchmarkDictAVX2/uint64/1k/D2             96931         12435 ns/op     658.76 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/16k/D2            16316         73626 ns/op    1780.23 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/64k/D2             7965        149037 ns/op    3517.84 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/1k/D8             96692         12334 ns/op     664.20 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/16k/D8             5631        215189 ns/op     609.10 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/64k/D8             3122        385771 ns/op    1359.06 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/1k/D16            96603         12562 ns/op     652.13 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/16k/D16            4048        294988 ns/op     444.33 MB/s          98 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/64k/D16            1803        668982 ns/op     783.71 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/1k/D32            94488         12507 ns/op     655.00 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/16k/D32            4100        290150 ns/op     451.74 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/64k/D32            1011       1178346 ns/op     444.94 MB/s         104 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/1k/D48            98282         12358 ns/op     662.91 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/16k/D48            4045        296885 ns/op     441.49 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint64/64k/D48             770       1570933 ns/op     333.74 MB/s          96 B/op          4 allocs/op

BenchmarkDictAVX2/uint32/1k/D1            197240          6081 ns/op    1347.07 MB/s         736 B/op          5 allocs/op
BenchmarkDictAVX2/uint32/16k/D1            56810         21168 ns/op    6192.08 MB/s         736 B/op          5 allocs/op
BenchmarkDictAVX2/uint32/64k/D1            19627         60562 ns/op    8656.97 MB/s         736 B/op          5 allocs/op
BenchmarkDictAVX2/uint32/1k/D2            118983         10162 ns/op     806.14 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/16k/D2            29560         40310 ns/op    3251.60 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/64k/D2            10000        116034 ns/op    4518.42 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/1k/D8            116616         10469 ns/op     782.49 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/16k/D8             5386        216216 ns/op     606.21 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/64k/D8             3072        388124 ns/op    1350.83 MB/s          99 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/1k/D16           117637         10251 ns/op     799.16 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/16k/D16            3452        347573 ns/op     377.11 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/64k/D16            1516        796192 ns/op     658.49 MB/s         101 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/1k/D32           115320         10211 ns/op     802.24 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/16k/D32            3481        343694 ns/op     381.36 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/64k/D32             931       1307386 ns/op     401.02 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/1k/D48           118647         10176 ns/op     805.05 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/16k/D48            3476        342003 ns/op     383.25 MB/s          96 B/op          4 allocs/op
BenchmarkDictAVX2/uint32/64k/D48             736       1626660 ns/op     322.31 MB/s          96 B/op          4 allocs/op
```
