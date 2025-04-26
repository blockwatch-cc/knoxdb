# Bitpack Benchmarks

## Apple M1 Max

BenchmarkEncode/u8/dups_64k         18484 ns/op    3545.63 MB/s      3.546 vals/ns
BenchmarkEncode/u8/runs_64k         18025 ns/op    3635.81 MB/s      3.636 vals/ns
BenchmarkEncode/u8/seq_64k          17737 ns/op    3694.84 MB/s      3.695 vals/ns
BenchmarkEncode/u16/dups_64k        17945 ns/op    7304.28 MB/s      3.652 vals/ns
BenchmarkEncode/u16/runs_64k        17827 ns/op    7352.42 MB/s      3.676 vals/ns
BenchmarkEncode/u16/seq_64k         17948 ns/op    7302.73 MB/s      3.651 vals/ns
BenchmarkEncode/u32/dups_64k        15389 ns/op    17034.41 MB/s     4.259 vals/ns
BenchmarkEncode/u32/runs_64k        15231 ns/op    17211.39 MB/s     4.303 vals/ns
BenchmarkEncode/u32/seq_64k         11419 ns/op    22957.10 MB/s     5.739 vals/ns
BenchmarkEncode/u64/dups_64k        15781 ns/op    33222.59 MB/s     4.153 vals/ns
BenchmarkEncode/u64/runs_64k        15613 ns/op    33580.60 MB/s     4.198 vals/ns
BenchmarkEncode/u64/seq_64k         12601 ns/op    41606.24 MB/s     5.201 vals/ns

BenchmarkDecode/u8/dups_64k         18175 ns/op    3605.86 MB/s      3.606 vals/ns
BenchmarkDecode/u8/runs_64k         18159 ns/op    3608.94 MB/s      3.609 vals/ns
BenchmarkDecode/u8/seq_64k          18127 ns/op    3615.43 MB/s      3.615 vals/ns
BenchmarkDecode/u16/dups_64k        18173 ns/op    7212.39 MB/s      3.606 vals/ns
BenchmarkDecode/u16/runs_64k        18000 ns/op    7281.58 MB/s      3.641 vals/ns
BenchmarkDecode/u16/seq_64k         17956 ns/op    7299.68 MB/s      3.650 vals/ns
BenchmarkDecode/u32/dups_64k        15281 ns/op    17154.56 MB/s     4.289 vals/ns
BenchmarkDecode/u32/runs_64k        15270 ns/op    17167.20 MB/s     4.292 vals/ns
BenchmarkDecode/u32/seq_64k         14403 ns/op    18200.22 MB/s     4.550 vals/ns
BenchmarkDecode/u64/dups_64k        17802 ns/op    29450.90 MB/s     3.681 vals/ns
BenchmarkDecode/u64/runs_64k        17920 ns/op    29257.59 MB/s     3.657 vals/ns
BenchmarkDecode/u64/seq_64k         16548 ns/op    31683.46 MB/s     3.960 vals/ns

BenchmarkCmpEqual/u64/dups_64k      30014 ns/op    17468.02 MB/s     2.184 vals/ns
BenchmarkCmpEqual/u64/runs_64k      29981 ns/op    17487.07 MB/s     2.186 vals/ns
BenchmarkCmpEqual/u64/seq_64k       28872 ns/op    18159.30 MB/s     2.270 vals/ns
BenchmarkCmpEqual/u32/dups_64k      30014 ns/op    8734.09 MB/s      2.184 vals/ns
BenchmarkCmpEqual/u32/runs_64k      29982 ns/op    8743.25 MB/s      2.186 vals/ns
BenchmarkCmpEqual/u32/seq_64k       29095 ns/op    9009.96 MB/s      2.252 vals/ns
BenchmarkCmpEqual/u16/dups_64k      30048 ns/op    4362.12 MB/s      2.181 vals/ns
BenchmarkCmpEqual/u16/runs_64k      28915 ns/op    4533.02 MB/s      2.267 vals/ns
BenchmarkCmpEqual/u16/seq_64k       28878 ns/op    4538.75 MB/s      2.269 vals/ns
BenchmarkCmpEqual/u8/dups_64k       27804 ns/op    2357.10 MB/s      2.357 vals/ns
BenchmarkCmpEqual/u8/runs_64k       28161 ns/op    2327.23 MB/s      2.327 vals/ns
BenchmarkCmpEqual/u8/seq_64k        28205 ns/op    2323.53 MB/s      2.324 vals/ns

## Intel i9-12900K

BenchmarkEncode/u8/dups_64k         13581 ns/op    4825.54 MB/s      4.826 vals/ns
BenchmarkEncode/u8/runs_64k         13479 ns/op    4862.03 MB/s      4.862 vals/ns
BenchmarkEncode/u8/seq_64k          13504 ns/op    4852.93 MB/s      4.853 vals/ns
BenchmarkEncode/u16/dups_64k        13505 ns/op    9705.13 MB/s      4.853 vals/ns
BenchmarkEncode/u16/runs_64k        13498 ns/op    9710.47 MB/s      4.855 vals/ns
BenchmarkEncode/u16/seq_64k         13565 ns/op    9662.80 MB/s      4.831 vals/ns
BenchmarkEncode/u32/dups_64k        12881 ns/op    20350.44 MB/s     5.088 vals/ns
BenchmarkEncode/u32/runs_64k        12986 ns/op    20185.97 MB/s     5.046 vals/ns
BenchmarkEncode/u32/seq_64k          9284 ns/op    28234.59 MB/s     7.059 vals/ns
BenchmarkEncode/u64/dups_64k        13783 ns/op    38039.36 MB/s     4.755 vals/ns
BenchmarkEncode/u64/runs_64k        13674 ns/op    38340.93 MB/s     4.793 vals/ns
BenchmarkEncode/u64/seq_64k          9919 ns/op    52856.66 MB/s     6.607 vals/ns

BenchmarkDecode/u8/dups_64k         13563 ns/op    4832.10 MB/s      4.832 vals/ns
BenchmarkDecode/u8/runs_64k         13557 ns/op    4833.98 MB/s      4.834 vals/ns
BenchmarkDecode/u8/seq_64k          13498 ns/op    4855.27 MB/s      4.855 vals/ns
BenchmarkDecode/u16/dups_64k        13555 ns/op    9669.60 MB/s      4.835 vals/ns
BenchmarkDecode/u16/runs_64k        13580 ns/op    9651.72 MB/s      4.826 vals/ns
BenchmarkDecode/u16/seq_64k         13595 ns/op    9641.50 MB/s      4.821 vals/ns
BenchmarkDecode/u32/dups_64k        13127 ns/op    19969.69 MB/s     4.992 vals/ns
BenchmarkDecode/u32/runs_64k        13192 ns/op    19870.86 MB/s     4.968 vals/ns
BenchmarkDecode/u32/seq_64k         10767 ns/op    24345.97 MB/s     6.086 vals/ns
BenchmarkDecode/u64/dups_64k        13482 ns/op    38888.32 MB/s     4.861 vals/ns
BenchmarkDecode/u64/runs_64k        13226 ns/op    39640.28 MB/s     4.955 vals/ns
BenchmarkDecode/u64/seq_64k         12745 ns/op    41137.30 MB/s     5.142 vals/ns

BenchmarkCmpEqual/u64/dups_64k      26244 ns/op    19977.11 MB/s     2.497 vals/ns
BenchmarkCmpEqual/u64/runs_64k      25342 ns/op    20688.70 MB/s     2.586 vals/ns
BenchmarkCmpEqual/u64/seq_64k       23084 ns/op    22711.82 MB/s     2.839 vals/ns
BenchmarkCmpEqual/u32/dups_64k      25468 ns/op    10292.88 MB/s     2.573 vals/ns
BenchmarkCmpEqual/u32/runs_64k      25240 ns/op    10386.05 MB/s     2.597 vals/ns
BenchmarkCmpEqual/u32/seq_64k       22938 ns/op    11428.58 MB/s     2.857 vals/ns
BenchmarkCmpEqual/u16/dups_64k      22848 ns/op    5736.67 MB/s      2.868 vals/ns
BenchmarkCmpEqual/u16/runs_64k      22969 ns/op    5706.57 MB/s      2.853 vals/ns
BenchmarkCmpEqual/u16/seq_64k       22865 ns/op    5732.44 MB/s      2.866 vals/ns
BenchmarkCmpEqual/u8/dups_64k       23214 ns/op    2823.09 MB/s      2.823 vals/ns
BenchmarkCmpEqual/u8/runs_64k       23219 ns/op    2822.53 MB/s      2.823 vals/ns
BenchmarkCmpEqual/u8/seq_64k        23319 ns/op    2810.46 MB/s      2.810 vals/ns
