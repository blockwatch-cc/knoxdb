### Benchmark Cases

Benchmarks below are only for `Uint64Equal` and run against code from `d3f9115` (branch stefan_new_data_types) which is the last commit where the different optimization steps are implemented individually.

- `original` legacy version currently in production
- `unorderd` non-sequential memory access order
- `perm` save VPERMD operations
- `perm-loop` less VPERMD ops, negative CX-based loop instead of LEAQ
- `perm-loop-unroll` less VPERMD, CX loop, and loop unrolling with 64bit output writes
- `final` current state at `0316b33` (branch stefan_new_data_types)


### Intel Core i7 4870HQ (MacBookPro 11,5)

- 2.5 GHz, DDR3-1600, Memory Bandwidth  25.6 GB/s
- https://ark.intel.com/content/www/us/en/ark/products/83504/intel-core-i7-4870hq-processor-6m-cache-up-to-3-70-ghz.html

*AVX2*
```
benchstat original.txt  perm-loop-unroll.txt

name                       old time/op    new time/op    delta
MatchUint64EqualAVX2/128     19.3ns ± 6%    15.7ns ± 5%  -18.97%  (p=0.000 n=10+9)
MatchUint64EqualAVX2/1K       148ns ± 9%     101ns ± 3%  -32.03%  (p=0.000 n=9+10)
MatchUint64EqualAVX2/16K     2.59µs ±12%    1.92µs ± 3%  -25.82%  (p=0.000 n=9+10)
MatchUint64EqualAVX2/64K     14.0µs ± 5%    14.2µs ± 5%     ~     (p=0.305 n=10+10)
MatchUint64EqualAVX2/128K    26.6µs ± 3%    27.5µs ±18%     ~     (p=0.971 n=10+10)
MatchUint64EqualAVX2/1M       338µs ± 9%     414µs ±34%  +22.50%  (p=0.035 n=10+10)
MatchUint64EqualAVX2/128M    83.8ms ± 9%   146.3ms ±36%  +74.46%  (p=0.000 n=10+9)

name                       old speed      new speed      delta
MatchUint64EqualAVX2/128   53.1GB/s ± 6%  65.4GB/s ± 5%  +23.30%  (p=0.000 n=10+9)
MatchUint64EqualAVX2/1K    54.5GB/s ±16%  81.5GB/s ± 3%  +49.46%  (p=0.000 n=10+10)
MatchUint64EqualAVX2/16K   50.7GB/s ±11%  68.2GB/s ± 3%  +34.54%  (p=0.000 n=9+10)
MatchUint64EqualAVX2/64K   37.5GB/s ± 5%  37.0GB/s ± 5%     ~     (p=0.315 n=10+10)
MatchUint64EqualAVX2/128K  39.4GB/s ± 2%  38.3GB/s ±15%     ~     (p=0.971 n=10+10)
MatchUint64EqualAVX2/1M    24.9GB/s ±10%  21.2GB/s ±35%  -14.95%  (p=0.035 n=10+10)
MatchUint64EqualAVX2/128M  12.8GB/s ± 9%   7.2GB/s ±49%  -43.94%  (p=0.000 n=10+10)
```

### XEON-E-2176G (play.bwd.cx)

- 3.70GHz, DDR4-2666, Mem Bandwidth 41.6 GB/s
- https://ark.intel.com/content/www/us/en/ark/products/134860/intel-xeon-e-2176g-processor-12m-cache-up-to-4-70-ghz.html

*AVX2*
```
benchstat original.txt  perm-loop-unroll.txt

name                       old time/op    new time/op     delta
MatchUint64EqualAVX2/128     12.1ns ± 1%     10.0ns ± 2%  -17.14%  (p=0.000 n=10+10)
MatchUint64EqualAVX2/1K      87.2ns ± 1%     65.4ns ± 1%  -24.97%  (p=0.000 n=10+8)
MatchUint64EqualAVX2/16K     1.64µs ± 1%     1.25µs ± 2%  -23.93%  (p=0.000 n=10+10)
MatchUint64EqualAVX2/64K     8.02µs ± 1%     7.40µs ± 1%   -7.70%  (p=0.000 n=10+10)
MatchUint64EqualAVX2/128K    16.3µs ± 1%     15.5µs ± 1%   -5.00%  (p=0.000 n=10+9)
MatchUint64EqualAVX2/1M       170µs ± 1%      159µs ± 1%   -6.12%  (p=0.000 n=10+10)
MatchUint64EqualAVX2/128M    49.1ms ± 1%     47.9ms ± 0%   -2.46%  (p=0.000 n=10+9)

name                       old speed      new speed       delta
MatchUint64EqualAVX2/128   85.0GB/s ± 1%  102.5GB/s ± 2%  +20.69%  (p=0.000 n=10+10)
MatchUint64EqualAVX2/1K    93.9GB/s ± 1%  125.2GB/s ± 1%  +33.27%  (p=0.000 n=10+8)
MatchUint64EqualAVX2/16K   80.0GB/s ± 1%  105.2GB/s ± 2%  +31.46%  (p=0.000 n=10+10)
MatchUint64EqualAVX2/64K   65.4GB/s ± 1%   70.8GB/s ± 1%   +8.34%  (p=0.000 n=10+10)
MatchUint64EqualAVX2/128K  64.2GB/s ± 1%   67.6GB/s ± 1%   +5.27%  (p=0.000 n=10+9)
MatchUint64EqualAVX2/1M    49.5GB/s ± 1%   52.7GB/s ± 1%   +6.52%  (p=0.000 n=10+10)
MatchUint64EqualAVX2/128M  21.9GB/s ± 1%   22.4GB/s ± 0%   +2.52%  (p=0.000 n=10+9)
```


### XEON-W-2145 (btc1.bwd.cx)

- 3.70GHz, DDR4-2666, Mem Bandwidth 85.3 GB/s
- https://ark.intel.com/content/www/us/en/ark/products/126707/intel-xeon-w-2145-processor-11m-cache-3-70-ghz.html

*AVX2*
```
benchstat original.txt  perm-loop-unroll.txt

name                       old time/op    new time/op     delta
MatchUint64EqualAVX2/128     12.1ns ± 0%     10.0ns ± 0%  -17.58%  (p=0.000 n=10+9)
MatchUint64EqualAVX2/1K      87.9ns ± 0%     65.9ns ± 0%  -25.03%  (p=0.000 n=9+9)
MatchUint64EqualAVX2/16K     1.50µs ± 0%     1.12µs ± 0%  -25.20%  (p=0.000 n=10+10)
MatchUint64EqualAVX2/64K     5.98µs ± 0%     4.46µs ± 0%  -25.48%  (p=0.000 n=10+9)
MatchUint64EqualAVX2/128K    21.1µs ± 0%     18.8µs ± 0%  -10.65%  (p=0.000 n=9+10)
MatchUint64EqualAVX2/1M       324µs ± 0%      330µs ± 1%   +1.85%  (p=0.000 n=9+10)
MatchUint64EqualAVX2/128M    75.3ms ± 0%     73.9ms ± 0%   -1.86%  (p=0.000 n=9+9)

name                       old speed      new speed       delta
MatchUint64EqualAVX2/128   84.7GB/s ± 0%  102.6GB/s ± 1%  +21.12%  (p=0.000 n=10+10)
MatchUint64EqualAVX2/1K    93.2GB/s ± 0%  124.3GB/s ± 0%  +33.41%  (p=0.000 n=9+9)
MatchUint64EqualAVX2/16K   87.3GB/s ± 0%  116.8GB/s ± 0%  +33.69%  (p=0.000 n=10+10)
MatchUint64EqualAVX2/64K   87.6GB/s ± 0%  117.6GB/s ± 0%  +34.19%  (p=0.000 n=10+9)
MatchUint64EqualAVX2/128K  49.8GB/s ± 0%   55.7GB/s ± 0%  +11.92%  (p=0.000 n=9+10)
MatchUint64EqualAVX2/1M    25.9GB/s ± 0%   25.4GB/s ± 1%   -1.82%  (p=0.000 n=9+10)
MatchUint64EqualAVX2/128M  14.3GB/s ± 0%   14.5GB/s ± 0%   +1.89%  (p=0.000 n=9+9)
```

*AVX512*

Note: for comparison reason we renamed the benchmark function to match that of the original AVX2 implementation

```
benchstat original.txt  avx512.txt

name                       old time/op    new time/op    delta
MatchUint64EqualAVX2/128     12.1ns ± 0%    11.7ns ± 0%  -3.31%  (p=0.000 n=10+8)
MatchUint64EqualAVX2/1K      87.9ns ± 0%    93.0ns ± 0%  +5.78%  (p=0.000 n=9+9)
MatchUint64EqualAVX2/16K     1.50µs ± 0%    1.50µs ± 0%  -0.25%  (p=0.017 n=10+10)
MatchUint64EqualAVX2/64K     5.98µs ± 0%    5.95µs ± 0%  -0.51%  (p=0.000 n=10+8)
MatchUint64EqualAVX2/128K    21.1µs ± 0%    20.6µs ± 0%  -2.43%  (p=0.000 n=9+9)
MatchUint64EqualAVX2/1M       324µs ± 0%     335µs ± 0%  +3.45%  (p=0.000 n=9+10)
MatchUint64EqualAVX2/128M    75.3ms ± 0%    73.9ms ± 0%  -1.79%  (p=0.000 n=9+9)

name                       old speed      new speed      delta
MatchUint64EqualAVX2/128   84.7GB/s ± 0%  87.8GB/s ± 0%  +3.71%  (p=0.000 n=10+9)
MatchUint64EqualAVX2/1K    93.2GB/s ± 0%  88.1GB/s ± 0%  -5.49%  (p=0.000 n=9+10)
MatchUint64EqualAVX2/16K   87.3GB/s ± 0%  87.6GB/s ± 0%  +0.25%  (p=0.011 n=10+10)
MatchUint64EqualAVX2/64K   87.6GB/s ± 0%  88.1GB/s ± 0%  +0.51%  (p=0.000 n=10+8)
MatchUint64EqualAVX2/128K  49.8GB/s ± 0%  51.0GB/s ± 0%  +2.49%  (p=0.000 n=9+9)
MatchUint64EqualAVX2/1M    25.9GB/s ± 0%  25.1GB/s ± 0%  -3.34%  (p=0.000 n=9+10)
MatchUint64EqualAVX2/128M  14.3GB/s ± 0%  14.5GB/s ± 0%  +1.82%  (p=0.000 n=9+9)
```