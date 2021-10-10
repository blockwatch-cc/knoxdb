// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// go test ./vec/... -bench=.
package vec

import (
	"testing"

	"blockwatch.cc/knoxdb/util"
)

type bitsetBenchmarkSize struct {
	name string
	l    int
}

var bitsetSizes = []int{
	7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33,
	63, 64, 65, 127,
	128,   // min AVX size
	129,   // AVX + 1bit
	160,   // AVX + i32
	161,   // AVX + i32 + 1
	255,   // AVX + i32 + 7
	256,   // 2x AVX
	257,   // 2x AVX + 1
	512,   // 4x AVX
	1024,  // 8x AVX
	2048,  // min AVX2 size
	2176,  // AVX2 + AVX size
	2208,  // AVX2 + AVX + i32 size
	2216,  // AVX2 + AVX + i32 + i8 size
	2217,  // AVX2 + AVX + i32 + i8 size + 1 bit
	4096,  // 2x AVX2
	4224,  // 2x AVX2 + AVX
	4256,  // 2x AVX2 + AVX + i32
	4264,  // 2x AVX2 + AVX + i32 +i8
	4265,  // 2x AVX2 + AVX + i32 +i8 + 1 bit
	8192,  // 4x AVX2
	16384, // 16x AVX2
}

var bitsetBenchmarkSizes = []bitsetBenchmarkSize{
	// AVX2 multiples
	//  {"32", 32},
	//  {"128", 128},
	// {"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"32K", 32 * 1024},
	{"64K", 64 * 1024},
	// {"128K", 128 * 1024},
	// {"1M", 1024 * 1024},
	//  {"16M", 16 * 1024 * 1024},
	// {"128M", 128 * 1024 * 1024},

	// not multiples of AVX2 size
	// {"1K-8", 1*1024 - 8},
	// {"16K-8", 16*1024 - 8},
	// {"32K-8", 32*1024 - 8},
	// {"64K-8", 64*1024 - 8},
	// {"128K-8", 128*1024 - 8},
	// {"1M-8", 1024*1024 - 8},
	// {"128M-8", 128*1024*1024 - 8},
}

type bitsetBenchmarkDensity struct {
	name string
	d    float64
}

var bitsetBenchmarkDensities = []bitsetBenchmarkDensity{
	{"1/2", 1.0 / 2},
	// {"1/4", 1.0 / 4},
	// {"1/8", 1.0 / 8},
	{"1/16", 1.0 / 16},
	{"1/32", 1.0 / 32},
	{"1/64", 1.0 / 64},
	{"1/128", 1.0 / 128},
	// {"1/256", 1.0 / 256},
	// {"1/512", 1.0 / 512},
	{"1/1024", 1.0 / 1024},
	// {"1/2048", 1.0 / 2048},
	// {"1/4096", 1.0 / 4096},
	// {"1/8192", 1.0 / 8192},
	{"1/16384", 1.0 / 16384},
}

// Bitset low-level benchmarks
//
// goos: darwin
// goarch: amd64
// pkg: blockwatch.cc/knoxdb/vec
// cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
// BenchmarkBitsetSwap/16K-8    238421696            5.047 ns/op    405820.01 MB/s
// BenchmarkBitsetSwap/32K-8    243715171            4.978 ns/op    822898.01 MB/s
// BenchmarkBitsetSwap/64K-8    241352004            4.971 ns/op    1648122.20 MB/s
func BenchmarkBitsetSwap(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			bs := NewBitsetFromBytes(bits, n.l)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bs.Swap(0, n.l/2)
			}
		})
	}
}

// BenchmarkBitsetSwapBool/16K-8  803336072          1.468 ns/op    1395204.93 MB/s
// BenchmarkBitsetSwapBool/32K-8  805347015          1.460 ns/op    2805483.41 MB/s
// BenchmarkBitsetSwapBool/64K-8  781899357          1.450 ns/op    5647949.71 MB/s
func BenchmarkBitsetSwapBool(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			bs := NewBitsetFromBytes(bits, n.l)
			slice := bs.Slice()
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				slice[0], slice[n.l/2] = slice[n.l/2], slice[0]
			}
		})
	}
}

// pkg: blockwatch.cc/knoxdb/vec
// cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
// BenchmarkBitsetIndexNative/16K-1/2-8         	   22941	     53661 ns/op	 152.66 MB/s
// BenchmarkBitsetIndexNative/16K-1/16-8        	   84627	     12920 ns/op	  79.26 MB/s
// BenchmarkBitsetIndexNative/16K-1/32-8        	  242636	      4991 ns/op	 102.58 MB/s
// BenchmarkBitsetIndexNative/16K-1/64-8        	  355519	      3391 ns/op	  75.49 MB/s
// BenchmarkBitsetIndexNative/16K-1/128-8       	  418766	      2947 ns/op	  43.43 MB/s
// BenchmarkBitsetIndexNative/16K-1/1024-8      	  585559	      2016 ns/op	   7.94 MB/s
// BenchmarkBitsetIndexNative/16K-1/16384-8     	  624786	      1804 ns/op	   0.55 MB/s
// BenchmarkBitsetIndexNative/32K-1/2-8         	   10000	    107867 ns/op	 151.89 MB/s
// BenchmarkBitsetIndexNative/32K-1/16-8        	   35779	     30630 ns/op	  66.86 MB/s
// BenchmarkBitsetIndexNative/32K-1/32-8        	   74311	     15636 ns/op	  65.49 MB/s
// BenchmarkBitsetIndexNative/32K-1/64-8        	  129874	      8849 ns/op	  57.86 MB/s
// BenchmarkBitsetIndexNative/32K-1/128-8       	  177238	      6292 ns/op	  40.69 MB/s
// BenchmarkBitsetIndexNative/32K-1/1024-8      	  299630	      4054 ns/op	   7.89 MB/s
// BenchmarkBitsetIndexNative/32K-1/16384-8     	  318415	      3553 ns/op	   0.56 MB/s
// BenchmarkBitsetIndexNative/64K-1/2-8         	    5515	    216143 ns/op	 151.60 MB/s
// BenchmarkBitsetIndexNative/64K-1/16-8        	   17809	     65765 ns/op	  62.28 MB/s
// BenchmarkBitsetIndexNative/64K-1/32-8        	   32431	     36286 ns/op	  56.44 MB/s
// BenchmarkBitsetIndexNative/64K-1/64-8        	   55150	     21153 ns/op	  48.41 MB/s
// BenchmarkBitsetIndexNative/64K-1/128-8       	   85554	     13804 ns/op	  37.09 MB/s
// BenchmarkBitsetIndexNative/64K-1/1024-8      	  143848	      7856 ns/op	   8.15 MB/s
// BenchmarkBitsetIndexNative/64K-1/16384-8     	  164778	      7107 ns/op	   0.56 MB/s
func BenchmarkBitsetIndexNative(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		for _, d := range bitsetBenchmarkDensities {
			B.Run(n.name+"-"+d.name, func(B *testing.B) {
				bits := fillBitsetRand(nil, n.l, d.d)
				cnt := int(bitsetPopCountGeneric(bits, n.l))
				slice := make([]int, cnt, n.l)
				bs := NewBitsetFromBytes(bits, n.l)
				// we count hits in a bitset instead of raw throughput
				B.SetBytes(int64(cnt))
				B.ResetTimer()
				for i := 0; i < B.N; i++ {
					_ = bs.Indexes(slice)
				}
			})
		}
	}
}

// pkg: blockwatch.cc/knoxdb/vec
// cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
// BenchmarkBitsetIndexOpt/16K-1/2-8         	  260499	      4598 ns/op	1781.74 MB/s
// BenchmarkBitsetIndexOpt/16K-1/16-8        	  241903	      4709 ns/op	 217.44 MB/s
// BenchmarkBitsetIndexOpt/16K-1/32-8        	  365690	      4026 ns/op	 127.17 MB/s
// BenchmarkBitsetIndexOpt/16K-1/64-8        	  706698	      1666 ns/op	 153.68 MB/s
// BenchmarkBitsetIndexOpt/16K-1/128-8       	 1288018	       921.1 ns/op	 138.96 MB/s
// BenchmarkBitsetIndexOpt/16K-1/1024-8      	 6835682	       170.8 ns/op	  93.70 MB/s
// BenchmarkBitsetIndexOpt/16K-1/16384-8     	16140313	        69.30 ns/op	  14.43 MB/s
// BenchmarkBitsetIndexOpt/32K-1/2-8         	  136348	      8548 ns/op	1916.71 MB/s
// BenchmarkBitsetIndexOpt/32K-1/16-8        	  111206	     11000 ns/op	 186.17 MB/s
// BenchmarkBitsetIndexOpt/32K-1/32-8        	  122942	      9127 ns/op	 112.20 MB/s
// BenchmarkBitsetIndexOpt/32K-1/64-8        	  333958	      3418 ns/op	 149.79 MB/s
// BenchmarkBitsetIndexOpt/32K-1/128-8       	  633484	      2034 ns/op	 125.84 MB/s
// BenchmarkBitsetIndexOpt/32K-1/1024-8      	 3702079	       363.3 ns/op	  88.09 MB/s
// BenchmarkBitsetIndexOpt/32K-1/16384-8     	 9283716	       114.6 ns/op	  17.46 MB/s
// BenchmarkBitsetIndexOpt/64K-1/2-8         	   68407	     17111 ns/op	1915.06 MB/s
// BenchmarkBitsetIndexOpt/64K-1/16-8        	   50427	     23687 ns/op	 172.92 MB/s
// BenchmarkBitsetIndexOpt/64K-1/32-8        	   66420	     17518 ns/op	 116.91 MB/s
// BenchmarkBitsetIndexOpt/64K-1/64-8        	  157280	      7695 ns/op	 133.08 MB/s
// BenchmarkBitsetIndexOpt/64K-1/128-8       	  286348	      3836 ns/op	 133.47 MB/s
// BenchmarkBitsetIndexOpt/64K-1/1024-8      	 1799337	       627.1 ns/op	 102.06 MB/s
// BenchmarkBitsetIndexOpt/64K-1/16384-8     	 5559492	       210.9 ns/op	  18.96 MB/s
func BenchmarkBitsetIndexOpt(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		for _, d := range bitsetBenchmarkDensities {
			B.Run(n.name+"-"+d.name, func(B *testing.B) {
				bits := fillBitsetRand(nil, n.l, d.d)
				cnt := int(bitsetPopCountGeneric(bits, n.l))
				slice := make([]uint32, cnt, n.l)
				bs := NewBitsetFromBytes(bits, n.l)
				// we count hits in a bitset instead of raw throughput
				B.SetBytes(int64(cnt))
				B.ResetTimer()
				for i := 0; i < B.N; i++ {
					_ = bs.IndexesU32(slice)
				}
			})
		}
	}
}

// goos: darwin
// goarch: amd64
// pkg: blockwatch.cc/knoxdb/vec
// cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
// BenchmarkBitsetIndexGeneric/16K-1/2-8       52915         23211 ns/op      88.23 MB/s
// BenchmarkBitsetIndexGeneric/16K-1/16-8     233978          5213 ns/op     392.87 MB/s
// BenchmarkBitsetIndexGeneric/16K-1/32-8     276736          4142 ns/op     494.42 MB/s
// BenchmarkBitsetIndexGeneric/16K-1/64-8     314340          3445 ns/op     594.50 MB/s
// BenchmarkBitsetIndexGeneric/16K-1/128-8    393903          3051 ns/op     671.27 MB/s
// BenchmarkBitsetIndexGeneric/16K-1/1024-8   569643          1970 ns/op    1039.79 MB/s
// BenchmarkBitsetIndexGeneric/16K-1/16384-8  606422          1801 ns/op    1137.40 MB/s
// BenchmarkBitsetIndexGeneric/32K-1/2-8       24706         47703 ns/op      85.87 MB/s
// BenchmarkBitsetIndexGeneric/32K-1/16-8      67032         17421 ns/op     235.12 MB/s
// BenchmarkBitsetIndexGeneric/32K-1/32-8     109290         10747 ns/op     381.12 MB/s
// BenchmarkBitsetIndexGeneric/32K-1/64-8     131457          9016 ns/op     454.32 MB/s
// BenchmarkBitsetIndexGeneric/32K-1/128-8    163376          6438 ns/op     636.21 MB/s
// BenchmarkBitsetIndexGeneric/32K-1/1024-8   287936          4115 ns/op     995.32 MB/s
// BenchmarkBitsetIndexGeneric/32K-1/16384-8  312508          3640 ns/op    1125.22 MB/s
// BenchmarkBitsetIndexGeneric/64K-1/2-8       12129         98919 ns/op      82.82 MB/s
// BenchmarkBitsetIndexGeneric/64K-1/16-8      28220         41932 ns/op     195.36 MB/s
// BenchmarkBitsetIndexGeneric/64K-1/32-8      47152         24668 ns/op     332.09 MB/s
// BenchmarkBitsetIndexGeneric/64K-1/64-8      68542         18157 ns/op     451.18 MB/s
// BenchmarkBitsetIndexGeneric/64K-1/128-8     93169         13109 ns/op     624.92 MB/s
// BenchmarkBitsetIndexGeneric/64K-1/1024-8   143715          7825 ns/op    1046.88 MB/s
// BenchmarkBitsetIndexGeneric/64K-1/16384-8  162100          7339 ns/op    1116.21 MB/s
func BenchmarkBitsetIndexGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		for _, d := range bitsetBenchmarkDensities {
			B.Run(n.name+"-"+d.name, func(B *testing.B) {
				bits := fillBitsetRand(nil, n.l, d.d)
				slice := make([]uint32, int(bitsetPopCountGeneric(bits, n.l)))
				B.ResetTimer()
				B.SetBytes(int64(bitFieldLen(n.l)))
				for i := 0; i < B.N; i++ {
					_ = bitsetIndexesGeneric(bits, n.l, slice)
				}
			})
		}
	}
}

// BenchmarkBitsetIndexGenericSkip16/16K-1/2-8       44601     26688 ns/op      76.74 MB/s
// BenchmarkBitsetIndexGenericSkip16/16K-1/16-8     177523      6302 ns/op     324.96 MB/s
// BenchmarkBitsetIndexGenericSkip16/16K-1/32-8     285123      3853 ns/op     531.50 MB/s
// BenchmarkBitsetIndexGenericSkip16/16K-1/64-8     399382      2610 ns/op     784.57 MB/s
// BenchmarkBitsetIndexGenericSkip16/16K-1/128-8    562926      2049 ns/op     999.61 MB/s
// BenchmarkBitsetIndexGenericSkip16/16K-1/1024-8   823992      1432 ns/op    1430.30 MB/s
// BenchmarkBitsetIndexGenericSkip16/16K-1/16384-8  960610      1220 ns/op    1678.08 MB/s
// BenchmarkBitsetIndexGenericSkip16/32K-1/2-8       21174     55921 ns/op      73.25 MB/s
// BenchmarkBitsetIndexGenericSkip16/32K-1/16-8      62199     19489 ns/op     210.17 MB/s
// BenchmarkBitsetIndexGenericSkip16/32K-1/32-8     113377     11279 ns/op     363.15 MB/s
// BenchmarkBitsetIndexGenericSkip16/32K-1/64-8     217669      5388 ns/op     760.18 MB/s
// BenchmarkBitsetIndexGenericSkip16/32K-1/128-8    272076      4281 ns/op     956.69 MB/s
// BenchmarkBitsetIndexGenericSkip16/32K-1/1024-8   394606      3001 ns/op    1364.79 MB/s
// BenchmarkBitsetIndexGenericSkip16/32K-1/16384-8  450474      2414 ns/op    1696.96 MB/s
// BenchmarkBitsetIndexGenericSkip16/64K-1/2-8        9114    120455 ns/op      68.01 MB/s
// BenchmarkBitsetIndexGenericSkip16/64K-1/16-8      25092     51552 ns/op     158.91 MB/s
// BenchmarkBitsetIndexGenericSkip16/64K-1/32-8      34672     30435 ns/op     269.17 MB/s
// BenchmarkBitsetIndexGenericSkip16/64K-1/64-8      77578     16271 ns/op     503.46 MB/s
// BenchmarkBitsetIndexGenericSkip16/64K-1/128-8    122650      9337 ns/op     877.39 MB/s
// BenchmarkBitsetIndexGenericSkip16/64K-1/1024-8   197580      5844 ns/op    1401.83 MB/s
// BenchmarkBitsetIndexGenericSkip16/64K-1/16384-8  243391      4738 ns/op    1729.15 MB/s
func BenchmarkBitsetIndexGenericSkip16(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		for _, d := range bitsetBenchmarkDensities {
			B.Run(n.name+"-"+d.name, func(B *testing.B) {
				bits := fillBitsetRand(nil, n.l, d.d)
				slice := make([]uint32, int(bitsetPopCountGeneric(bits, n.l)))
				B.ResetTimer()
				B.SetBytes(int64(bitFieldLen(n.l)))
				for i := 0; i < B.N; i++ {
					_ = bitsetIndexesGenericSkip16(bits, n.l, slice)
				}
			})
		}
	}
}

// BenchmarkBitsetIndexGenericSkip64/16K-1/2-8          49392     23878 ns/op      85.77 MB/s
// BenchmarkBitsetIndexGenericSkip64/16K-1/16-8        162763      7054 ns/op     290.34 MB/s
// BenchmarkBitsetIndexGenericSkip64/16K-1/32-8        301663      3703 ns/op     552.99 MB/s
// BenchmarkBitsetIndexGenericSkip64/16K-1/64-8        452002      2408 ns/op     850.55 MB/s
// BenchmarkBitsetIndexGenericSkip64/16K-1/128-8       787962      1469 ns/op    1394.62 MB/s
// BenchmarkBitsetIndexGenericSkip64/16K-1/1024-8     2184736       549.2 ns/op  3728.92 MB/s
// BenchmarkBitsetIndexGenericSkip64/16K-1/16384-8    3207848       364.6 ns/op  5616.95 MB/s
// BenchmarkBitsetIndexGenericSkip64/32K-1/2-8          21880     54722 ns/op      74.85 MB/s
// BenchmarkBitsetIndexGenericSkip64/32K-1/16-8         54285     21923 ns/op     186.83 MB/s
// BenchmarkBitsetIndexGenericSkip64/32K-1/32-8        108319     10516 ns/op     389.50 MB/s
// BenchmarkBitsetIndexGenericSkip64/32K-1/64-8        185266      5573 ns/op     734.98 MB/s
// BenchmarkBitsetIndexGenericSkip64/32K-1/128-8       350641      3227 ns/op    1269.46 MB/s
// BenchmarkBitsetIndexGenericSkip64/32K-1/1024-8     1000000      1065 ns/op    3845.40 MB/s
// BenchmarkBitsetIndexGenericSkip64/32K-1/16384-8    1665762       722.6 ns/op  5668.28 MB/s
// BenchmarkBitsetIndexGenericSkip64/64K-1/2-8          10000    113464 ns/op      72.20 MB/s
// BenchmarkBitsetIndexGenericSkip64/64K-1/16-8         23634     50005 ns/op     163.82 MB/s
// BenchmarkBitsetIndexGenericSkip64/64K-1/32-8         40422     28133 ns/op     291.19 MB/s
// BenchmarkBitsetIndexGenericSkip64/64K-1/64-8         81307     13879 ns/op     590.23 MB/s
// BenchmarkBitsetIndexGenericSkip64/64K-1/128-8       157500      7289 ns/op    1123.81 MB/s
// BenchmarkBitsetIndexGenericSkip64/64K-1/1024-8      525898      2185 ns/op    3748.92 MB/s
// BenchmarkBitsetIndexGenericSkip64/64K-1/16384-8     799963      1461 ns/op    5606.43 MB/s
func BenchmarkBitsetIndexGenericSkip64(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		for _, d := range bitsetBenchmarkDensities {
			B.Run(n.name+"-"+d.name, func(B *testing.B) {
				bits := fillBitsetRand(nil, n.l, d.d)
				slice := make([]uint32, int(bitsetPopCountGeneric(bits, n.l)))
				B.ResetTimer()
				B.SetBytes(int64(bitFieldLen(n.l)))
				for i := 0; i < B.N; i++ {
					_ = bitsetIndexesGenericSkip64(bits, n.l, slice)
				}
			})
		}
	}
}

// BenchmarkBitsetIndexAVX2/16K-1/2-8         484700     2553 ns/op   802.30 MB/s
// BenchmarkBitsetIndexAVX2/16K-1/16-8        485622     2166 ns/op   945.35 MB/s
// BenchmarkBitsetIndexAVX2/16K-1/32-8        510966     2558 ns/op   800.61 MB/s
// BenchmarkBitsetIndexAVX2/16K-1/64-8        552931     2196 ns/op   932.66 MB/s
// BenchmarkBitsetIndexAVX2/16K-1/128-8       557016     2116 ns/op   968.04 MB/s
// BenchmarkBitsetIndexAVX2/16K-1/1024-8      537540     2182 ns/op   938.76 MB/s
// BenchmarkBitsetIndexAVX2/16K-1/16384-8     536750     2198 ns/op   931.64 MB/s
// BenchmarkBitsetIndexAVX2/32K-1/2-8         221613     5179 ns/op   790.93 MB/s
// BenchmarkBitsetIndexAVX2/32K-1/16-8        252799     4602 ns/op   890.11 MB/s
// BenchmarkBitsetIndexAVX2/32K-1/32-8        189948     8473 ns/op   483.44 MB/s
// BenchmarkBitsetIndexAVX2/32K-1/64-8        228379     5194 ns/op   788.65 MB/s
// BenchmarkBitsetIndexAVX2/32K-1/128-8       263274     4331 ns/op   945.70 MB/s
// BenchmarkBitsetIndexAVX2/32K-1/1024-8      278726     4242 ns/op   965.62 MB/s
// BenchmarkBitsetIndexAVX2/32K-1/16384-8     271714     4242 ns/op   965.66 MB/s
// BenchmarkBitsetIndexAVX2/64K-1/2-8         112766    10118 ns/op   809.63 MB/s
// BenchmarkBitsetIndexAVX2/64K-1/16-8        134863     8787 ns/op   932.27 MB/s
// BenchmarkBitsetIndexAVX2/64K-1/32-8        112908    10473 ns/op   782.19 MB/s
// BenchmarkBitsetIndexAVX2/64K-1/64-8        128101     8958 ns/op   914.48 MB/s
// BenchmarkBitsetIndexAVX2/64K-1/128-8       110577    10224 ns/op   801.26 MB/s
// BenchmarkBitsetIndexAVX2/64K-1/1024-8      138932     8270 ns/op   990.61 MB/s
// BenchmarkBitsetIndexAVX2/64K-1/16384-8     133310     8640 ns/op   948.19 MB/s
func BenchmarkBitsetIndexAVX2Full(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		for _, d := range bitsetBenchmarkDensities {
			B.Run(n.name+"-"+d.name, func(B *testing.B) {
				bits := fillBitsetRand(nil, n.l, d.d)
				slice := make([]uint32, int(bitsetPopCountGeneric(bits, n.l))+8)
				B.ResetTimer()
				B.SetBytes(int64(bitFieldLen(n.l)))
				for i := 0; i < B.N; i++ {
					_ = bitsetIndexesAVX2Full(bits, n.l, slice)
				}
			})
		}
	}
}

// BenchmarkBitsetIndexAVX2New/16K-1/2-8          268418   4344 ns/op    471.45 MB/s
// BenchmarkBitsetIndexAVX2New/16K-1/16-8         225654   4854 ns/op    421.90 MB/s
// BenchmarkBitsetIndexAVX2New/16K-1/32-8         399063   3000 ns/op    682.78 MB/s
// BenchmarkBitsetIndexAVX2New/16K-1/64-8         681187   1755 ns/op   1166.98 MB/s
// BenchmarkBitsetIndexAVX2New/16K-1/128-8       1275790   1400 ns/op   1463.13 MB/s
// BenchmarkBitsetIndexAVX2New/16K-1/1024-8      6962569  177.6 ns/op  11532.16 MB/s
// BenchmarkBitsetIndexAVX2New/16K-1/16384-8    17839926  80.51 ns/op  25438.45 MB/s
// BenchmarkBitsetIndexAVX2New/32K-1/2-8          134426   8832 ns/op    463.74 MB/s
// BenchmarkBitsetIndexAVX2New/32K-1/16-8         100221  11887 ns/op    344.57 MB/s
// BenchmarkBitsetIndexAVX2New/32K-1/32-8         108640  10954 ns/op    373.94 MB/s
// BenchmarkBitsetIndexAVX2New/32K-1/64-8         283364   4065 ns/op   1007.53 MB/s
// BenchmarkBitsetIndexAVX2New/32K-1/128-8        532834   2257 ns/op   1814.65 MB/s
// BenchmarkBitsetIndexAVX2New/32K-1/1024-8      3020888  366.4 ns/op  11178.96 MB/s
// BenchmarkBitsetIndexAVX2New/32K-1/16384-8     8968371  123.2 ns/op  33247.42 MB/s
// BenchmarkBitsetIndexAVX2New/64K-1/2-8           68348  17650 ns/op    464.15 MB/s
// BenchmarkBitsetIndexAVX2New/64K-1/16-8          42525  28517 ns/op    287.26 MB/s
// BenchmarkBitsetIndexAVX2New/64K-1/32-8          65037  15466 ns/op    529.68 MB/s
// BenchmarkBitsetIndexAVX2New/64K-1/64-8         159476   9102 ns/op    900.04 MB/s
// BenchmarkBitsetIndexAVX2New/64K-1/128-8        309228   3948 ns/op   2074.80 MB/s
// BenchmarkBitsetIndexAVX2New/64K-1/1024-8      1882911  697.2 ns/op  11749.93 MB/s
// BenchmarkBitsetIndexAVX2New/64K-1/16384-8     5219270  216.9 ns/op  37773.08 MB/s
func BenchmarkBitsetIndexAVX2Skip(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		for _, d := range bitsetBenchmarkDensities {
			B.Run(n.name+"-"+d.name, func(B *testing.B) {
				bits := fillBitsetRand(nil, n.l, d.d)
				slice := make([]uint32, int(bitsetPopCountGeneric(bits, n.l))+8)
				B.ResetTimer()
				B.SetBytes(int64(bitFieldLen(n.l)))
				for i := 0; i < B.N; i++ {
					_ = bitsetIndexesAVX2Skip(bits, n.l, slice)
				}
			})
		}
	}
}

// goos: darwin
// goarch: amd64
// pkg: blockwatch.cc/knoxdb/vec
// cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
// BenchmarkBitsetRunGeneric/16K-1/2-8             19231         58183 ns/op      35.20 MB/s
// BenchmarkBitsetRunGeneric/16K-1/16-8            82839         14525 ns/op     140.99 MB/s
// BenchmarkBitsetRunGeneric/16K-1/128-8          578571          1880 ns/op    1089.57 MB/s
// BenchmarkBitsetRunGeneric/16K-1/1024-8        2541619           451.5 ns/op  4536.00 MB/s
// BenchmarkBitsetRunGeneric/16K-1/16384-8       4088905           293.0 ns/op  6990.85 MB/s
// BenchmarkBitsetRunGeneric/32K-1/2-8              9787        125573 ns/op      32.62 MB/s
// BenchmarkBitsetRunGeneric/32K-1/16-8            32683         37784 ns/op     108.41 MB/s
// BenchmarkBitsetRunGeneric/32K-1/128-8          285609          4360 ns/op     939.38 MB/s
// BenchmarkBitsetRunGeneric/32K-1/1024-8        1000000          1021 ns/op    4012.30 MB/s
// BenchmarkBitsetRunGeneric/32K-1/16384-8       1852549           661.2 ns/op  6195.26 MB/s
// BenchmarkBitsetRunGeneric/64K-1/2-8              4069        282978 ns/op      28.95 MB/s
// BenchmarkBitsetRunGeneric/64K-1/16-8            14385         84333 ns/op      97.14 MB/s
// BenchmarkBitsetRunGeneric/64K-1/128-8          140224         14507 ns/op     564.70 MB/s
// BenchmarkBitsetRunGeneric/64K-1/1024-8         492675          2203 ns/op    3718.74 MB/s
// BenchmarkBitsetRunGeneric/64K-1/16384-8        926565          1251 ns/op    6547.52 MB/s
func BenchmarkBitsetRunGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		for _, d := range bitsetBenchmarkDensities {
			B.Run(n.name+"-"+d.name, func(B *testing.B) {
				bits := fillBitsetRand(nil, n.l, d.d)
				B.ResetTimer()
				B.SetBytes(int64(bitFieldLen(n.l)))
				for i := 0; i < B.N; i++ {
					var idx, length int
					for idx > -1 {
						idx, length = bitsetRunGeneric(bits, idx+length, n.l)
					}
				}
			})
		}
	}
}

// goos: darwin
// goarch: amd64
// pkg: blockwatch.cc/knoxdb/vec
// cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
// BenchmarkBitsetRunAVX2/16K-1/2-8                15961         79018 ns/op      25.92 MB/s
// BenchmarkBitsetRunAVX2/16K-1/16-8               54166         21775 ns/op      94.05 MB/s
// BenchmarkBitsetRunAVX2/16K-1/128-8             338038          3449 ns/op     593.79 MB/s
// BenchmarkBitsetRunAVX2/16K-1/1024-8           2600422           460.0 ns/op  4452.56 MB/s
// BenchmarkBitsetRunAVX2/16K-1/16384-8         15464408            94.49 ns/op 21674.09 MB/s
// BenchmarkBitsetRunAVX2/32K-1/2-8                 7308        188588 ns/op      21.72 MB/s
// BenchmarkBitsetRunAVX2/32K-1/16-8               25029         44224 ns/op      92.62 MB/s
// BenchmarkBitsetRunAVX2/32K-1/128-8             198180          6115 ns/op     669.81 MB/s
// BenchmarkBitsetRunAVX2/32K-1/1024-8           1288021           999.8 ns/op  4096.63 MB/s
// BenchmarkBitsetRunAVX2/32K-1/16384-8          7299532           157.0 ns/op  26094.06 MB/s
// BenchmarkBitsetRunAVX2/64K-1/2-8                 3660        317149 ns/op      25.83 MB/s
// BenchmarkBitsetRunAVX2/64K-1/16-8               12945         94675 ns/op      86.53 MB/s
// BenchmarkBitsetRunAVX2/64K-1/128-8              92480         12611 ns/op     649.59 MB/s
// BenchmarkBitsetRunAVX2/64K-1/1024-8            731426          1690 ns/op    4848.62 MB/s
// BenchmarkBitsetRunAVX2/64K-1/16384-8          3774499           279.7 ns/op  29291.16 MB/s
func BenchmarkBitsetRunAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		for _, d := range bitsetBenchmarkDensities {
			B.Run(n.name+"-"+d.name, func(B *testing.B) {
				bits := fillBitsetRand(nil, n.l, d.d)
				B.ResetTimer()
				B.SetBytes(int64(bitFieldLen(n.l)))
				for i := 0; i < B.N; i++ {
					var idx, length int
					for idx > -1 {
						idx, length = bitsetRunAVX2Wrapper(bits, idx+length, n.l)
					}
				}
			})
		}
	}
}

// BenchmarkBitsetPopCountGeneric/32-8          200000000            7.45 ns/op  537.09 MB/s
// BenchmarkBitsetPopCountGeneric/128-8         100000000           13.4 ns/op  1190.32 MB/s
// BenchmarkBitsetPopCountGeneric/1K-8          30000000            42.9 ns/op  2986.69 MB/s
// BenchmarkBitsetPopCountGeneric/16K-8          3000000           540 ns/op    3788.38 MB/s
// BenchmarkBitsetPopCountGeneric/128K-8          300000          4235 ns/op    3867.94 MB/s
// BenchmarkBitsetPopCountGeneric/1M-8             50000         34329 ns/op    3818.10 MB/s
// BenchmarkBitsetPopCountGeneric/16M-8             3000        560950 ns/op    3738.57 MB/s
// BenchmarkBitsetPopCountGeneric/128M-8             300       4358409 ns/op    3849.39 MB/s
// BenchmarkBitsetPopCountGeneric/512M-8             100      18061159 ns/op    3715.65 MB/s
func BenchmarkBitsetPopCountGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetPopCountGeneric(bits, n.l)
			}
		})
	}
}

// BenchmarkBitsetPopCountAVX2/32-8             300000000            6.12 ns/op  653.36 MB/s
// BenchmarkBitsetPopCountAVX2/128-8            200000000            9.16 ns/op 1746.30 MB/s
// BenchmarkBitsetPopCountAVX2/1K-8             100000000           10.5 ns/op  12173.29 MB/s
// BenchmarkBitsetPopCountAVX2/16K-8            30000000            62.6 ns/op  32699.70 MB/s
// BenchmarkBitsetPopCountAVX2/128K-8            3000000           358 ns/op    45673.24 MB/s
// BenchmarkBitsetPopCountAVX2/1M-8               500000          3008 ns/op    43568.68 MB/s
// BenchmarkBitsetPopCountAVX2/16M-8               30000         59189 ns/op    35431.28 MB/s
// BenchmarkBitsetPopCountAVX2/128M-8               2000        894400 ns/op    18758.06 MB/s
// BenchmarkBitsetPopCountAVX2/512M-8                500       3709751 ns/op    18089.85 MB/s
func BenchmarkBitsetPopCountAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetPopCountAVX2(bits)
			}
		})
	}
}

func BenchmarkBitsetAndGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetAndGeneric(bits, cmp, n.l)
			}
		})
	}
}

func BenchmarkBitsetAndGenericFlag(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetAndGenericFlag(bits, cmp, n.l)
			}
		})
	}
}

func BenchmarkBitsetAndAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetAndAVX2(bits, cmp)
			}
		})
	}
}

func BenchmarkBitsetAndAVX2Flag(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetAndAVX2Flag(bits, cmp, n.l)
			}
		})
	}
}

// BenchmarkBitsetAndNotGeneric/32-8            200000000            8.04 ns/op  497.60 MB/s
// BenchmarkBitsetAndNotGeneric/128-8           100000000           15.3 ns/op  1046.45 MB/s
// BenchmarkBitsetAndNotGeneric/1K-8            20000000            89.4 ns/op  1432.12 MB/s
// BenchmarkBitsetAndNotGeneric/16K-8            1000000          1268 ns/op    1614.63 MB/s
// BenchmarkBitsetAndNotGeneric/128K-8            200000         10361 ns/op    1581.21 MB/s
// BenchmarkBitsetAndNotGeneric/1M-8               20000         81666 ns/op    1604.97 MB/s
// BenchmarkBitsetAndNotGeneric/16M-8               1000       1384304 ns/op    1514.95 MB/s
// BenchmarkBitsetAndNotGeneric/128M-8               100      11017526 ns/op    1522.78 MB/s
// BenchmarkBitsetAndNotGeneric/512M-8                30      45853262 ns/op    1463.56 MB/s
func BenchmarkBitsetAndNotGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetAndNotGeneric(bits, cmp, n.l)
			}
		})
	}
}

// BenchmarkBitsetAndNotAVX2/32-8           200000000            6.67 ns/op  599.59 MB/s
// BenchmarkBitsetAndNotAVX2/128-8          200000000            8.81 ns/op 1816.07 MB/s
// BenchmarkBitsetAndNotAVX2/1K-8           200000000            8.24 ns/op 15528.55 MB/s
// BenchmarkBitsetAndNotAVX2/16K-8          50000000            27.2 ns/op  75205.08 MB/s
// BenchmarkBitsetAndNotAVX2/128K-8         10000000           190 ns/op    86011.87 MB/s
// BenchmarkBitsetAndNotAVX2/1M-8             200000          5680 ns/op    23075.02 MB/s
// BenchmarkBitsetAndNotAVX2/16M-8             10000        133204 ns/op    15743.80 MB/s
// BenchmarkBitsetAndNotAVX2/128M-8             1000       1844008 ns/op    9098.23 MB/s
// BenchmarkBitsetAndNotAVX2/512M-8              100      10232017 ns/op    6558.71 MB/s
func BenchmarkBitsetAndNotAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetAndNotAVX2(bits, cmp)
			}
		})
	}
}

func BenchmarkBitsetOrGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetOrGeneric(bits, cmp, n.l)
			}
		})
	}
}

func BenchmarkBitsetOrGenericFlag(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetOrGenericFlag(bits, cmp, n.l)
			}
		})
	}
}

func BenchmarkBitsetOrAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetOrAVX2(bits, cmp)
			}
		})
	}
}

func BenchmarkBitsetOrAVX2Flag(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetOrAVX2Flag(bits, cmp, n.l)
			}
		})
	}
}

// BenchmarkBitsetXorGeneric/32-8           200000000            7.88 ns/op  507.62 MB/s
// BenchmarkBitsetXorGeneric/128-8          100000000           15.3 ns/op  1042.87 MB/s
// BenchmarkBitsetXorGeneric/1K-8           20000000            88.1 ns/op  1452.42 MB/s
// BenchmarkBitsetXorGeneric/16K-8           1000000          1201 ns/op    1704.00 MB/s
// BenchmarkBitsetXorGeneric/128K-8           200000         10056 ns/op    1629.13 MB/s
// BenchmarkBitsetXorGeneric/1M-8              20000         79915 ns/op    1640.14 MB/s
// BenchmarkBitsetXorGeneric/16M-8              1000       1307923 ns/op    1603.42 MB/s
// BenchmarkBitsetXorGeneric/128M-8              100      10600042 ns/op    1582.75 MB/s
// BenchmarkBitsetXorGeneric/512M-8               30      44760594 ns/op    1499.28 MB/s
func BenchmarkBitsetXorGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetXorGeneric(bits, cmp, n.l)
			}
		})
	}
}

// BenchmarkBitsetXorAVX2/32-8              200000000            6.25 ns/op  639.86 MB/s
// BenchmarkBitsetXorAVX2/128-8             200000000            8.37 ns/op 1911.01 MB/s
// BenchmarkBitsetXorAVX2/1K-8              200000000            9.09 ns/op 14087.81 MB/s
// BenchmarkBitsetXorAVX2/16K-8             50000000            25.9 ns/op  79163.49 MB/s
// BenchmarkBitsetXorAVX2/128K-8            10000000           188 ns/op    86805.89 MB/s
// BenchmarkBitsetXorAVX2/1M-8                300000          5619 ns/op    23323.86 MB/s
// BenchmarkBitsetXorAVX2/16M-8                10000        138406 ns/op    15152.13 MB/s
// BenchmarkBitsetXorAVX2/128M-8                1000       2075723 ns/op    8082.59 MB/s
// BenchmarkBitsetXorAVX2/512M-8                 100      12700923 ns/op    5283.78 MB/s
func BenchmarkBitsetXorAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetXorAVX2(bits, cmp)
			}
		})
	}
}

// BenchmarkBitsetNotGeneric/32-8           200000000            7.42 ns/op  538.80 MB/s
// BenchmarkBitsetNotGeneric/128-8          100000000           13.6 ns/op  1175.29 MB/s
// BenchmarkBitsetNotGeneric/1K-8           20000000            66.8 ns/op  1916.45 MB/s
// BenchmarkBitsetNotGeneric/16K-8           2000000           824 ns/op    2484.28 MB/s
// BenchmarkBitsetNotGeneric/128K-8           200000          6269 ns/op    2613.18 MB/s
// BenchmarkBitsetNotGeneric/1M-8              30000         50854 ns/op    2577.39 MB/s
// BenchmarkBitsetNotGeneric/16M-8              2000        836395 ns/op    2507.37 MB/s
// BenchmarkBitsetNotGeneric/128M-8              200       6627973 ns/op    2531.27 MB/s
// BenchmarkBitsetNotGeneric/512M-8               50      27560713 ns/op    2434.95 MB/s
func BenchmarkBitsetNotGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetNegGeneric(bits, n.l)
			}
		})
	}
}

// BenchmarkBitsetNotAVX2/32-8              300000000            5.24 ns/op  763.87 MB/s
// BenchmarkBitsetNotAVX2/128-8             200000000            7.79 ns/op 2054.36 MB/s
// BenchmarkBitsetNotAVX2/1K-8              200000000            8.05 ns/op 15897.01 MB/s
// BenchmarkBitsetNotAVX2/16K-8             100000000           23.4 ns/op  87516.27 MB/s
// BenchmarkBitsetNotAVX2/128K-8            10000000           159 ns/op    102570.09 MB/s
// BenchmarkBitsetNotAVX2/1M-8                300000          3931 ns/op    33338.47 MB/s
// BenchmarkBitsetNotAVX2/16M-8                20000         81274 ns/op    25803.45 MB/s
// BenchmarkBitsetNotAVX2/128M-8                1000       1072039 ns/op    15649.81 MB/s
// BenchmarkBitsetNotAVX2/512M-8                 300       4580533 ns/op    14650.88 MB/s
func BenchmarkBitsetNotAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetNegAVX2(bits)
			}
		})
	}
}

// BenchmarkBitsetReverseGeneric/16K-8  981885        1200 ns/op    1707.02 MB/s
// BenchmarkBitsetReverseGeneric/32K-8  484957        2392 ns/op    1712.68 MB/s
// BenchmarkBitsetReverseGeneric/64K-8  243278        4828 ns/op    1696.92 MB/s
func BenchmarkBitsetReverseGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetReverseGeneric(bits)
			}
		})
	}
}

// BenchmarkBitsetReverseAVX2/16K-8  12762820          94.90 ns/op  21579.98 MB/s
// BenchmarkBitsetReverseAVX2/32K-8   6202468          190.5 ns/op  21500.72 MB/s
// BenchmarkBitsetReverseAVX2/64K-8   3164384          375.5 ns/op  21816.63 MB/s
func BenchmarkBitsetReverseAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetReverseAVX2(bits, bitsetReverseLut256)
			}
		})
	}
}
