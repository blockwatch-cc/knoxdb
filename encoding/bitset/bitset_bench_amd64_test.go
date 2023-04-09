// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package bitset

import (
	"bytes"
	"reflect"
	"testing"

	"blockwatch.cc/knoxdb/util"
)

func TestBitAndAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}

	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				bitsetAndAVX2(dst, dst)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				bitsetAndAVX2(dst, src)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// all zeros
				copy(dst, src)
				bitsetAndAVX2(dst, zeros)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all ones
				copy(dst, src)
				bitsetAndAVX2(dst, ones)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitAndAVX2Flag(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				any, all := bitsetAndAVX2Flag(dst, dst, sz)
				if pt == 0x80 && sz == 7 {
					if any {
						T.Errorf("dst===src: unexpected return value %v, expected false", any)
					}
				} else {
					if !any {
						T.Errorf("dst===src: unexpected return value %v, expected true", any)
					}
				}
				if pt == 0xff {
					if !all {
						T.Errorf("dst===src: unexpected return value %v, expected true", all)
					}
				} else {
					if all {
						T.Errorf("dst===src: unexpected return value %v, expected false", all)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				any, all = bitsetAndAVX2Flag(dst, src, sz)
				if pt == 0x80 && sz == 7 {
					if any {
						T.Errorf("dst==src: unexpected return value %v, expected false", any)
					}
				} else {
					if !any {
						T.Errorf("dst==src: unexpected return value %v, expected true", any)
					}
				}
				if pt == 0xff {
					if !all {
						T.Errorf("dst==src: unexpected return value %v, expected true", all)
					}
				} else {
					if all {
						T.Errorf("dst==src: unexpected return value %v, expected false", all)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// all zeros
				copy(dst, src)
				any, all = bitsetAndAVX2Flag(dst, zeros, sz)
				if any {
					T.Errorf("zeros: unexpected return value %v, expected false", any)
				}
				if all {
					T.Errorf("zeros: unexpected return value %v, expected false", all)
				}
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all ones
				copy(dst, src)
				any, all = bitsetAndAVX2Flag(dst, ones, sz)
				if pt == 0x80 && sz == 7 {
					if any {
						T.Errorf("ones: unexpected return value %v, expected false", any)
					}
				} else {
					if !any {
						T.Errorf("ones: unexpected return value %v, expected true", any)
					}
				}
				if pt == 0xff {
					if !all {
						T.Errorf("ones: unexpected return value %v, expected true", all)
					}
				} else {
					if all {
						T.Errorf("ones: unexpected return value %v, expected false", all)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitAndNotAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := make([]byte, len(src))

				// same value, same slice
				bitsetAndNot(dst, dst, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				bitsetAndNot(dst, src, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val AND NOT zeros == val
				copy(dst, src)
				bitsetAndNot(dst, zeros, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all AND NOT ones == zero
				copy(dst, src)
				bitsetAndNot(dst, ones, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitOrAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				bitsetOr(dst, dst, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				bitsetOr(dst, src, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val OR zeros == val
				copy(dst, src)
				bitsetOr(dst, zeros, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all OR ones == ones
				copy(dst, src)
				bitsetOr(dst, ones, sz)
				if bytes.Compare(dst, ones) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, ones)
				}
				if got, want := popcount(dst), popcount(ones); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitOrAVX2Flag(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				any, all := bitsetOrAVX2Flag(dst, dst, sz)
				if pt == 0x80 && sz == 7 {
					if any {
						T.Errorf("dst===src: unexpected return value %v, expected false", any)
					}
				} else {
					if !any {
						T.Errorf("dst===src: unexpected return value %v, expected true", any)
					}
				}
				if pt == 0xff {
					if !all {
						T.Errorf("dst===src: unexpected return value %v, expected true", all)
					}
				} else {
					if all {
						T.Errorf("dst===src: unexpected return value %v, expected false", all)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				any, all = bitsetOrAVX2Flag(dst, src, sz)
				if pt == 0x80 && sz == 7 {
					if any {
						T.Errorf("dst==src: unexpected return value %v, expected false", any)
					}
				} else {
					if !any {
						T.Errorf("dst==src: unexpected return value %v, expected true", any)
					}
				}
				if pt == 0xff {
					if !all {
						T.Errorf("dst==src: unexpected return value %v, expected true", all)
					}
				} else {
					if all {
						T.Errorf("dst==src: unexpected return value %v, expected false", all)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// all zeros
				copy(dst, src)
				any, all = bitsetOrAVX2Flag(dst, zeros, sz)
				if pt == 0x80 && sz == 7 {
					if any {
						T.Errorf("zeros: unexpected return value %v, expected false", any)
					}
				} else {
					if !any {
						T.Errorf("zeros: unexpected return value %v, expected true", any)
					}
				}
				if pt == 0xff {
					if !all {
						T.Errorf("zeros: unexpected return value %v, expected true", all)
					}
				} else {
					if all {
						T.Errorf("zeros: unexpected return value %v, expected 0", all)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all ones
				copy(dst, src)
				any, all = bitsetOrAVX2Flag(dst, ones, sz)
				if !any {
					T.Errorf("ones: unexpected return value %v, expected true", any)
				}
				if !all {
					T.Errorf("ones: unexpected return value %v, expected true", all)
				}
				if bytes.Compare(dst, ones) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, ones)
				}
				if got, want := popcount(dst), popcount(ones); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitXorAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				bitsetXor(dst, dst, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				bitsetXor(dst, src, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val XOR zeros == val
				copy(dst, src)
				bitsetXor(dst, zeros, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// val XOR ones == neg(val)
				copy(dst, src)
				bitsetXor(dst, ones, sz)
				cmp := fillBitset(nil, sz, ^pt)
				if bytes.Compare(dst, cmp) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, cmp)
				}
				if got, want := popcount(dst), popcount(cmp); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitNegAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				cmp := fillBitset(nil, sz, ^pt)

				bitsetNeg(src, sz)
				if bytes.Compare(src, cmp) != 0 {
					T.Errorf("unexpected result %x, expected %x", src, cmp)
				}
				if got, want := popcount(src), popcount(cmp); got != want {
					T.Errorf("unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitsetPopCountAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range bitsetCases {
		// call the function selector to do proper last byte masking!
		T.Run(c.name, func(t *testing.T) {
			cnt := bitsetPopCount(c.source, c.size)
			if got, want := int(cnt), c.count; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
		})
	}
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				buf := fillBitset(nil, sz, pt)
				// call the function selector to do proper last byte masking!
				if got, want := int(bitsetPopCount(buf, sz)), popcount(buf); got != want {
					T.Errorf("unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitsetReverseAVX2(T *testing.T) {
	for _, sz := range bitsetSizes {
		bits := fillBitsetSaw(nil, sz)
		cmp := make([]byte, len(bits))
		copy(cmp, bits)
		bitsetReverseGeneric(cmp)
		bitsetReverseAVX2(bits, bitsetReverseLut256)

		if got, want := len(bits), len(cmp); got != want {
			T.Errorf("%d: unexpected buf length %d, expected %d", sz, got, want)
		}
		if got, want := popcount(bits), popcount(cmp); got != want {
			T.Errorf("%d: unexpected count %d, expected %d", sz, got, want)
		}
		if bytes.Compare(bits, cmp) != 0 {
			T.Errorf("%d: unexpected result %x, expected %x", sz, bits, cmp)
		}
	}
}

func TestBitsetIndexAVX2Full(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range runTestcases {
		idx := make([]uint32, len(c.idx)+8)
		T.Run(c.name, func(t *testing.T) {
			var ret = bitsetIndexesAVX2Full(c.buf, c.size, idx)
			if got, want := ret, popcount(c.buf); got != want {
				T.Errorf("unexpected index vector length %d, expected %d", got, want)
			}
			idx = idx[:ret]
			if got, want := ret, len(c.idx); got != want {
				T.Errorf("unexpected return value %d, expected %d", got, want)
			}
			if !reflect.DeepEqual(idx, c.idx) {
				T.Errorf("unexpected result %d, expected %d", idx, c.idx)
			}
		})
	}
}

func TestBitsetIndexAVX2Skip(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range runTestcases {
		c.buf = append(c.buf, 0xff)
		c.buf = c.buf[:len(c.buf)-1]
		idx := make([]uint32, len(c.idx)+8)
		T.Run(c.name, func(t *testing.T) {
			var ret = bitsetIndexesAVX2Skip(c.buf, c.size, idx)
			if got, want := ret, popcount(c.buf); got != want {
				T.Errorf("unexpected index vector length %d, expected %d", got, want)
			}
			idx = idx[:ret]
			if got, want := ret, len(c.idx); got != want {
				T.Errorf("unexpected return value %d, expected %d", got, want)
			}
			if !reflect.DeepEqual(idx, c.idx) {
				T.Errorf("unexpected result %d, expected %d", idx, c.idx)
			}
		})
	}
}

func TestBitsetRunAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range runTestcases {
		bits := NewBitsetFromBytes(c.buf, c.size)
		var idx, length int
		for i, r := range c.runs {
			T.Run(f("%s_%d", c.name, i), func(t *testing.T) {
				idx, length = bitsetRunAVX2Wrapper(bits.Bytes(), idx+length, bits.Len())
				if got, want := idx, r[0]; got != want {
					T.Errorf("unexpected index %d, expected %d", got, want)
				}
				if got, want := length, r[1]; got != want {
					T.Errorf("unexpected length %d, expected %d", got, want)
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

// BenchmarkBitsetReverseAVX2/16K-8  12762820          94.90 ns/op  21579.98 MB/s
// BenchmarkBitsetReverseAVX2/32K-8   6202468          190.5 ns/op  21500.72 MB/s
// BenchmarkBitsetReverseAVX2/64K-8   3164384          375.5 ns/op  21816.63 MB/s
func BenchmarkBitsetReverseAVX2(B *testing.B) {
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
