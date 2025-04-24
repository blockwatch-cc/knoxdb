// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitpack

import (
	"testing"

	bptest "blockwatch.cc/knoxdb/internal/encode/bitpack/tests"
	"blockwatch.cc/knoxdb/internal/types"
)

// -------------------------------
// Tests

func TestCmpEqual(t *testing.T) {
	bptest.CompareTest[uint64](t, Equal, types.FilterModeEqual, Encode)
}

func TestCmpNotEqual(t *testing.T) {
	bptest.CompareTest[uint64](t, NotEqual, types.FilterModeNotEqual, Encode)
}

func TestCmpLess(t *testing.T) {
	bptest.CompareTest[uint64](t, Less, types.FilterModeLt, Encode)
}

func TestCmpLessEqual(t *testing.T) {
	bptest.CompareTest[uint64](t, LessEqual, types.FilterModeLe, Encode)
}

func TestCmpGreater(t *testing.T) {
	bptest.CompareTest[uint64](t, Greater, types.FilterModeGt, Encode)
}

func TestCmpGreaterEqual(t *testing.T) {
	bptest.CompareTest[uint64](t, GreaterEqual, types.FilterModeGe, Encode)
}

func TestCmpBetween(t *testing.T) {
	bptest.CompareTest2[uint64](t, Between, types.FilterModeRange, Encode)
}

// -------------------------------
// Benchmarks
//

// equal
func BenchmarkCmpEqual(b *testing.B) {
	bptest.CompareBenchmark(b, Encode[uint64], Equal)
	bptest.CompareBenchmark(b, Encode[uint32], Equal)
	bptest.CompareBenchmark(b, Encode[uint16], Equal)
	bptest.CompareBenchmark(b, Encode[uint8], Equal)
}

// not equal
func BenchmarkCmpNotEqual(b *testing.B) {
	bptest.CompareBenchmark(b, Encode[uint64], NotEqual)
	bptest.CompareBenchmark(b, Encode[uint32], NotEqual)
	bptest.CompareBenchmark(b, Encode[uint16], NotEqual)
	bptest.CompareBenchmark(b, Encode[uint8], NotEqual)
}

// less
func BenchmarkCmpLess(b *testing.B) {
	bptest.CompareBenchmark(b, Encode[uint64], Less)
	bptest.CompareBenchmark(b, Encode[uint32], Less)
	bptest.CompareBenchmark(b, Encode[uint16], Less)
	bptest.CompareBenchmark(b, Encode[uint8], Less)
}

// less equal
func BenchmarkCmpLessEqual(b *testing.B) {
	bptest.CompareBenchmark(b, Encode[uint64], LessEqual)
	bptest.CompareBenchmark(b, Encode[uint32], LessEqual)
	bptest.CompareBenchmark(b, Encode[uint16], LessEqual)
	bptest.CompareBenchmark(b, Encode[uint8], LessEqual)
}

// greater
func BenchmarkCmpGreater(b *testing.B) {
	bptest.CompareBenchmark(b, Encode[uint64], Greater)
	bptest.CompareBenchmark(b, Encode[uint32], Greater)
	bptest.CompareBenchmark(b, Encode[uint16], Greater)
	bptest.CompareBenchmark(b, Encode[uint8], Greater)
}

// greater equal
func BenchmarkCmpGreaterEqual(b *testing.B) {
	bptest.CompareBenchmark(b, Encode[uint64], GreaterEqual)
	bptest.CompareBenchmark(b, Encode[uint32], GreaterEqual)
	bptest.CompareBenchmark(b, Encode[uint16], GreaterEqual)
	bptest.CompareBenchmark(b, Encode[uint8], GreaterEqual)
}

// between
func BenchmarkCmpBetween(b *testing.B) {
	bptest.CompareBenchmark2(b, Encode[uint64], Between)
	bptest.CompareBenchmark2(b, Encode[uint32], Between)
	bptest.CompareBenchmark2(b, Encode[uint16], Between)
	bptest.CompareBenchmark2(b, Encode[uint8], Between)
}

// // Serial Execution (unpack simple8 + compare kernel)

// func BenchmarkCmpEqualFused(b *testing.B) {
// 	CmpEqualFusedBenchmark[uint64](b)
// 	CmpEqualFusedBenchmark[uint32](b)
// 	CmpEqualFusedBenchmark[uint16](b)
// 	CmpEqualFusedBenchmark[uint8](b)
// }

// func BenchmarkCmpEqualUnpacked(b *testing.B) {
// 	CmpEqualUnpackedBenchmark[uint64](b)
// 	CmpEqualUnpackedBenchmark[uint32](b)
// 	CmpEqualUnpackedBenchmark[uint16](b)
// 	CmpEqualUnpackedBenchmark[uint8](b)
// }

// func BenchmarkCmpEqualLoop(b *testing.B) {
// 	CmpEqualLoopBenchmark[uint64](b)
// 	CmpEqualLoopBenchmark[uint32](b)
// 	CmpEqualLoopBenchmark[uint16](b)
// 	CmpEqualLoopBenchmark[uint8](b)
// }

// func CmpEqualFusedBenchmark[T types.Unsigned](b *testing.B) {
// 	for _, c := range tests.BenchmarkSizes {
// 		for _, p := range tests.BenchmarkPatterns {
// 			data, val := tests.GenEqual[T](c.N, p.Pct)
// 			buf := make([]byte, 8*c.N)
// 			bits := bitset.NewBitset(c.N)
// 			maxw := int(unsafe.Sizeof(T(0)) * 8)

// 			for w := range maxw - 1 {
// 				w++
// 				PackVec(buf, data, w)
// 				mask := 1<<w - 1
// 				val &= T(mask)

// 				b.Run(fmt.Sprintf("u%d/%s/%s/%d_bits", maxw, c.Name, p.Name, w), func(b *testing.B) {
// 					b.SetBytes(int64(c.N * maxw / 8))
// 					for range b.N {
// 						Equal(buf, w, uint64(val), c.N, bits)
// 					}
// 				})
// 			}
// 		}
// 	}
// }

// // 5-7x slower (unpack: 90-150 µs, match: 25 µs, total: 115-180 µs = 8.2 cycles/value)
// func CmpEqualUnpackedBenchmark[T types.Unsigned](b *testing.B) {
// 	for _, c := range tests.BenchmarkSizes {
// 		for _, p := range tests.BenchmarkPatterns {
// 			data, val := tests.GenEqual[T](c.N, p.Pct)
// 			buf := make([]byte, 8*c.N)
// 			bits := bitset.NewBitset(c.N)
// 			maxw := int(unsafe.Sizeof(T(0)) * 8)

// 			for w := range maxw - 1 {
// 				w++
// 				PackVec(buf, data, w)
// 				mask := 1<<w - 1
// 				val &= T(mask)

// 				b.Run(fmt.Sprintf("u%d/%s/%s/%d_bits", maxw, c.Name, p.Name, w), func(b *testing.B) {
// 					b.SetBytes(int64(c.N * maxw / 8))
// 					for range b.N {
// 						dst := make([]T, c.N)
// 						Decode(dst, buf, w, 0)
// 						switch any(T(0)).(type) {
// 						case uint64:
// 							cmp.MatchUint64Equal(util.ReinterpretSlice[T, uint64](dst), uint64(val), bits, nil)
// 						case uint32:
// 							cmp.MatchUint32Equal(util.ReinterpretSlice[T, uint32](dst), uint32(val), bits, nil)
// 						case uint16:
// 							cmp.MatchUint16Equal(util.ReinterpretSlice[T, uint16](dst), uint16(val), bits, nil)
// 						case uint8:
// 							cmp.MatchUint8Equal(util.ReinterpretSlice[T, uint8](dst), uint8(val), bits, nil)
// 						}
// 					}
// 				})
// 			}
// 		}
// 	}
// }

// func CmpEqualLoopBenchmark[T types.Unsigned](b *testing.B) {
// 	for _, c := range tests.BenchmarkSizes {
// 		for _, p := range tests.BenchmarkPatterns {
// 			data, val := tests.GenEqual[T](c.N, p.Pct)
// 			bits := bitset.NewBitset(c.N)
// 			buf := make([]byte, 8*c.N)
// 			maxw := int(unsafe.Sizeof(T(0)) * 8)

// 			for w := range maxw - 1 {
// 				w++
// 				PackVec(buf, data, w)
// 				mask := 1<<w - 1
// 				val &= T(mask)

// 				b.Run(fmt.Sprintf("%s/%s/%d_bits", c.Name, p.Name, w), func(b *testing.B) {
// 					b.SetBytes(int64(len(data) * int(unsafe.Sizeof(T(0)))))
// 					for range b.N {
// 						for i := range c.N {
// 							if T(Unpack(buf, i, w)) == val {
// 								bits.Set(i)
// 							}
// 						}
// 					}
// 				})
// 			}
// 		}
// 	}
// }
