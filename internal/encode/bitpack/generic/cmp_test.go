// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"fmt"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/encode/bitpack/tests"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

// -------------------------------
// Tests
//

func TestCmpEqual(t *testing.T) {
	tests.CompareTest(t, Equal, types.FilterModeEqual)
}

func TestCmpNotEqual(t *testing.T) {
	tests.CompareTest(t, NotEqual, types.FilterModeNotEqual)
}

func TestCmpLess(t *testing.T) {
	tests.CompareTest(t, Less, types.FilterModeLt)
}

func TestCmpLessEqual(t *testing.T) {
	tests.CompareTest(t, LessEqual, types.FilterModeLe)
}

func TestCmpGreater(t *testing.T) {
	tests.CompareTest(t, Greater, types.FilterModeGt)
}

func TestCmpGreaterEqual(t *testing.T) {
	tests.CompareTest(t, GreaterEqual, types.FilterModeGe)
}

func TestCmpBetween(t *testing.T) {
	tests.CompareTest2(t, Between, types.FilterModeRange)
}

// -------------------------------
// Benchmarks
//

// equal
func BenchmarkCmpEqual(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], Equal)
	tests.CompareBenchmark(b, Encode[uint32], Equal)
	tests.CompareBenchmark(b, Encode[uint16], Equal)
	tests.CompareBenchmark(b, Encode[uint8], Equal)
}

// not equal
func BenchmarkCmpNotEqual(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], NotEqual)
	tests.CompareBenchmark(b, Encode[uint32], NotEqual)
	tests.CompareBenchmark(b, Encode[uint16], NotEqual)
	tests.CompareBenchmark(b, Encode[uint8], NotEqual)
}

// less
func BenchmarkCmpLess(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], Less)
	tests.CompareBenchmark(b, Encode[uint32], Less)
	tests.CompareBenchmark(b, Encode[uint16], Less)
	tests.CompareBenchmark(b, Encode[uint8], Less)
}

// less equal
func BenchmarkCmpLessEqual(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], LessEqual)
	tests.CompareBenchmark(b, Encode[uint32], LessEqual)
	tests.CompareBenchmark(b, Encode[uint16], LessEqual)
	tests.CompareBenchmark(b, Encode[uint8], LessEqual)
}

// greater
func BenchmarkCmpGreater(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], Greater)
	tests.CompareBenchmark(b, Encode[uint32], Greater)
	tests.CompareBenchmark(b, Encode[uint16], Greater)
	tests.CompareBenchmark(b, Encode[uint8], Greater)
}

// greater equal
func BenchmarkCmpGreaterEqual(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], GreaterEqual)
	tests.CompareBenchmark(b, Encode[uint32], GreaterEqual)
	tests.CompareBenchmark(b, Encode[uint16], GreaterEqual)
	tests.CompareBenchmark(b, Encode[uint8], GreaterEqual)
}

// between
func BenchmarkCmpBetween(b *testing.B) {
	tests.CompareBenchmark2(b, Encode[uint64], Between)
	tests.CompareBenchmark2(b, Encode[uint32], Between)
	tests.CompareBenchmark2(b, Encode[uint16], Between)
	tests.CompareBenchmark2(b, Encode[uint8], Between)
}

// Serial Execution (unpack simple8 + compare kernel)

func BenchmarkCmpEqualFused(b *testing.B) {
	CmpEqualFusedBenchmark[uint64](b)
	CmpEqualFusedBenchmark[uint32](b)
	CmpEqualFusedBenchmark[uint16](b)
	CmpEqualFusedBenchmark[uint8](b)
}

func BenchmarkCmpEqualUnpacked(b *testing.B) {
	CmpEqualUnpackedBenchmark[uint64](b)
	CmpEqualUnpackedBenchmark[uint32](b)
	CmpEqualUnpackedBenchmark[uint16](b)
	CmpEqualUnpackedBenchmark[uint8](b)
}

func BenchmarkCmpEqualLoop(b *testing.B) {
	CmpEqualLoopBenchmark[uint64](b)
	CmpEqualLoopBenchmark[uint32](b)
	CmpEqualLoopBenchmark[uint16](b)
	CmpEqualLoopBenchmark[uint8](b)
}

func CmpEqualFusedBenchmark[T types.Unsigned](b *testing.B) {
	for _, c := range etests.BenchmarkSizes {
		for _, p := range etests.BenchmarkPatterns {
			data, val := etests.GenEqual[T](c.N, p.Pct)
			buf := make([]byte, 8*c.N)
			bits := bitset.NewBitset(c.N)
			maxw := int(unsafe.Sizeof(T(0)) * 8)

			for w := range maxw - 1 {
				w++
				PackVec(buf, data, w)
				mask := 1<<w - 1
				val &= T(mask)

				b.Run(fmt.Sprintf("u%d/%s/%s/%d_bits", maxw, c.Name, p.Name, w), func(b *testing.B) {
					b.SetBytes(int64(c.N * maxw / 8))
					for range b.N {
						Equal(buf, w, uint64(val), c.N, bits)
					}
				})
			}
		}
	}
}

// 5-7x slower (unpack: 90-150 µs, match: 25 µs, total: 115-180 µs = 8.2 cycles/value)
func CmpEqualUnpackedBenchmark[T types.Unsigned](b *testing.B) {
	for _, c := range etests.BenchmarkSizes {
		for _, p := range etests.BenchmarkPatterns {
			data, val := etests.GenEqual[T](c.N, p.Pct)
			buf := make([]byte, 8*c.N)
			bits := bitset.NewBitset(c.N)
			maxw := int(unsafe.Sizeof(T(0)) * 8)

			for w := range maxw - 1 {
				w++
				PackVec(buf, data, w)
				mask := 1<<w - 1
				val &= T(mask)

				b.Run(fmt.Sprintf("u%d/%s/%s/%d_bits", maxw, c.Name, p.Name, w), func(b *testing.B) {
					b.SetBytes(int64(c.N * maxw / 8))
					for range b.N {
						dst := make([]T, c.N)
						Decode(dst, buf, w, 0)
						switch any(T(0)).(type) {
						case uint64:
							cmp.MatchUint64Equal(util.ReinterpretSlice[T, uint64](dst), uint64(val), bits, nil)
						case uint32:
							cmp.MatchUint32Equal(util.ReinterpretSlice[T, uint32](dst), uint32(val), bits, nil)
						case uint16:
							cmp.MatchUint16Equal(util.ReinterpretSlice[T, uint16](dst), uint16(val), bits, nil)
						case uint8:
							cmp.MatchUint8Equal(util.ReinterpretSlice[T, uint8](dst), uint8(val), bits, nil)
						}
					}
				})
			}
		}
	}
}

func CmpEqualLoopBenchmark[T types.Unsigned](b *testing.B) {
	for _, c := range etests.BenchmarkSizes {
		for _, p := range etests.BenchmarkPatterns {
			data, val := etests.GenEqual[T](c.N, p.Pct)
			bits := bitset.NewBitset(c.N)
			buf := make([]byte, 8*c.N)
			maxw := int(unsafe.Sizeof(T(0)) * 8)

			for w := range maxw - 1 {
				w++
				PackVec(buf, data, w)
				mask := 1<<w - 1
				val &= T(mask)

				b.Run(fmt.Sprintf("%s/%s/%d_bits", c.Name, p.Name, w), func(b *testing.B) {
					b.SetBytes(int64(len(data) * int(unsafe.Sizeof(T(0)))))
					for range b.N {
						for i := range c.N {
							if T(Unpack(buf, i, w)) == val {
								bits.Set(i)
							}
						}
					}
				})
			}
		}
	}
}
