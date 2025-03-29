// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"slices"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/encode/s8b/tests"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

// -------------------------------
// Tests
//

func TestCmpEqual(t *testing.T) {
	tests.CompareTest(t, Encode[uint64], Equal, types.FilterModeEqual)
}

func TestCmpNotEqual(t *testing.T) {
	tests.CompareTest(t, Encode[uint64], NotEqual, types.FilterModeNotEqual)
}

func TestCmpLess(t *testing.T) {
	tests.CompareTest(t, Encode[uint64], Less, types.FilterModeLt)
}

func TestCmpLessEqual(t *testing.T) {
	tests.CompareTest(t, Encode[uint64], LessEqual, types.FilterModeLe)
}

func TestCmpGreater(t *testing.T) {
	tests.CompareTest(t, Encode[uint64], Greater, types.FilterModeGt)
}

func TestCmpGreaterEqual(t *testing.T) {
	tests.CompareTest(t, Encode[uint64], GreaterEqual, types.FilterModeGe)
}

func TestCmpBetween(t *testing.T) {
	tests.CompareTest2(t, Encode[uint64], Between, types.FilterModeRange)
}

// -------------------------------
// Benchmarks
//

// equal
func BenchmarkCmpEQ64(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], Equal)
}

func BenchmarkCmpEQ32(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint32], Equal)
}

func BenchmarkCmpEQ16(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint16], Equal)
}

func BenchmarkCmpEQ8(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint8], Equal)
}

// not equal
func BenchmarkCmpNE64(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], NotEqual)
}

func BenchmarkCmpNE32(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint32], NotEqual)
}

func BenchmarkCmpNE16(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint16], NotEqual)
}

func BenchmarkCmpNE8(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint8], NotEqual)
}

// less
func BenchmarkCmpLT64(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], Less)
}

func BenchmarkCmpLT32(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint32], Less)
}

func BenchmarkCmpLT16(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint16], Less)
}

func BenchmarkCmpLT8(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint8], Less)
}

// less equal
func BenchmarkCmpLE64(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], LessEqual)
}

func BenchmarkCmpLE32(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint32], LessEqual)
}

func BenchmarkCmpLE16(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint16], LessEqual)
}

func BenchmarkCmpLE8(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint8], LessEqual)
}

// greater
func BenchmarkCmpGT64(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], Greater)
}

func BenchmarkCmpGT32(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint32], Greater)
}

func BenchmarkCmpGT16(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint16], Greater)
}

func BenchmarkCmpGT8(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint8], Greater)
}

// greater equal
func BenchmarkCmpGE64(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], GreaterEqual)
}

func BenchmarkCmpGE32(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint32], GreaterEqual)
}

func BenchmarkCmpGE16(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint16], GreaterEqual)
}

func BenchmarkCmpGE8(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint8], GreaterEqual)
}

// between
func BenchmarkCmpRG64(b *testing.B) {
	tests.CompareBenchmark2(b, Encode[uint64], Between)
}

func BenchmarkCmpRG32(b *testing.B) {
	tests.CompareBenchmark2(b, Encode[uint32], Between)
}

func BenchmarkCmpRG16(b *testing.B) {
	tests.CompareBenchmark2(b, Encode[uint16], Between)
}

func BenchmarkCmpRG8(b *testing.B) {
	tests.CompareBenchmark2(b, Encode[uint8], Between)
}

// Serial Execution (unpack simple8 + compare kernel)

func BenchmarkCmpSerial64(b *testing.B) {
	CmpSerialBenchmark[uint64](b)
}

func BenchmarkCmpSerial32(b *testing.B) {
	CmpSerialBenchmark[uint32](b)
}

func BenchmarkCmpSerial16(b *testing.B) {
	CmpSerialBenchmark[uint16](b)
}

func BenchmarkCmpSerial8(b *testing.B) {
	CmpSerialBenchmark[uint8](b)
}

func CmpSerialBenchmark[T types.Unsigned](b *testing.B) {
	for _, c := range etests.MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf, err := Encode[T](make([]byte, 8*len(c.Data)), c.Data, minv, maxv)
		require.NoError(b, err)
		bits := bitset.NewBitset(len(c.Data))
		val := c.Data[len(c.Data)/2]

		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * int(unsafe.Sizeof(T(0)))))
			for range b.N {
				dst := make([]T, len(c.Data))
				_, err := Decode(dst, buf)
				require.NoError(b, err)
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
