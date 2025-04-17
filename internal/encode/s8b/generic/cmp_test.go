// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"fmt"
	"slices"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/cmp"
	stests "blockwatch.cc/knoxdb/internal/encode/s8b/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

// -------------------------------
// Tests
//

func TestCmpEqual(t *testing.T) {
	stests.CompareTest(t, Encode[uint64], Equal, types.FilterModeEqual)
}

func TestCmpNotEqual(t *testing.T) {
	stests.CompareTest(t, Encode[uint64], NotEqual, types.FilterModeNotEqual)
}

func TestCmpLess(t *testing.T) {
	stests.CompareTest(t, Encode[uint64], Less, types.FilterModeLt)
}

func TestCmpLessEqual(t *testing.T) {
	stests.CompareTest(t, Encode[uint64], LessEqual, types.FilterModeLe)
}

func TestCmpGreater(t *testing.T) {
	stests.CompareTest(t, Encode[uint64], Greater, types.FilterModeGt)
}

func TestCmpGreaterEqual(t *testing.T) {
	stests.CompareTest(t, Encode[uint64], GreaterEqual, types.FilterModeGe)
}

func TestCmpBetween(t *testing.T) {
	stests.CompareTest2(t, Encode[uint64], Between, types.FilterModeRange)
}

// -------------------------------
// Benchmarks
//

// equal
func BenchmarkCmpEqual(b *testing.B) {
	stests.CompareBenchmark(b, Encode[uint64], Equal)
	stests.CompareBenchmark(b, Encode[uint32], Equal)
	stests.CompareBenchmark(b, Encode[uint16], Equal)
	stests.CompareBenchmark(b, Encode[uint8], Equal)
}

// not equal
func BenchmarkCmpNotEqual(b *testing.B) {
	stests.CompareBenchmark(b, Encode[uint64], NotEqual)
	stests.CompareBenchmark(b, Encode[uint32], NotEqual)
	stests.CompareBenchmark(b, Encode[uint16], NotEqual)
	stests.CompareBenchmark(b, Encode[uint8], NotEqual)
}

// less
func BenchmarkCmpLess(b *testing.B) {
	stests.CompareBenchmark(b, Encode[uint64], Less)
	stests.CompareBenchmark(b, Encode[uint32], Less)
	stests.CompareBenchmark(b, Encode[uint16], Less)
	stests.CompareBenchmark(b, Encode[uint8], Less)
}

// less equal
func BenchmarkCmpLessEqual(b *testing.B) {
	stests.CompareBenchmark(b, Encode[uint64], LessEqual)
	stests.CompareBenchmark(b, Encode[uint32], LessEqual)
	stests.CompareBenchmark(b, Encode[uint16], LessEqual)
	stests.CompareBenchmark(b, Encode[uint8], LessEqual)
}

// greater
func BenchmarkCmpGreater(b *testing.B) {
	stests.CompareBenchmark(b, Encode[uint64], Greater)
	stests.CompareBenchmark(b, Encode[uint32], Greater)
	stests.CompareBenchmark(b, Encode[uint16], Greater)
	stests.CompareBenchmark(b, Encode[uint8], Greater)
}

// greater equal
func BenchmarkCmpGreaterEqual(b *testing.B) {
	stests.CompareBenchmark(b, Encode[uint64], GreaterEqual)
	stests.CompareBenchmark(b, Encode[uint32], GreaterEqual)
	stests.CompareBenchmark(b, Encode[uint16], GreaterEqual)
	stests.CompareBenchmark(b, Encode[uint8], GreaterEqual)
}

// between
func BenchmarkCmpBetween(b *testing.B) {
	stests.CompareBenchmark2(b, Encode[uint64], Between)
	stests.CompareBenchmark2(b, Encode[uint32], Between)
	stests.CompareBenchmark2(b, Encode[uint16], Between)
	stests.CompareBenchmark2(b, Encode[uint8], Between)
}

// Serial Execution (unpack simple8 + compare kernel)

func BenchmarkCmpEqualUnpacked(b *testing.B) {
	CmpEqualUnpackedBenchmark[uint64](b)
	CmpEqualUnpackedBenchmark[uint32](b)
	CmpEqualUnpackedBenchmark[uint16](b)
	CmpEqualUnpackedBenchmark[uint8](b)
}

func CmpEqualUnpackedBenchmark[T types.Unsigned](b *testing.B) {
	for _, c := range tests.MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf, err := Encode[T](make([]byte, 8*len(c.Data)), c.Data, minv, maxv)
		require.NoError(b, err)
		bits := bitset.NewBitset(len(c.Data))
		val := c.Data[len(c.Data)/2]

		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * int(unsafe.Sizeof(T(0)))))
			for range b.N {
				dst := make([]T, len(c.Data))
				_, err := Decode(dst, buf)
				require.NoError(b, err)
				var n int64
				switch any(T(0)).(type) {
				case uint64:
					n = cmp.Uint64Equal(util.ReinterpretSlice[T, uint64](dst), uint64(val), bits.Bytes())
				case uint32:
					n = cmp.Uint32Equal(util.ReinterpretSlice[T, uint32](dst), uint32(val), bits.Bytes())
				case uint16:
					n = cmp.Uint16Equal(util.ReinterpretSlice[T, uint16](dst), uint16(val), bits.Bytes())
				case uint8:
					n = cmp.Uint8Equal(util.ReinterpretSlice[T, uint8](dst), uint8(val), bits.Bytes())
				}
				bits.ResetCount(int(n))
			}
		})
	}
}
