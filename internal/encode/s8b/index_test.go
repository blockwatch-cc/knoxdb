// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package s8b

import (
	"fmt"
	"slices"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/cpu"
	"blockwatch.cc/knoxdb/internal/encode/s8b/generic"
	stests "blockwatch.cc/knoxdb/internal/encode/s8b/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

type IndexFunc[T uint16 | uint32] func([]byte, []T) Index

func BenchmarkIndex16(b *testing.B) {
	IndexBenchmark[uint16](b, Encode[uint16], MakeIndex[uint16])
}

func BenchmarkIndex16Find(b *testing.B) {
	IndexFindBenchmark[uint16](b, Encode[uint16], MakeIndex[uint16])
}

func BenchmarkIndex32(b *testing.B) {
	IndexBenchmark[uint32](b, Encode[uint32], MakeIndex[uint32])
}

func BenchmarkIndex32Find(b *testing.B) {
	IndexFindBenchmark[uint32](b, Encode[uint32], MakeIndex[uint32])
}

func IndexBenchmark[T types.Unsigned, I uint16 | uint32](b *testing.B, enc stests.EncodeFunc[T], idx IndexFunc[I]) {
	for _, c := range tests.MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf, err := enc(make([]byte, 8*len(c.Data)), c.Data, minv, maxv)
		require.NoError(b, err)
		dst := make([]I, len(c.Data))
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * int(unsafe.Sizeof(T(0)))))
			for range b.N {
				idx(buf, dst)
			}
		})
	}
}

func IndexFindBenchmark[T types.Unsigned, I uint16 | uint32](b *testing.B, enc stests.EncodeFunc[T], mkidx IndexFunc[I]) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[T](c.N)
		minv, maxv := slices.Min(data), slices.Max(data)
		buf, err := enc(make([]byte, 8*len(data)), data, minv, maxv)
		require.NoError(b, err)
		dst := make([]I, len(data))
		idx := mkidx(buf, dst)
		b.Run(c.Name, func(b *testing.B) {
			for i := range b.N {
				idx.Find(i % len(data))
			}
		})
	}
}

// Serial Execution (unpack simple8 + compare kernel)

func BenchmarkCmpEqualUnpacked(b *testing.B) {
	CmpEqualUnpackedBenchmark[uint64](b)
	CmpEqualUnpackedBenchmark[uint32](b)
	CmpEqualUnpackedBenchmark[uint16](b)
	CmpEqualUnpackedBenchmark[uint8](b)
}

func CmpEqualUnpackedBenchmark[T types.Unsigned](b *testing.B) {
	if cpu.UseAVX2 {
		b.Log("AVX2 enabled")
	} else {
		b.Log("WARN: using generic algorithms only")
	}
	for _, c := range tests.MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf, err := generic.Encode[T](make([]byte, 8*len(c.Data)), c.Data, minv, maxv)
		require.NoError(b, err)
		bits := bitset.New(len(c.Data))
		val := c.Data[len(c.Data)/2]
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * int(unsafe.Sizeof(T(0)))))
			for b.Loop() {
				dst := make([]T, len(c.Data))
				var n int64
				switch any(T(0)).(type) {
				case uint64:
					u64 := util.ReinterpretSlice[T, uint64](dst)
					_, err = DecodeUint64(u64, buf, uint64(minv))
					require.NoError(b, err)
					n = cmp.Uint64Equal(u64, uint64(val), bits.Bytes())
				case uint32:
					u32 := util.ReinterpretSlice[T, uint32](dst)
					_, err = DecodeUint32(u32, buf, uint32(minv))
					require.NoError(b, err)
					n = cmp.Uint32Equal(u32, uint32(val), bits.Bytes())
				case uint16:
					u16 := util.ReinterpretSlice[T, uint16](dst)
					_, err = DecodeUint16(u16, buf, uint16(minv))
					require.NoError(b, err)
					n = cmp.Uint16Equal(u16, uint16(val), bits.Bytes())
				case uint8:
					u8 := util.ReinterpretSlice[T, uint8](dst)
					_, err = DecodeUint8(u8, buf, uint8(minv))
					require.NoError(b, err)
					n = cmp.Uint8Equal(u8, uint8(val), bits.Bytes())
				}
				bits.ResetCount(int(n))
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}
