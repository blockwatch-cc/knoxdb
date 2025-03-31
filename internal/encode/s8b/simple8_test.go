// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package s8b

import (
	"slices"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/encode/s8b/tests"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/require"
)

type IndexFunc[T uint16 | uint32] func([]byte, []T) Index

func BenchmarkIndex16(b *testing.B) {
	IndexBenchmark[uint16](b, EncodeUint16, MakeIndex[uint16])
}

func BenchmarkIndex16Find(b *testing.B) {
	IndexFindBenchmark[uint16](b, EncodeUint16, MakeIndex[uint16])
}

func BenchmarkIndex32(b *testing.B) {
	IndexBenchmark[uint32](b, EncodeUint32, MakeIndex[uint32])
}

func BenchmarkIndex32Find(b *testing.B) {
	IndexFindBenchmark[uint32](b, EncodeUint32, MakeIndex[uint32])
}

func IndexBenchmark[T types.Unsigned, I uint16 | uint32](b *testing.B, enc tests.EncodeFunc[T], idx IndexFunc[I]) {
	for _, c := range etests.MakeBenchmarks[T]() {
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

func IndexFindBenchmark[T types.Unsigned, I uint16 | uint32](b *testing.B, enc tests.EncodeFunc[T], mkidx IndexFunc[I]) {
	for _, c := range etests.BenchmarkSizes {
		data := etests.GenRnd[T](c.N)
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
