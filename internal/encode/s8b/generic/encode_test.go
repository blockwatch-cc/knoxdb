// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"slices"
	"testing"

	stests "blockwatch.cc/knoxdb/internal/encode/s8b/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {
	stests.EncodeTest[uint64](t, Encode[uint64], DecodeLegacyWrapper[uint64])
	stests.EncodeTest[uint32](t, Encode[uint32], DecodeLegacyWrapper[uint32])
	stests.EncodeTest[uint16](t, Encode[uint16], DecodeLegacyWrapper[uint16])
	stests.EncodeTest[uint8](t, Encode[uint8], DecodeLegacyWrapper[uint8])
}

func TestDecode(t *testing.T) {
	stests.EncodeTest[uint64](t, Encode[uint64], Decode[uint64])
	stests.EncodeTest[uint32](t, Encode[uint32], Decode[uint32])
	stests.EncodeTest[uint16](t, Encode[uint16], Decode[uint16])
	stests.EncodeTest[uint8](t, Encode[uint8], Decode[uint8])
}

func DecodeLegacyWrapper[T types.Unsigned](dst []T, buf []byte) (int, error) {
	src := util.FromByteSlice[uint64](buf)
	switch any(T(0)).(type) {
	case uint64:
		return DecodeLegacy(util.ReinterpretSlice[T, uint64](dst), src)
	default:
		u64 := make([]uint64, len(dst))
		n, err := DecodeLegacy(u64, src)
		if err != nil {
			return 0, err
		}
		for i := 0; i < n; i++ {
			dst[i] = T(u64[i])
		}
		return n, nil
	}
}

func BenchmarkEncode(b *testing.B) {
	stests.EncodeBenchmark[uint64](b, Encode[uint64])
	stests.EncodeBenchmark[uint32](b, Encode[uint32])
	stests.EncodeBenchmark[uint16](b, Encode[uint16])
	stests.EncodeBenchmark[uint8](b, Encode[uint8])
}

func BenchmarkDecode(b *testing.B) {
	stests.DecodeBenchmark[uint64](b, Encode[uint64], Decode[uint64])
	stests.DecodeBenchmark[uint32](b, Encode[uint32], Decode[uint32])
	stests.DecodeBenchmark[uint16](b, Encode[uint16], Decode[uint16])
	stests.DecodeBenchmark[uint8](b, Encode[uint8], Decode[uint8])
}

func BenchmarkCount(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf, err := Encode[uint64](make([]byte, 8*len(c.Data)), c.Data, minv, maxv)
		require.NoError(b, err)
		b.Run("uint64/"+c.Name, func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * 8))
			for i := 0; i < b.N; i++ {
				_ = CountValues(buf)
			}
		})
	}
}
