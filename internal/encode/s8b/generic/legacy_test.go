// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"slices"
	"testing"

	stests "blockwatch.cc/knoxdb/internal/encode/s8b/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestLegacy(t *testing.T) {
	for _, test := range stests.MakeTests[uint64]() {
		t.Run(test.Name, func(t *testing.T) {
			in := test.Data
			if test.Gen != nil {
				in = test.Gen()
			}
			encoded, err := EncodeLegacy(slices.Clone(in))
			if test.Err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			decoded := make([]uint64, len(in))
			n, err := DecodeLegacy(decoded, encoded)
			require.NoError(t, err)
			if len(encoded) > 0 {
				require.Equal(t, in, decoded[:n])
			}
		})
	}
}

func BenchmarkEncodeLegacy(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		b.Run("uint64/"+c.Name, func(b *testing.B) {
			b.SetBytes(int64(8 * len(c.Data)))
			for range b.N {
				EncodeLegacy(slices.Clone(c.Data))
			}
		})
	}
}

func BenchmarkDecodeLegacy(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		enc, _ := EncodeLegacy(slices.Clone(c.Data))
		dec := make([]uint64, len(c.Data))
		b.Run("uint64/"+c.Name, func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * 8))
			for range b.N {
				_, _ = DecodeLegacy(dec, enc)
			}
		})
	}
}

func BenchmarkCountLegacy(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		enc, _ := EncodeLegacy(slices.Clone(c.Data))
		b.Run("uint64/"+c.Name, func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * 8))
			for range b.N {
				_, _ = CountLegacy(util.ToByteSlice(enc))
			}
		})
	}
}
