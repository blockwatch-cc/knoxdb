// Copyright (c) 2018-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitpack

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/encode/tests"
)

func BenchmarkCmpEqual(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		buf := make([]byte, 2*c.N)
		data := tests.GenDups[uint16](c.N, 10)
		bits := bitset.NewBitset(c.N)
		for d := range 16 {
			PackVec(buf, data, d)
			b.Run(fmt.Sprintf("%s/%d_bits", c.Name, d), func(b *testing.B) {
				b.ResetTimer()
				b.SetBytes(int64(2 * c.N))
				for i := 0; i < b.N; i++ {
					Equal[d](buf, 42, c.N, bits)
				}
			})
		}
	}
}

func BenchmarkCmpEqualUnpacked(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		buf := make([]byte, 2*c.N)
		data := tests.GenDups[uint16](c.N, 10)
		bits := bitset.NewBitset(c.N)
		for d := range 16 {
			PackVec(buf, data, d)
			unpacked := make([]uint16, c.N)
			b.Run(fmt.Sprintf("%s/%d_bits", c.Name, d), func(b *testing.B) {
				b.ResetTimer()
				b.SetBytes(int64(2 * c.N))
				for i := 0; i < b.N; i++ {
					UnpackVec(buf, unpacked, d)
					cmp.MatchUint16Equal(unpacked, 42, bits, nil)
				}
			})
		}
	}
}
