// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode_bench

import (
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/filter/llb"
	"blockwatch.cc/knoxdb/internal/hash"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/xroar"
)

// -----------------------------------------------
// Microbenchmarks
//

func BenchmarkUniqueMap(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[int16](c.N)
		var card int
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				u := make(map[int16]struct{}, c.N)
				for _, v := range data {
					u[v] = struct{}{}
				}
				card = len(u)
			}
			_ = card
		})
	}
}

func BenchmarkUniqueArray(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[int16](c.N)
		minx := slices.Min(data)
		maxx := slices.Max(data)
		var card int
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				u := make([]uint16, int(maxx)-int(minx)+1)
				for _, v := range data {
					u[int(v)-int(minx)] = 1
				}
				for _, v := range u {
					if v > 0 {
						card++
					}
				}
			}
		})
	}
}

func BenchmarkUniqueBitset(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[int16](c.N)
		minx := slices.Min(data)
		maxx := slices.Max(data)
		var card int
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				u := bitset.New(int(maxx) - int(minx) + 1)
				for _, v := range data {
					u.Set(int(v) - int(minx))
				}
				card = u.Count()
				u.Close()
			}
		})
		_ = card
	}
}

func BenchmarkUniqueRoaring(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[int16](c.N)
		minx := slices.Min(data)
		var card int
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				u := xroar.New()
				for _, v := range data {
					u.Set(uint64(v) - uint64(minx))
				}
				card = u.Count()
			}
		})
		_ = card
	}
}

func BenchmarkUniqueLLB(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[uint32](c.N)
		var card int
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				hashes := hash.Vec32(
					arena.Alloc[uint64](len(data))[:len(data)],
					data,
				)
				var scratch [256]byte // need 256 byte scratch space
				unique, _ := llb.NewFilterBuffer(scratch[:], 8)
				unique.Add(hashes...)
				card = int(unique.Cardinality())
				arena.Free(data)
			}
		})
		_ = card
	}
}
