// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hash

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"testing"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/hash/xxhash32"
	"blockwatch.cc/knoxdb/internal/hash/xxhash64"
	"blockwatch.cc/knoxdb/internal/tests"
)

var benchSizes = []int{4, 8, 16, 64, 128, 1024}

func BenchmarkHash(b *testing.B) {
	HashBench(b, "fnv", func(buf []byte) uint64 {
		h := fnv.New64a()
		h.Write(buf)
		return h.Sum64()
	})
	HashBench(b, "xxhash64", xxhash64.Sum64)
	HashBench(b, "xxhash32", func(buf []byte) uint64 {
		return uint64(xxhash32.Checksum(buf, 0))
	})
	HashBench(b, "wyhash", func(buf []byte) uint64 {
		return WyHash(buf, 0)
	})
	HashBench(b, "aeshash", func(buf []byte) uint64 {
		return MemHash(buf, 0)
	})
}

func HashBench(b *testing.B, name string, fn func([]byte) uint64) {
	for _, sz := range benchSizes {
		buf := bytes.Repeat([]byte{0xFA}, sz)
		b.Run(fmt.Sprintf("%s/%d", name, sz), func(b *testing.B) {
			b.SetBytes(int64(sz))
			for range b.N {
				_ = fn(buf)
			}
		})
	}
}

// ------------------------------
// Integer hashers
//

func BenchmarkXxhashVec64(b *testing.B) {
	b.SetBytes(8)
	for range b.N {
		_ = xxhash.Hash64u64(42)
	}
}

func BenchmarkXxhashVec32(b *testing.B) {
	b.SetBytes(8)
	for range b.N {
		_ = xxhash.Hash32u64(42, 0)
	}
}

func BenchmarkXxh3Vec64(b *testing.B) {
	b.SetBytes(8)
	for range b.N {
		_ = xxhash.XXH3u64(42)
	}
}

func BenchmarkWyhash64(b *testing.B) {
	b.SetBytes(8)
	for range b.N {
		_ = WyHash64(42, 0)
	}
}

func BenchmarkWyhash32(b *testing.B) {
	b.SetBytes(4)
	for range b.N {
		_ = WyHash32(42, 0)
	}
}

func BenchmarkMultiHash(b *testing.B) {
	HashBenchMulti64(b, "xxhash64", xxhash.Vec64u64)
	HashBenchMulti32(b, "xxhash32", xxhash.Vec32u64)
	HashBenchMulti64(b, "xxh3", xxhash.VecXXH3u64)
	HashBenchMulti64(b, "wyhash", WyVec64u64)
}

func HashBenchMulti64(b *testing.B, name string, fn func([]uint64, []uint64) []uint64) {
	for _, sz := range tests.BenchmarkSizes {
		data := tests.GenRnd[uint64](sz.N)
		res := make([]uint64, sz.N)
		b.Run(fmt.Sprintf("%s/%s", name, sz.Name), func(b *testing.B) {
			b.SetBytes(int64(sz.N) * 8)
			for range b.N {
				_ = fn(data, res)
			}
		})
	}
}

func HashBenchMulti32(b *testing.B, name string, fn func([]uint64, []uint32, uint32) []uint32) {
	for _, sz := range tests.BenchmarkSizes {
		data := tests.GenRnd[uint64](sz.N)
		res := make([]uint32, sz.N)
		b.Run(fmt.Sprintf("%s/%s", name, sz.Name), func(b *testing.B) {
			b.SetBytes(int64(sz.N) * 8)
			for range b.N {
				fn(data, res, 0)
			}
		})
	}
}
