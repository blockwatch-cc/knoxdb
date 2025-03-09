// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hash

import (
	"encoding/binary"
	"hash/fnv"
	"testing"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/hash/xxhashVec"
)

func BenchmarkFnv(b *testing.B) {
	b.SetBytes(8)
	for i := 0; i < b.N; i++ {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], 42)
		h := fnv.New64a()
		h.Write(b[:])
		_ = h.Sum64()
	}
}

func BenchmarkXxhash64(b *testing.B) {
	b.SetBytes(8)
	for i := 0; i < b.N; i++ {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], 42)
		_ = xxhash.Sum64(b[:])
	}
}

func BenchmarkXxhashVec64(b *testing.B) {
	b.SetBytes(8)
	for i := 0; i < b.N; i++ {
		_ = xxhashVec.XXHash64Uint64(42)
	}
}

func BenchmarkXxhashVec32(b *testing.B) {
	b.SetBytes(8)
	for i := 0; i < b.N; i++ {
		_ = HashUint64(42).Uint64()
	}
}

func BenchmarkXxh3Vec64(b *testing.B) {
	b.SetBytes(8)
	for i := 0; i < b.N; i++ {
		_ = xxhashVec.XXH3Uint64(42)
	}
}
