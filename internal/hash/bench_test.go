// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hash

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
)

var benchSizes = []int{4, 8, 16, 64, 128, 1024}

func BenchmarkHash(b *testing.B) {
	HashBench(b, "fnv", func(buf []byte) uint64 {
		h := fnv.New64a()
		h.Write(buf)
		return h.Sum64()
	})
	// HashBench(b, "xxhash64", xxhash64.Sum64)
	// HashBench(b, "xxhash32", func(buf []byte) uint64 {
	// 	return uint64(xxhash32.Checksum(buf, 0))
	// })
	HashBench(b, "xxh3", Hash)
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
			for b.Loop() {
				_ = fn(buf)
			}
		})
	}
}

// ------------------------------
// Integer hashers
//

func BenchmarkXxhash64(b *testing.B) {
	b.SetBytes(8)
	for b.Loop() {
		_ = Uint64(42)
	}
}

func BenchmarkXxhash32(b *testing.B) {
	b.SetBytes(4)
	for b.Loop() {
		_ = Uint32(42)
	}
}

func BenchmarkWyhash64(b *testing.B) {
	b.SetBytes(8)
	for b.Loop() {
		_ = WyHash64(42, 0)
	}
}

func BenchmarkWyhash32(b *testing.B) {
	b.SetBytes(4)
	for b.Loop() {
		_ = WyHash32(42, 0)
	}
}

func BenchmarkMultiHash(b *testing.B) {
	HashBenchMulti64(b, "xxh3_64", Vec64)
	HashBenchMulti64(b, "wyhash64", WyVec64u64)
}

type BenchmarkSize struct {
	Name string
	N    int
}

var BenchmarkSizes = []BenchmarkSize{
	{"1k", 1024},
	{"16k", 16 * 1024},
	{"64k", 64 * 1024},
}

func HashBenchMulti64(b *testing.B, name string, fn func([]uint64, []uint64) []uint64) {
	for _, sz := range BenchmarkSizes {
		data := util.RandUints[uint64](sz.N)
		res := make([]uint64, sz.N)
		b.Run(fmt.Sprintf("%s/%s", name, sz.Name), func(b *testing.B) {
			b.SetBytes(int64(sz.N) * 8)
			for b.Loop() {
				_ = fn(data, res)
			}
		})
	}
}

func HashBenchMulti32(b *testing.B, name string, fn func([]uint64, []uint32, uint32) []uint32) {
	for _, sz := range BenchmarkSizes {
		data := util.RandUints[uint64](sz.N)
		res := make([]uint32, sz.N)
		b.Run(fmt.Sprintf("%s/%s", name, sz.Name), func(b *testing.B) {
			b.SetBytes(int64(sz.N) * 8)
			for b.Loop() {
				fn(data, res, 0)
			}
		})
	}
}
