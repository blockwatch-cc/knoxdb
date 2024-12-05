// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package hash

import (
	"encoding/binary"
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/filter/cuckoo"
	"blockwatch.cc/knoxdb/internal/hash/fnv"
	"blockwatch.cc/knoxdb/pkg/util"
	"golang.org/x/exp/slices"
)

var filterTestSizes = []int{10000}

func BenchmarkUint64MapFromSorted(b *testing.B) {
	for _, n := range filterTestSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := util.RandUints[uint64](n)
			slices.Sort(a)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				m := make(map[uint64]struct{}, len(a))
				for _, v := range a {
					m[v] = struct{}{}
				}
			}
		})
	}
}

// Bytes(32) in hash map
func BenchmarkBytes32HashMap(b *testing.B) {
	for _, n := range filterTestSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := util.RandByteSlices(n, 32)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				m := make(map[uint64]struct{}, len(a))
				for _, v := range a {
					h := fnv.New64a()
					h.Write(v)
					m[h.Sum64()] = struct{}{}
				}
				if got, want := len(m), len(a); got != want {
					b.Errorf("hash collision got=%d want=%d", got, want)
				}
			}
		})
	}
}

// Bloom filter on uint64
// const maxFilterError float64 = 0.02
// const bloomFillFactor = 1
const cuckooFillFactor = 0.75

func BenchmarkUint64BloomFromUnsortedLE(b *testing.B) {
	for _, n := range filterTestSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := util.RandUints[uint64](n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filter := bloom.NewFilter(n * 4)
				for _, v := range a {
					var buf [8]byte
					binary.LittleEndian.PutUint64(buf[:], v)
					filter.Add(buf[:])
				}
			}
		})
	}
}

func BenchmarkUint64BloomFromSortedLE(b *testing.B) {
	for _, n := range filterTestSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := util.RandUints[uint64](n)
			slices.Sort(a)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filter := bloom.NewFilter(n * 4)
				for _, v := range a {
					var buf [8]byte
					binary.LittleEndian.PutUint64(buf[:], v)
					filter.Add(buf[:])
				}
			}
		})
	}
}

func BenchmarkUint64BloomFromUnsortedBE(b *testing.B) {
	for _, n := range filterTestSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := util.RandUints[uint64](n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filter := bloom.NewFilter(n * 4)
				for _, v := range a {
					var buf [8]byte
					binary.BigEndian.PutUint64(buf[:], v)
					filter.Add(buf[:])
				}
			}
		})
	}
}

func BenchmarkUint64BloomFromSortedBE(b *testing.B) {
	for _, n := range filterTestSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := util.RandUints[uint64](n)
			slices.Sort(a)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filter := bloom.NewFilter(n * 4)
				for _, v := range a {
					var buf [8]byte
					binary.BigEndian.PutUint64(buf[:], v)
					filter.Add(buf[:])
				}
			}
		})
	}
}

func BenchmarkBytes32Bloom(b *testing.B) {
	for _, n := range filterTestSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := util.RandByteSlices(n, 32)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filter := bloom.NewFilter(n * 4)
				for _, v := range a {
					filter.Add(v)
				}
			}
		})
	}
}

// Cuckoo filter on uint64
//

func BenchmarkUint64CuckooFromUnsortedLE(b *testing.B) {
	for _, n := range filterTestSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := util.RandUints[uint64](n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filter := cuckoo.NewFilter(uint(float64(len(a)) / cuckooFillFactor))
				for _, v := range a {
					var buf [8]byte
					binary.LittleEndian.PutUint64(buf[:], v)
					filter.Add(buf[:])
				}
			}
		})
	}
}

func BenchmarkUint64CuckooFromSortedLE(b *testing.B) {
	for _, n := range filterTestSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := util.RandUints[uint64](n)
			slices.Sort(a)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filter := cuckoo.NewFilter(uint(float64(len(a)) / cuckooFillFactor))
				for _, v := range a {
					var buf [8]byte
					binary.LittleEndian.PutUint64(buf[:], v)
					filter.Add(buf[:])
				}
			}
		})
	}
}

func BenchmarkUint64CuckooFromUnsortedBE(b *testing.B) {
	for _, n := range filterTestSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := util.RandUints[uint64](n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filter := cuckoo.NewFilter(uint(float64(len(a)) / cuckooFillFactor))
				for _, v := range a {
					var buf [8]byte
					binary.BigEndian.PutUint64(buf[:], v)
					filter.Add(buf[:])
				}
			}
		})
	}
}

func BenchmarkUint64CuckooFromSortedBE(b *testing.B) {
	for _, n := range filterTestSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := util.RandUints[uint64](n)
			slices.Sort(a)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filter := cuckoo.NewFilter(uint(float64(len(a)) / cuckooFillFactor))
				for _, v := range a {
					var buf [8]byte
					binary.BigEndian.PutUint64(buf[:], v)
					filter.Add(buf[:])
				}
			}
		})
	}
}

func BenchmarkBytes32Cuckoo(b *testing.B) {
	for _, n := range filterTestSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := util.RandByteSlices(n, 32)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filter := cuckoo.NewFilter(uint(float64(len(a)) / cuckooFillFactor))
				for _, v := range a {
					filter.Add(v)
				}
			}
		})
	}
}
