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

func BenchmarkUint64MapFromSorted(B *testing.B) {
	for _, n := range filterTestSizes {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := util.RandUints[uint64](n)
			slices.Sort(a)
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
				m := make(map[uint64]struct{}, len(a))
				for _, v := range a {
					m[v] = struct{}{}
				}
			}
		})
	}
}

// Bytes(32) in hash map
func BenchmarkBytes32HashMap(B *testing.B) {
	for _, n := range filterTestSizes {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := util.RandByteSlices(n, 32)
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
				m := make(map[uint64]struct{}, len(a))
				for _, v := range a {
					h := fnv.New64a()
					h.Write(v)
					m[h.Sum64()] = struct{}{}
				}
				if got, want := len(m), len(a); got != want {
					B.Errorf("hash collision got=%d want=%d", got, want)
				}
			}
		})
	}
}

// Bloom filter on uint64
const maxFilterError float64 = 0.02
const cuckooFillFactor = 0.75
const bloomFillFactor = 1

func BenchmarkUint64BloomFromUnsortedLE(B *testing.B) {
	for _, n := range filterTestSizes {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := util.RandUints[uint64](n)
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
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

func BenchmarkUint64BloomFromSortedLE(B *testing.B) {
	for _, n := range filterTestSizes {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := util.RandUints[uint64](n)
			slices.Sort(a)
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
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

func BenchmarkUint64BloomFromUnsortedBE(B *testing.B) {
	for _, n := range filterTestSizes {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := util.RandUints[uint64](n)
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
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

func BenchmarkUint64BloomFromSortedBE(B *testing.B) {
	for _, n := range filterTestSizes {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := util.RandUints[uint64](n)
			slices.Sort(a)
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
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

func BenchmarkBytes32Bloom(B *testing.B) {
	for _, n := range filterTestSizes {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := util.RandByteSlices(n, 32)
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
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

func BenchmarkUint64CuckooFromUnsortedLE(B *testing.B) {
	for _, n := range filterTestSizes {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := util.RandUints[uint64](n)
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
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

func BenchmarkUint64CuckooFromSortedLE(B *testing.B) {
	for _, n := range filterTestSizes {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := util.RandUints[uint64](n)
			slices.Sort(a)
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
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

func BenchmarkUint64CuckooFromUnsortedBE(B *testing.B) {
	for _, n := range filterTestSizes {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := util.RandUints[uint64](n)
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
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

func BenchmarkUint64CuckooFromSortedBE(B *testing.B) {
	for _, n := range filterTestSizes {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := util.RandUints[uint64](n)
			slices.Sort(a)
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
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

func BenchmarkBytes32Cuckoo(B *testing.B) {
	for _, n := range filterTestSizes {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := util.RandByteSlices(n, 32)
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
				filter := cuckoo.NewFilter(uint(float64(len(a)) / cuckooFillFactor))
				for _, v := range a {
					filter.Add(v)
				}
			}
		})
	}
}
