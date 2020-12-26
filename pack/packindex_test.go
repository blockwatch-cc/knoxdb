// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"testing/quick"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/vec"
)

func randUint64Slice(n, u int) []uint64 {
	s := make([]uint64, n*u)
	for i := 0; i < n; i++ {
		s[i] = uint64(rand.Int63())
	}
	for i := 0; i < u; i++ {
		s = append(s, s[:n]...)
	}
	return s
}

func makeSortedPackageHeaderList(n int) PackageHeaderList {
	// generate random values
	values := randUint64Slice(n, 1)

	// strip duplicates and sort
	values = vec.UniqueUint64Slice(values)

	// generate pack headers
	heads := make(PackageHeaderList, 0)
	for i, v := range values {
		max := uint64(v + 1000)
		if i < len(values)-1 {
			max = values[i+1] - 1
		}
		head := PackageHeader{
			Key:     make([]byte, 4),
			NFields: 1,
			NValues: 1,
			BlockHeaders: block.HeaderList{
				block.Header{
					MinValue: uint64(v),
					MaxValue: max,
				},
			},
		}
		binary.BigEndian.PutUint32(head.Key, uint32(i))
		heads = append(heads, head)
	}
	return heads
}

func makeUnsortedPackageHeaderList(n int) PackageHeaderList {
	// generate random values
	values := randUint64Slice(n, 1)

	// strip duplicates and sort
	values = vec.UniqueUint64Slice(values)
	maxvalues := make([]uint64, len(values))
	minvalues := make([]uint64, len(values))

	// shuffle but keep original max values
	for i, v := range rand.Perm(len(values)) {
		max := uint64(values[v] + 1000)
		if v < len(values)-1 {
			max = values[v+1] - 1
		}
		minvalues[i] = values[v]
		maxvalues[i] = max
	}

	// generate pack headers
	heads := make(PackageHeaderList, 0)
	for i, v := range minvalues {
		head := PackageHeader{
			Key:     make([]byte, 4),
			NFields: 1,
			NValues: 1,
			BlockHeaders: block.HeaderList{
				block.Header{
					MinValue: v,
					MaxValue: maxvalues[i],
				},
			},
		}
		binary.BigEndian.PutUint32(head.Key, uint32(i))
		heads = append(heads, head)
	}
	return heads
}

func TestBestPackSorted_Quick(t *testing.T) {
	for _, n := range []int{
		0,
		1,
		1000,
		10000,
	} {
		t.Run(fmt.Sprintf("%d", n), func(T *testing.T) {
			heads := makeSortedPackageHeaderList(n)
			v1 := NewPackIndexV1(heads, 0)
			h2 := make(PackageHeaderList, len(heads))
			copy(h2, heads)
			v2 := NewPackIndex(h2, 0)
			err := quick.CheckEqual(
				func(val uint64, last int) (int, uint64, uint64) {
					if last < 0 {
						last = -last
					}
					if n > 0 {
						last = last % n
					} else {
						last = 0
					}
					return v1.Best(val, last)
				}, func(val uint64, last int) (int, uint64, uint64) {
					return v2.Best(val)
				},
				nil,
			)
			if err != nil {
				T.Error(err)
			}
		})
	}
}

func TestBestPackUnsorted_Quick(t *testing.T) {
	for _, n := range []int{
		0,
		1,
		10,
		1000,
		10000,
	} {
		t.Run(fmt.Sprintf("%d", n), func(T *testing.T) {
			heads := makeUnsortedPackageHeaderList(n)
			for _, v := range heads {
				T.Logf("key %08x min %016x max %016x", v.Key, v.BlockHeaders[0].MinValue, v.BlockHeaders[0].MaxValue)
			}
			h2 := make(PackageHeaderList, len(heads))
			copy(h2, heads)
			v1 := NewPackIndexV1(heads, 0)
			v2 := NewPackIndex(h2, 0)
			err := quick.CheckEqual(
				func(val uint64, last int) (int, uint64, uint64) {
					if last < 0 {
						last = -last
					}
					if n > 0 {
						last = last % n
					} else {
						last = 0
					}
					return v1.Best(val, last)
				}, func(val uint64, last int) (int, uint64, uint64) {
					return v2.Best(val)
				},
				nil,
			)
			if err != nil {
				T.Error(err)
			}
		})
	}
}

type packIndexTestListItem struct {
	Key []byte
	Min uint64
	Max uint64
}

type packIndexTestValueItem struct {
	Value  uint64
	ExpKey []byte
	ExpMin uint64
	ExpMax uint64
	NotV1  bool
	NotV2  bool
}

type packIndexTestCase struct {
	Name   string
	List   []packIndexTestListItem
	Values []packIndexTestValueItem
}

var packIndexTestCases = []packIndexTestCase{
	packIndexTestCase{
		Name: "single",
		List: []packIndexTestListItem{
			packIndexTestListItem{
				Key: []byte{0x0, 0x0, 0x0, 0x1},
				Min: 1000,
				Max: 2000,
			},
		},
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			packIndexTestValueItem{
				Value:  100,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact min match
			packIndexTestValueItem{
				Value:  1000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			packIndexTestValueItem{
				Value:  2000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// after max match
			packIndexTestValueItem{
				Value:  3000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
		},
	},
	packIndexTestCase{
		Name: "multi-sorted",
		List: []packIndexTestListItem{
			packIndexTestListItem{
				Key: []byte{0x0, 0x0, 0x0, 0x1},
				Min: 1000,
				Max: 2000,
			},
			packIndexTestListItem{
				Key: []byte{0x0, 0x0, 0x0, 0x2},
				Min: 3000,
				Max: 4000,
			},
		},
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			packIndexTestValueItem{
				Value:  100,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact min first match
			packIndexTestValueItem{
				Value:  1000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// min between match
			packIndexTestValueItem{
				Value:  1500,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max first match
			packIndexTestValueItem{
				Value:  2000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// max first +1
			packIndexTestValueItem{
				Value:  2001,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// min second -1 (v2 selects left pack)
			packIndexTestValueItem{
				Value:  2999,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
				NotV1:  true,
			},
			// min second -1 (v1 selects closes pack)
			packIndexTestValueItem{
				Value:  2999,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x2},
				ExpMin: 3000,
				ExpMax: 4000,
				NotV2:  true,
			},
			// within second pack
			packIndexTestValueItem{
				Value:  3500,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x2},
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// after max match
			packIndexTestValueItem{
				Value:  5000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x2},
				ExpMin: 3000,
				ExpMax: 4000,
			},
		},
	},
}

func buildPackHeader(key []byte, min, max uint64) PackageHeader {
	return PackageHeader{
		Key:     key,
		NFields: 1,
		NValues: 1,
		BlockHeaders: block.HeaderList{
			block.Header{
				MinValue: min,
				MaxValue: max,
			},
		},
	}
}

func buildPackHeaderInt(key int, min, max uint64) PackageHeader {
	var k [4]byte
	binary.BigEndian.PutUint32(k[:], uint32(key))
	return buildPackHeader(k[:], min, max)
}

func buildPackHeaderList(items []packIndexTestListItem) PackageHeaderList {
	heads := make(PackageHeaderList, len(items))
	for i, v := range items {
		heads[i] = buildPackHeader(v.Key, v.Min, v.Max)
	}
	return heads
}

func TestPackIndexBest(t *testing.T) {
	for _, c := range packIndexTestCases {
		h1 := buildPackHeaderList(c.List)
		h2 := buildPackHeaderList(c.List)
		v1 := NewPackIndexV1(h1, 0)
		v2 := NewPackIndex(h2, 0)

		// test on v1 impl
		for _, v := range c.Values {
			if v.NotV1 {
				continue
			}
			p, min, max := v1.Best(v.Value, 0)
			if exp, got := v.ExpKey, v1.heads[p].Key; bytes.Compare(exp, got) != 0 {
				// min, max := v1.MinMax(p1)
				t.Errorf("invalid pack selected by v1 exp=%08x [%d/%d] got=%08x [%d/%d] for value %d",
					exp, v.ExpMin, v.ExpMax, got, min, max, v.Value)
			}
		}

		// test on v2 impl
		for _, v := range c.Values {
			if v.NotV2 {
				continue
			}
			p, min, max := v2.Best(v.Value)
			if exp, got := v.ExpKey, v1.heads[p].Key; bytes.Compare(exp, got) != 0 {
				// min, max := v1.MinMax(p1)
				t.Errorf("invalid pack selected by v2 exp=%08x [%d/%d] got=%08x [%d/%d] for value %d",
					exp, v.ExpMin, v.ExpMax, got, min, max, v.Value)
			}
		}
	}
}

var packListAddTestCases = []packIndexTestCase{
	packIndexTestCase{
		Name: "add_middle",
		List: []packIndexTestListItem{
			packIndexTestListItem{
				Key: []byte{0x0, 0x0, 0x0, 0x1},
				Min: 1000,
				Max: 2000,
			},
			packIndexTestListItem{
				Key: []byte{0x0, 0x0, 0x0, 0x2},
				Min: 5000,
				Max: 6000,
			},
		},
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			packIndexTestValueItem{
				Value:  100,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact min match
			packIndexTestValueItem{
				Value:  1000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			packIndexTestValueItem{
				Value:  2000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// added pack min match
			packIndexTestValueItem{
				Value:  3000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x3},
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// added pack middle match
			packIndexTestValueItem{
				Value:  3500,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x3},
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// added pack max match
			packIndexTestValueItem{
				Value:  4000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x3},
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// added pack max + 1 match
			packIndexTestValueItem{
				Value:  4001,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x3},
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// last pack match
			packIndexTestValueItem{
				Value:  5500,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x2},
				ExpMin: 5000,
				ExpMax: 6000,
			},
		},
	},
}

func TestPackIndexAfterAdd(t *testing.T) {
	for _, c := range packListAddTestCases {
		h1 := buildPackHeaderList(c.List)
		h2 := buildPackHeaderList(c.List)
		v1 := NewPackIndexV1(h1, 0)
		v2 := NewPackIndex(h2, 0)

		// add an new middle pack
		head := buildPackHeaderInt(3, 3000, 4000)
		v1.AddOrUpdate(head)
		v2.AddOrUpdate(head)

		// test on v1 impl
		for _, v := range c.Values {
			if v.NotV1 {
				continue
			}
			p, min, max := v1.Best(v.Value, 0)
			if exp, got := v.ExpKey, v1.heads[p].Key; bytes.Compare(exp, got) != 0 {
				// min, max := v1.MinMax(p1)
				t.Errorf("invalid pack selected by v1 exp=%08x [%d/%d] got=%08x [%d/%d] for value %d",
					exp, v.ExpMin, v.ExpMax, got, min, max, v.Value)
			}
		}

		// test on v2 impl
		for _, v := range c.Values {
			if v.NotV2 {
				continue
			}
			p, min, max := v2.Best(v.Value)
			if exp, got := v.ExpKey, v1.heads[p].Key; bytes.Compare(exp, got) != 0 {
				// min, max := v1.MinMax(p1)
				t.Errorf("invalid pack selected by v2 exp=%08x [%d/%d] got=%08x [%d/%d] for value %d",
					exp, v.ExpMin, v.ExpMax, got, min, max, v.Value)
			}
		}
	}
}

var packListRemoveTestCases = []packIndexTestCase{
	packIndexTestCase{
		Name: "remove_middle",
		List: []packIndexTestListItem{
			packIndexTestListItem{
				Key: []byte{0x0, 0x0, 0x0, 0x1},
				Min: 1000,
				Max: 2000,
			},
			packIndexTestListItem{
				Key: []byte{0x0, 0x0, 0x0, 0x2},
				Min: 3000,
				Max: 4000,
			},
			packIndexTestListItem{
				Key: []byte{0x0, 0x0, 0x0, 0x3},
				Min: 5000,
				Max: 6000,
			},
		},
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			packIndexTestValueItem{
				Value:  100,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact min match
			packIndexTestValueItem{
				Value:  1000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			packIndexTestValueItem{
				Value:  2000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// removed pack value redirected to min
			packIndexTestValueItem{
				Value:  3000,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// removed pack value closer to next in V1
			packIndexTestValueItem{
				Value:  3501,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x3},
				ExpMin: 5000,
				ExpMax: 6000,
				NotV2:  true,
			},
			// removed pack value closer to previous in V2
			packIndexTestValueItem{
				Value:  3501,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x1},
				ExpMin: 1000,
				ExpMax: 2000,
				NotV1:  true,
			},
			// last pack match
			packIndexTestValueItem{
				Value:  5500,
				ExpKey: []byte{0x0, 0x0, 0x0, 0x3},
				ExpMin: 5000,
				ExpMax: 6000,
			},
		},
	},
}

func TestPackIndexAfterRemove(t *testing.T) {
	for _, c := range packListRemoveTestCases {
		h1 := buildPackHeaderList(c.List)
		h2 := buildPackHeaderList(c.List)
		v1 := NewPackIndexV1(h1, 0)
		v2 := NewPackIndex(h2, 0)

		// add an new middle pack
		head := buildPackHeaderInt(2, 3000, 4000)
		v1.Remove(head)
		v2.Remove(head)

		// test on v1 impl
		for _, v := range c.Values {
			if v.NotV1 {
				continue
			}
			p, min, max := v1.Best(v.Value, 0)
			if exp, got := v.ExpKey, v1.heads[p].Key; bytes.Compare(exp, got) != 0 {
				// min, max := v1.MinMax(p1)
				t.Errorf("invalid pack selected by v1 exp=%08x [%d/%d] got=%08x [%d/%d] for value %d",
					exp, v.ExpMin, v.ExpMax, got, min, max, v.Value)
			}
		}

		// test on v2 impl
		for _, v := range c.Values {
			if v.NotV2 {
				continue
			}
			p, min, max := v2.Best(v.Value)
			if exp, got := v.ExpKey, v1.heads[p].Key; bytes.Compare(exp, got) != 0 {
				// min, max := v1.MinMax(p1)
				t.Errorf("invalid pack selected by v2 exp=%08x [%d/%d] got=%08x [%d/%d] for value %d",
					exp, v.ExpMin, v.ExpMax, got, min, max, v.Value)
			}
		}
	}
}

type benchmarkSize struct {
	name string
	l    int
}

var bestPackBenchmarkSizes = []benchmarkSize{
	{"1", 1},
	{"10", 10},
	{"1K", 1024},
	{"16k", 16 * 1024},
	{"32k", 32 * 1024},
	{"64k", 64 * 1024},
}

func BenchmarkPackIndexBestSortedV1(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v1 := NewPackIndexV1(makeSortedPackageHeaderList(n.l), 0)
			max := v1.maxpks[v1.Len()-1]
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				v1.Best(uint64(rand.Int63n(int64(max))+1), 0)
			}
		})
	}
}

func BenchmarkPackIndexBestUnsortedV1(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v1 := NewPackIndexV1(makeUnsortedPackageHeaderList(n.l), 0)
			max := v1.maxpks[v1.Len()-1]
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				v1.Best(uint64(rand.Int63n(int64(max))+1), 0)
			}
		})
	}
}

func BenchmarkPackIndexAppendV1(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v1 := NewPackIndexV1(makeSortedPackageHeaderList(n.l), 0)
			l := v1.Len()
			max := v1.maxpks[v1.Len()-1]
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				v1.AddOrUpdate(buildPackHeaderInt(i+l, max+1, max+1000))
				max += 1000
			}
		})
	}
}

func BenchmarkPackIndexAddV1(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v1 := NewPackIndexV1(makeSortedPackageHeaderList(n.l), 0)
			l := v1.Len() / 2
			min, max := v1.MinMax(l)
			head := buildPackHeaderInt(l, min, max)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				// remove and re-append an existing header
				v1.Remove(head)
				v1.AddOrUpdate(head)
			}
		})
	}
}

func BenchmarkPackIndexUpdateV1(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v1 := NewPackIndexV1(makeSortedPackageHeaderList(n.l), 0)
			pos, min, max := v1.Len()/2, v1.minpks[v1.Len()/2], v1.maxpks[v1.Len()/2]
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				// replace the middle pack, toggle min between min and min+1
				// to force updates
				setmin := min
				if i&0x1 == 1 {
					setmin = min + 1
				}
				v1.AddOrUpdate(buildPackHeaderInt(pos, setmin, max))
			}
		})
	}
}

func BenchmarkPackIndexBestSortedV2(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v2 := NewPackIndex(makeSortedPackageHeaderList(n.l), 0)
			max := v2.heads[v2.pairs[len(v2.pairs)-1].pos].BlockHeaders[v2.pkidx].MaxValue.(uint64)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				v2.Best(uint64(rand.Int63n(int64(max)) + 1))
			}
		})
	}
}

func BenchmarkPackIndexBestUnsortedV2(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v2 := NewPackIndex(makeUnsortedPackageHeaderList(n.l), 0)
			max := v2.heads[v2.pairs[len(v2.pairs)-1].pos].BlockHeaders[v2.pkidx].MaxValue.(uint64)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				v2.Best(uint64(rand.Int63n(int64(max)) + 1))
			}
		})
	}
}

func BenchmarkPackIndexAppendV2(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v2 := NewPackIndex(makeSortedPackageHeaderList(n.l), 0)
			l := v2.Len()
			_, max := v2.MinMax(l - 1)
			// head := buildPackHeaderInt(l, max+1, max+1000)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				// append to end of list
				v2.AddOrUpdate(buildPackHeaderInt(i+l, max+1, max+1000))
				max += 1000
			}
		})
	}
}

func BenchmarkPackIndexAddV2(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v2 := NewPackIndex(makeSortedPackageHeaderList(n.l), 0)
			l := v2.Len() / 2
			min, max := v2.MinMax(l)
			head := buildPackHeaderInt(l, min, max)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				// remove and re-append an existing header
				v2.Remove(head)
				v2.AddOrUpdate(head)
			}
		})
	}
}

func BenchmarkPackIndexUpdateV2(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v2 := NewPackIndex(makeSortedPackageHeaderList(n.l), 0)
			pos := v2.Len() / 2
			min, max := v2.MinMax(pos)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				// replace the middle pack, toggle min between min and min+1
				// to force updates
				setmin := min
				if i&0x1 == 1 {
					setmin = min + 1
				}
				v2.AddOrUpdate(buildPackHeaderInt(pos, setmin, max))
			}
		})
	}
}

// DEPRECATED V1 for benchmarking and testing
//
type PackIndexV1 struct {
	heads  PackageHeaderList
	deads  PackageHeaderList
	pkidx  int
	minpks []uint64
	maxpks []uint64
}

// may be used in {Index|Table}.loadPackHeaders
func NewPackIndexV1(heads PackageHeaderList, pkidx int) *PackIndexV1 {
	if heads == nil {
		heads = make(PackageHeaderList, 0)
	}
	l := &PackIndexV1{
		heads:  heads,
		pkidx:  pkidx,
		minpks: make([]uint64, len(heads), cap(heads)),
		maxpks: make([]uint64, len(heads), cap(heads)),
	}
	sort.Sort(l.heads)
	l.rebuild()
	return l
}

func (l *PackIndexV1) Len() int {
	return len(l.heads)
}

func (l *PackIndexV1) MinMax(n int) (uint64, uint64) {
	if n >= l.Len() {
		return 0, 0
	}
	bh := l.heads[n].BlockHeaders[l.pkidx]
	return bh.MinValue.(uint64), bh.MaxValue.(uint64)
}

// called by storePack
func (l *PackIndexV1) AddOrUpdate(head PackageHeader) {
	head.dirty = true
	l.deads.RemoveKey(head.Key)
	l.heads.Add(head)
	l.rebuild()
}

// called by storePack when packs are empty (Table only)
func (l *PackIndexV1) Remove(head PackageHeader) {
	l.heads.RemoveKey(head.Key)
	l.deads.Add(head)
	l.rebuild()
}

// run a full rebuild because the list has changed
func (l *PackIndexV1) rebuild() {
	numpacks := l.Len()
	if l.minpks == nil || cap(l.minpks) < numpacks {
		l.minpks = make([]uint64, numpacks)
		l.maxpks = make([]uint64, numpacks)
	}
	l.minpks = l.minpks[:numpacks]
	l.maxpks = l.maxpks[:numpacks]
	for i := 0; i < numpacks; i++ {
		head := l.heads[i].BlockHeaders[l.pkidx]
		l.minpks[i], l.maxpks[i] = head.MinValue.(uint64), head.MaxValue.(uint64)
	}
}

// old and unscalable implementation from Index (almost similar to impl in Table)
func (l *PackIndexV1) Best(val uint64, lastpack int) (int, uint64, uint64) {
	numpacks := len(l.heads)

	// initially we stick to the first pack until split
	if numpacks == 0 {
		return lastpack, 0, 0
	}

	// collect best distance to any pack for step 2 below
	var (
		bestdist         uint64 = math.MaxUint64
		bestpack         int    = lastpack
		bestmin, bestmax uint64
	)

	// start search at current pack, i.e. lastpack and cycle through full list
	for i, n := lastpack, numpacks+lastpack; i < n; i++ {
		// Note: j := i % numpacks is slow
		j := i
		if j >= numpacks {
			j -= numpacks
		}
		// lookup min/max pks from header list (we use pre-built slices
		// for faster lookups and update them when the pack list changes on save)
		min, max := l.minpks[j], l.maxpks[j]
		if val >= min && val <= max {
			return j, min, max
		}
		if val < min {
			if dist := min - val; bestdist > dist {
				bestdist = dist
				bestpack = j
				bestmin = min
				bestmax = max
			}
		} else {
			if dist := val - max; bestdist > dist {
				bestdist = dist
				bestpack = j
				bestmin = min
				bestmax = max
			}
		}
	}

	// ----------------------------------------------
	// THIS IS THE DIFFERENCE IN TABLE IMPL
	//
	// make sure there's room in the pack
	// if t.heads[bestpack].NValues >= 1<<uint(t.opts.PackSizeLog2) {
	// 	return numpacks // triggers new pack creation
	// }
	// TABLE DIFF END
	// ----------------------------------------------

	return bestpack, bestmin, bestmax
}
