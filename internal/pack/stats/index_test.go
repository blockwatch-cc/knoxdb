// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"testing"

	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
)

func randUint64Slice(n int) []uint64 {
	return slicex.Unique(util.RandUints[uint64](n))
}

func makeSortedPackStatsList(n int) PackStatsList {
	// generate random values, strip duplicates and sort
	values := randUint64Slice(n)

	// generate pack packers
	packs := make(PackStatsList, 0)
	for i, v := range values {
		max := v + 1000
		if i < len(values)-1 {
			max = values[i+1] - 1
		}
		pack := &PackStats{
			Key:     uint32(i),
			NValues: 1,
			Blocks: []BlockStats{
				{
					MinValue: v,
					MaxValue: max,
				},
			},
		}
		packs = append(packs, pack)
	}
	return packs
}

func makeUnsortedPackStatsList(n int) PackStatsList {
	// generate random values, strip duplicates and sort
	values := randUint64Slice(n)

	maxvalues := make([]uint64, len(values))
	minvalues := make([]uint64, len(values))

	// shuffle but keep original max values
	for i, v := range util.RandPerm(len(values)) {
		max := values[v] + 1000
		if v < len(values)-1 {
			max = values[v+1] - 1
		}
		minvalues[i] = values[v]
		maxvalues[i] = max
	}

	// generate pack packers
	packs := make(PackStatsList, 0)
	for i, v := range minvalues {
		pack := &PackStats{
			Key:     uint32(i),
			NValues: 1,
			Blocks: []BlockStats{
				{
					MinValue: v,
					MaxValue: maxvalues[i],
				},
			},
		}
		packs = append(packs, pack)
	}
	return packs
}

type packIndexTestListItem struct {
	Key uint32
	Min uint64
	Max uint64
}

type packIndexTestValueItem struct {
	Value  uint64
	ExpKey uint32
	ExpMin uint64
	ExpMax uint64
}

type packIndexTestCase struct {
	Name   string
	Info   *PackStats // used for Add
	Key    uint32     // used for Remove
	List   []packIndexTestListItem
	Values []packIndexTestValueItem
}

var packIndexTestCases = []packIndexTestCase{
	{
		Name: "single",
		List: []packIndexTestListItem{
			{
				Key: 1,
				Min: 1000,
				Max: 2000,
			},
		},
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			{
				Value:  100,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact min match
			{
				Value:  1000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			{
				Value:  2000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// after max match
			{
				Value:  3000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
		},
	},
	{
		Name: "multi-sorted",
		List: []packIndexTestListItem{
			{
				Key: 1,
				Min: 1000,
				Max: 2000,
			},
			{
				Key: 2,
				Min: 3000,
				Max: 4000,
			},
		},
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			{
				Value:  100,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact min first match
			{
				Value:  1000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// min between match
			{
				Value:  1500,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max first match
			{
				Value:  2000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// max first +1
			{
				Value:  2001,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// min second -1
			{
				Value:  2999,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// within second pack
			{
				Value:  3500,
				ExpKey: 2,
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// after max match
			{
				Value:  5000,
				ExpKey: 2,
				ExpMin: 3000,
				ExpMax: 4000,
			},
		},
	},
	{
		Name: "multi-duplicate",
		List: []packIndexTestListItem{
			{
				Key: 1,
				Min: 1000,
				Max: 1000,
			},
			{
				Key: 2,
				Min: 1000,
				Max: 1000,
			},
			{
				Key: 3,
				Min: 1000,
				Max: 2000,
			},
			{
				Key: 4,
				Min: 2001,
				Max: 3000,
			},
		},
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			{
				Value:  100,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 1000,
			},
			// exact min match, should return last pack where min equals
			{
				Value:  1000,
				ExpKey: 3,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// min between match
			{
				Value:  1500,
				ExpKey: 3,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			{
				Value:  2000,
				ExpKey: 3,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// max first +1
			{
				Value:  2001,
				ExpKey: 4,
				ExpMin: 2001,
				ExpMax: 3000,
			},
			// max -1
			{
				Value:  2999,
				ExpKey: 4,
				ExpMin: 2001,
				ExpMax: 3000,
			},
			// after max match
			{
				Value:  5000,
				ExpKey: 4,
				ExpMin: 2001,
				ExpMax: 3000,
			},
		},
	},
}

func buildPackHeader(key uint32, min, max uint64) *PackStats {
	return &PackStats{
		Key:     key,
		NValues: 1,
		Blocks: []BlockStats{
			{
				MinValue: min,
				MaxValue: max,
			},
		},
	}
}

func buildPackHeaderInt(key int, min, max uint64) *PackStats {
	return buildPackHeader(uint32(key), min, max)
}

func buildPackStatsList(items []packIndexTestListItem) PackStatsList {
	packs := make(PackStatsList, len(items))
	for i, v := range items {
		packs[i] = buildPackHeader(v.Key, v.Min, v.Max)
	}
	return packs
}

func TestPackIndexBest(t *testing.T) {
	for _, c := range packIndexTestCases {
		idx := NewStatsIndex(0, 1)
		for _, v := range buildPackStatsList(c.List) {
			idx.AddOrUpdate(v)
		}
		for _, v := range c.Values {
			p, min, max, _, _ := idx.Best(v.Value)
			if exp, got := v.ExpKey, idx.packs[p].Key; exp != got {
				// min, max := v1.MinMax(p1)
				t.Errorf("invalid pack selected by exp=%08x [%d/%d] got=%08x [%d/%d] for value %d",
					exp, v.ExpMin, v.ExpMax, got, min, max, v.Value)
			}
		}
	}
}

var packListAddTestCases = []packIndexTestCase{
	{
		Name: "add_middle",
		List: []packIndexTestListItem{
			{
				Key: 1,
				Min: 1000,
				Max: 2000,
			},
			{
				Key: 2,
				Min: 5000,
				Max: 6000,
			},
		},
		Info: buildPackHeaderInt(3, 3000, 4000),
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			{
				Value:  100,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact min match
			{
				Value:  1000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			{
				Value:  2000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// added pack min match
			{
				Value:  3000,
				ExpKey: 3,
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// added pack middle match
			{
				Value:  3500,
				ExpKey: 3,
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// added pack max match
			{
				Value:  4000,
				ExpKey: 3,
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// added pack max + 1 match
			{
				Value:  4001,
				ExpKey: 3,
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// last pack match
			{
				Value:  5500,
				ExpKey: 2,
				ExpMin: 5000,
				ExpMax: 6000,
			},
		},
	},
	{
		Name: "add-duplicate",
		List: []packIndexTestListItem{
			{
				Key: 1,
				Min: 1000,
				Max: 1000,
			},
			{
				Key: 2,
				Min: 1000,
				Max: 1000,
			},
			{
				Key: 3,
				Min: 1000,
				Max: 2000,
			},
			{
				Key: 4,
				Min: 2001,
				Max: 3000,
			},
		},
		Info: buildPackHeaderInt(5, 1000, 1000),
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			{
				Value:  100,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 1000,
			},
			// exact min match, should return last pack where min equals
			{
				Value:  1000,
				ExpKey: 3,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// min between match
			{
				Value:  1500,
				ExpKey: 3,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			{
				Value:  2000,
				ExpKey: 3,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// max first +1
			{
				Value:  2001,
				ExpKey: 4,
				ExpMin: 2001,
				ExpMax: 3000,
			},
			// max -1
			{
				Value:  2999,
				ExpKey: 4,
				ExpMin: 2001,
				ExpMax: 3000,
			},
			// after max match
			{
				Value:  5000,
				ExpKey: 4,
				ExpMin: 2001,
				ExpMax: 3000,
			},
		},
	},
}

func TestPackIndexAfterAdd(t *testing.T) {
	for _, c := range packListAddTestCases {
		idx := NewStatsIndex(0, 1)
		for _, v := range buildPackStatsList(c.List) {
			idx.AddOrUpdate(v)
		}
		idx.AddOrUpdate(c.Info)
		for _, v := range c.Values {
			p, min, max, _, _ := idx.Best(v.Value)
			if exp, got := v.ExpKey, idx.packs[p].Key; exp != got {
				t.Errorf("invalid pack selected by exp=%08x [%d/%d] got=%08x [%d/%d] for value %d",
					exp, v.ExpMin, v.ExpMax, got, min, max, v.Value)
			}
		}
	}
}

var packListRemoveTestCases = []packIndexTestCase{
	{
		Name: "remove_middle",
		List: []packIndexTestListItem{
			{
				Key: 1,
				Min: 1000,
				Max: 2000,
			},
			{
				Key: 2,
				Min: 3000,
				Max: 4000,
			},
			{
				Key: 3,
				Min: 5000,
				Max: 6000,
			},
		},
		Key: 2,
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			{
				Value:  100,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact min match
			{
				Value:  1000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			{
				Value:  2000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// removed pack value redirected to min
			{
				Value:  3000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// removed pack value closer to previous
			{
				Value:  3501,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// last pack match
			{
				Value:  5500,
				ExpKey: 3,
				ExpMin: 5000,
				ExpMax: 6000,
			},
		},
	},
}

func TestPackIndexAfterRemove(t *testing.T) {
	for _, c := range packListRemoveTestCases {
		idx := NewStatsIndex(0, 1)
		for _, v := range buildPackStatsList(c.List) {
			idx.AddOrUpdate(v)
		}
		idx.Remove(c.Key)
		for _, v := range c.Values {
			p, min, max, _, _ := idx.Best(v.Value)
			if exp, got := v.ExpKey, idx.packs[p].Key; exp != got {
				// min, max := v1.MinMax(p1)
				t.Errorf("invalid pack selected by exp=%08x [%d/%d] got=%08x [%d/%d] for value %d",
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

func BenchmarkPackIndexBestSorted(b *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			v2 := NewStatsIndex(0, 1)
			for _, v := range makeSortedPackStatsList(n.l) {
				v2.AddOrUpdate(v)
			}
			max := v2.packs[v2.pos[len(v2.pos)-1]].Blocks[v2.pki].MaxValue.(uint64)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v2.Best(uint64(util.RandInt64n(int64(max)) + 1))
			}
		})
	}
}

func BenchmarkPackIndexBestUnsorted(b *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			v2 := NewStatsIndex(0, 1)
			for _, v := range makeUnsortedPackStatsList(n.l) {
				v2.AddOrUpdate(v)
			}
			max := v2.packs[v2.pos[len(v2.pos)-1]].Blocks[v2.pki].MaxValue.(uint64)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v2.Best(uint64(util.RandInt64n(int64(max)) + 1))
			}
		})
	}
}

func BenchmarkPackIndexAppend(b *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			v2 := NewStatsIndex(0, 1)
			for _, v := range makeSortedPackStatsList(n.l) {
				v2.AddOrUpdate(v)
			}
			l := v2.Len()
			_, max := v2.MinMax(l - 1)
			// pack := buildPackHeaderInt(l, max+1, max+1000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// append to end of list
				v2.AddOrUpdate(buildPackHeaderInt(i+l, max+1, max+1000))
				max += 1000
			}
		})
	}
}

func BenchmarkPackIndexAdd(b *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			v2 := NewStatsIndex(0, 1)
			for _, v := range makeSortedPackStatsList(n.l) {
				v2.AddOrUpdate(v)
			}
			l := v2.Len() / 2
			min, max := v2.MinMax(l)
			pack := buildPackHeaderInt(l, min, max)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// remove and re-append an existing packer
				v2.Remove(pack.Key)
				v2.AddOrUpdate(pack)
			}
		})
	}
}

func BenchmarkPackIndexUpdate(b *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			v2 := NewStatsIndex(0, 1)
			for _, v := range makeSortedPackStatsList(n.l) {
				v2.AddOrUpdate(v)
			}
			pos := v2.Len() / 2
			min, max := v2.MinMax(pos)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
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
