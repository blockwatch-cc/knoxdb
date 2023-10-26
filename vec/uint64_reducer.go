// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"blockwatch.cc/knoxdb/util"
	"math"
)

type Uint64Reducer struct {
	n    int
	sum  uint64
	min  uint64
	max  uint64
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
}

func (b Uint64Reducer) Len() int {
	return b.n
}

func (b Uint64Reducer) Sum() uint64 {
	return b.sum
}

func (b Uint64Reducer) Min() uint64 {
	return b.min
}

func (b Uint64Reducer) Max() uint64 {
	return b.max
}

func (b *Uint64Reducer) Add(val uint64) {
	if b.n == 0 {
		b.min = val
		b.max = val
	} else {
		b.min = util.Min(b.min, val)
		b.max = util.Max(b.max, val)
	}
	b.sum += val
	b.n++
	// summarize means and squared distances from mean using
	// Welford's Online algorithm, see
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
	delta := float64(val) - b.mean
	b.mean += delta / float64(b.n)
	b.m2 += delta * (float64(val) - b.mean)
}

func (b *Uint64Reducer) AddN(val ...uint64) {
	b.AddSlice(val)
}

func (b *Uint64Reducer) AddSlice(val []uint64) {
	for _, v := range val {
		b.Add(v)
	}
}

// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
func (b Uint64Reducer) Stddev() float64 {
	v := b.Var()
	if math.IsNaN(v) {
		return v
	}
	return math.Sqrt(v)
}

func (b Uint64Reducer) Var() float64 {
	if b.n < 2 {
		return math.NaN()
	}
	return b.m2 / float64(b.n-1)
}

func (b Uint64Reducer) Mean() float64 {
	return b.mean
}

type WindowUint64Reducer struct {
	Uint64Reducer
	values   []uint64
	isSorted bool
}

func NewWindowUint64Reducer(size int) *WindowUint64Reducer {
	return &WindowUint64Reducer{
		values:   make([]uint64, 0, size),
		isSorted: true,
	}
}

func (b *WindowUint64Reducer) UseSlice(val []uint64) {
	b.values = make([]uint64, len(val))
	copy(b.values, val)
	b.isSorted = false
	for _, v := range b.values {
		b.Uint64Reducer.Add(v)
	}
}

func (b *WindowUint64Reducer) Add(val uint64) {
	b.isSorted = b.isSorted && val >= b.Max()
	b.Uint64Reducer.Add(val)
	b.values = append(b.values, val)
}

func (b *WindowUint64Reducer) AddN(val ...uint64) {
	b.AddSlice(val)
}

func (b *WindowUint64Reducer) AddSorted(val uint64) {
	b.Uint64Reducer.Add(val)
	b.values = append(b.values, val)
}

func (b *WindowUint64Reducer) AddSortedN(val ...uint64) {
	b.AddSortedSlice(val)
}

func (b *WindowUint64Reducer) AddSortedSlice(val []uint64) {
	for _, v := range val {
		b.Uint64Reducer.Add(v)
	}
	b.values = append(b.values, val...)
}

func (b *WindowUint64Reducer) Median() float64 {
	l := len(b.values)
	if l == 0 {
		return 0
	}
	if !b.isSorted {
		Uint64Sorter(b.values).Sort()
		b.isSorted = true
	}
	if l%2 == 0 {
		lo, hi := b.values[l/2-1], b.values[(l/2)]
		return float64(lo) + float64((hi-lo)/2)
	} else {
		return float64(b.values[l/2])
	}
}
