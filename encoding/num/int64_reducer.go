// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"blockwatch.cc/knoxdb/util"
	"math"
)

type Int64Reducer struct {
	n    int
	sum  int64
	min  int64
	max  int64
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
}

func (b Int64Reducer) Len() int {
	return b.n
}

func (b Int64Reducer) Sum() int64 {
	return b.sum
}

func (b Int64Reducer) Min() int64 {
	return b.min
}

func (b Int64Reducer) Max() int64 {
	return b.max
}

func (b *Int64Reducer) Add(val int64) {
	if b.n == 0 {
		b.min = val
		b.max = val
	} else {
		b.min = util.Min64(b.min, val)
		b.max = util.Max64(b.max, val)
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

func (b *Int64Reducer) AddN(val ...int64) {
	b.AddSlice(val)
}

func (b *Int64Reducer) AddSlice(val []int64) {
	for _, v := range val {
		b.Add(v)
	}
}

// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
func (b Int64Reducer) Stddev() float64 {
	v := b.Var()
	if math.IsNaN(v) {
		return v
	}
	return math.Sqrt(v)
}

func (b Int64Reducer) Var() float64 {
	if b.n < 2 {
		return math.NaN()
	}
	return b.m2 / float64(b.n-1)
}

func (b Int64Reducer) Mean() float64 {
	return b.mean
}

type WindowInt64Reducer struct {
	Int64Reducer
	values   []int64
	isSorted bool
}

func NewWindowInt64Reducer(size int) *WindowInt64Reducer {
	return &WindowInt64Reducer{
		values:   make([]int64, 0, size),
		isSorted: true,
	}
}

func (b *WindowInt64Reducer) UseSlice(val []int64) {
	b.values = make([]int64, len(val))
	copy(b.values, val)
	b.isSorted = false
	for _, v := range b.values {
		b.Int64Reducer.Add(v)
	}
}

func (b *WindowInt64Reducer) Add(val int64) {
	b.isSorted = b.isSorted && val >= b.Max()
	b.Int64Reducer.Add(val)
	b.values = append(b.values, val)
}

func (b *WindowInt64Reducer) AddN(val ...int64) {
	b.AddSlice(val)
}

func (b *WindowInt64Reducer) AddSorted(val int64) {
	b.Int64Reducer.Add(val)
	b.values = append(b.values, val)
}

func (b *WindowInt64Reducer) AddSortedN(val ...int64) {
	for _, v := range val {
		b.Int64Reducer.Add(v)
	}
	b.values = append(b.values, val...)
}

func (b *WindowInt64Reducer) AddSortedSlice(val []int64) {
	for _, v := range val {
		b.Int64Reducer.Add(v)
	}
	b.values = append(b.values, val...)
}

func (b *WindowInt64Reducer) Median() float64 {
	l := len(b.values)
	if l == 0 {
		return 0
	}
	if !b.isSorted {
		Int64Sorter(b.values).Sort()
		b.isSorted = true
	}
	if l%2 == 0 {
		lo, hi := b.values[l/2-1], b.values[(l/2)]
		return float64(lo) + float64((hi-lo)/2)
	} else {
		return float64(b.values[l/2])
	}
}
