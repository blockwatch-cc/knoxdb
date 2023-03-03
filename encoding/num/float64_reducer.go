// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"blockwatch.cc/knoxdb/util"
	"math"
)

type Float64Reducer struct {
	n    int
	sum  float64
	min  float64
	max  float64
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
}

func (b Float64Reducer) Len() int {
	return b.n
}

func (b Float64Reducer) Sum() float64 {
	return b.sum
}

func (b Float64Reducer) Min() float64 {
	return b.min
}

func (b Float64Reducer) Max() float64 {
	return b.max
}

func (b *Float64Reducer) Add(val float64) {
	if b.n == 0 {
		b.min = val
		b.max = val
	} else {
		b.min = util.MinF64(b.min, val)
		b.max = util.MaxF64(b.max, val)
	}
	b.sum += val
	b.n++
	// summarize means and squared distances from mean using
	// Welford's Online algorithm, see
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
	delta := val - b.mean
	b.mean += delta / float64(b.n)
	b.m2 += delta * (val - b.mean)
}

func (b *Float64Reducer) AddN(val ...float64) {
	for _, v := range val {
		b.Add(v)
	}
}

func (b *Float64Reducer) AddSlice(val []float64) {
	for _, v := range val {
		b.Add(v)
	}
}

// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
func (b Float64Reducer) Stddev() float64 {
	v := b.Var()
	if math.IsNaN(v) {
		return v
	}
	return math.Sqrt(v)
}

func (b Float64Reducer) Var() float64 {
	if b.n < 2 {
		return math.NaN()
	}
	return b.m2 / float64(b.n-1)
}

func (b Float64Reducer) Mean() float64 {
	return b.mean
}

type WindowFloat64Reducer struct {
	Float64Reducer
	values   []float64
	isSorted bool
}

func NewWindowFloat64Reducer(size int) *WindowFloat64Reducer {
	return &WindowFloat64Reducer{
		values:   make([]float64, 0, size),
		isSorted: true,
	}
}

func (b *WindowFloat64Reducer) UseSlice(val []float64) {
	b.values = make([]float64, len(val))
	copy(b.values, val)
	b.isSorted = false
	for _, v := range b.values {
		b.Float64Reducer.Add(v)
	}
}

func (b *WindowFloat64Reducer) Add(val float64) {
	b.isSorted = b.isSorted && val >= b.Max()
	b.Float64Reducer.Add(val)
	b.values = append(b.values, val)
}

func (b *WindowFloat64Reducer) AddN(val ...float64) {
	b.AddSlice(val)
}

func (b *WindowFloat64Reducer) AddSorted(val float64) {
	b.Float64Reducer.Add(val)
	b.values = append(b.values, val)
}

func (b *WindowFloat64Reducer) AddSortedN(val ...float64) {
	for _, v := range val {
		b.Float64Reducer.Add(v)
	}
	b.values = append(b.values, val...)
}

func (b *WindowFloat64Reducer) AddSortedSlice(val []float64) {
	for _, v := range val {
		b.Float64Reducer.Add(v)
	}
	b.values = append(b.values, val...)
}

func (b *WindowFloat64Reducer) Median() float64 {
	l := len(b.values)
	if l == 0 {
		return 0
	}
	if !b.isSorted {
		Float64Sorter(b.values).Sort()
		b.isSorted = true
	}
	if l%2 == 0 {
		lo, hi := b.values[l/2-1], b.values[(l/2)]
		return lo + (hi-lo)/2
	} else {
		return b.values[l/2]
	}
}
