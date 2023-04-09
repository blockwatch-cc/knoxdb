// Copyright (c) 2018 - 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"container/heap"
	"sort"
)

type Topable interface {
	Value() int
	Label() string
}

type TopItem struct {
	label string
	value int
}

func (t TopItem) Value() int {
	return t.value
}

func (t TopItem) Label() string {
	return t.label
}

type TopHeap struct {
	vals     []Topable
	calls    int
	sum      int
	total    int
	issorted bool
}

func (h TopHeap) Len() int           { return len(h.vals) }
func (h TopHeap) Less(i, j int) bool { return h.vals[i].Value() < h.vals[j].Value() }
func (h TopHeap) Swap(i, j int)      { h.vals[i], h.vals[j] = h.vals[j], h.vals[i] }

func (h *TopHeap) Push(x interface{}) {
	// Push and Pop modify the slice's length, not just its contents.
	h.vals = append(h.vals, x.(Topable))
}

func (h *TopHeap) Pop() interface{} {
	old := h.vals
	n := len(old)
	x := old[n-1]
	h.vals = old[0 : n-1]
	return x
}

func NewTopHeap(size int) *TopHeap {
	h := &TopHeap{
		vals: make([]Topable, 0, size),
	}
	heap.Init(h)
	return h
}

func (h *TopHeap) AddMap(m map[string]int) {
	for l, v := range m {
		h.Add(TopItem{label: l, value: v})
	}
}

func (h *TopHeap) Add(x Topable) {
	l, c := h.Len(), cap(h.vals)
	h.total += x.Value()
	h.sum += x.Value()
	h.calls++

	// restore heap invariant after sort
	if h.issorted {
		heap.Init(h)
	}

	// maybe add value
	if l < c || x.Value() > h.vals[0].Value() {
		if l == c {
			removed := heap.Pop(h)
			h.sum -= removed.(Topable).Value()
		}
		heap.Push(h, x)
	}
}

func (h *TopHeap) Total() int {
	return h.total
}

func (h *TopHeap) Calls() int {
	return h.calls
}

func (h *TopHeap) Sum() int {
	return h.sum
}

func (h *TopHeap) sorted() []Topable {
	if !h.issorted {
		sort.Slice(h.vals, func(i, j int) bool { return h.vals[i].Value() > h.vals[j].Value() })
		h.issorted = true
	}
	return h.vals
}

func (h *TopHeap) TopN(n int) []Topable {
	n = Min(n, len(h.vals))
	return h.sorted()[:n]
}

func (h *TopHeap) SumN(n int) int {
	n = Min(n, len(h.vals))
	var sum int
	for _, v := range h.sorted()[:n] {
		sum += v.Value()
	}
	return sum
}

// based on https://en.wikipedia.org/wiki/Gini_coefficient
// (alternate expressions, 2nd formula)
func (h *TopHeap) Gini() float64 {
	// sorts descending to satisfy top criteria
	h.sorted()

	var acc float64
	n := h.Len()

	// algo assumes ascending order, so we walk backwards
	for i := n - 1; i >= 0; i-- {
		acc += float64(n-i) * float64(h.vals[i].Value())
	}
	return 2*acc/(float64(n)*float64(h.Sum())) - float64(n+1)/float64(n)
}

func (h *TopHeap) GiniCapped(cutoff int) float64 {
	// sorts descending to satisfy top criteria
	h.sorted()

	// use binary search to find first value after cutoff
	idx := sort.Search(h.Len(), func(i int) bool { return h.vals[i].Value() < cutoff })
	arr := h.TopN(idx)
	n := len(arr)

	// algo assumes ascending order, so we walk backwards
	var acc float64
	for i := n - 1; i >= 0; i-- {
		acc += float64(n-i) * float64(arr[i].Value())
	}
	return 2*acc/(float64(n)*float64(h.SumN(idx))) - float64(n+1)/float64(n)
}
