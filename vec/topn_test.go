// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// go test -bench=. -benchmem

package vec

import (
	"math/rand"
	"strconv"
	"testing"
)

const n = 100000

type testcase struct {
	n string
	v int
}

var cases = []testcase{
	testcase{n: "1k", v: 1000},
	testcase{n: "10k", v: 10000},
	testcase{n: "100k", v: 100000},
}

func generateFloatHeap(n int) *TopFloat64Heap {
	h := NewTopFloat64Heap(n)
	for i := 0; i < n; i++ {
		h.Add(rand.Float64() * 21000000 / 2)
	}
	return h
}

func BenchmarkFloatHeapInsert(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			B.StopTimer()
			h := generateFloatHeap(c.v)
			B.StartTimer()
			for i := 0; i < B.N; i++ {
				h.Add(rand.Float64() * 21000000)
			}
		})
	}
}

func BenchmarkFloatHeapTop(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			for i := 0; i < B.N; i++ {
				B.StopTimer()
				h := generateFloatHeap(c.v)
				B.StartTimer()
				_ = h.TopN(c.v)
			}
		})
	}
}

func BenchmarkFloatHeapSum(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			for i := 0; i < B.N; i++ {
				B.StopTimer()
				h := generateFloatHeap(c.v)
				B.StartTimer()
				_ = h.SumN(c.v)
			}
		})
	}
}

func BenchmarkFloatHeapGini(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			for i := 0; i < B.N; i++ {
				B.StopTimer()
				h := generateFloatHeap(c.v)
				B.StartTimer()
				_ = h.Gini()
			}
		})
	}
}

func generateUint64Heap(n int) *TopUint64Heap {
	h := NewTopUint64Heap(n)
	for i := 0; i < n; i++ {
		h.Add(uint64(rand.Int63n(21000000 / 2)))
	}
	return h
}

func BenchmarkUint64HeapInsert(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			h := generateUint64Heap(c.v)
			for i := 0; i < B.N; i++ {
				h.Add(uint64(rand.Int63n(21000000)))
			}
		})
	}
}

func BenchmarkUint64HeapTop(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			for i := 0; i < B.N; i++ {
				B.StopTimer()
				h := generateUint64Heap(c.v)
				B.StartTimer()
				_ = h.TopN(c.v)
			}
		})
	}
}

func BenchmarkUint64HeapSum(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			for i := 0; i < B.N; i++ {
				B.StopTimer()
				h := generateUint64Heap(c.v)
				B.StartTimer()
				_ = h.SumN(c.v)
			}
		})
	}
}

func BenchmarkUint64HeapGini(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			for i := 0; i < B.N; i++ {
				B.StopTimer()
				h := generateUint64Heap(c.v)
				B.StartTimer()
				_ = h.Gini()
			}
		})
	}
}

func generateTopHeap(n int) *TopHeap {
	h := NewTopHeap(n)
	for i := 0; i < n; i++ {
		val := rand.Int()
		h.Add(TopItem{value: val, label: strconv.Itoa(val)})
	}
	return h
}

func generateTopMap(n int) map[string]int {
	m := make(map[string]int, n)
	for i := 0; i < n; i++ {
		val := rand.Int()
		m[strconv.Itoa(val)] = val
	}
	return m
}

func BenchmarkTopHeapInsert(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			B.StopTimer()
			h := generateTopHeap(c.v)
			B.StartTimer()
			for i := 0; i < B.N; i++ {
				// B.StopTimer()
				val := rand.Int()
				item := TopItem{label: strconv.Itoa(val), value: val}
				// B.StartTimer()
				h.Add(item)
			}
		})
	}
}

func BenchmarkTopHeapInsertMap(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			B.StopTimer()
			h := generateTopHeap(c.v)
			m := generateTopMap(c.v)
			B.StartTimer()
			for i := 0; i < B.N; i++ {
				h.AddMap(m)
			}
		})
	}
}

func BenchmarkTopHeapTop(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			for i := 0; i < B.N; i++ {
				B.StopTimer()
				h := generateTopHeap(c.v)
				B.StartTimer()
				_ = h.TopN(c.v)
			}
		})
	}
}

func BenchmarkTopHeapSum(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			for i := 0; i < B.N; i++ {
				B.StopTimer()
				h := generateTopHeap(c.v)
				B.StartTimer()
				_ = h.SumN(c.v)
			}
		})
	}
}

func BenchmarkTopHeapGini(B *testing.B) {
	for _, c := range cases {
		B.Run(c.n, func(B *testing.B) {
			for i := 0; i < B.N; i++ {
				B.StopTimer()
				h := generateTopHeap(c.v)
				B.StartTimer()
				_ = h.Gini()
			}
		})
	}
}
