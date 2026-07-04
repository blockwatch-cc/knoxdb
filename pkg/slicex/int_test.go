// Copyright (c) 2023-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"fmt"
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/tests/testutil"
	"github.com/stretchr/testify/assert"
)

func TestOrderedIntegersContains(t *testing.T) {
	// nil slice
	if ContainsSorted([]int(nil), 1) {
		t.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if ContainsSorted([]int{}, 1) {
		t.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !ContainsSorted([]int{1}, 1) {
		t.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if ContainsSorted([]int{1}, 2) {
		t.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !ContainsSorted([]int{1, 3, 5, 7, 11, 13}, 1) {
		t.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !ContainsSorted([]int{1, 3, 5, 7, 11, 13}, 5) {
		t.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !ContainsSorted([]int{1, 3, 5, 7, 11, 13}, 13) {
		t.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if ContainsSorted([]int{1, 3, 5, 7, 11, 13}, 0) {
		t.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if ContainsSorted([]int{1, 3, 5, 7, 11, 13}, 2) {
		t.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if ContainsSorted([]int{1, 3, 5, 7, 11, 13}, 14) {
		t.Errorf("N-element after slice value wrong match")
	}
}

func TestOrderedIntegersUnique(t *testing.T) {
	var tests = []struct {
		n string
		a []int
		b []int
		r []int
	}{
		{
			n: "empty",
			a: []int(nil),
			b: []int(nil),
			r: []int(nil),
		},
		{
			n: "empty a",
			a: []int(nil),
			b: []int{1, 2},
			r: []int{1, 2},
		},
		{
			n: "empty b",
			a: []int{1, 2},
			b: []int(nil),
			r: []int{1, 2},
		},
		{
			n: "distinct unique",
			a: []int{1, 2},
			b: []int{3, 4},
			r: []int{1, 2, 3, 4},
		},
		{
			n: "distinct unique gap",
			a: []int{1, 2},
			b: []int{4, 5},
			r: []int{1, 2, 4, 5},
		},
		{
			n: "overlap duplicates",
			a: []int{1, 2},
			b: []int{2, 3},
			r: []int{1, 2, 3},
		},
	}

	for _, c := range tests {
		res := Union(c.a, c.b)
		assert.Equal(t, c.r, res, c.n)
	}
}

func TestOrderedIntegersIntersect(t *testing.T) {
	var tests = []struct {
		n string
		a []int
		b []int
		r []int
	}{
		{
			n: "empty",
			a: []int(nil),
			b: []int(nil),
			r: []int(nil),
		},
		{
			n: "empty a",
			a: []int(nil),
			b: []int{1, 2},
			r: []int{},
		},
		{
			n: "empty b",
			a: []int{1, 2},
			b: []int(nil),
			r: []int{},
		},
		{
			n: "distinct unique",
			a: []int{1, 2},
			b: []int{3, 4},
			r: []int{},
		},
		{
			n: "distinct unique gap",
			a: []int{1, 2},
			b: []int{4, 5},
			r: []int{},
		},
		{
			n: "overlap duplicates",
			a: []int{1, 2},
			b: []int{2, 3},
			r: []int{2},
		},
	}

	for _, c := range tests {
		res := Intersect(c.a, c.b)
		assert.Equal(t, c.r, res, c.n)
	}
}

func TestOrderedIntegersDifference(t *testing.T) {
	var tests = []struct {
		n string
		a []int
		b []int
		r []int
	}{
		{
			n: "empty",
			a: []int(nil),
			b: []int(nil),
			r: []int(nil),
		},
		{
			n: "empty a",
			a: []int(nil),
			b: []int{1, 2},
			r: []int(nil),
		},
		{
			n: "empty b",
			a: []int{1, 2},
			b: []int(nil),
			r: []int{1, 2},
		},
		{
			n: "distinct unique",
			a: []int{1, 2},
			b: []int{3, 4},
			r: []int{1, 2},
		},
		{
			n: "distinct unique gap",
			a: []int{1, 2},
			b: []int{4, 5},
			r: []int{1, 2},
		},
		{
			n: "overlap duplicates",
			a: []int{1, 2},
			b: []int{2, 3},
			r: []int{1},
		},
	}

	for _, c := range tests {
		res := Remove(slices.Clone(c.a), c.b...)
		assert.Equal(t, c.r, res, c.n)
	}
}

func BenchmarkOrderedIntegersContains(b *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d-neg", n), func(b *testing.B) {
			a := Unique(testutil.RandInts[int64](n))
			b.ResetTimer()
			for b.Loop() {
				ContainsSorted(a, testutil.RandInt64())
			}
		})
	}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d-pos", n), func(b *testing.B) {
			a := Unique(testutil.RandInts[int64](n))
			b.ResetTimer()
			for b.Loop() {
				ContainsSorted(a, a[testutil.RandIntn(len(a))])
			}
		})
	}
}

func TestOrderedIntegersContainsRange(t *testing.T) {
	type TestRange struct {
		Name  string
		From  int
		To    int
		Match bool
	}

	type Testcase struct {
		Slice  []int
		Ranges []TestRange
	}

	var tests = []Testcase{
		// nil slice
		{
			Slice: nil,
			Ranges: []TestRange{
				{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// empty slice
		{
			Slice: []int{},
			Ranges: []TestRange{
				{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// 1-element slice
		{
			Slice: []int{3},
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Match: false},   // Case A
				{Name: "B1", From: 1, To: 3, Match: true},   // Case B.1, D1
				{Name: "B3", From: 3, To: 4, Match: true},   // Case B.3, D3
				{Name: "E", From: 15, To: 16, Match: false}, // Case E
				{Name: "F", From: 1, To: 4, Match: true},    // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: []int{3},
			Ranges: []TestRange{
				{Name: "BCD", From: 3, To: 3, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		{
			Slice: []int{3, 5, 7, 11, 13},
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Match: false},    // Case A
				{Name: "B1a", From: 1, To: 3, Match: true},   // Case B.1
				{Name: "B1b", From: 3, To: 3, Match: true},   // Case B.1
				{Name: "B2a", From: 1, To: 4, Match: true},   // Case B.2
				{Name: "B2b", From: 1, To: 5, Match: true},   // Case B.2
				{Name: "B3a", From: 3, To: 4, Match: true},   // Case B.3
				{Name: "B3b", From: 3, To: 5, Match: true},   // Case B.3
				{Name: "C1a", From: 4, To: 5, Match: true},   // Case C.1
				{Name: "C1b", From: 4, To: 6, Match: true},   // Case C.1
				{Name: "C1c", From: 4, To: 7, Match: true},   // Case C.1
				{Name: "C1d", From: 5, To: 5, Match: true},   // Case C.1
				{Name: "C2a", From: 8, To: 8, Match: false},  // Case C.2
				{Name: "C2b", From: 8, To: 10, Match: false}, // Case C.2
				{Name: "D1a", From: 11, To: 13, Match: true}, // Case D.1
				{Name: "D1b", From: 12, To: 13, Match: true}, // Case D.1
				{Name: "D2", From: 12, To: 14, Match: true},  // Case D.2
				{Name: "D3a", From: 13, To: 13, Match: true}, // Case D.3
				{Name: "D3b", From: 13, To: 14, Match: true}, // Case D.3
				{Name: "E", From: 15, To: 16, Match: false},  // Case E
				{Name: "Fa", From: 0, To: 16, Match: true},   // Case F
				{Name: "Fb", From: 0, To: 13, Match: true},   // Case F
				{Name: "Fc", From: 3, To: 13, Match: true},   // Case F
			},
		},
		// real-word testcase
		{
			Slice: []int{
				699421, 1374016, 1692360, 1797909, 1809339,
				2552208, 2649552, 2740915, 2769610, 3043393,
			},
			Ranges: []TestRange{
				{Name: "1", From: 2785281, To: 2818048, Match: false},
				{Name: "2", From: 2818049, To: 2850816, Match: false},
				{Name: "3", From: 2850817, To: 2883584, Match: false},
				{Name: "4", From: 2883585, To: 2916352, Match: false},
				{Name: "5", From: 2916353, To: 2949120, Match: false},
				{Name: "6", From: 2949121, To: 2981888, Match: false},
				{Name: "7", From: 2981889, To: 3014656, Match: false},
				{Name: "8", From: 3014657, To: 3047424, Match: true},
			},
		},
	}

	for i, v := range tests {
		for _, r := range v.Ranges {
			s := slices.Clone(v.Slice)
			if want, got := r.Match, ContainsRangeSorted(s, r.From, r.To); want != got {
				t.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkOrderedIntegersContainsRange(b *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := Unique(testutil.RandUints[uint64](n))
			b.ResetTimer()
			for b.Loop() {
				min, max := testutil.RandUint64(), testutil.RandUint64()
				if min > max {
					min, max = max, min
				}
				ContainsRangeSorted(a, min, max)
			}
		})
	}
}

func TestOrderedIntegersRemove(t *testing.T) {
	type TestList struct {
		Name     string
		List     []int
		Expected []int
	}

	type Testcase struct {
		Slice []int
		Lists []TestList
	}

	var tests = []Testcase{
		// nil slice
		{
			Slice: nil,
			Lists: []TestList{
				{Name: "NIL", List: []int{}, Expected: []int(nil)},
			},
		},
		// empty slice
		{
			Slice: []int{},
			Lists: []TestList{
				{Name: "EMPTY", List: []int{0, 1, 2}, Expected: []int{}},
			},
		},
		// 1-element slice
		{
			Slice: []int{3},
			Lists: []TestList{
				{Name: "A", List: []int{0, 1, 2}, Expected: []int{3}},   // Case A
				{Name: "B1", List: []int{1, 2, 3}, Expected: []int{}},   // Case B.1, D1
				{Name: "B3", List: []int{3, 4}, Expected: []int{}},      // Case B.3, D3
				{Name: "E", List: []int{15, 16}, Expected: []int{3}},    // Case E
				{Name: "F", List: []int{1, 2, 3, 4}, Expected: []int{}}, // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: []int{3},
			Lists: []TestList{
				{Name: "BCD", List: []int{3}, Expected: []int{}}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		{
			Slice: []int{3, 5, 7, 11, 13},
			Lists: []TestList{
				{Name: "A", List: []int{0, 1, 2}, Expected: []int{3, 5, 7, 11, 13}},                                    // Case A
				{Name: "B1a", List: []int{1, 2, 3}, Expected: []int{5, 7, 11, 13}},                                     // Case B.1
				{Name: "B1b", List: []int{3}, Expected: []int{5, 7, 11, 13}},                                           // Case B.1
				{Name: "B2a", List: []int{1, 2, 3, 4}, Expected: []int{5, 7, 11, 13}},                                  // Case B.2
				{Name: "B2b", List: []int{1, 2, 3, 4, 5}, Expected: []int{7, 11, 13}},                                  // Case B.2
				{Name: "B3a", List: []int{3, 4}, Expected: []int{5, 7, 11, 13}},                                        // Case B.3
				{Name: "B3b", List: []int{3, 4, 5}, Expected: []int{7, 11, 13}},                                        // Case B.3
				{Name: "C1a", List: []int{4, 5}, Expected: []int{3, 7, 11, 13}},                                        // Case C.1
				{Name: "C1b", List: []int{4, 5, 6}, Expected: []int{3, 7, 11, 13}},                                     // Case C.1
				{Name: "C1c", List: []int{4, 5, 6, 7}, Expected: []int{3, 11, 13}},                                     // Case C.1
				{Name: "C1d", List: []int{5}, Expected: []int{3, 7, 11, 13}},                                           // Case C.1
				{Name: "C2a", List: []int{8}, Expected: []int{3, 5, 7, 11, 13}},                                        // Case C.2
				{Name: "C2b", List: []int{8, 9, 10}, Expected: []int{3, 5, 7, 11, 13}},                                 // Case C.2
				{Name: "D1a", List: []int{11, 12, 13}, Expected: []int{3, 5, 7}},                                       // Case D.1
				{Name: "D1b", List: []int{12, 13}, Expected: []int{3, 5, 7, 11}},                                       // Case D.1
				{Name: "D2", List: []int{12, 13, 14}, Expected: []int{3, 5, 7, 11}},                                    // Case D.2
				{Name: "D3a", List: []int{13}, Expected: []int{3, 5, 7, 11}},                                           // Case D.3
				{Name: "D3b", List: []int{13, 14}, Expected: []int{3, 5, 7, 11}},                                       // Case D.3
				{Name: "E", List: []int{15, 16}, Expected: []int{3, 5, 7, 11, 13}},                                     // Case E
				{Name: "Fa", List: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, Expected: []int{}}, // Case F
				{Name: "Fb", List: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, Expected: []int{}},             // Case F
				{Name: "Fc", List: []int{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, Expected: []int{}},                      // Case F
			},
		},
	}

	for _, v := range tests {
		for _, r := range v.Lists {
			s := slices.Clone(v.Slice)
			assert.Equal(t, r.Expected, Remove(s, r.List...), r.Name)
		}
	}
}

func TestOrderedIntegersIntersectRange(t *testing.T) {
	type TestRange struct {
		Name     string
		From     int
		To       int
		Expected []int
	}

	type Testcase struct {
		Slice  []int
		Ranges []TestRange
	}

	var tests = []Testcase{
		// nil slice
		{
			Slice: nil,
			Ranges: []TestRange{
				{Name: "NIL", From: 0, To: 2, Expected: []int(nil)},
			},
		},
		// empty slice
		{
			Slice: []int{},
			Ranges: []TestRange{
				{Name: "EMPTY", From: 0, To: 2, Expected: []int(nil)},
			},
		},
		// 1-element slice
		{
			Slice: []int{3},
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: []int{}},      // Case A
				{Name: "B1", From: 1, To: 3, Expected: []int{3}},    // Case B.1, D1
				{Name: "B3", From: 3, To: 4, Expected: []int{3}},    // Case B.3, D3
				{Name: "E", From: 15, To: 16, Expected: []int(nil)}, // Case E
				{Name: "F", From: 1, To: 4, Expected: []int{3}},     // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: []int{3},
			Ranges: []TestRange{
				{Name: "BCD", From: 3, To: 3, Expected: []int{3}}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		{
			Slice: []int{3, 5, 7, 11, 13},
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: []int{}},                  // Case A
				{Name: "B1a", From: 1, To: 3, Expected: []int{3}},               // Case B.1
				{Name: "B1b", From: 3, To: 3, Expected: []int{3}},               // Case B.1
				{Name: "B2a", From: 1, To: 4, Expected: []int{3}},               // Case B.2
				{Name: "B2b", From: 1, To: 5, Expected: []int{3, 5}},            // Case B.2
				{Name: "B3a", From: 3, To: 4, Expected: []int{3}},               // Case B.3
				{Name: "B3b", From: 3, To: 5, Expected: []int{3, 5}},            // Case B.3
				{Name: "C1a", From: 4, To: 5, Expected: []int{5}},               // Case C.1
				{Name: "C1b", From: 4, To: 6, Expected: []int{5}},               // Case C.1
				{Name: "C1c", From: 4, To: 7, Expected: []int{5, 7}},            // Case C.1
				{Name: "C1d", From: 5, To: 5, Expected: []int{5}},               // Case C.1
				{Name: "C2a", From: 8, To: 8, Expected: []int{}},                // Case C.2
				{Name: "C2b", From: 8, To: 10, Expected: []int{}},               // Case C.2
				{Name: "D1a", From: 11, To: 13, Expected: []int{11, 13}},        // Case D.1
				{Name: "D1b", From: 12, To: 13, Expected: []int{13}},            // Case D.1
				{Name: "D2", From: 12, To: 14, Expected: []int{13}},             // Case D.2
				{Name: "D3a", From: 13, To: 13, Expected: []int{13}},            // Case D.3
				{Name: "D3b", From: 13, To: 14, Expected: []int{13}},            // Case D.3
				{Name: "E", From: 15, To: 16, Expected: []int(nil)},             // Case E
				{Name: "Fa", From: 0, To: 16, Expected: []int{3, 5, 7, 11, 13}}, // Case F
				{Name: "Fb", From: 0, To: 13, Expected: []int{3, 5, 7, 11, 13}}, // Case F
				{Name: "Fc", From: 3, To: 13, Expected: []int{3, 5, 7, 11, 13}}, // Case F
			},
		},
	}

	for _, v := range tests {
		for _, r := range v.Ranges {
			s := slices.Clone(v.Slice)
			assert.Equal(t, r.Expected, IntersectRange(s, r.From, r.To), r.Name)
		}
	}
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

func BenchmarkUniqueMap(b *testing.B) {
	for _, c := range BenchmarkSizes {
		data := tests.GenRnd[uint64](c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(8 * c.N))
			for b.Loop() {
				f := make(map[uint64]struct{}, len(data))
				for _, v := range data {
					f[v] = struct{}{}
				}
				_ = len(f)
			}
		})
	}
}

func BenchmarkUniqueSort(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[uint64](c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(8 * c.N))
			for b.Loop() {
				res := Unique(slices.Clone(data))
				_ = len(res)
			}
		})
	}
}
