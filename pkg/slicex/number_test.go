// Copyright (c) 2023-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"fmt"
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
)

func TestOrderedNumbersContains(t *testing.T) {
	// nil slice
	if NewOrderedNumbers[int](nil).Contains(1) {
		t.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if NewOrderedNumbers([]int{}).Contains(1) {
		t.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !NewOrderedNumbers([]int{1}).Contains(1) {
		t.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if NewOrderedNumbers([]int{1}).Contains(2) {
		t.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !NewOrderedNumbers([]int{1, 3, 5, 7, 11, 13}).Contains(1) {
		t.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !NewOrderedNumbers([]int{1, 3, 5, 7, 11, 13}).Contains(5) {
		t.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !NewOrderedNumbers([]int{1, 3, 5, 7, 11, 13}).Contains(13) {
		t.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if NewOrderedNumbers([]int{1, 3, 5, 7, 11, 13}).Contains(0) {
		t.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if NewOrderedNumbers([]int{1, 3, 5, 7, 11, 13}).Contains(2) {
		t.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if NewOrderedNumbers([]int{1, 3, 5, 7, 11, 13}).Contains(14) {
		t.Errorf("N-element after slice value wrong match")
	}
}

func TestOrderedNumbersUnique(t *testing.T) {
	var tests = []struct {
		n string
		a *OrderedNumbers[int]
		b *OrderedNumbers[int]
		r *OrderedNumbers[int]
	}{
		{
			n: "empty",
			a: NewOrderedNumbers[int](nil).SetUnique(),
			b: NewOrderedNumbers[int](nil).SetUnique(),
			r: NewOrderedNumbers[int](nil).SetUnique(),
		},
		{
			n: "empty a",
			a: NewOrderedNumbers[int](nil).SetUnique(),
			b: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			r: NewOrderedNumbers([]int{1, 2}).SetUnique(),
		},
		{
			n: "empty b",
			a: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			b: NewOrderedNumbers[int](nil).SetUnique(),
			r: NewOrderedNumbers([]int{1, 2}).SetUnique(),
		},
		{
			n: "distinct unique",
			a: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			b: NewOrderedNumbers([]int{3, 4}).SetUnique(),
			r: NewOrderedNumbers([]int{1, 2, 3, 4}).SetUnique(),
		},
		{
			n: "distinct unique gap",
			a: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			b: NewOrderedNumbers([]int{4, 5}).SetUnique(),
			r: NewOrderedNumbers([]int{1, 2, 4, 5}).SetUnique(),
		},
		{
			n: "overlap duplicates",
			a: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			b: NewOrderedNumbers([]int{2, 3}).SetUnique(),
			r: NewOrderedNumbers([]int{1, 2, 3}).SetUnique(),
		},
		{
			n: "overlap duplicates not unique",
			a: NewOrderedNumbers([]int{1, 2}),
			b: NewOrderedNumbers([]int{2, 3}),
			r: NewOrderedNumbers([]int{1, 2, 2, 3}),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := c.a.Union(c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestOrderedNumbersIntersect(t *testing.T) {
	var tests = []struct {
		n string
		a *OrderedNumbers[int]
		b *OrderedNumbers[int]
		r *OrderedNumbers[int]
	}{
		{
			n: "empty",
			a: NewOrderedNumbers[int](nil).SetUnique(),
			b: NewOrderedNumbers[int](nil).SetUnique(),
			r: NewOrderedNumbers[int](nil).SetUnique(),
		},
		{
			n: "empty a",
			a: NewOrderedNumbers[int](nil).SetUnique(),
			b: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			r: NewOrderedNumbers([]int{}).SetUnique(),
		},
		{
			n: "empty b",
			a: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			b: NewOrderedNumbers[int](nil).SetUnique(),
			r: NewOrderedNumbers([]int{}).SetUnique(),
		},
		{
			n: "distinct unique",
			a: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			b: NewOrderedNumbers([]int{3, 4}).SetUnique(),
			r: NewOrderedNumbers([]int{}).SetUnique(),
		},
		{
			n: "distinct unique gap",
			a: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			b: NewOrderedNumbers([]int{4, 5}).SetUnique(),
			r: NewOrderedNumbers([]int{}).SetUnique(),
		},
		{
			n: "overlap duplicates",
			a: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			b: NewOrderedNumbers([]int{2, 3}).SetUnique(),
			r: NewOrderedNumbers([]int{2}).SetUnique(),
		},
		{
			n: "overlap duplicates not unique",
			a: NewOrderedNumbers([]int{1, 2}),
			b: NewOrderedNumbers([]int{2, 3}),
			r: NewOrderedNumbers([]int{2}),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := c.a.Intersect(c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestOrderedNumbersDifference(t *testing.T) {
	var tests = []struct {
		n string
		a *OrderedNumbers[int]
		b *OrderedNumbers[int]
		r *OrderedNumbers[int]
	}{
		{
			n: "empty",
			a: NewOrderedNumbers[int](nil).SetUnique(),
			b: NewOrderedNumbers[int](nil).SetUnique(),
			r: NewOrderedNumbers[int](nil).SetUnique(),
		},
		{
			n: "empty a",
			a: NewOrderedNumbers[int](nil).SetUnique(),
			b: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			r: NewOrderedNumbers([]int{}).SetUnique(),
		},
		{
			n: "empty b",
			a: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			b: NewOrderedNumbers[int](nil).SetUnique(),
			r: NewOrderedNumbers([]int{1, 2}).SetUnique(),
		},
		{
			n: "distinct unique",
			a: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			b: NewOrderedNumbers([]int{3, 4}).SetUnique(),
			r: NewOrderedNumbers([]int{1, 2}).SetUnique(),
		},
		{
			n: "distinct unique gap",
			a: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			b: NewOrderedNumbers([]int{4, 5}).SetUnique(),
			r: NewOrderedNumbers([]int{1, 2}).SetUnique(),
		},
		{
			n: "overlap duplicates",
			a: NewOrderedNumbers([]int{1, 2}).SetUnique(),
			b: NewOrderedNumbers([]int{2, 3}).SetUnique(),
			r: NewOrderedNumbers([]int{1}).SetUnique(),
		},
		{
			n: "overlap duplicates not unique",
			a: NewOrderedNumbers([]int{1, 2}),
			b: NewOrderedNumbers([]int{2, 3}),
			r: NewOrderedNumbers([]int{1}),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := c.a.Difference(c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func BenchmarkOrderedNumbersContains(b *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d-neg", n), func(b *testing.B) {
			a := NewOrderedNumbers(util.RandInts[int](n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				a.Contains(rand.Int())
			}
		})
	}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d-pos", n), func(b *testing.B) {
			a := NewOrderedNumbers(util.RandInts[int](n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				a.Contains(a.Values[rand.Intn(len(a.Values))])
			}
		})
	}
}

func TestOrderedNumbersContainsRange(t *testing.T) {
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
			if want, got := r.Match, NewOrderedNumbers(v.Slice).ContainsRange(r.From, r.To); want != got {
				t.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkOrderedNumbersContainsRange(b *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := NewOrderedNumbers(util.RandUints[uint64](n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				min, max := util.RandUint64(), util.RandUint64()
				if min > max {
					min, max = max, min
				}
				a.ContainsRange(min, max)
			}
		})
	}
}

func TestOrderedNumbersRemoveRange(t *testing.T) {
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
				{Name: "NIL", From: 0, To: 2, Expected: []int{}},
			},
		},
		// empty slice
		{
			Slice: []int{},
			Ranges: []TestRange{
				{Name: "EMPTY", From: 0, To: 2, Expected: []int{}},
			},
		},
		// 1-element slice
		{
			Slice: []int{3},
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: []int{3}},   // Case A
				{Name: "B1", From: 1, To: 3, Expected: []int{}},   // Case B.1, D1
				{Name: "B3", From: 3, To: 4, Expected: []int{}},   // Case B.3, D3
				{Name: "E", From: 15, To: 16, Expected: []int{3}}, // Case E
				{Name: "F", From: 1, To: 4, Expected: []int{}},    // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: []int{3},
			Ranges: []TestRange{
				{Name: "BCD", From: 3, To: 3, Expected: []int{}}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		{
			Slice: []int{3, 5, 7, 11, 13},
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: []int{3, 5, 7, 11, 13}},    // Case A
				{Name: "B1a", From: 1, To: 3, Expected: []int{5, 7, 11, 13}},     // Case B.1
				{Name: "B1b", From: 3, To: 3, Expected: []int{5, 7, 11, 13}},     // Case B.1
				{Name: "B2a", From: 1, To: 4, Expected: []int{5, 7, 11, 13}},     // Case B.2
				{Name: "B2b", From: 1, To: 5, Expected: []int{7, 11, 13}},        // Case B.2
				{Name: "B3a", From: 3, To: 4, Expected: []int{5, 7, 11, 13}},     // Case B.3
				{Name: "B3b", From: 3, To: 5, Expected: []int{7, 11, 13}},        // Case B.3
				{Name: "C1a", From: 4, To: 5, Expected: []int{3, 7, 11, 13}},     // Case C.1
				{Name: "C1b", From: 4, To: 6, Expected: []int{3, 7, 11, 13}},     // Case C.1
				{Name: "C1c", From: 4, To: 7, Expected: []int{3, 11, 13}},        // Case C.1
				{Name: "C1d", From: 5, To: 5, Expected: []int{3, 7, 11, 13}},     // Case C.1
				{Name: "C2a", From: 8, To: 8, Expected: []int{3, 5, 7, 11, 13}},  // Case C.2
				{Name: "C2b", From: 8, To: 10, Expected: []int{3, 5, 7, 11, 13}}, // Case C.2
				{Name: "D1a", From: 11, To: 13, Expected: []int{3, 5, 7}},        // Case D.1
				{Name: "D1b", From: 12, To: 13, Expected: []int{3, 5, 7, 11}},    // Case D.1
				{Name: "D2", From: 12, To: 14, Expected: []int{3, 5, 7, 11}},     // Case D.2
				{Name: "D3a", From: 13, To: 13, Expected: []int{3, 5, 7, 11}},    // Case D.3
				{Name: "D3b", From: 13, To: 14, Expected: []int{3, 5, 7, 11}},    // Case D.3
				{Name: "E", From: 15, To: 16, Expected: []int{3, 5, 7, 11, 13}},  // Case E
				{Name: "Fa", From: 0, To: 16, Expected: []int{}},                 // Case F
				{Name: "Fb", From: 0, To: 13, Expected: []int{}},                 // Case F
				{Name: "Fc", From: 3, To: 13, Expected: []int{}},                 // Case F
			},
		},
	}

	for _, v := range tests {
		for _, r := range v.Ranges {
			t.Run(r.Name, func(t *testing.T) {
				assert.Equal(t, r.Expected, NewOrderedNumbers(v.Slice).RemoveRange(r.From, r.To).Values)
			})
		}
	}
}

func TestOrderedNumbersIntersectRange(t *testing.T) {
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
				{Name: "NIL", From: 0, To: 2, Expected: []int{}},
			},
		},
		// empty slice
		{
			Slice: []int{},
			Ranges: []TestRange{
				{Name: "EMPTY", From: 0, To: 2, Expected: []int{}},
			},
		},
		// 1-element slice
		{
			Slice: []int{3},
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: []int{}},   // Case A
				{Name: "B1", From: 1, To: 3, Expected: []int{3}}, // Case B.1, D1
				{Name: "B3", From: 3, To: 4, Expected: []int{3}}, // Case B.3, D3
				{Name: "E", From: 15, To: 16, Expected: []int{}}, // Case E
				{Name: "F", From: 1, To: 4, Expected: []int{3}},  // Case F
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
				{Name: "E", From: 15, To: 16, Expected: []int{}},                // Case E
				{Name: "Fa", From: 0, To: 16, Expected: []int{3, 5, 7, 11, 13}}, // Case F
				{Name: "Fb", From: 0, To: 13, Expected: []int{3, 5, 7, 11, 13}}, // Case F
				{Name: "Fc", From: 3, To: 13, Expected: []int{3, 5, 7, 11, 13}}, // Case F
			},
		},
	}

	for _, v := range tests {
		for _, r := range v.Ranges {
			t.Run(r.Name, func(t *testing.T) {
				assert.Equal(t, r.Expected, NewOrderedNumbers(v.Slice).IntersectRange(r.From, r.To).Values)
			})
		}
	}
}
