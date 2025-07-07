// Copyright (c) 2023-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrderedIntegersContains(t *testing.T) {
	// nil slice
	if NewOrderedIntegers[int](nil).Contains(1) {
		t.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if NewOrderedIntegers([]int{}).Contains(1) {
		t.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !NewOrderedIntegers([]int{1}).Contains(1) {
		t.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if NewOrderedIntegers([]int{1}).Contains(2) {
		t.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !NewOrderedIntegers([]int{1, 3, 5, 7, 11, 13}).Contains(1) {
		t.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !NewOrderedIntegers([]int{1, 3, 5, 7, 11, 13}).Contains(5) {
		t.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !NewOrderedIntegers([]int{1, 3, 5, 7, 11, 13}).Contains(13) {
		t.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if NewOrderedIntegers([]int{1, 3, 5, 7, 11, 13}).Contains(0) {
		t.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if NewOrderedIntegers([]int{1, 3, 5, 7, 11, 13}).Contains(2) {
		t.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if NewOrderedIntegers([]int{1, 3, 5, 7, 11, 13}).Contains(14) {
		t.Errorf("N-element after slice value wrong match")
	}
}

func TestOrderedIntegersUnique(t *testing.T) {
	var tests = []struct {
		n string
		a *OrderedIntegers[int]
		b *OrderedIntegers[int]
		r *OrderedIntegers[int]
	}{
		{
			n: "empty",
			a: NewOrderedIntegers[int](nil).SetUnique(),
			b: NewOrderedIntegers[int](nil).SetUnique(),
			r: NewOrderedIntegers[int](nil).SetUnique(),
		},
		{
			n: "empty a",
			a: NewOrderedIntegers[int](nil).SetUnique(),
			b: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			r: NewOrderedIntegers([]int{1, 2}).SetUnique(),
		},
		{
			n: "empty b",
			a: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			b: NewOrderedIntegers[int](nil).SetUnique(),
			r: NewOrderedIntegers([]int{1, 2}).SetUnique(),
		},
		{
			n: "distinct unique",
			a: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			b: NewOrderedIntegers([]int{3, 4}).SetUnique(),
			r: NewOrderedIntegers([]int{1, 2, 3, 4}).SetUnique(),
		},
		{
			n: "distinct unique gap",
			a: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			b: NewOrderedIntegers([]int{4, 5}).SetUnique(),
			r: NewOrderedIntegers([]int{1, 2, 4, 5}).SetUnique(),
		},
		{
			n: "overlap duplicates",
			a: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			b: NewOrderedIntegers([]int{2, 3}).SetUnique(),
			r: NewOrderedIntegers([]int{1, 2, 3}).SetUnique(),
		},
		{
			n: "overlap duplicates not unique",
			a: NewOrderedIntegers([]int{1, 2}),
			b: NewOrderedIntegers([]int{2, 3}),
			r: NewOrderedIntegers([]int{1, 2, 2, 3}),
		},
	}

	for _, c := range tests {
		res := c.a.Union(c.b)
		assert.Equal(t, c.r, res, c.n)
	}
}

func TestOrderedIntegersIntersect(t *testing.T) {
	var tests = []struct {
		n string
		a *OrderedIntegers[int]
		b *OrderedIntegers[int]
		r *OrderedIntegers[int]
	}{
		{
			n: "empty",
			a: NewOrderedIntegers[int](nil).SetUnique(),
			b: NewOrderedIntegers[int](nil).SetUnique(),
			r: NewOrderedIntegers[int](nil).SetUnique(),
		},
		{
			n: "empty a",
			a: NewOrderedIntegers[int](nil).SetUnique(),
			b: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			r: NewOrderedIntegers([]int{}).SetUnique(),
		},
		{
			n: "empty b",
			a: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			b: NewOrderedIntegers[int](nil).SetUnique(),
			r: NewOrderedIntegers([]int{}).SetUnique(),
		},
		{
			n: "distinct unique",
			a: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			b: NewOrderedIntegers([]int{3, 4}).SetUnique(),
			r: NewOrderedIntegers([]int{}).SetUnique(),
		},
		{
			n: "distinct unique gap",
			a: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			b: NewOrderedIntegers([]int{4, 5}).SetUnique(),
			r: NewOrderedIntegers([]int{}).SetUnique(),
		},
		{
			n: "overlap duplicates",
			a: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			b: NewOrderedIntegers([]int{2, 3}).SetUnique(),
			r: NewOrderedIntegers([]int{2}).SetUnique(),
		},
		{
			n: "overlap duplicates not unique",
			a: NewOrderedIntegers([]int{1, 2}),
			b: NewOrderedIntegers([]int{2, 3}),
			r: NewOrderedIntegers([]int{2}),
		},
	}

	for _, c := range tests {
		res := c.a.Intersect(c.b)
		assert.Equal(t, c.r, res, c.n)
	}
}

func TestOrderedIntegersDifference(t *testing.T) {
	var tests = []struct {
		n string
		a *OrderedIntegers[int]
		b *OrderedIntegers[int]
		r *OrderedIntegers[int]
	}{
		{
			n: "empty",
			a: NewOrderedIntegers[int](nil).SetUnique(),
			b: NewOrderedIntegers[int](nil).SetUnique(),
			r: NewOrderedIntegers[int](nil).SetUnique(),
		},
		{
			n: "empty a",
			a: NewOrderedIntegers[int](nil).SetUnique(),
			b: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			r: NewOrderedIntegers([]int{}).SetUnique(),
		},
		{
			n: "empty b",
			a: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			b: NewOrderedIntegers[int](nil).SetUnique(),
			r: NewOrderedIntegers([]int{1, 2}).SetUnique(),
		},
		{
			n: "distinct unique",
			a: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			b: NewOrderedIntegers([]int{3, 4}).SetUnique(),
			r: NewOrderedIntegers([]int{1, 2}).SetUnique(),
		},
		{
			n: "distinct unique gap",
			a: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			b: NewOrderedIntegers([]int{4, 5}).SetUnique(),
			r: NewOrderedIntegers([]int{1, 2}).SetUnique(),
		},
		{
			n: "overlap duplicates",
			a: NewOrderedIntegers([]int{1, 2}).SetUnique(),
			b: NewOrderedIntegers([]int{2, 3}).SetUnique(),
			r: NewOrderedIntegers([]int{1}).SetUnique(),
		},
		{
			n: "overlap duplicates not unique",
			a: NewOrderedIntegers([]int{1, 2}),
			b: NewOrderedIntegers([]int{2, 3}),
			r: NewOrderedIntegers([]int{1}),
		},
	}

	for _, c := range tests {
		res := c.a.Difference(c.b)
		assert.Equal(t, c.r, res, c.n)
	}
}

func BenchmarkOrderedIntegersContains(b *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d-neg", n), func(b *testing.B) {
			a := NewOrderedIntegers(util.RandInts[int](n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				a.Contains(rand.Int())
			}
		})
	}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d-pos", n), func(b *testing.B) {
			a := NewOrderedIntegers(util.RandInts[int](n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				a.Contains(a.Values[rand.Intn(len(a.Values))])
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
			if want, got := r.Match, NewOrderedIntegers(slices.Clone(v.Slice)).ContainsRange(r.From, r.To); want != got {
				t.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkOrderedIntegersContainsRange(b *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := NewOrderedIntegers(util.RandUints[uint64](n))
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

func TestOrderedIntegersInsert(t *testing.T) {
	o := NewOrderedIntegers[int](nil).SetUnique()
	require.NotNil(t, o.Values)
	require.Equal(t, 0, o.Len())
	o.Insert(42)
	require.Equal(t, 1, o.Len())
	require.Equal(t, []int{42}, o.Values)
	o.Insert(1)
	require.Equal(t, 2, o.Len())
	require.Equal(t, []int{1, 42}, o.Values)
	o.Insert(50, 5, 42, 5)
	require.Equal(t, 4, o.Len(), o.Values)
	require.Equal(t, []int{1, 5, 42, 50}, o.Values)
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
				{Name: "NIL", List: []int{}, Expected: []int{}},
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
			assert.Equal(t, r.Expected, NewOrderedIntegers(slices.Clone(v.Slice)).Remove(r.List...).Values, r.Name)
		}
	}
}

func TestOrderedIntegersRemoveRange(t *testing.T) {
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
			assert.Equal(t, r.Expected, NewOrderedIntegers(slices.Clone(v.Slice)).RemoveRange(r.From, r.To).Values, r.Name)
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
			assert.Equal(t, r.Expected, NewOrderedIntegers(slices.Clone(v.Slice)).IntersectRange(r.From, r.To).Values, r.Name)
		}
	}
}
