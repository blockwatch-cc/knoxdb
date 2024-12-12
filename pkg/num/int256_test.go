// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func i256s(n ...int) []Int256 {
	s := make([]Int256, len(n))
	for i, v := range n {
		s[i] = Int256FromInt64(int64(v))
	}
	return s

}

func i256(n int) Int256 {
	return Int256FromInt64(int64(n))
}

func TestInt256Unique(t *testing.T) {
	var tests = []struct {
		n string
		a []Int256
		b []Int256
		r []Int256
	}{
		{
			n: "empty",
			a: nil,
			b: nil,
			r: nil,
		},
		{
			n: "empty a",
			a: nil,
			b: i256s(1, 2),
			r: i256s(1, 2),
		},
		{
			n: "empty b",
			a: i256s(1, 2),
			b: nil,
			r: i256s(1, 2),
		},
		{
			n: "distinct unique",
			a: i256s(1, 2),
			b: i256s(3, 4),
			r: i256s(1, 2, 3, 4),
		},
		{
			n: "distinct unique gap",
			a: i256s(1, 2),
			b: i256s(4, 5),
			r: i256s(1, 2, 4, 5),
		},
		{
			n: "overlap duplicates",
			a: i256s(1, 2),
			b: i256s(2, 3),
			r: i256s(1, 2, 3),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := Int256Union(c.a, c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestInt256Intersect(t *testing.T) {
	var tests = []struct {
		n string
		a []Int256
		b []Int256
		r []Int256
	}{
		{
			n: "empty",
			a: nil,
			b: nil,
			r: nil,
		},
		{
			n: "empty a",
			a: nil,
			b: i256s(1, 2),
			r: nil,
		},
		{
			n: "empty b",
			a: i256s(1, 2),
			b: nil,
			r: nil,
		},
		{
			n: "distinct unique",
			a: i256s(1, 2),
			b: i256s(3, 4),
			r: i256s(),
		},
		{
			n: "distinct unique gap",
			a: i256s(1, 2),
			b: i256s(4, 5),
			r: i256s(),
		},
		{
			n: "overlap duplicates",
			a: i256s(1, 2),
			b: i256s(2, 3),
			r: i256s(2),
		},
		{
			n: "overlap duplicates not unique",
			a: i256s(1, 2),
			b: i256s(2, 3),
			r: i256s(2),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := Int256Intersect(c.a, c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestInt256Difference(t *testing.T) {
	var tests = []struct {
		n string
		a []Int256
		b []Int256
		r []Int256
	}{
		{
			n: "empty",
			a: nil,
			b: nil,
			r: nil,
		},
		{
			n: "empty a",
			a: nil,
			b: i256s(1, 2),
			r: nil,
		},
		{
			n: "empty b",
			a: i256s(1, 2),
			b: nil,
			r: i256s(1, 2),
		},
		{
			n: "distinct unique",
			a: i256s(1, 2),
			b: i256s(3, 4),
			r: i256s(1, 2),
		},
		{
			n: "distinct unique gap",
			a: i256s(1, 2),
			b: i256s(4, 5),
			r: i256s(1, 2),
		},
		{
			n: "overlap duplicates",
			a: i256s(1, 2),
			b: i256s(2, 3),
			r: i256s(1),
		},
		{
			n: "overlap duplicates not unique",
			a: i256s(1, 2),
			b: i256s(2, 3),
			r: i256s(1),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := Int256Difference(c.a, c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestInt256RemoveRange(t *testing.T) {
	type TestRange struct {
		Name     string
		From     int
		To       int
		Expected []Int256
	}

	type Testcase struct {
		Slice  []Int256
		Ranges []TestRange
	}

	var tests = []Testcase{
		// nil slice
		{
			Slice: nil,
			Ranges: []TestRange{
				{Name: "NIL", From: 0, To: 2, Expected: nil},
			},
		},
		// empty slice
		{
			Slice: i256s(),
			Ranges: []TestRange{
				{Name: "EMPTY", From: 0, To: 2, Expected: i256s()},
			},
		},
		// 1-element slice
		{
			Slice: i256s(3),
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: i256s(3)},   // Case A
				{Name: "B1", From: 1, To: 3, Expected: i256s()},   // Case B.1, D1
				{Name: "B3", From: 3, To: 4, Expected: i256s()},   // Case B.3, D3
				{Name: "E", From: 15, To: 16, Expected: i256s(3)}, // Case E
				{Name: "F", From: 1, To: 4, Expected: i256s()},    // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: i256s(3),
			Ranges: []TestRange{
				{Name: "BCD", From: 3, To: 3, Expected: i256s()}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		{
			Slice: i256s(3, 5, 7, 11, 13),
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: i256s(3, 5, 7, 11, 13)},    // Case A
				{Name: "B1a", From: 1, To: 3, Expected: i256s(5, 7, 11, 13)},     // Case B.1
				{Name: "B1b", From: 3, To: 3, Expected: i256s(5, 7, 11, 13)},     // Case B.1
				{Name: "B2a", From: 1, To: 4, Expected: i256s(5, 7, 11, 13)},     // Case B.2
				{Name: "B2b", From: 1, To: 5, Expected: i256s(7, 11, 13)},        // Case B.2
				{Name: "B3a", From: 3, To: 4, Expected: i256s(5, 7, 11, 13)},     // Case B.3
				{Name: "B3b", From: 3, To: 5, Expected: i256s(7, 11, 13)},        // Case B.3
				{Name: "C1a", From: 4, To: 5, Expected: i256s(3, 7, 11, 13)},     // Case C.1
				{Name: "C1b", From: 4, To: 6, Expected: i256s(3, 7, 11, 13)},     // Case C.1
				{Name: "C1c", From: 4, To: 7, Expected: i256s(3, 11, 13)},        // Case C.1
				{Name: "C1d", From: 5, To: 5, Expected: i256s(3, 7, 11, 13)},     // Case C.1
				{Name: "C2a", From: 8, To: 8, Expected: i256s(3, 5, 7, 11, 13)},  // Case C.2
				{Name: "C2b", From: 8, To: 10, Expected: i256s(3, 5, 7, 11, 13)}, // Case C.2
				{Name: "D1a", From: 11, To: 13, Expected: i256s(3, 5, 7)},        // Case D.1
				{Name: "D1b", From: 12, To: 13, Expected: i256s(3, 5, 7, 11)},    // Case D.1
				{Name: "D2", From: 12, To: 14, Expected: i256s(3, 5, 7, 11)},     // Case D.2
				{Name: "D3a", From: 13, To: 13, Expected: i256s(3, 5, 7, 11)},    // Case D.3
				{Name: "D3b", From: 13, To: 14, Expected: i256s(3, 5, 7, 11)},    // Case D.3
				{Name: "E", From: 15, To: 16, Expected: i256s(3, 5, 7, 11, 13)},  // Case E
				{Name: "Fa", From: 0, To: 16, Expected: i256s()},                 // Case F
				{Name: "Fb", From: 0, To: 13, Expected: i256s()},                 // Case F
				{Name: "Fc", From: 3, To: 13, Expected: i256s()},                 // Case F
			},
		},
	}

	for _, v := range tests {
		for _, r := range v.Ranges {
			assert.Equal(t, r.Expected, Int256RemoveRange(v.Slice, i256(r.From), i256(r.To)), r.Name)
		}
	}
}

func TestInt256IntersectRange(t *testing.T) {
	type TestRange struct {
		Name     string
		From     int
		To       int
		Expected []Int256
	}

	type Testcase struct {
		Slice  []Int256
		Ranges []TestRange
	}

	var tests = []Testcase{
		// nil slice
		{
			Slice: nil,
			Ranges: []TestRange{
				{Name: "NIL", From: 0, To: 2, Expected: nil},
			},
		},
		// empty slice
		{
			Slice: i256s(),
			Ranges: []TestRange{
				{Name: "EMPTY", From: 0, To: 2, Expected: i256s()},
			},
		},
		// 1-element slice
		{
			Slice: i256s(3),
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: i256s()},   // Case A
				{Name: "B1", From: 1, To: 3, Expected: i256s(3)}, // Case B.1, D1
				{Name: "B3", From: 3, To: 4, Expected: i256s(3)}, // Case B.3, D3
				{Name: "E", From: 15, To: 16, Expected: i256s()}, // Case E
				{Name: "F", From: 1, To: 4, Expected: i256s(3)},  // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: i256s(3),
			Ranges: []TestRange{
				{Name: "BCD", From: 3, To: 3, Expected: i256s(3)}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		{
			Slice: i256s(3, 5, 7, 11, 13),
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: i256s()},                  // Case A
				{Name: "B1a", From: 1, To: 3, Expected: i256s(3)},               // Case B.1
				{Name: "B1b", From: 3, To: 3, Expected: i256s(3)},               // Case B.1
				{Name: "B2a", From: 1, To: 4, Expected: i256s(3)},               // Case B.2
				{Name: "B2b", From: 1, To: 5, Expected: i256s(3, 5)},            // Case B.2
				{Name: "B3a", From: 3, To: 4, Expected: i256s(3)},               // Case B.3
				{Name: "B3b", From: 3, To: 5, Expected: i256s(3, 5)},            // Case B.3
				{Name: "C1a", From: 4, To: 5, Expected: i256s(5)},               // Case C.1
				{Name: "C1b", From: 4, To: 6, Expected: i256s(5)},               // Case C.1
				{Name: "C1c", From: 4, To: 7, Expected: i256s(5, 7)},            // Case C.1
				{Name: "C1d", From: 5, To: 5, Expected: i256s(5)},               // Case C.1
				{Name: "C2a", From: 8, To: 8, Expected: i256s()},                // Case C.2
				{Name: "C2b", From: 8, To: 10, Expected: i256s()},               // Case C.2
				{Name: "D1a", From: 11, To: 13, Expected: i256s(11, 13)},        // Case D.1
				{Name: "D1b", From: 12, To: 13, Expected: i256s(13)},            // Case D.1
				{Name: "D2", From: 12, To: 14, Expected: i256s(13)},             // Case D.2
				{Name: "D3a", From: 13, To: 13, Expected: i256s(13)},            // Case D.3
				{Name: "D3b", From: 13, To: 14, Expected: i256s(13)},            // Case D.3
				{Name: "E", From: 15, To: 16, Expected: i256s()},                // Case E
				{Name: "Fa", From: 0, To: 16, Expected: i256s(3, 5, 7, 11, 13)}, // Case F
				{Name: "Fb", From: 0, To: 13, Expected: i256s(3, 5, 7, 11, 13)}, // Case F
				{Name: "Fc", From: 3, To: 13, Expected: i256s(3, 5, 7, 11, 13)}, // Case F
			},
		},
	}

	for _, v := range tests {
		for _, r := range v.Ranges {
			assert.Equal(t, r.Expected, Int256IntersectRange(v.Slice, i256(r.From), i256(r.To)), r.Name)
		}
	}
}
