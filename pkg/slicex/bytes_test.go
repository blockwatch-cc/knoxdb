// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package slicex

import (
	"bytes"
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
)

// -----------------------------------------------------------------------
// Byte Slice
func TestBytesContains(t *testing.T) {
	// nil slice
	if NewOrderedBytes(nil).Contains([]byte{1}) {
		t.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if NewOrderedBytes([][]byte{}).Contains([]byte{1}) {
		t.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !NewOrderedBytes([][]byte{{1}}).Contains([]byte{1}) {
		t.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if NewOrderedBytes([][]byte{{1}}).Contains([]byte{2}) {
		t.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	nelem := [][]byte{{1}, {3}, {5}, {7}, {11}, {13}}
	if !NewOrderedBytes(nelem).Contains([]byte{1}) {
		t.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !NewOrderedBytes(nelem).Contains([]byte{5}) {
		t.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !NewOrderedBytes(nelem).Contains([]byte{13}) {
		t.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if NewOrderedBytes(nelem).Contains([]byte{0}) {
		t.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if NewOrderedBytes(nelem).Contains([]byte{2}) {
		t.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if NewOrderedBytes(nelem).Contains([]byte{14}) {
		t.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkBytesContains(b *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d-neg", n), func(b *testing.B) {
			a := NewOrderedBytes(util.RandByteSlices(n, 32))
			check := make([][]byte, 1024)
			for i := range check {
				check[i] = util.RandBytes(32)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				a.Contains(check[i%1024])
			}
		})
	}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d-pos", n), func(b *testing.B) {
			a := NewOrderedBytes(util.RandByteSlices(n, 32))
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				a.Contains(a.Values[util.RandIntn(len(a.Values))])
			}
		})
	}
}

func TestBytesContainsRange(t *testing.T) {
	type TestRange struct {
		Name  string
		From  []byte
		To    []byte
		Match bool
	}

	type Testcase struct {
		Slice  [][]byte
		Ranges []TestRange
	}

	var tests = []Testcase{
		// nil slice
		{
			Slice: nil,
			Ranges: []TestRange{
				{Name: "X", From: []byte{0}, To: []byte{2}, Match: false},
			},
		},
		// empty slice
		{
			Slice: bs(),
			Ranges: []TestRange{
				{Name: "X", From: []byte{0}, To: []byte{2}, Match: false},
			},
		},
		// 1-element slice
		{
			Slice: bs(3),
			Ranges: []TestRange{
				{Name: "A", From: []byte{0}, To: []byte{2}, Match: false},   // Case A
				{Name: "B1", From: []byte{1}, To: []byte{3}, Match: true},   // Case B.1, D1
				{Name: "B3", From: []byte{3}, To: []byte{4}, Match: true},   // Case B.3, D3
				{Name: "E", From: []byte{15}, To: []byte{16}, Match: false}, // Case E
				{Name: "F", From: []byte{1}, To: []byte{4}, Match: true},    // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: bs(3),
			Ranges: []TestRange{
				{Name: "BCD", From: []byte{3}, To: []byte{3}, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice (odd element count)
		{
			Slice: bs(3, 5, 7, 11, 13),
			Ranges: []TestRange{
				{Name: "A", From: []byte{0}, To: []byte{2}, Match: false},    // Case A
				{Name: "B1a", From: []byte{1}, To: []byte{3}, Match: true},   // Case B.1
				{Name: "B1b", From: []byte{3}, To: []byte{3}, Match: true},   // Case B.1
				{Name: "B2a", From: []byte{1}, To: []byte{4}, Match: true},   // Case B.2
				{Name: "B2b", From: []byte{1}, To: []byte{5}, Match: true},   // Case B.2
				{Name: "B3a", From: []byte{3}, To: []byte{4}, Match: true},   // Case B.3
				{Name: "B3b", From: []byte{3}, To: []byte{5}, Match: true},   // Case B.3
				{Name: "C1a", From: []byte{4}, To: []byte{5}, Match: true},   // Case C.1
				{Name: "C1b", From: []byte{4}, To: []byte{6}, Match: true},   // Case C.1
				{Name: "C1c", From: []byte{4}, To: []byte{7}, Match: true},   // Case C.1
				{Name: "C1d", From: []byte{5}, To: []byte{5}, Match: true},   // Case C.1
				{Name: "C2a", From: []byte{8}, To: []byte{8}, Match: false},  // Case C.2
				{Name: "C2b", From: []byte{8}, To: []byte{10}, Match: false}, // Case C.2
				{Name: "D1a", From: []byte{10}, To: []byte{13}, Match: true}, // Case D.1
				{Name: "D1b", From: []byte{12}, To: []byte{13}, Match: true}, // Case D.1
				{Name: "D2", From: []byte{12}, To: []byte{14}, Match: true},  // Case D.2
				{Name: "D3a", From: []byte{13}, To: []byte{14}, Match: true}, // Case D.3
				{Name: "D3b", From: []byte{13}, To: []byte{13}, Match: true}, // Case D.3
				{Name: "E", From: []byte{15}, To: []byte{16}, Match: false},  // Case E
				{Name: "Fa", From: []byte{0}, To: []byte{16}, Match: true},   // Case F
				{Name: "Fb", From: []byte{0}, To: []byte{13}, Match: true},   // Case F
				{Name: "Fc", From: []byte{3}, To: []byte{13}, Match: true},   // Case F
			},
		},
		// real-word testcase
		{
			Slice: [][]byte{
				[]byte("0699421"), []byte("1374016"), []byte("1692360"),
				[]byte("1797909"), []byte("1809339"), []byte("2552208"),
				[]byte("2649552"), []byte("2740915"), []byte("2769610"),
				[]byte("3043393"),
			},
			Ranges: []TestRange{
				{Name: "1", From: []byte("2785281"), To: []byte("2818048"), Match: false},
				{Name: "2", From: []byte("2818049"), To: []byte("2850816"), Match: false},
				{Name: "3", From: []byte("2850817"), To: []byte("2883584"), Match: false},
				{Name: "4", From: []byte("2883585"), To: []byte("2916352"), Match: false},
				{Name: "5", From: []byte("2916353"), To: []byte("2949120"), Match: false},
				{Name: "6", From: []byte("2949121"), To: []byte("2981888"), Match: false},
				{Name: "7", From: []byte("2981889"), To: []byte("3014656"), Match: false},
				{Name: "8", From: []byte("3014657"), To: []byte("3047424"), Match: true},
			},
		},
	}

	for i, v := range tests {
		for _, r := range v.Ranges {
			if want, got := r.Match, NewOrderedBytes(v.Slice).ContainsRange(r.From, r.To); want != got {
				t.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkBytes32ContainsRange(b *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := NewOrderedBytes(util.RandByteSlices(n, 32))
			ranges := make([][2][]byte, 1024)
			for i := range ranges {
				min, max := util.RandBytes(32), util.RandBytes(32)
				if bytes.Compare(min, max) > 0 {
					min, max = max, min
				}
				ranges[i] = [2][]byte{min, max}
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				a.ContainsRange(ranges[i%1024][0], ranges[i%1024][1])
			}
		})
	}
}

func bs(n ...int) [][]byte {
	b := make([][]byte, len(n))
	for i, v := range n {
		b[i] = []byte{byte(v)}
	}
	return b
}

func TestBytesUnique(t *testing.T) {
	var tests = []struct {
		n string
		a *OrderedBytes
		b *OrderedBytes
		r *OrderedBytes
	}{
		{
			n: "empty",
			a: NewOrderedBytes(nil).SetUnique(),
			b: NewOrderedBytes(nil).SetUnique(),
			r: NewOrderedBytes(nil).SetUnique(),
		},
		{
			n: "empty a",
			a: NewOrderedBytes(nil).SetUnique(),
			b: NewOrderedBytes(bs(1, 2)).SetUnique(),
			r: NewOrderedBytes(bs(1, 2)).SetUnique(),
		},
		{
			n: "empty b",
			a: NewOrderedBytes(bs(1, 2)).SetUnique(),
			b: NewOrderedBytes(nil).SetUnique(),
			r: NewOrderedBytes(bs(1, 2)).SetUnique(),
		},
		{
			n: "distinct unique",
			a: NewOrderedBytes(bs(1, 2)).SetUnique(),
			b: NewOrderedBytes(bs(3, 4)).SetUnique(),
			r: NewOrderedBytes(bs(1, 2, 3, 4)).SetUnique(),
		},
		{
			n: "distinct unique gap",
			a: NewOrderedBytes(bs(1, 2)).SetUnique(),
			b: NewOrderedBytes(bs(4, 5)).SetUnique(),
			r: NewOrderedBytes(bs(1, 2, 4, 5)).SetUnique(),
		},
		{
			n: "overlap duplicates",
			a: NewOrderedBytes(bs(1, 2)).SetUnique(),
			b: NewOrderedBytes(bs(2, 3)).SetUnique(),
			r: NewOrderedBytes(bs(1, 2, 3)).SetUnique(),
		},
		{
			n: "overlap duplicates not unique",
			a: NewOrderedBytes(bs(1, 2)),
			b: NewOrderedBytes(bs(2, 3)),
			r: NewOrderedBytes(bs(1, 2, 2, 3)),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := c.a.Union(c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestBytesIntersect(t *testing.T) {
	var tests = []struct {
		n string
		a *OrderedBytes
		b *OrderedBytes
		r *OrderedBytes
	}{
		{
			n: "empty",
			a: NewOrderedBytes(nil).SetUnique(),
			b: NewOrderedBytes(nil).SetUnique(),
			r: NewOrderedBytes(nil).SetUnique(),
		},
		{
			n: "empty a",
			a: NewOrderedBytes(nil).SetUnique(),
			b: NewOrderedBytes(bs(1, 2)).SetUnique(),
			r: NewOrderedBytes(bs()).SetUnique(),
		},
		{
			n: "empty b",
			a: NewOrderedBytes(bs(1, 2)).SetUnique(),
			b: NewOrderedBytes(nil).SetUnique(),
			r: NewOrderedBytes(bs()).SetUnique(),
		},
		{
			n: "distinct unique",
			a: NewOrderedBytes(bs(1, 2)).SetUnique(),
			b: NewOrderedBytes(bs(3, 4)).SetUnique(),
			r: NewOrderedBytes(bs()).SetUnique(),
		},
		{
			n: "distinct unique gap",
			a: NewOrderedBytes(bs(1, 2)).SetUnique(),
			b: NewOrderedBytes(bs(4, 5)).SetUnique(),
			r: NewOrderedBytes(bs()).SetUnique(),
		},
		{
			n: "overlap duplicates",
			a: NewOrderedBytes(bs(1, 2)).SetUnique(),
			b: NewOrderedBytes(bs(2, 3)).SetUnique(),
			r: NewOrderedBytes(bs(2)).SetUnique(),
		},
		{
			n: "overlap duplicates not unique",
			a: NewOrderedBytes(bs(1, 2)),
			b: NewOrderedBytes(bs(2, 3)),
			r: NewOrderedBytes(bs(2)),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := c.a.Intersect(c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestBytesDifference(t *testing.T) {
	var tests = []struct {
		n string
		a *OrderedBytes
		b *OrderedBytes
		r *OrderedBytes
	}{
		{
			n: "empty",
			a: NewOrderedBytes(nil).SetUnique(),
			b: NewOrderedBytes(nil).SetUnique(),
			r: NewOrderedBytes(nil).SetUnique(),
		},
		{
			n: "empty a",
			a: NewOrderedBytes(nil).SetUnique(),
			b: NewOrderedBytes(bs(1, 2)).SetUnique(),
			r: NewOrderedBytes(bs()).SetUnique(),
		},
		{
			n: "empty b",
			a: NewOrderedBytes(bs(1, 2)).SetUnique(),
			b: NewOrderedBytes(nil).SetUnique(),
			r: NewOrderedBytes(bs(1, 2)).SetUnique(),
		},
		{
			n: "distinct unique",
			a: NewOrderedBytes(bs(1, 2)).SetUnique(),
			b: NewOrderedBytes(bs(3, 4)).SetUnique(),
			r: NewOrderedBytes(bs(1, 2)).SetUnique(),
		},
		{
			n: "distinct unique gap",
			a: NewOrderedBytes(bs(1, 2)).SetUnique(),
			b: NewOrderedBytes(bs(4, 5)).SetUnique(),
			r: NewOrderedBytes(bs(1, 2)).SetUnique(),
		},
		{
			n: "overlap duplicates",
			a: NewOrderedBytes(bs(1, 2)).SetUnique(),
			b: NewOrderedBytes(bs(2, 3)).SetUnique(),
			r: NewOrderedBytes(bs(1)).SetUnique(),
		},
		{
			n: "overlap duplicates not unique",
			a: NewOrderedBytes(bs(1, 2)),
			b: NewOrderedBytes(bs(2, 3)),
			r: NewOrderedBytes(bs(1)),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := c.a.Difference(c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestBytesRemoveRange(t *testing.T) {
	type TestRange struct {
		Name     string
		From     byte
		To       byte
		Expected [][]byte
	}

	type Testcase struct {
		Slice  [][]byte
		Ranges []TestRange
	}

	var tests = []Testcase{
		// nil slice
		{
			Slice: nil,
			Ranges: []TestRange{
				{Name: "NIL", From: 0, To: 2, Expected: bs()},
			},
		},
		// empty slice
		{
			Slice: bs(),
			Ranges: []TestRange{
				{Name: "EMPTY", From: 0, To: 2, Expected: bs()},
			},
		},
		// 1-element slice
		{
			Slice: bs(3),
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: bs(3)},   // Case A
				{Name: "B1", From: 1, To: 3, Expected: bs()},   // Case B.1, D1
				{Name: "B3", From: 3, To: 4, Expected: bs()},   // Case B.3, D3
				{Name: "E", From: 15, To: 16, Expected: bs(3)}, // Case E
				{Name: "F", From: 1, To: 4, Expected: bs()},    // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: bs(3),
			Ranges: []TestRange{
				{Name: "BCD", From: 3, To: 3, Expected: bs()}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		{
			Slice: bs(3, 5, 7, 11, 13),
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: bs(3, 5, 7, 11, 13)},    // Case A
				{Name: "B1a", From: 1, To: 3, Expected: bs(5, 7, 11, 13)},     // Case B.1
				{Name: "B1b", From: 3, To: 3, Expected: bs(5, 7, 11, 13)},     // Case B.1
				{Name: "B2a", From: 1, To: 4, Expected: bs(5, 7, 11, 13)},     // Case B.2
				{Name: "B2b", From: 1, To: 5, Expected: bs(7, 11, 13)},        // Case B.2
				{Name: "B3a", From: 3, To: 4, Expected: bs(5, 7, 11, 13)},     // Case B.3
				{Name: "B3b", From: 3, To: 5, Expected: bs(7, 11, 13)},        // Case B.3
				{Name: "C1a", From: 4, To: 5, Expected: bs(3, 7, 11, 13)},     // Case C.1
				{Name: "C1b", From: 4, To: 6, Expected: bs(3, 7, 11, 13)},     // Case C.1
				{Name: "C1c", From: 4, To: 7, Expected: bs(3, 11, 13)},        // Case C.1
				{Name: "C1d", From: 5, To: 5, Expected: bs(3, 7, 11, 13)},     // Case C.1
				{Name: "C2a", From: 8, To: 8, Expected: bs(3, 5, 7, 11, 13)},  // Case C.2
				{Name: "C2b", From: 8, To: 10, Expected: bs(3, 5, 7, 11, 13)}, // Case C.2
				{Name: "D1a", From: 11, To: 13, Expected: bs(3, 5, 7)},        // Case D.1
				{Name: "D1b", From: 12, To: 13, Expected: bs(3, 5, 7, 11)},    // Case D.1
				{Name: "D2", From: 12, To: 14, Expected: bs(3, 5, 7, 11)},     // Case D.2
				{Name: "D3a", From: 13, To: 13, Expected: bs(3, 5, 7, 11)},    // Case D.3
				{Name: "D3b", From: 13, To: 14, Expected: bs(3, 5, 7, 11)},    // Case D.3
				{Name: "E", From: 15, To: 16, Expected: bs(3, 5, 7, 11, 13)},  // Case E
				{Name: "Fa", From: 0, To: 16, Expected: bs()},                 // Case F
				{Name: "Fb", From: 0, To: 13, Expected: bs()},                 // Case F
				{Name: "Fc", From: 3, To: 13, Expected: bs()},                 // Case F
			},
		},
	}

	for _, v := range tests {
		for _, r := range v.Ranges {
			t.Run(r.Name, func(t *testing.T) {
				a, b := []byte{r.From}, []byte{r.To}
				assert.Equal(t, r.Expected, NewOrderedBytes(v.Slice).RemoveRange(a, b).Values)
			})
		}
	}
}

func TestOrderedBytesIntersectRange(t *testing.T) {
	type TestRange struct {
		Name     string
		From     byte
		To       byte
		Expected [][]byte
	}

	type Testcase struct {
		Slice  [][]byte
		Ranges []TestRange
	}

	var tests = []Testcase{
		// nil slice
		{
			Slice: nil,
			Ranges: []TestRange{
				{Name: "NIL", From: 0, To: 2, Expected: bs()},
			},
		},
		// empty slice
		{
			Slice: bs(),
			Ranges: []TestRange{
				{Name: "EMPTY", From: 0, To: 2, Expected: bs()},
			},
		},
		// 1-element slice
		{
			Slice: bs(3),
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: bs()},   // Case A
				{Name: "B1", From: 1, To: 3, Expected: bs(3)}, // Case B.1, D1
				{Name: "B3", From: 3, To: 4, Expected: bs(3)}, // Case B.3, D3
				{Name: "E", From: 15, To: 16, Expected: bs()}, // Case E
				{Name: "F", From: 1, To: 4, Expected: bs(3)},  // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: bs(3),
			Ranges: []TestRange{
				{Name: "BCD", From: 3, To: 3, Expected: bs(3)}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		{
			Slice: bs(3, 5, 7, 11, 13),
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: bs()},                  // Case A
				{Name: "B1a", From: 1, To: 3, Expected: bs(3)},               // Case B.1
				{Name: "B1b", From: 3, To: 3, Expected: bs(3)},               // Case B.1
				{Name: "B2a", From: 1, To: 4, Expected: bs(3)},               // Case B.2
				{Name: "B2b", From: 1, To: 5, Expected: bs(3, 5)},            // Case B.2
				{Name: "B3a", From: 3, To: 4, Expected: bs(3)},               // Case B.3
				{Name: "B3b", From: 3, To: 5, Expected: bs(3, 5)},            // Case B.3
				{Name: "C1a", From: 4, To: 5, Expected: bs(5)},               // Case C.1
				{Name: "C1b", From: 4, To: 6, Expected: bs(5)},               // Case C.1
				{Name: "C1c", From: 4, To: 7, Expected: bs(5, 7)},            // Case C.1
				{Name: "C1d", From: 5, To: 5, Expected: bs(5)},               // Case C.1
				{Name: "C2a", From: 8, To: 8, Expected: bs()},                // Case C.2
				{Name: "C2b", From: 8, To: 10, Expected: bs()},               // Case C.2
				{Name: "D1a", From: 11, To: 13, Expected: bs(11, 13)},        // Case D.1
				{Name: "D1b", From: 12, To: 13, Expected: bs(13)},            // Case D.1
				{Name: "D2", From: 12, To: 14, Expected: bs(13)},             // Case D.2
				{Name: "D3a", From: 13, To: 13, Expected: bs(13)},            // Case D.3
				{Name: "D3b", From: 13, To: 14, Expected: bs(13)},            // Case D.3
				{Name: "E", From: 15, To: 16, Expected: bs()},                // Case E
				{Name: "Fa", From: 0, To: 16, Expected: bs(3, 5, 7, 11, 13)}, // Case F
				{Name: "Fb", From: 0, To: 13, Expected: bs(3, 5, 7, 11, 13)}, // Case F
				{Name: "Fc", From: 3, To: 13, Expected: bs(3, 5, 7, 11, 13)}, // Case F
			},
		},
	}

	for _, v := range tests {
		for _, r := range v.Ranges {
			t.Run(r.Name, func(t *testing.T) {
				a, b := []byte{r.From}, []byte{r.To}
				assert.Equal(t, r.Expected, NewOrderedBytes(v.Slice).IntersectRange(a, b).Values)
			})
		}
	}
}
