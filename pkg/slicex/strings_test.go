// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package slicex

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
)

// -----------------------------------------------------------------------
// Strings
func TestStringSliceContains(t *testing.T) {
	// nil slice
	if NewOrderedStrings(nil).Contains("1") {
		t.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if NewOrderedStrings([]string{}).Contains("1") {
		t.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !NewOrderedStrings([]string{"1"}).Contains("1") {
		t.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if NewOrderedStrings([]string{"1"}).Contains("2") {
		t.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element (ASCII numbers < ASCI letters)
	nelem := []string{"1", "3", "5", "7", "B", "D"}
	if !NewOrderedStrings(nelem).Contains("1") {
		t.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !NewOrderedStrings(nelem).Contains("5") {
		t.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !NewOrderedStrings(nelem).Contains("D") {
		t.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if NewOrderedStrings(nelem).Contains("0") {
		t.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if NewOrderedStrings(nelem).Contains("2") {
		t.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if NewOrderedStrings(nelem).Contains("E") {
		t.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkStringSlice32Contains(b *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d-neg", n), func(b *testing.B) {
			a := NewOrderedStrings(util.RandStringSlices(n, 32))
			check := make([]string, 1024)
			for i := range check {
				check[i] = util.RandString(32)
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
			a := NewOrderedStrings(util.RandStringSlices(n, 32))
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				a.Contains(a.Values[util.RandIntn(len(a.Values))])
			}
		})
	}
}

func TestStringContainsRange(t *testing.T) {
	type TestRange struct {
		Name  string
		From  string
		To    string
		Match bool
	}

	type Testcase struct {
		Slice  []string
		Ranges []TestRange
	}

	var tests = []Testcase{
		// nil slice
		{
			Slice: nil,
			Ranges: []TestRange{
				{Name: "X", From: "0", To: "2", Match: false},
			},
		},
		// empty slice
		{
			Slice: []string{},
			Ranges: []TestRange{
				{Name: "X", From: "0", To: "2", Match: false},
			},
		},
		// 1-element slice
		{
			Slice: []string{"3"},
			Ranges: []TestRange{
				{Name: "A", From: "0", To: "2", Match: false}, // Case A
				{Name: "B1", From: "1", To: "3", Match: true}, // Case B.1, D1
				{Name: "B3", From: "3", To: "4", Match: true}, // Case B.3, D3
				{Name: "E", From: "F", To: "G", Match: false}, // Case E
				{Name: "F", From: "1", To: "4", Match: true},  // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: []string{"3"},
			Ranges: []TestRange{
				{Name: "BCD", From: "3", To: "3", Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		{
			Slice: []string{"3", "5", "7", "B", "D"},
			Ranges: []TestRange{
				{Name: "A", From: "0", To: "2", Match: false},   // Case A
				{Name: "B1a", From: "1", To: "3", Match: true},  // Case B.1
				{Name: "B1b", From: "3", To: "3", Match: true},  // Case B.1
				{Name: "B2a", From: "1", To: "4", Match: true},  // Case B.2
				{Name: "B2b", From: "1", To: "5", Match: true},  // Case B.2
				{Name: "B3a", From: "3", To: "4", Match: true},  // Case B.3
				{Name: "B3b", From: "3", To: "5", Match: true},  // Case B.3
				{Name: "C1a", From: "4", To: "5", Match: true},  // Case C.1
				{Name: "C1b", From: "4", To: "6", Match: true},  // Case C.1
				{Name: "C1c", From: "4", To: "7", Match: true},  // Case C.1
				{Name: "C1d", From: "5", To: "5", Match: true},  // Case C.1
				{Name: "C2a", From: "8", To: "8", Match: false}, // Case C.2
				{Name: "C2b", From: "8", To: "A", Match: false}, // Case C.2
				{Name: "D1a", From: "A", To: "D", Match: true},  // Case D.1
				{Name: "D1b", From: "C", To: "D", Match: true},  // Case D.1
				{Name: "D2", From: "C", To: "E", Match: true},   // Case D.2
				{Name: "D3a", From: "D", To: "E", Match: true},  // Case D.3
				{Name: "D3b", From: "D", To: "D", Match: true},  // Case D.3
				{Name: "E", From: "F", To: "G", Match: false},   // Case E
				{Name: "Fa", From: "0", To: "G", Match: true},   // Case F
				{Name: "Fb", From: "0", To: "D", Match: true},   // Case F
				{Name: "Fc", From: "3", To: "D", Match: true},   // Case F
			},
		},
		// real-word testcase
		{
			Slice: []string{
				"0699421", "1374016", "1692360", "1797909", "1809339",
				"2552208", "2649552", "2740915", "2769610", "3043393",
			},
			Ranges: []TestRange{
				{Name: "1", From: "2785281", To: "2818048", Match: false},
				{Name: "2", From: "2818049", To: "2850816", Match: false},
				{Name: "3", From: "2850817", To: "2883584", Match: false},
				{Name: "4", From: "2883585", To: "2916352", Match: false},
				{Name: "5", From: "2916353", To: "2949120", Match: false},
				{Name: "6", From: "2949121", To: "2981888", Match: false},
				{Name: "7", From: "2981889", To: "3014656", Match: false},
				{Name: "8", From: "3014657", To: "3047424", Match: true},
			},
		},
	}

	for i, v := range tests {
		for _, r := range v.Ranges {
			if want, got := r.Match, NewOrderedStrings(v.Slice).ContainsRange(r.From, r.To); want != got {
				t.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkStringSlice32ContainsRange(b *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			a := NewOrderedStrings(util.RandStringSlices(n, 32))
			ranges := make([][2]string, 1024)
			for i := range ranges {
				min, max := util.RandString(32), util.RandString(32)
				if min > max {
					min, max = max, min
				}
				ranges[i] = [2]string{min, max}
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				a.ContainsRange(ranges[i%1024][0], ranges[i%1024][1])
			}
		})
	}
}

func ss(n ...int) []string {
	b := make([]string, len(n))
	for i, v := range n {
		b[i] = strings.ToUpper(strconv.FormatInt(int64(v), 16))
	}
	return b
}

func TestStringSliceUnique(t *testing.T) {
	var tests = []struct {
		n string
		a *OrderedStrings
		b *OrderedStrings
		r *OrderedStrings
	}{
		{
			n: "empty",
			a: NewOrderedStrings(nil).SetUnique(),
			b: NewOrderedStrings(nil).SetUnique(),
			r: NewOrderedStrings(nil).SetUnique(),
		},
		{
			n: "empty a",
			a: NewOrderedStrings(nil).SetUnique(),
			b: NewOrderedStrings(ss(1, 2)).SetUnique(),
			r: NewOrderedStrings(ss(1, 2)).SetUnique(),
		},
		{
			n: "empty b",
			a: NewOrderedStrings(ss(1, 2)).SetUnique(),
			b: NewOrderedStrings(nil).SetUnique(),
			r: NewOrderedStrings(ss(1, 2)).SetUnique(),
		},
		{
			n: "distinct unique",
			a: NewOrderedStrings(ss(1, 2)).SetUnique(),
			b: NewOrderedStrings(ss(3, 4)).SetUnique(),
			r: NewOrderedStrings(ss(1, 2, 3, 4)).SetUnique(),
		},
		{
			n: "distinct unique gap",
			a: NewOrderedStrings(ss(1, 2)).SetUnique(),
			b: NewOrderedStrings(ss(4, 5)).SetUnique(),
			r: NewOrderedStrings(ss(1, 2, 4, 5)).SetUnique(),
		},
		{
			n: "overlap duplicates",
			a: NewOrderedStrings(ss(1, 2)).SetUnique(),
			b: NewOrderedStrings(ss(2, 3)).SetUnique(),
			r: NewOrderedStrings(ss(1, 2, 3)).SetUnique(),
		},
		{
			n: "overlap duplicates not unique",
			a: NewOrderedStrings(ss(1, 2)),
			b: NewOrderedStrings(ss(2, 3)),
			r: NewOrderedStrings(ss(1, 2, 2, 3)),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := c.a.Union(c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestStringSliceIntersect(t *testing.T) {
	var tests = []struct {
		n string
		a *OrderedStrings
		b *OrderedStrings
		r *OrderedStrings
	}{
		{
			n: "empty",
			a: NewOrderedStrings(nil).SetUnique(),
			b: NewOrderedStrings(nil).SetUnique(),
			r: NewOrderedStrings(nil).SetUnique(),
		},
		{
			n: "empty a",
			a: NewOrderedStrings(nil).SetUnique(),
			b: NewOrderedStrings(ss(1, 2)).SetUnique(),
			r: NewOrderedStrings(ss()).SetUnique(),
		},
		{
			n: "empty b",
			a: NewOrderedStrings(ss(1, 2)).SetUnique(),
			b: NewOrderedStrings(nil).SetUnique(),
			r: NewOrderedStrings(ss()).SetUnique(),
		},
		{
			n: "distinct unique",
			a: NewOrderedStrings(ss(1, 2)).SetUnique(),
			b: NewOrderedStrings(ss(3, 4)).SetUnique(),
			r: NewOrderedStrings(ss()).SetUnique(),
		},
		{
			n: "distinct unique gap",
			a: NewOrderedStrings(ss(1, 2)).SetUnique(),
			b: NewOrderedStrings(ss(4, 5)).SetUnique(),
			r: NewOrderedStrings(ss()).SetUnique(),
		},
		{
			n: "overlap duplicates",
			a: NewOrderedStrings(ss(1, 2)).SetUnique(),
			b: NewOrderedStrings(ss(2, 3)).SetUnique(),
			r: NewOrderedStrings(ss(2)).SetUnique(),
		},
		{
			n: "overlap duplicates not unique",
			a: NewOrderedStrings(ss(1, 2)),
			b: NewOrderedStrings(ss(2, 3)),
			r: NewOrderedStrings(ss(2)),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := c.a.Intersect(c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestStringSliceDifference(t *testing.T) {
	var tests = []struct {
		n string
		a *OrderedStrings
		b *OrderedStrings
		r *OrderedStrings
	}{
		{
			n: "empty",
			a: NewOrderedStrings(nil).SetUnique(),
			b: NewOrderedStrings(nil).SetUnique(),
			r: NewOrderedStrings(nil).SetUnique(),
		},
		{
			n: "empty a",
			a: NewOrderedStrings(nil).SetUnique(),
			b: NewOrderedStrings(ss(1, 2)).SetUnique(),
			r: NewOrderedStrings(ss()).SetUnique(),
		},
		{
			n: "empty b",
			a: NewOrderedStrings(ss(1, 2)).SetUnique(),
			b: NewOrderedStrings(nil).SetUnique(),
			r: NewOrderedStrings(ss(1, 2)).SetUnique(),
		},
		{
			n: "distinct unique",
			a: NewOrderedStrings(ss(1, 2)).SetUnique(),
			b: NewOrderedStrings(ss(3, 4)).SetUnique(),
			r: NewOrderedStrings(ss(1, 2)).SetUnique(),
		},
		{
			n: "distinct unique gap",
			a: NewOrderedStrings(ss(1, 2)).SetUnique(),
			b: NewOrderedStrings(ss(4, 5)).SetUnique(),
			r: NewOrderedStrings(ss(1, 2)).SetUnique(),
		},
		{
			n: "overlap duplicates",
			a: NewOrderedStrings(ss(1, 2)).SetUnique(),
			b: NewOrderedStrings(ss(2, 3)).SetUnique(),
			r: NewOrderedStrings(ss(1)).SetUnique(),
		},
		{
			n: "overlap duplicates not unique",
			a: NewOrderedStrings(ss(1, 2)),
			b: NewOrderedStrings(ss(2, 3)),
			r: NewOrderedStrings(ss(1)),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := c.a.Difference(c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestStringsRemoveRange(t *testing.T) {
	type TestRange struct {
		Name     string
		From     string
		To       string
		Expected []string
	}

	type Testcase struct {
		Slice  []string
		Ranges []TestRange
	}

	var tests = []Testcase{
		// nil slice
		{
			Slice: nil,
			Ranges: []TestRange{
				{Name: "NIL", From: "0", To: "2", Expected: ss()},
			},
		},
		// empty slice
		{
			Slice: ss(),
			Ranges: []TestRange{
				{Name: "EMPTY", From: "0", To: "2", Expected: ss()},
			},
		},
		// 1-element slice
		{
			Slice: ss(3),
			Ranges: []TestRange{
				{Name: "A", From: "0", To: "2", Expected: ss(3)}, // Case A
				{Name: "B1", From: "1", To: "3", Expected: ss()}, // Case B.1, D1
				{Name: "B3", From: "3", To: "4", Expected: ss()}, // Case B.3, D3
				{Name: "E", From: "F", To: "G", Expected: ss(3)}, // Case E
				{Name: "F", From: "1", To: "4", Expected: ss()},  // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: ss(3),
			Ranges: []TestRange{
				{Name: "BCD", From: "3", To: "3", Expected: ss()}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		{
			Slice: []string{"3", "5", "7", "B", "D"},
			Ranges: []TestRange{
				{Name: "A", From: "0", To: "2", Expected: []string{"3", "5", "7", "B", "D"}},   // Case A
				{Name: "B1a", From: "1", To: "3", Expected: []string{"5", "7", "B", "D"}},      // Case B.1
				{Name: "B1b", From: "3", To: "3", Expected: []string{"5", "7", "B", "D"}},      // Case B.1
				{Name: "B2a", From: "1", To: "4", Expected: []string{"5", "7", "B", "D"}},      // Case B.2
				{Name: "B2b", From: "1", To: "5", Expected: []string{"7", "B", "D"}},           // Case B.2
				{Name: "B3a", From: "3", To: "4", Expected: []string{"5", "7", "B", "D"}},      // Case B.3
				{Name: "B3b", From: "3", To: "5", Expected: []string{"7", "B", "D"}},           // Case B.3
				{Name: "C1a", From: "4", To: "5", Expected: []string{"3", "7", "B", "D"}},      // Case C.1
				{Name: "C1b", From: "4", To: "6", Expected: []string{"3", "7", "B", "D"}},      // Case C.1
				{Name: "C1c", From: "4", To: "7", Expected: []string{"3", "B", "D"}},           // Case C.1
				{Name: "C1d", From: "5", To: "5", Expected: []string{"3", "7", "B", "D"}},      // Case C.1
				{Name: "C2a", From: "8", To: "8", Expected: []string{"3", "5", "7", "B", "D"}}, // Case C.2
				{Name: "C2b", From: "8", To: "A", Expected: []string{"3", "5", "7", "B", "D"}}, // Case C.2
				{Name: "D1a", From: "B", To: "D", Expected: []string{"3", "5", "7"}},           // Case D.1
				{Name: "D1b", From: "C", To: "D", Expected: []string{"3", "5", "7", "B"}},      // Case D.1
				{Name: "D2", From: "C", To: "E", Expected: []string{"3", "5", "7", "B"}},       // Case D.2
				{Name: "D3a", From: "D", To: "D", Expected: []string{"3", "5", "7", "B"}},      // Case D.3
				{Name: "D3b", From: "D", To: "E", Expected: []string{"3", "5", "7", "B"}},      // Case D.3
				{Name: "E", From: "F", To: "G", Expected: []string{"3", "5", "7", "B", "D"}},   // Case E
				{Name: "Fa", From: "0", To: "G", Expected: ss()},                               // Case F
				{Name: "Fb", From: "0", To: "D", Expected: ss()},                               // Case F
				{Name: "Fc", From: "3", To: "D", Expected: ss()},                               // Case F
			},
		},
	}

	for _, v := range tests {
		for _, r := range v.Ranges {
			t.Run(r.Name, func(t *testing.T) {
				assert.Equal(t, r.Expected, NewOrderedStrings(v.Slice).RemoveRange(r.From, r.To).Values)
			})
		}
	}
}

func TestOrderedStringsIntersectRange(t *testing.T) {
	type TestRange struct {
		Name     string
		From     string
		To       string
		Expected []string
	}

	type Testcase struct {
		Slice  []string
		Ranges []TestRange
	}

	var tests = []Testcase{
		// nil slice
		{
			Slice: nil,
			Ranges: []TestRange{
				{Name: "NIL", From: "0", To: "2", Expected: ss()},
			},
		},
		// empty slice
		{
			Slice: ss(),
			Ranges: []TestRange{
				{Name: "EMPTY", From: "0", To: "2", Expected: ss()},
			},
		},
		// 1-element slice
		{
			Slice: ss(3),
			Ranges: []TestRange{
				{Name: "A", From: "0", To: "2", Expected: ss()},   // Case A
				{Name: "B1", From: "1", To: "3", Expected: ss(3)}, // Case B.1, D1
				{Name: "B3", From: "3", To: "4", Expected: ss(3)}, // Case B.3, D3
				{Name: "E", From: "15", To: "16", Expected: ss()}, // Case E
				{Name: "F", From: "1", To: "4", Expected: ss(3)},  // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: ss(3),
			Ranges: []TestRange{
				{Name: "BCD", From: "3", To: "3", Expected: ss(3)}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		{
			Slice: []string{"3", "5", "7", "B", "D"},
			Ranges: []TestRange{
				{Name: "A", From: "0", To: "2", Expected: ss()},                               // Case A
				{Name: "B1a", From: "1", To: "3", Expected: ss(3)},                            // Case B.1
				{Name: "B1b", From: "3", To: "3", Expected: ss(3)},                            // Case B.1
				{Name: "B2a", From: "1", To: "4", Expected: ss(3)},                            // Case B.2
				{Name: "B2b", From: "1", To: "5", Expected: ss(3, 5)},                         // Case B.2
				{Name: "B3a", From: "3", To: "4", Expected: ss(3)},                            // Case B.3
				{Name: "B3b", From: "3", To: "5", Expected: ss(3, 5)},                         // Case B.3
				{Name: "C1a", From: "4", To: "5", Expected: ss(5)},                            // Case C.1
				{Name: "C1b", From: "4", To: "6", Expected: ss(5)},                            // Case C.1
				{Name: "C1c", From: "4", To: "7", Expected: ss(5, 7)},                         // Case C.1
				{Name: "C1d", From: "5", To: "5", Expected: ss(5)},                            // Case C.1
				{Name: "C2a", From: "8", To: "8", Expected: ss()},                             // Case C.2
				{Name: "C2b", From: "8", To: "A", Expected: ss()},                             // Case C.2
				{Name: "D1a", From: "B", To: "D", Expected: []string{"B", "D"}},               // Case D.1
				{Name: "D1b", From: "C", To: "D", Expected: []string{"D"}},                    // Case D.1
				{Name: "D2", From: "C", To: "E", Expected: []string{"D"}},                     // Case D.2
				{Name: "D3a", From: "D", To: "D", Expected: []string{"D"}},                    // Case D.3
				{Name: "D3b", From: "D", To: "E", Expected: []string{"D"}},                    // Case D.3
				{Name: "E", From: "F", To: "G", Expected: ss()},                               // Case E
				{Name: "Fa", From: "0", To: "G", Expected: []string{"3", "5", "7", "B", "D"}}, // Case F
				{Name: "Fb", From: "0", To: "D", Expected: []string{"3", "5", "7", "B", "D"}}, // Case F
				{Name: "Fc", From: "3", To: "D", Expected: []string{"3", "5", "7", "B", "D"}}, // Case F
			},
		},
	}

	for _, v := range tests {
		for _, r := range v.Ranges {
			t.Run(r.Name, func(t *testing.T) {
				assert.Equal(t, r.Expected, NewOrderedStrings(v.Slice).IntersectRange(r.From, r.To).Values)
			})
		}
	}
}
