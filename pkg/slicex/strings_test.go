// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package slicex

import (
	"encoding/binary"
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
)

// -----------------------------------------------------------------------
// Strings
func TestStringSliceContains(T *testing.T) {
	// nil slice
	if NewOrderedStrings(nil).Contains("1") {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if NewOrderedStrings([]string{}).Contains("1") {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !NewOrderedStrings([]string{"1"}).Contains("1") {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if NewOrderedStrings([]string{"1"}).Contains("2") {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element (ASCII numbers < ASCI letters)
	nelem := []string{"1", "3", "5", "7", "B", "D"}
	if !NewOrderedStrings(nelem).Contains("1") {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !NewOrderedStrings(nelem).Contains("5") {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !NewOrderedStrings(nelem).Contains("D") {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if NewOrderedStrings(nelem).Contains("0") {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if NewOrderedStrings(nelem).Contains("2") {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if NewOrderedStrings(nelem).Contains("E") {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkStringSlice32Contains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := NewOrderedStrings(util.RandStringSlices(n, 32))
			check := make([]string, 1024)
			for i := range check {
				check[i] = util.RandString(32)
			}
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
				a.Contains(check[i%1024])
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := NewOrderedStrings(util.RandStringSlices(n, 32))
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
				a.Contains(a.Values[util.RandIntn(len(a.Values))])
			}
		})
	}
}

func TestStringContainsRange(T *testing.T) {
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
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkStringSlice32ContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := NewOrderedStrings(util.RandStringSlices(n, 32))
			ranges := make([][2]string, 1024)
			for i := range ranges {
				min, max := util.RandString(32), util.RandString(32)
				if min > max {
					min, max = max, min
				}
				ranges[i] = [2]string{min, max}
			}
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
				a.ContainsRange(ranges[i%1024][0], ranges[i%1024][1])
			}
		})
	}
}

func ss(n ...int) []string {
	b := make([]string, len(n))
	for i, v := range n {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(v))
		b[i] = string(buf[:])
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
