// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package dedup

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

// generates n slices of length u
func randByteSlice(n, u int) [][]byte {
	s := make([][]byte, n)
	for i := 0; i < n; i++ {
		s[i] = randBytes(u)
	}
	return s
}

func randBytes(n int) []byte {
	v := make([]byte, n)
	for i, _ := range v {
		v[i] = byte(rand.Intn(256))
	}
	return v
}

// -----------------------------------------------------------------------
// Byte Slice
func TestByteSliceContains(T *testing.T) {
	// nil slice
	if Bytes.Contains(nil, []byte{1}) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Bytes.Contains([][]byte{}, []byte{1}) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Bytes.Contains([][]byte{[]byte{1}}, []byte{1}) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Bytes.Contains([][]byte{[]byte{1}}, []byte{2}) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	nelem := [][]byte{[]byte{1}, []byte{3}, []byte{5}, []byte{7}, []byte{11}, []byte{13}}
	if !Bytes.Contains(nelem, []byte{1}) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Bytes.Contains(nelem, []byte{5}) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Bytes.Contains(nelem, []byte{13}) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Bytes.Contains(nelem, []byte{0}) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Bytes.Contains(nelem, []byte{2}) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Bytes.Contains(nelem, []byte{14}) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkByteSlice32Contains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Bytes.Sort(randByteSlice(n, 32))
			check := make([][]byte, 1024)
			for i, _ := range check {
				check[i] = randBytes(32)
			}
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
				Bytes.Contains(a, check[i%1024])
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Bytes.Sort(randByteSlice(n, 32))
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
				Bytes.Contains(a, a[rand.Intn(len(a))])
			}
		})
	}
}

func TestByteSliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  []byte
		To    []byte
		Match bool
	}

	type VecTestcase struct {
		Slice  [][]byte
		Ranges []VecTestRange
	}

	var tests = []VecTestcase{
		// nil slice
		VecTestcase{
			Slice: nil,
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: []byte{0}, To: []byte{2}, Match: false},
			},
		},
		// empty slice
		VecTestcase{
			Slice: [][]byte{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: []byte{0}, To: []byte{2}, Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: [][]byte{[]byte{3}},
			Ranges: []VecTestRange{
				VecTestRange{Name: "A", From: []byte{0}, To: []byte{2}, Match: false},   // Case A
				VecTestRange{Name: "B1", From: []byte{1}, To: []byte{3}, Match: true},   // Case B.1, D1
				VecTestRange{Name: "B3", From: []byte{3}, To: []byte{4}, Match: true},   // Case B.3, D3
				VecTestRange{Name: "E", From: []byte{15}, To: []byte{16}, Match: false}, // Case E
				VecTestRange{Name: "F", From: []byte{1}, To: []byte{4}, Match: true},    // Case F
			},
		},
		// 1-element slice, from == to
		VecTestcase{
			Slice: [][]byte{[]byte{3}},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: []byte{3}, To: []byte{3}, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice (odd element count)
		VecTestcase{
			Slice: [][]byte{[]byte{3}, []byte{5}, []byte{7}, []byte{11}, []byte{13}},
			Ranges: []VecTestRange{
				VecTestRange{Name: "A", From: []byte{0}, To: []byte{2}, Match: false},    // Case A
				VecTestRange{Name: "B1a", From: []byte{1}, To: []byte{3}, Match: true},   // Case B.1
				VecTestRange{Name: "B1b", From: []byte{3}, To: []byte{3}, Match: true},   // Case B.1
				VecTestRange{Name: "B2a", From: []byte{1}, To: []byte{4}, Match: true},   // Case B.2
				VecTestRange{Name: "B2b", From: []byte{1}, To: []byte{5}, Match: true},   // Case B.2
				VecTestRange{Name: "B3a", From: []byte{3}, To: []byte{4}, Match: true},   // Case B.3
				VecTestRange{Name: "B3b", From: []byte{3}, To: []byte{5}, Match: true},   // Case B.3
				VecTestRange{Name: "C1a", From: []byte{4}, To: []byte{5}, Match: true},   // Case C.1
				VecTestRange{Name: "C1b", From: []byte{4}, To: []byte{6}, Match: true},   // Case C.1
				VecTestRange{Name: "C1c", From: []byte{4}, To: []byte{7}, Match: true},   // Case C.1
				VecTestRange{Name: "C1d", From: []byte{5}, To: []byte{5}, Match: true},   // Case C.1
				VecTestRange{Name: "C2a", From: []byte{8}, To: []byte{8}, Match: false},  // Case C.2
				VecTestRange{Name: "C2b", From: []byte{8}, To: []byte{10}, Match: false}, // Case C.2
				VecTestRange{Name: "D1a", From: []byte{10}, To: []byte{13}, Match: true}, // Case D.1
				VecTestRange{Name: "D1b", From: []byte{12}, To: []byte{13}, Match: true}, // Case D.1
				VecTestRange{Name: "D2", From: []byte{12}, To: []byte{14}, Match: true},  // Case D.2
				VecTestRange{Name: "D3a", From: []byte{13}, To: []byte{14}, Match: true}, // Case D.3
				VecTestRange{Name: "D3b", From: []byte{13}, To: []byte{13}, Match: true}, // Case D.3
				VecTestRange{Name: "E", From: []byte{15}, To: []byte{16}, Match: false},  // Case E
				VecTestRange{Name: "Fa", From: []byte{0}, To: []byte{16}, Match: true},   // Case F
				VecTestRange{Name: "Fb", From: []byte{0}, To: []byte{13}, Match: true},   // Case F
				VecTestRange{Name: "Fc", From: []byte{3}, To: []byte{13}, Match: true},   // Case F
			},
		},
		// real-word testcase
		VecTestcase{
			Slice: [][]byte{
				[]byte("0699421"), []byte("1374016"), []byte("1692360"),
				[]byte("1797909"), []byte("1809339"), []byte("2552208"),
				[]byte("2649552"), []byte("2740915"), []byte("2769610"),
				[]byte("3043393"),
			},
			Ranges: []VecTestRange{
				VecTestRange{Name: "1", From: []byte("2785281"), To: []byte("2818048"), Match: false},
				VecTestRange{Name: "2", From: []byte("2818049"), To: []byte("2850816"), Match: false},
				VecTestRange{Name: "3", From: []byte("2850817"), To: []byte("2883584"), Match: false},
				VecTestRange{Name: "4", From: []byte("2883585"), To: []byte("2916352"), Match: false},
				VecTestRange{Name: "5", From: []byte("2916353"), To: []byte("2949120"), Match: false},
				VecTestRange{Name: "6", From: []byte("2949121"), To: []byte("2981888"), Match: false},
				VecTestRange{Name: "7", From: []byte("2981889"), To: []byte("3014656"), Match: false},
				VecTestRange{Name: "8", From: []byte("3014657"), To: []byte("3047424"), Match: true},
			},
		},
	}

	for i, v := range tests {
		for _, r := range v.Ranges {
			if want, got := r.Match, Bytes.ContainsRange(v.Slice, r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkByteSlice32ContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Bytes.Sort(randByteSlice(n, 32))
			ranges := make([][2][]byte, 1024)
			for i, _ := range ranges {
				min, max := randBytes(32), randBytes(32)
				if bytes.Compare(min, max) > 0 {
					min, max = max, min
				}
				ranges[i] = [2][]byte{min, max}
			}
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
				Bytes.ContainsRange(a, ranges[i%1024][0], ranges[i%1024][1])
			}
		})
	}
}
