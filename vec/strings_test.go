// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package vec

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var randsrc = rand.NewSource(time.Now().UnixNano())

func randString(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, randsrc.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = randsrc.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func randStringSlice(n, u int) []string {
	s := make([]string, n)
	for i := 0; i < n; i++ {
		s[i] = randString(u)
	}
	return s
}

// -----------------------------------------------------------------------
// Strings
//
func TestStringSliceContains(T *testing.T) {
	// nil slice
	if Strings.Contains(nil, "1") {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Strings.Contains([]string{}, "1") {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Strings.Contains([]string{"1"}, "1") {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Strings.Contains([]string{"1"}, "2") {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element (ASCII numbers < ASCI letters)
	nelem := []string{"1", "3", "5", "7", "B", "D"}
	if !Strings.Contains(nelem, "1") {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Strings.Contains(nelem, "5") {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Strings.Contains(nelem, "D") {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Strings.Contains(nelem, "0") {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Strings.Contains(nelem, "2") {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Strings.Contains(nelem, "E") {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkStringSlice32Contains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Strings.Sort(randStringSlice(n, 32))
			check := make([]string, 1024)
			for i, _ := range check {
				check[i] = randString(32)
			}
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
				Strings.Contains(a, check[i%1024])
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Strings.Sort(randStringSlice(n, 32))
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
				Strings.Contains(a, a[rand.Intn(len(a))])
			}
		})
	}
}

func TestStringContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  string
		To    string
		Match bool
	}

	type VecTestcase struct {
		Slice  []string
		Ranges []VecTestRange
	}

	var tests = []VecTestcase{
		// nil slice
		VecTestcase{
			Slice: nil,
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: "0", To: "2", Match: false},
			},
		},
		// empty slice
		VecTestcase{
			Slice: []string{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: "0", To: "2", Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: []string{"3"},
			Ranges: []VecTestRange{
				VecTestRange{Name: "A", From: "0", To: "2", Match: false}, // Case A
				VecTestRange{Name: "B1", From: "1", To: "3", Match: true}, // Case B.1, D1
				VecTestRange{Name: "B3", From: "3", To: "4", Match: true}, // Case B.3, D3
				VecTestRange{Name: "E", From: "F", To: "G", Match: false}, // Case E
				VecTestRange{Name: "F", From: "1", To: "4", Match: true},  // Case F
			},
		},
		// 1-element slice, from == to
		VecTestcase{
			Slice: []string{"3"},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: "3", To: "3", Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: []string{"3", "5", "7", "B", "D"},
			Ranges: []VecTestRange{
				VecTestRange{Name: "A", From: "0", To: "2", Match: false},   // Case A
				VecTestRange{Name: "B1a", From: "1", To: "3", Match: true},  // Case B.1
				VecTestRange{Name: "B1b", From: "3", To: "3", Match: true},  // Case B.1
				VecTestRange{Name: "B2a", From: "1", To: "4", Match: true},  // Case B.2
				VecTestRange{Name: "B2b", From: "1", To: "5", Match: true},  // Case B.2
				VecTestRange{Name: "B3a", From: "3", To: "4", Match: true},  // Case B.3
				VecTestRange{Name: "B3b", From: "3", To: "5", Match: true},  // Case B.3
				VecTestRange{Name: "C1a", From: "4", To: "5", Match: true},  // Case C.1
				VecTestRange{Name: "C1b", From: "4", To: "6", Match: true},  // Case C.1
				VecTestRange{Name: "C1c", From: "4", To: "7", Match: true},  // Case C.1
				VecTestRange{Name: "C1d", From: "5", To: "5", Match: true},  // Case C.1
				VecTestRange{Name: "C2a", From: "8", To: "8", Match: false}, // Case C.2
				VecTestRange{Name: "C2b", From: "8", To: "A", Match: false}, // Case C.2
				VecTestRange{Name: "D1a", From: "A", To: "D", Match: true},  // Case D.1
				VecTestRange{Name: "D1b", From: "C", To: "D", Match: true},  // Case D.1
				VecTestRange{Name: "D2", From: "C", To: "E", Match: true},   // Case D.2
				VecTestRange{Name: "D3a", From: "D", To: "E", Match: true},  // Case D.3
				VecTestRange{Name: "D3b", From: "D", To: "D", Match: true},  // Case D.3
				VecTestRange{Name: "E", From: "F", To: "G", Match: false},   // Case E
				VecTestRange{Name: "Fa", From: "0", To: "G", Match: true},   // Case F
				VecTestRange{Name: "Fb", From: "0", To: "D", Match: true},   // Case F
				VecTestRange{Name: "Fc", From: "3", To: "D", Match: true},   // Case F
			},
		},
		// real-word testcase
		VecTestcase{
			Slice: []string{
				"0699421", "1374016", "1692360", "1797909", "1809339",
				"2552208", "2649552", "2740915", "2769610", "3043393",
			},
			Ranges: []VecTestRange{
				VecTestRange{Name: "1", From: "2785281", To: "2818048", Match: false},
				VecTestRange{Name: "2", From: "2818049", To: "2850816", Match: false},
				VecTestRange{Name: "3", From: "2850817", To: "2883584", Match: false},
				VecTestRange{Name: "4", From: "2883585", To: "2916352", Match: false},
				VecTestRange{Name: "5", From: "2916353", To: "2949120", Match: false},
				VecTestRange{Name: "6", From: "2949121", To: "2981888", Match: false},
				VecTestRange{Name: "7", From: "2981889", To: "3014656", Match: false},
				VecTestRange{Name: "8", From: "3014657", To: "3047424", Match: true},
			},
		},
	}

	for i, v := range tests {
		for _, r := range v.Ranges {
			if want, got := r.Match, Strings.ContainsRange(v.Slice, r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkStringSlice32ContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Strings.Sort(randStringSlice(n, 32))
			ranges := make([][2]string, 1024)
			for i, _ := range ranges {
				min, max := randString(32), randString(32)
				if min > max {
					min, max = max, min
				}
				ranges[i] = [2]string{min, max}
			}
			B.ResetTimer()
			B.ReportAllocs()
			for i := 0; i < B.N; i++ {
				Strings.ContainsRange(a, ranges[i%1024][0], ranges[i%1024][1])
			}
		})
	}
}
