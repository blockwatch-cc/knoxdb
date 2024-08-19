// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"bytes"
	"math"
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

var (
	randInt64Slice         = tests.RandInt64Slice
	int64EqualCases        = tests.Int64EqualCases
	int64NotEqualCases     = tests.Int64NotEqualCases
	int64LessCases         = tests.Int64LessCases
	int64LessEqualCases    = tests.Int64LessEqualCases
	int64GreaterCases      = tests.Int64GreaterCases
	int64GreaterEqualCases = tests.Int64GreaterEqualCases
	int64BetweenCases      = tests.Int64BetweenCases

	// instantiate
	matchInt64Equal        = MatchEqual[int64]
	matchInt64NotEqual     = MatchNotEqual[int64]
	matchInt64Less         = MatchLess[int64]
	matchInt64LessEqual    = MatchLessEqual[int64]
	matchInt64Greater      = MatchGreater[int64]
	matchInt64GreaterEqual = MatchGreaterEqual[int64]
	matchInt64Between      = MatchBetween[int64]
)

const Int64Size = 8

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt64Equal(T *testing.T) {
	for _, c := range int64EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt64Equal(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
	}
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt64Equal(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64Equal(a, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt64NotEqual(T *testing.T) {
	for _, c := range int64NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt64NotEqual(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
	}
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt64NotEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64NotEqual(a, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt64Less(T *testing.T) {
	for _, c := range int64LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt64Less(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
	}
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt64Less(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64Less(a, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt64LessEqual(T *testing.T) {
	for _, c := range int64LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt64LessEqual(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
	}
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt64LessEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64LessEqual(a, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt64Greater(T *testing.T) {
	for _, c := range int64GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt64Greater(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
	}
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt64Greater(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64Greater(a, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt64GreaterEqual(T *testing.T) {
	for _, c := range int64GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt64GreaterEqual(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
	}
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt64GreaterEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64GreaterEqual(a, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt64Between(T *testing.T) {
	for _, c := range int64BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt64Between(c.Slice, c.Match, c.Match2, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
	}
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt64Between(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64Between(a, math.MaxInt64/4, math.MaxInt64/2, bits)
			}
		})
	}
}
