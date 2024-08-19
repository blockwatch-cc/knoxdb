// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package generic

import (
	"bytes"
	"math"
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

var (
	randInt32Slice         = tests.RandInt32Slice
	int32EqualCases        = tests.Int32EqualCases
	int32NotEqualCases     = tests.Int32NotEqualCases
	int32LessCases         = tests.Int32LessCases
	int32LessEqualCases    = tests.Int32LessEqualCases
	int32GreaterCases      = tests.Int32GreaterCases
	int32GreaterEqualCases = tests.Int32GreaterEqualCases
	int32BetweenCases      = tests.Int32BetweenCases

	// instantiate
	matchInt32Equal        = MatchEqual[int32]
	matchInt32NotEqual     = MatchNotEqual[int32]
	matchInt32Less         = MatchLess[int32]
	matchInt32LessEqual    = MatchLessEqual[int32]
	matchInt32Greater      = MatchGreater[int32]
	matchInt32GreaterEqual = MatchGreaterEqual[int32]
	matchInt32Between      = MatchBetween[int32]
)

const Int32Size = 4

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt32Equal(T *testing.T) {
	for _, c := range int32EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt32Equal(c.Slice, c.Match, bits)
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
func BenchmarkMatchInt32Equal(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32Equal(a, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt32NotEqual(T *testing.T) {
	for _, c := range int32NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt32NotEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchInt32NotEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32NotEqual(a, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt32Less(T *testing.T) {
	for _, c := range int32LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt32Less(c.Slice, c.Match, bits)
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
func BenchmarkMatchInt32Less(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32Less(a, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt32LessEqual(T *testing.T) {
	for _, c := range int32LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt32LessEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchInt32LessEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32LessEqual(a, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt32Greater(T *testing.T) {
	for _, c := range int32GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt32Greater(c.Slice, c.Match, bits)
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
func BenchmarkMatchInt32Greater(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32Greater(a, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt32GreaterEqual(T *testing.T) {
	for _, c := range int32GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt32GreaterEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchInt32GreaterEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32GreaterEqual(a, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt32Between(T *testing.T) {
	for _, c := range int32BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt32Between(c.Slice, c.Match, c.Match2, bits)
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
func BenchmarkMatchInt32Between(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32Between(a, math.MaxInt32/4, math.MaxInt32/2, bits)
			}
		})
	}
}
