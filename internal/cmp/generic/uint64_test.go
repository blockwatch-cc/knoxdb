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
	randUint64Slice         = tests.RandUint64Slice
	uint64EqualCases        = tests.Uint64EqualCases
	uint64NotEqualCases     = tests.Uint64NotEqualCases
	uint64LessCases         = tests.Uint64LessCases
	uint64LessEqualCases    = tests.Uint64LessEqualCases
	uint64GreaterCases      = tests.Uint64GreaterCases
	uint64GreaterEqualCases = tests.Uint64GreaterEqualCases
	uint64BetweenCases      = tests.Uint64BetweenCases

	// instantiate
	matchUint64Equal        = MatchEqual[uint64]
	matchUint64NotEqual     = MatchNotEqual[uint64]
	matchUint64Less         = MatchLess[uint64]
	matchUint64LessEqual    = MatchLessEqual[uint64]
	matchUint64Greater      = MatchGreater[uint64]
	matchUint64GreaterEqual = MatchGreaterEqual[uint64]
	matchUint64Between      = MatchBetween[uint64]
)

const Uint64Size = 8

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchUint64Equal(T *testing.T) {
	for _, c := range uint64EqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint64Equal(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint64Equal(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64Equal(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
func TestMatchUint64NotEqual(T *testing.T) {
	for _, c := range uint64NotEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint64NotEqual(c.Slice, c.Match, bits)
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
// NotEqual benchmarks
func BenchmarkMatchUint64NotEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64NotEqual(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchUint64Less(T *testing.T) {
	for _, c := range uint64LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint64Less(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint64Less(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64Less(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint64LessEqual(T *testing.T) {
	for _, c := range uint64LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint64LessEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint64LessEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64LessEqual(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint64Greater(T *testing.T) {
	for _, c := range uint64GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint64Greater(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint64Greater(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64Greater(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint64GreaterEqual(T *testing.T) {
	for _, c := range uint64GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint64GreaterEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint64GreaterEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64GreaterEqual(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchUint64Between(T *testing.T) {
	for _, c := range uint64BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint64Between(c.Slice, c.Match, c.Match2, bits)
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
func BenchmarkMatchUint64Between(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64Between(a, math.MaxUint64/4, math.MaxUint64/2, bits)
			}
		})
	}
}
