// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"bytes"
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

var (
	benchmarkSizes = tests.BenchmarkSizes

	randFloat64Slice         = tests.RandFloat64Slice
	float64EqualCases        = tests.Float64EqualCases
	float64NotEqualCases     = tests.Float64NotEqualCases
	float64LessCases         = tests.Float64LessCases
	float64LessEqualCases    = tests.Float64LessEqualCases
	float64GreaterCases      = tests.Float64GreaterCases
	float64GreaterEqualCases = tests.Float64GreaterEqualCases
	float64BetweenCases      = tests.Float64BetweenCases

	// instantiate
	matchFloat64Equal        = MatchEqualFloat[float64]
	matchFloat64NotEqual     = MatchNotEqualFloat[float64]
	matchFloat64Less         = MatchLessFloat[float64]
	matchFloat64LessEqual    = MatchLessEqualFloat[float64]
	matchFloat64Greater      = MatchGreaterFloat[float64]
	matchFloat64GreaterEqual = MatchGreaterEqualFloat[float64]
	matchFloat64Between      = MatchBetweenFloat[float64]
)

const Float64Size = 8

func TestMatchFloat64Equal(T *testing.T) {
	for _, c := range float64EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat64Equal(c.Slice, c.Match, bits)
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
func BenchmarkMatchFloat64Equal(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randFloat64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64Equal(a, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchFloat64NotEqual(T *testing.T) {
	for _, c := range float64NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat64NotEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchFloat64NotEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randFloat64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64NotEqual(a, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases

func TestMatchFloat64Less(T *testing.T) {
	for _, c := range float64LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat64Less(c.Slice, c.Match, bits)
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
func BenchmarkMatchFloat64Less(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randFloat64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64Less(a, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchFloat64LessEqual(T *testing.T) {
	for _, c := range float64LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat64LessEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchFloat64LessEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randFloat64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64LessEqual(a, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchFloat64Greater(T *testing.T) {
	for _, c := range float64GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat64Greater(c.Slice, c.Match, bits)
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
func BenchmarkMatchFloat64Greater(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randFloat64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64Greater(a, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchFloat64GreaterEqual(T *testing.T) {
	for _, c := range float64GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat64GreaterEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchFloat64GreaterEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randFloat64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64GreaterEqual(a, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchFloat64Between(T *testing.T) {
	for _, c := range float64BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat64Between(c.Slice, c.Match, c.Match2, bits)
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
func BenchmarkMatchFloat64Between(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randFloat64Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64Between(a, 0.25, 0.5, bits)
			}
		})
	}
}
