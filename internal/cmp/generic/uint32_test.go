// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package generic

import (
	"bytes"
	"math"
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	randUint32Slice         = util.RandUints[uint32]
	uint32EqualCases        = tests.Uint32EqualCases
	uint32NotEqualCases     = tests.Uint32NotEqualCases
	uint32LessCases         = tests.Uint32LessCases
	uint32LessEqualCases    = tests.Uint32LessEqualCases
	uint32GreaterCases      = tests.Uint32GreaterCases
	uint32GreaterEqualCases = tests.Uint32GreaterEqualCases
	uint32BetweenCases      = tests.Uint32BetweenCases

	// instantiate
	matchUint32Equal        = MatchEqual[uint32]
	matchUint32NotEqual     = MatchNotEqual[uint32]
	matchUint32Less         = MatchLess[uint32]
	matchUint32LessEqual    = MatchLessEqual[uint32]
	matchUint32Greater      = MatchGreater[uint32]
	matchUint32GreaterEqual = MatchGreaterEqual[uint32]
	matchUint32Between      = MatchBetween[uint32]
)

const Uint32Size = 4

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchUint32Equal(T *testing.T) {
	for _, c := range uint32EqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint32Equal(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint32Equal(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32Equal(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
func TestMatchUint32NotEqual(T *testing.T) {
	for _, c := range uint32NotEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint32NotEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint32NotEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32NotEqual(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchUint32Less(T *testing.T) {
	for _, c := range uint32LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint32Less(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint32Less(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32Less(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint32LessEqual(T *testing.T) {
	for _, c := range uint32LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint32LessEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint32LessEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32LessEqual(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint32Greater(T *testing.T) {
	for _, c := range uint32GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint32Greater(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint32Greater(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32Greater(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint32GreaterEqual(T *testing.T) {
	for _, c := range uint32GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint32GreaterEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint32GreaterEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32GreaterEqual(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchUint32Between(T *testing.T) {
	for _, c := range uint32BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint32Between(c.Slice, c.Match, c.Match2, bits)
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
func BenchmarkMatchUint32Between(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32Between(a, math.MaxUint32/4, math.MaxUint32/2, bits)
			}
		})
	}
}
