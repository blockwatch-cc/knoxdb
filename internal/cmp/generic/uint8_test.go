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
	randUint8Slice         = tests.RandUint8Slice
	uint8EqualCases        = tests.Uint8EqualCases
	uint8NotEqualCases     = tests.Uint8NotEqualCases
	uint8LessCases         = tests.Uint8LessCases
	uint8LessEqualCases    = tests.Uint8LessEqualCases
	uint8GreaterCases      = tests.Uint8GreaterCases
	uint8GreaterEqualCases = tests.Uint8GreaterEqualCases
	uint8BetweenCases      = tests.Uint8BetweenCases

	// instantiate
	matchUint8Equal        = MatchEqual[uint8]
	matchUint8NotEqual     = MatchNotEqual[uint8]
	matchUint8Less         = MatchLess[uint8]
	matchUint8LessEqual    = MatchLessEqual[uint8]
	matchUint8Greater      = MatchGreater[uint8]
	matchUint8GreaterEqual = MatchGreaterEqual[uint8]
	matchUint8Between      = MatchBetween[uint8]
)

const Uint8Size = 1

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchUint8Equal(T *testing.T) {
	for _, c := range uint8EqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint8Equal(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint8Equal(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint8Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8Equal(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
func TestMatchUint8NotEqual(T *testing.T) {
	for _, c := range uint8NotEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint8NotEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint8NotEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint8Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8NotEqual(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchUint8Less(T *testing.T) {
	for _, c := range uint8LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint8Less(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint8Less(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint8Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8Less(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint8LessEqual(T *testing.T) {
	for _, c := range uint8LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint8LessEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint8LessEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint8Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8LessEqual(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint8Greater(T *testing.T) {
	for _, c := range uint8GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint8Greater(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint8Greater(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint8Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8Greater(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint8GreaterEqual(T *testing.T) {
	for _, c := range uint8GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint8GreaterEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint8GreaterEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint8Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8GreaterEqual(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchUint8Between(T *testing.T) {
	for _, c := range uint8BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchUint8Between(c.Slice, c.Match, c.Match2, bits)
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
func BenchmarkMatchUint8Between(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randUint8Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8Between(a, math.MaxUint8/4, math.MaxUint8/2, bits)
			}
		})
	}
}
