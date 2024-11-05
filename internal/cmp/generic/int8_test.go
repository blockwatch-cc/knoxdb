// Copyright (c) 2020 Blockwatch Data Inc.
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
	randInt8Slice         = util.RandInts[int8]
	int8EqualCases        = tests.Int8EqualCases
	int8NotEqualCases     = tests.Int8NotEqualCases
	int8LessCases         = tests.Int8LessCases
	int8LessEqualCases    = tests.Int8LessEqualCases
	int8GreaterCases      = tests.Int8GreaterCases
	int8GreaterEqualCases = tests.Int8GreaterEqualCases
	int8BetweenCases      = tests.Int8BetweenCases

	// instantiate
	matchInt8Equal        = MatchEqual[int8]
	matchInt8NotEqual     = MatchNotEqual[int8]
	matchInt8Less         = MatchLess[int8]
	matchInt8LessEqual    = MatchLessEqual[int8]
	matchInt8Greater      = MatchGreater[int8]
	matchInt8GreaterEqual = MatchGreaterEqual[int8]
	matchInt8Between      = MatchBetween[int8]
)

const Int8Size = 1

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt8Equal(T *testing.T) {
	for _, c := range int8EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt8Equal(c.Slice, c.Match, bits)
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
func BenchmarkMatchInt8Equal(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt8Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8Equal(a, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt8NotEqual(T *testing.T) {
	for _, c := range int8NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt8NotEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchInt8NotEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt8Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8NotEqual(a, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt8Less(T *testing.T) {
	for _, c := range int8LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt8Less(c.Slice, c.Match, bits)
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
func BenchmarkMatchInt8Less(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt8Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8Less(a, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt8LessEqual(T *testing.T) {
	for _, c := range int8LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt8LessEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchInt8LessEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt8Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessEqual(a, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt8Greater(T *testing.T) {
	for _, c := range int8GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt8Greater(c.Slice, c.Match, bits)
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
func BenchmarkMatchInt8Greater(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt8Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8Greater(a, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt8GreaterEqual(T *testing.T) {
	for _, c := range int8GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt8GreaterEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchInt8GreaterEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt8Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterEqual(a, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt8Between(T *testing.T) {
	for _, c := range int8BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt8Between(c.Slice, c.Match, c.Match2, bits)
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
func BenchmarkMatchInt8Between(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt8Slice(n.L)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8Between(a, math.MaxInt8/4, math.MaxInt8/2, bits)
			}
		})
	}
}
