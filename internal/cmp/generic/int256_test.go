// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"bytes"
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
	"blockwatch.cc/knoxdb/pkg/num"
)

var (
	randInt256Slice         = tests.RandInt256Slice
	Int256EqualCases        = tests.Int256EqualCases
	Int256NotEqualCases     = tests.Int256NotEqualCases
	Int256LessCases         = tests.Int256LessCases
	Int256LessEqualCases    = tests.Int256LessEqualCases
	Int256GreaterCases      = tests.Int256GreaterCases
	Int256GreaterEqualCases = tests.Int256GreaterEqualCases
	Int256BetweenCases      = tests.Int256BetweenCases

	matchInt256Equal        = MatchInt256Equal
	matchInt256NotEqual     = MatchInt256NotEqual
	matchInt256Less         = MatchInt256Less
	matchInt256LessEqual    = MatchInt256LessEqual
	matchInt256Greater      = MatchInt256Greater
	matchInt256GreaterEqual = MatchInt256GreaterEqual
	matchInt256Between      = MatchInt256Between

	MaxInt256      = num.MaxInt256
	Int256Optimize = num.Int256Optimize
)

const Int256Size = 32

// Equal Testcases
func TestMatchInt256Equal(T *testing.T) {
	for _, c := range Int256EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt256Equal(Int256Optimize(c.Slice), c.Match, bits, nil)
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
func BenchmarkMatchInt256Equal(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int256Optimize(randInt256Slice(n.L, 1))
		mask := fillBitset(nil, a.Len(), 0xff)
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256Equal(a, MaxInt256.Rsh(1), bits, mask)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt256NotEqual(T *testing.T) {
	for _, c := range Int256NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt256NotEqual(Int256Optimize(c.Slice), c.Match, bits, nil)
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
func BenchmarkMatchInt256NotEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int256Optimize(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256NotEqual(a, MaxInt256.Rsh(1), bits, nil)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt256Less(T *testing.T) {
	for _, c := range Int256LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt256Less(Int256Optimize(c.Slice), c.Match, bits, nil)
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
func BenchmarkMatchInt256Less(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int256Optimize(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256Less(a, MaxInt256.Rsh(1), bits, nil)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt256LessEqual(T *testing.T) {
	for _, c := range Int256LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt256LessEqual(Int256Optimize(c.Slice), c.Match, bits, nil)
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
func BenchmarkMatchInt256LessEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int256Optimize(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256LessEqual(a, MaxInt256.Rsh(1), bits, nil)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt256Greater(T *testing.T) {
	for _, c := range Int256GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt256Greater(Int256Optimize(c.Slice), c.Match, bits, nil)
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
func BenchmarkMatchInt256Greater(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int256Optimize(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256Greater(a, MaxInt256.Rsh(1), bits, nil)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt256GreaterEqual(T *testing.T) {
	for _, c := range Int256GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt256GreaterEqual(Int256Optimize(c.Slice), c.Match, bits, nil)
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
func BenchmarkMatchInt256GreaterEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int256Optimize(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256GreaterEqual(a, MaxInt256.Rsh(1), bits, nil)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt256Between(T *testing.T) {
	for _, c := range Int256BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt256Between(Int256Optimize(c.Slice), c.Match, c.Match2, bits, nil)
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
func BenchmarkMatchInt256Between(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int256Optimize(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256Between(a, MaxInt256.Rsh(2), MaxInt256.Rsh(1), bits, nil)
			}
		})
	}
}
