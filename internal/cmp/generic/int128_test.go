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
	randInt128Slice         = tests.RandInt128Slice
	Int128EqualCases        = tests.Int128EqualCases
	Int128NotEqualCases     = tests.Int128NotEqualCases
	Int128LessCases         = tests.Int128LessCases
	Int128LessEqualCases    = tests.Int128LessEqualCases
	Int128GreaterCases      = tests.Int128GreaterCases
	Int128GreaterEqualCases = tests.Int128GreaterEqualCases
	Int128BetweenCases      = tests.Int128BetweenCases

	matchInt128Equal        = MatchInt128Equal
	matchInt128NotEqual     = MatchInt128NotEqual
	matchInt128Less         = MatchInt128Less
	matchInt128LessEqual    = MatchInt128LessEqual
	matchInt128Greater      = MatchInt128Greater
	matchInt128GreaterEqual = MatchInt128GreaterEqual
	matchInt128Between      = MatchInt128Between

	MaxInt128      = num.MaxInt128
	Int128Optimize = num.Int128Optimize
)

const Int128Size = 16

func fillBitset(buf []byte, size int, val byte) []byte {
	if len(buf) == 0 {
		buf = make([]byte, bitFieldLen(size))
	}
	buf[0] = val
	for bp := 1; bp < len(buf); bp *= 2 {
		copy(buf[bp:], buf[:bp])
	}
	buf[len(buf)-1] &= bytemask(size)
	return buf
}

// -----------------------------------------------------------------------------
func TestMatchInt128Equal(T *testing.T) {
	for _, c := range Int128EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt128Equal(Int128Optimize(c.Slice), c.Match, bits, nil)
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
func BenchmarkMatchInt128Equal(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int128Optimize(randInt128Slice(n.L))
		mask := fillBitset(nil, a.Len(), 0xff)
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128Equal(a, MaxInt128.Rsh(1), bits, mask)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt128NotEqual(T *testing.T) {
	for _, c := range Int128NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt128NotEqual(Int128Optimize(c.Slice), c.Match, bits, nil)
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
func BenchmarkMatchInt128NotEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int128Optimize(randInt128Slice(n.L))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128NotEqual(a, MaxInt128.Rsh(1), bits, nil)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt128Less(T *testing.T) {
	for _, c := range Int128LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt128Less(Int128Optimize(c.Slice), c.Match, bits, nil)
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
func BenchmarkMatchInt128Less(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int128Optimize(randInt128Slice(n.L))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128Less(a, MaxInt128.Rsh(1), bits, nil)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt128LessEqual(T *testing.T) {
	for _, c := range Int128LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt128LessEqual(Int128Optimize(c.Slice), c.Match, bits, nil)
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
func BenchmarkMatchInt128LessEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int128Optimize(randInt128Slice(n.L))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128LessEqual(a, MaxInt128.Rsh(1), bits, nil)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt128Greater(T *testing.T) {
	for _, c := range Int128GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt128Greater(Int128Optimize(c.Slice), c.Match, bits, nil)
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
func BenchmarkMatchInt128Greater(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int128Optimize(randInt128Slice(n.L))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128Greater(a, MaxInt128.Rsh(1), bits, nil)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt128GreaterEqual(T *testing.T) {
	for _, c := range Int128GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt128GreaterEqual(Int128Optimize(c.Slice), c.Match, bits, nil)
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
func BenchmarkMatchInt128GreaterEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int128Optimize(randInt128Slice(n.L))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128GreaterEqual(a, MaxInt128.Rsh(1), bits, nil)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt128Between(T *testing.T) {
	for _, c := range Int128BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchInt128Between(Int128Optimize(c.Slice), c.Match, c.Match2, bits, nil)
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
func BenchmarkMatchInt128Between(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := Int128Optimize(randInt128Slice(n.L))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128Between(a, MaxInt128.Rsh(2), MaxInt128.Rsh(1), bits, nil)
			}
		})
	}
}
