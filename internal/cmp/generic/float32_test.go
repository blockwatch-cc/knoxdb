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
	randFloat32Slice         = util.RandFloats[float32]
	float32EqualCases        = tests.Float32EqualCases
	float32NotEqualCases     = tests.Float32NotEqualCases
	float32LessCases         = tests.Float32LessCases
	float32LessEqualCases    = tests.Float32LessEqualCases
	float32GreaterCases      = tests.Float32GreaterCases
	float32GreaterEqualCases = tests.Float32GreaterEqualCases
	float32BetweenCases      = tests.Float32BetweenCases

	// instantiate
	matchFloat32Equal        = MatchEqualFloat[float32]
	matchFloat32NotEqual     = MatchNotEqualFloat[float32]
	matchFloat32Less         = MatchLessFloat[float32]
	matchFloat32LessEqual    = MatchLessEqualFloat[float32]
	matchFloat32Greater      = MatchGreaterFloat[float32]
	matchFloat32GreaterEqual = MatchGreaterEqualFloat[float32]
	matchFloat32Between      = MatchBetweenFloat[float32]
)

const Float32Size = 4

func TestMatchFloat32Equal(T *testing.T) {
	for _, c := range float32EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat32Equal(c.Slice, c.Match, bits)
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
func BenchmarkMatchFloat32Equal(B *testing.B) {
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32Equal(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func TestMatchFloat32NotEqual(T *testing.T) {
	for _, c := range float32NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat32NotEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchFloat32NotEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32NotEqual(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func TestMatchFloat32Less(T *testing.T) {
	for _, c := range float32LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat32Less(c.Slice, c.Match, bits)
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
func BenchmarkMatchFloat32Less(B *testing.B) {
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32Less(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func TestMatchFloat32LessEqual(T *testing.T) {
	for _, c := range float32LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat32LessEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchFloat32LessEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32LessEqual(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func TestMatchFloat32Greater(T *testing.T) {
	for _, c := range float32GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat32Greater(c.Slice, c.Match, bits)
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
func BenchmarkMatchFloat32Greater(B *testing.B) {
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32Greater(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func TestMatchFloat32GreaterEqual(T *testing.T) {
	for _, c := range float32GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat32GreaterEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchFloat32GreaterEqual(B *testing.B) {
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32GreaterEqual(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func TestMatchFloat32Between(T *testing.T) {
	for _, c := range float32BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.Slice)))
		cnt := matchFloat32Between(c.Slice, c.Match, c.Match2, bits)
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
func BenchmarkMatchFloat32Between(B *testing.B) {
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32Between(a, 5, math.MaxFloat32/2, bits)
			}
		})
	}
}
