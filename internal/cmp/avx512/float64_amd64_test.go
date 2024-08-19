// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package avx512

import (
	"bytes"
	"math"
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	randFloat64Slice         = tests.RandFloat64Slice
	float64EqualCases        = tests.Float64EqualCases
	float64NotEqualCases     = tests.Float64NotEqualCases
	float64LessCases         = tests.Float64LessCases
	float64LessEqualCases    = tests.Float64LessEqualCases
	float64GreaterCases      = tests.Float64GreaterCases
	float64GreaterEqualCases = tests.Float64GreaterEqualCases
	float64BetweenCases      = tests.Float64BetweenCases

	Float64Size = 8
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchFloat64EqualAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat64EqualAVX512.")
	}
	for _, c := range float64EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+64)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat64Equal(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+64], bytes.Repeat([]byte{0xfa}, 64)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+64])
		}
	}
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchFloat64EqualAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat64EqualAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat64Slice(n.L, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				MatchFloat64Equal(a, math.MaxFloat64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchFloat64NotEqualAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat64NotEqualAVX512.")
	}
	for _, c := range float64NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+64)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat64NotEqual(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+64], bytes.Repeat([]byte{0xfa}, 64)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+64])
		}
	}
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchFloat64NotEqualAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat64NotEqualAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat64Slice(n.L, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				MatchFloat64NotEqual(a, math.MaxFloat64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchFloat64LessAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat64LessAVX512.")
	}
	for _, c := range float64LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+64)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat64Less(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+64], bytes.Repeat([]byte{0xfa}, 64)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+64])
		}
	}
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchFloat64LessAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat64LessAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat64Slice(n.L, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				MatchFloat64Less(a, math.MaxFloat64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchFloat64LessEqualAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat64LessEqualAVX512.")
	}
	for _, c := range float64LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+64)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat64LessEqual(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+64], bytes.Repeat([]byte{0xfa}, 64)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+64])
		}
	}
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchFloat64LessEqualAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat64LessEqualAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat64Slice(n.L, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				MatchFloat64LessEqual(a, math.MaxFloat64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchFloat64GreaterAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat64GreaterAVX512.")
	}
	for _, c := range float64GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+64)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat64Greater(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+64], bytes.Repeat([]byte{0xfa}, 64)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+64])
		}
	}
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchFloat64GreaterAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat64GreaterAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat64Slice(n.L, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				MatchFloat64Greater(a, math.MaxFloat64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchFloat64GreaterEqualAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat64GreaterEqualAVX512.")
	}
	for _, c := range float64GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+64)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat64GreaterEqual(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+64], bytes.Repeat([]byte{0xfa}, 64)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+64])
		}
	}
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchFloat64GreaterEqualAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat64GreaterEqualAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat64Slice(n.L, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				MatchFloat64GreaterEqual(a, math.MaxFloat64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchFloat64BetweenAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat64BetweenAVX512.")
	}
	for _, c := range float64BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+64)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat64Between(c.Slice, c.Match, c.Match2, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+64], bytes.Repeat([]byte{0xfa}, 64)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+64])
		}
	}
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchFloat64BetweenAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat64BetweenAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat64Slice(n.L, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float64Size))
			for i := 0; i < B.N; i++ {
				MatchFloat64Between(a, 5, 10, bits)
			}
		})
	}
}
