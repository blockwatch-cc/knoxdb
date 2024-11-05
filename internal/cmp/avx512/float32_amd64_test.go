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
	benchmarkSizes           = tests.BenchmarkSizes
	randFloat32Slice         = util.RandFloats[float32]
	float32EqualCases        = tests.Float32EqualCases
	float32NotEqualCases     = tests.Float32NotEqualCases
	float32LessCases         = tests.Float32LessCases
	float32LessEqualCases    = tests.Float32LessEqualCases
	float32GreaterCases      = tests.Float32GreaterCases
	float32GreaterEqualCases = tests.Float32GreaterEqualCases
	float32BetweenCases      = tests.Float32BetweenCases

	Float32Size = 4
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchFloat32EqualAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32EqualAVX512.")
	}
	for _, c := range float32EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat32Equal(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchFloat32EqualAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32EqualAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				MatchFloat32Equal(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchFloat32NotEqualAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32NotEqualAVX512.")
	}
	for _, c := range float32NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat32NotEqual(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchFloat32NotEqualAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32NotEqualAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				MatchFloat32NotEqual(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchFloat32LessAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32LessAVX512.")
	}
	for _, c := range float32LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat32Less(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchFloat32LessAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32LessAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				MatchFloat32Less(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchFloat32LessEqualAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32LessEqualAVX512.")
	}
	for _, c := range float32LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat32LessEqual(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchFloat32LessEqualAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32LessEqualAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				MatchFloat32LessEqual(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchFloat32GreaterAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32GreaterAVX512.")
	}
	for _, c := range float32GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat32Greater(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchFloat32GreaterAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32GreaterAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				MatchFloat32Greater(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchFloat32GreaterEqualAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32GreaterEqualAVX512.")
	}
	for _, c := range float32GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat32GreaterEqual(c.Slice, c.Match, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchFloat32GreaterEqualAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32GreaterEqualAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				MatchFloat32GreaterEqual(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchFloat32BetweenAVX512(T *testing.T) {
	if !util.UseAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32BetweenAVX512.")
	}
	for _, c := range float32BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchFloat32Between(c.Slice, c.Match, c.Match2, bits)
		if got, want := len(bits), len(c.Result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if got, want := cnt, c.Count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.Name, got, want)
		}
		if bytes.Compare(bits, c.Result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.Name, bits, c.Result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.Name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchFloat32BetweenAVX512(B *testing.B) {
	if !util.UseAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32BetweenAVX512.")
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			a := randFloat32Slice(n.L)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.L * Float32Size))
			for i := 0; i < B.N; i++ {
				MatchFloat32Between(a, 5, 10, bits)
			}
		})
	}
}
