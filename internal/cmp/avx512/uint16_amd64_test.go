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
	randUint16Slice         = tests.RandUint16Slice
	uint16EqualCases        = tests.Uint16EqualCases
	uint16NotEqualCases     = tests.Uint16NotEqualCases
	uint16LessCases         = tests.Uint16LessCases
	uint16LessEqualCases    = tests.Uint16LessEqualCases
	uint16GreaterCases      = tests.Uint16GreaterCases
	uint16GreaterEqualCases = tests.Uint16GreaterEqualCases
	uint16BetweenCases      = tests.Uint16BetweenCases

	Uint16Size = 2
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchUint16EqualAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range uint16EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint16Equal(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint16EqualAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint16Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint16Size))
			for i := 0; i < B.N; i++ {
				MatchUint16Equal(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
func TestMatchUint16NotEqualAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range uint16NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint16NotEqual(c.Slice, c.Match, bits)
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
// NotEqual benchmarks
func BenchmarkMatchUint16NotEqualAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint16Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint16Size))
			for i := 0; i < B.N; i++ {
				MatchUint16NotEqual(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchUint16LessAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range uint16LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint16Less(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint16LessAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint16Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint16Size))
			for i := 0; i < B.N; i++ {
				MatchUint16Less(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint16LessEqualAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range uint16LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint16LessEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint16LessEqualAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint16Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint16Size))
			for i := 0; i < B.N; i++ {
				MatchUint16LessEqual(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint16GreaterAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range uint16GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint16Greater(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint16GreaterAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint16Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint16Size))
			for i := 0; i < B.N; i++ {
				MatchUint16Greater(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint16GreaterEqualAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range uint16GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint16GreaterEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint16GreaterEqualAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint16Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint16Size))
			for i := 0; i < B.N; i++ {
				MatchUint16GreaterEqual(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchUint16BetweenAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range uint16BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint16Between(c.Slice, c.Match, c.Match2, bits)
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
func BenchmarkMatchUint16BetweenAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint16Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint16Size))
			for i := 0; i < B.N; i++ {
				MatchUint16Between(a, math.MaxUint16/4, math.MaxUint16/2, bits)
			}
		})
	}
}
