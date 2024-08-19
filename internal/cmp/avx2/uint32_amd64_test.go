// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package avx2

import (
	"bytes"
	"math"
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	randUint32Slice         = tests.RandUint32Slice
	uint32EqualCases        = tests.Uint32EqualCases
	uint32NotEqualCases     = tests.Uint32NotEqualCases
	uint32LessCases         = tests.Uint32LessCases
	uint32LessEqualCases    = tests.Uint32LessEqualCases
	uint32GreaterCases      = tests.Uint32GreaterCases
	uint32GreaterEqualCases = tests.Uint32GreaterEqualCases
	uint32BetweenCases      = tests.Uint32BetweenCases

	Uint32Size = 4
)

// -----------------------------------------------------------------------------
// Equal Testcases
//

func TestMatchUint32EqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range uint32EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint32Equal(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint32EqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				MatchUint32Equal(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
//

func TestMatchUint32NotEqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range uint32NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint32NotEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint32NotEqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				MatchUint32NotEqual(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//

func TestMatchUint32LessAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range uint32LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint32Less(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint32LessAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				MatchUint32Less(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint32LessEqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range uint32LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint32LessEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint32LessEqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				MatchUint32LessEqual(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint32GreaterAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range uint32GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint32Greater(c.Slice, c.Match, bits)
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
//

func BenchmarkMatchUint32GreaterAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				MatchUint32Greater(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint32GreaterEqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range uint32GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint32GreaterEqual(c.Slice, c.Match, bits)
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
func BenchmarkMatchUint32GreaterEqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				MatchUint32GreaterEqual(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//

func TestMatchUint32BetweenAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range uint32BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchUint32Between(c.Slice, c.Match, c.Match2, bits)
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
func BenchmarkMatchUint32BetweenAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randUint32Slice(n.L, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Uint32Size))
			for i := 0; i < B.N; i++ {
				MatchUint32Between(a, math.MaxUint32/4, math.MaxUint32/2, bits)
			}
		})
	}
}
