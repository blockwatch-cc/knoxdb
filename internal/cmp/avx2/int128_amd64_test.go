// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx2

import (
	"bytes"
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	AsInt128Stride = num.AsInt128Stride
	Int128Size     = 16
	MaxInt128      = num.MaxInt128

	randInt128Slice         = tests.RandInt128Slice
	Int128EqualCases        = tests.Int128EqualCases
	Int128NotEqualCases     = tests.Int128NotEqualCases
	Int128LessCases         = tests.Int128LessCases
	Int128LessEqualCases    = tests.Int128LessEqualCases
	Int128GreaterCases      = tests.Int128GreaterCases
	Int128GreaterEqualCases = tests.Int128GreaterEqualCases
	Int128BetweenCases      = tests.Int128BetweenCases
)

// -----------------------------------------------------------------------------
// Equal Testcases
//

func TestMatchInt128EqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt128Equal(AsInt128Stride(c.Slice), c.Match, bits)
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
func BenchmarkMatchInt128EqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {

		a := AsInt128Stride(randInt128Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				MatchInt128Equal(a, MaxInt128.Rsh(1), bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//

func TestMatchInt128NotEqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt128NotEqual(AsInt128Stride(c.Slice), c.Match, bits)
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
//

func BenchmarkMatchInt128NotEqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt128Stride(randInt128Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				MatchInt128NotEqual(a, MaxInt128.Rsh(1), bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//

func TestMatchInt128LessAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt128Less(AsInt128Stride(c.Slice), c.Match, bits)
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
//

func BenchmarkMatchInt128LessAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt128Stride(randInt128Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				MatchInt128Less(a, MaxInt128.Rsh(1), bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//

func TestMatchInt128LessEqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt128LessEqual(AsInt128Stride(c.Slice), c.Match, bits)
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
func BenchmarkMatchInt128LessEqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt128Stride(randInt128Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				MatchInt128LessEqual(a, MaxInt128.Rsh(1), bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt128GreaterAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt128Greater(AsInt128Stride(c.Slice), c.Match, bits)
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
func BenchmarkMatchInt128GreaterAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt128Stride(randInt128Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				MatchInt128Greater(a, MaxInt128.Rsh(1), bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

func TestMatchInt128GreaterEqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt128GreaterEqual(AsInt128Stride(c.Slice), c.Match, bits)
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
func BenchmarkMatchInt128GreaterEqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt128Stride(randInt128Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				MatchInt128GreaterEqual(a, MaxInt128.Rsh(1), bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//

func TestMatchInt128BetweenAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt128Between(AsInt128Stride(c.Slice), c.Match, c.Match2, bits)
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
//

func BenchmarkMatchInt128BetweenAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt128Stride(randInt128Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int128Size))
			for i := 0; i < B.N; i++ {
				MatchInt128Between(a, MaxInt128.Rsh(2), MaxInt128.Rsh(1), bits)
			}
		})
	}
}
