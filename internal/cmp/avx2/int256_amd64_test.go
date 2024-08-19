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
	AsInt256Stride = num.AsInt256Stride
	Int256Size     = 32
	MaxInt256      = num.MaxInt256

	randInt256Slice         = tests.RandInt256Slice
	Int256EqualCases        = tests.Int256EqualCases
	Int256NotEqualCases     = tests.Int256NotEqualCases
	Int256LessCases         = tests.Int256LessCases
	Int256LessEqualCases    = tests.Int256LessEqualCases
	Int256GreaterCases      = tests.Int256GreaterCases
	Int256GreaterEqualCases = tests.Int256GreaterEqualCases
	Int256BetweenCases      = tests.Int256BetweenCases
)

// -----------------------------------------------------------------------------
// Equal Testcases
//

func TestMatchInt256EqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt256Equal(AsInt256Stride(c.Slice), c.Match, bits)
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
//

func BenchmarkMatchInt256EqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt256Stride(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				MatchInt256Equal(a, MaxInt256.Rsh(1), bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//

func TestMatchInt256NotEqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt256NotEqual(AsInt256Stride(c.Slice), c.Match, bits)
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

func BenchmarkMatchInt256NotEqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt256Stride(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				MatchInt256NotEqual(a, MaxInt256.Rsh(1), bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//

func TestMatchInt256LessAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt256Less(AsInt256Stride(c.Slice), c.Match, bits)
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

func BenchmarkMatchInt256LessAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt256Stride(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				MatchInt256Less(a, MaxInt256.Rsh(1), bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//

func TestMatchInt256LessEqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt256LessEqual(AsInt256Stride(c.Slice), c.Match, bits)
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
func BenchmarkMatchInt256LessEqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt256Stride(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				MatchInt256LessEqual(a, MaxInt256.Rsh(1), bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
//

func TestMatchInt256GreaterAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt256Greater(AsInt256Stride(c.Slice), c.Match, bits)
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
func BenchmarkMatchInt256GreaterAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt256Stride(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				MatchInt256Greater(a, MaxInt256.Rsh(1), bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

func TestMatchInt256GreaterEqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt256GreaterEqual(AsInt256Stride(c.Slice), c.Match, bits)
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
//

func BenchmarkMatchInt256GreaterEqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt256Stride(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				MatchInt256GreaterEqual(a, MaxInt256.Rsh(1), bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt256BetweenAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.Slice))
		bits := make([]byte, l+32)
		for i := 0; i < 32; i++ {
			bits[l+i] = 0xfa
		}
		bits = bits[:l]
		cnt := MatchInt256Between(AsInt256Stride(c.Slice), c.Match, c.Match2, bits)
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
func BenchmarkMatchInt256BetweenAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := AsInt256Stride(randInt256Slice(n.L, 1))
		bits := make([]byte, bitFieldLen(a.Len()))
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int256Size))
			for i := 0; i < B.N; i++ {
				MatchInt256Between(a, MaxInt256.Rsh(2), MaxInt256.Rsh(1), bits)
			}
		})
	}
}
