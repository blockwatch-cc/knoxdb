// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package vec

import (
    "bytes"
    "math"
    "testing"

	"blockwatch.cc/knoxdb/util"
)

// -----------------------------------------------------------------------------
// Equal Testcases
//

func TestMatchUint16EqualAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range uint16EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16EqualAVX2(c.slice, c.match, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

func TestMatchUint16EqualAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint16EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16EqualAVX512(c.slice, c.match, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

// -----------------------------------------------------------------------------
// Equal benchmarks
//
func BenchmarkMatchUint16EqualAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16EqualAVX2(a, math.MaxUint16/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint16EqualAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16EqualAVX512(a, math.MaxUint16/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
//

func TestMatchUint16NotEqualAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range uint16NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16NotEqualAVX2(c.slice, c.match, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

func TestMatchUint16NotEqualAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint16NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16NotEqualAVX512(c.slice, c.match, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

// -----------------------------------------------------------------------------
// NotEqual benchmarks
//

func BenchmarkMatchUint16NotEqualAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16NotEqualAVX2(a, math.MaxUint16/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint16NotEqualAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16NotEqualAVX512(a, math.MaxUint16/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Testcases
//

func TestMatchUint16LessAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range uint16LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16LessThanAVX2(c.slice, c.match, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

func TestMatchUint16LessAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint16LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16LessThanAVX512(c.slice, c.match, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

// -----------------------------------------------------------------------------
// Less benchmarks
//
func BenchmarkMatchUint16LessAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16LessThanAVX2(a, math.MaxUint16/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint16LessAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16LessThanAVX512(a, math.MaxUint16/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//

func TestMatchUint16LessEqualAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range uint16LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16LessThanEqualAVX2(c.slice, c.match, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

func TestMatchUint16LessEqualAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint16LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16LessThanEqualAVX512(c.slice, c.match, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
//
func BenchmarkMatchUint16LessEqualAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16LessThanEqualAVX2(a, math.MaxUint16/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint16LessEqualAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16LessThanEqualAVX512(a, math.MaxUint16/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Testcases
//

func TestMatchUint16GreaterAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range uint16GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16GreaterThanAVX2(c.slice, c.match, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

func TestMatchUint16GreaterAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint16GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16GreaterThanAVX512(c.slice, c.match, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

// -----------------------------------------------------------------------------
// Greater benchmarks
//
func BenchmarkMatchUint16GreaterAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16GreaterThanAVX2(a, math.MaxUint16/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint16GreaterAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16GreaterThanAVX512(a, math.MaxUint16/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

func TestMatchUint16GreaterEqualAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range uint16GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16GreaterThanEqualAVX2(c.slice, c.match, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

func TestMatchUint16GreaterEqualAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint16GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16GreaterThanEqualAVX512(c.slice, c.match, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
//
func BenchmarkMatchUint16GreaterEqualAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16GreaterThanEqualAVX2(a, math.MaxUint16/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint16GreaterEqualAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16GreaterThanEqualAVX512(a, math.MaxUint16/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Between Testcases
//
func TestMatchUint16BetweenAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range uint16BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16BetweenAVX2(c.slice, c.match, c.match2, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

func TestMatchUint16BetweenAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint16BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint16BetweenAVX512(c.slice, c.match, c.match2, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
        if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
            T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
        }
    }
}

// -----------------------------------------------------------------------------
// Between benchmarks
//
func BenchmarkMatchUint16BetweenAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16BetweenAVX2(a, math.MaxUint16/4, math.MaxUint16/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint16BetweenAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint16Size))
            for i := 0; i < B.N; i++ {
                matchUint16BetweenAVX512(a, math.MaxUint16/4, math.MaxUint16/2, bits)
            }
        })
    }
}
