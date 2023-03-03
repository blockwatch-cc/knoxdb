// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package num

import (
    "bytes"
    "math"
    "testing"

	"blockwatch.cc/knoxdb/util"
)

// -----------------------------------------------------------------------------
// Equal Testcases
//

func TestMatchInt16EqualAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int16EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16EqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt16EqualAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int16EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16EqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt16EqualAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16EqualAVX2(a, math.MaxInt16/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt16EqualAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16EqualAVX512(a, math.MaxInt16/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//

func TestMatchInt16NotEqualAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int16NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16NotEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt16NotEqualAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int16NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16NotEqualAVX512(c.slice, c.match, bits)
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
// Not Equal benchmarks
//

func BenchmarkMatchInt16NotEqualAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16NotEqualAVX2(a, math.MaxInt16/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt16NotEqualAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16NotEqualAVX512(a, math.MaxInt16/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Testcases
//

func TestMatchInt16LessAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int16LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16LessThanAVX2(c.slice, c.match, bits)
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

func TestMatchInt16LessAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int16LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16LessThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt16LessAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16LessThanAVX2(a, math.MaxInt16/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt16LessAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16LessThanAVX512(a, math.MaxInt16/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//

func TestMatchInt16LessEqualAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int16LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16LessThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt16LessEqualAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int16LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16LessThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt16LessEqualAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16LessThanEqualAVX2(a, math.MaxInt16/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt16LessEqualAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16LessThanEqualAVX512(a, math.MaxInt16/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Testcases
//

func TestMatchInt16GreaterAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int16GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16GreaterThanAVX2(c.slice, c.match, bits)
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

func TestMatchInt16GreaterAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int16GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16GreaterThanAVX512(c.slice, c.match, bits)
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

func BenchmarkMatchInt16GreaterAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16GreaterThanAVX2(a, math.MaxInt16/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt16GreaterAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16GreaterThanAVX512(a, math.MaxInt16/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

func TestMatchInt16GreaterEqualAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int16GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16GreaterThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt16GreaterEqualAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int16GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16GreaterThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt16GreaterEqualAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16GreaterThanEqualAVX2(a, math.MaxInt16/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt16GreaterEqualAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16GreaterThanEqualAVX512(a, math.MaxInt16/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Between Testcases
//

func TestMatchInt16BetweenAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int16BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16BetweenAVX2(c.slice, c.match, c.match2, bits)
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

func TestMatchInt16BetweenAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int16BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt16BetweenAVX512(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchInt16BetweenAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16BetweenAVX2(a, math.MaxInt16/4, math.MaxInt16/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt16BetweenAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt16Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int16Size))
            for i := 0; i < B.N; i++ {
                matchInt16BetweenAVX512(a, math.MaxInt16/4, math.MaxInt16/2, bits)
            }
        })
    }
}
