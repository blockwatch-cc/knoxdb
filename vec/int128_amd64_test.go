// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
    "bytes"
    "fmt"
    "math"
    "math/bits"
    "math/rand"
    "testing"
)

// -----------------------------------------------------------------------------
// Equal Testcases
//

func TestMatchInt128EqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int128EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128EqualAVX2(c.slice.Int128LLSlice(), c.match, bits)
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

/*
func TestMatchInt128EqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int128EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128EqualAVX512(c.slice, c.match, bits)
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
*/
// -----------------------------------------------------------------------------
// Equal benchmarks
//
func BenchmarkMatchInt128EqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {

        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128EqualAVX2(a, MaxInt128.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt128EqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128EqualAVX512(a, MaxInt128.Rsh(1), bits)
            }
        })
    }
}
*/
// -----------------------------------------------------------------------------
// Not Equal Testcases
//

func TestMatchInt128NotEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int128NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128NotEqualAVX2(c.slice.Int128LLSlice(), c.match, bits)
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

/*
func TestMatchInt128NotEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int128NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128NotEqualAVX512(c.slice, c.match, bits)
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
*/
// -----------------------------------------------------------------------------
// Not Equal benchmarks
//

func BenchmarkMatchInt128NotEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128NotEqualAVX2(a, MaxInt128.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt128NotEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128NotEqualAVX512(a, MaxInt128.Rsh(1), bits)
            }
        })
    }
}
*/
// -----------------------------------------------------------------------------
// Less Testcases
//

func TestMatchInt128LessAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int128LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128LessThanAVX2(c.slice.Int128LLSlice(), c.match, bits)
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

/*
func TestMatchInt128LessAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int128LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128LessThanAVX512(c.slice, c.match, bits)
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
*/
// -----------------------------------------------------------------------------
// Less benchmarks
//

func BenchmarkMatchInt128LessAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128LessThanAVX2(a, MaxInt128.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt128LessAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128LessThanAVX512(a, MaxInt128.Rsh(1), bits)
            }
        })
    }
}
*/
// -----------------------------------------------------------------------------
// Less Equal Testcases
//

func TestMatchInt128LessEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int128LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128LessThanEqualAVX2(c.slice.Int128LLSlice(), c.match, bits)
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

/*
func TestMatchInt128LessEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int128LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128LessThanEqualAVX512(c.slice, c.match, bits)
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
*/
// -----------------------------------------------------------------------------
// Less equal benchmarks
//
func BenchmarkMatchInt128LessEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128LessThanEqualAVX2(a, MaxInt128.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt128LessEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128LessThanEqualAVX512(a, MaxInt128.Rsh(1), bits)
            }
        })
    }
}
*/
// -----------------------------------------------------------------------------
// Greater Testcases
//
func TestMatchInt128GreaterAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int128GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128GreaterThanAVX2(c.slice.Int128LLSlice(), c.match, bits)
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

/*
func TestMatchInt128GreaterAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int128GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128GreaterThanAVX512(c.slice, c.match, bits)
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
*/
// -----------------------------------------------------------------------------
// Greater benchmarks
//
func BenchmarkMatchInt128GreaterAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128GreaterThanAVX2(a, MaxInt128.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt128GreaterAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128GreaterThanAVX512(a, MaxInt128.Rsh(1), bits)
            }
        })
    }
}
*/
// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

func TestMatchInt128GreaterEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int128GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128GreaterThanEqualAVX2(c.slice.Int128LLSlice(), c.match, bits)
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

/*
func TestMatchInt128GreaterEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int128GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128GreaterThanEqualAVX512(c.slice, c.match, bits)
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
*/
// -----------------------------------------------------------------------------
// Greater equal benchmarks
//
func BenchmarkMatchInt128GreaterEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128GreaterThanEqualAVX2(a, MaxInt128.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt128GreaterEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128GreaterThanEqualAVX512(a, MaxInt128.Rsh(1), bits)
            }
        })
    }
}
*/
// -----------------------------------------------------------------------------
// Between Testcases
//

func TestMatchInt128BetweenAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int128BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128BetweenAVX2(c.slice.Int128LLSlice(), c.match, c.match2, bits)
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

/*
func TestMatchInt128BetweenAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int128BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt128BetweenAVX512(c.slice, c.match, c.match2, bits)
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
*/
// -----------------------------------------------------------------------------
// Between benchmarks
//

func BenchmarkMatchInt128BetweenAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128BetweenAVX2(a, MaxInt128.Rsh(2), MaxInt128.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt128BetweenAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt128Slice(n.l, 1).Int128LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int128Size))
            for i := 0; i < B.N; i++ {
                matchInt128BetweenAVX512(a, MaxInt128.Rsh(2), MaxInt128.Rsh(1), bits)
            }
        })
    }
}
*/
