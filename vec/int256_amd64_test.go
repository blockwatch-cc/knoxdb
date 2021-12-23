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

func TestMatchInt256EqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int256EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256EqualAVX2(c.slice.Int256LLSlice(), c.match, bits)
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
func TestMatchInt256EqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int256EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256EqualAVX512(c.slice.Int256LLSlice(), c.match, bits)
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

func BenchmarkMatchInt256EqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256EqualAVX2(a, MaxInt256.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt256EqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256EqualAVX512(a, MaxInt256.Rsh(1), bits)
            }
        })
    }
}
*/
// -----------------------------------------------------------------------------
// Not Equal Testcases
//

func TestMatchInt256NotEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int256NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256NotEqualAVX2(c.slice.Int256LLSlice(), c.match, bits)
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
func TestMatchInt256NotEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int256NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256NotEqualAVX512(c.slice.Int256LLSlice(), c.match, bits)
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

func BenchmarkMatchInt256NotEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256NotEqualAVX2(a, MaxInt256.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt256NotEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256NotEqualAVX512(a, MaxInt256.Rsh(1), bits)
            }
        })
    }
}
*/
// -----------------------------------------------------------------------------
// Less Testcases
//

func TestMatchInt256LessAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int256LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256LessThanAVX2(c.slice.Int256LLSlice(), c.match, bits)
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
func TestMatchInt256LessAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int256LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256LessThanAVX512(c.slice.Int256LLSlice(), c.match, bits)
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

func BenchmarkMatchInt256LessAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256LessThanAVX2(a, MaxInt256.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt256LessAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256LessThanAVX512(a, MaxInt256.Rsh(1), bits)
            }
        })
    }
}
*/
// -----------------------------------------------------------------------------
// Less Equal Testcases
//

func TestMatchInt256LessEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int256LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256LessThanEqualAVX2(c.slice.Int256LLSlice(), c.match, bits)
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
func TestMatchInt256LessEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int256LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256LessThanEqualAVX512(c.slice.Int256LLSlice(), c.match, bits)
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
func BenchmarkMatchInt256LessEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256LessThanEqualAVX2(a, MaxInt256.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt256LessEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256LessThanEqualAVX512(a, MaxInt256.Rsh(1), bits)
            }
        })
    }
}
*/
// -----------------------------------------------------------------------------
// Greater Testcases
//

func TestMatchInt256GreaterAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int256GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256GreaterThanAVX2(c.slice.Int256LLSlice(), c.match, bits)
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
func TestMatchInt256GreaterAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int256GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256GreaterThanAVX512(c.slice.Int256LLSlice(), c.match, bits)
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
func BenchmarkMatchInt256GreaterAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256GreaterThanAVX2(a, MaxInt256.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt256GreaterAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256GreaterThanAVX512(a, MaxInt256.Rsh(1), bits)
            }
        })
    }
}
*/
// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

func TestMatchInt256GreaterEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int256GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256GreaterThanEqualAVX2(c.slice.Int256LLSlice(), c.match, bits)
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
func TestMatchInt256GreaterEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int256GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256GreaterThanEqualAVX512(c.slice.Int256LLSlice(), c.match, bits)
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

func BenchmarkMatchInt256GreaterEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256GreaterThanEqualAVX2(a, MaxInt256.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt256GreaterEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256GreaterThanEqualAVX512(a, MaxInt256.Rsh(1), bits)
            }
        })
    }
}
*/
// -----------------------------------------------------------------------------
// Between Testcases
//
func TestMatchInt256BetweenAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range Int256BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i := 0; i < 32; i++ {
            bits[l+i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256BetweenAVX2(c.slice.Int256LLSlice(), c.match, c.match2, bits)
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
func TestMatchInt256BetweenAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range Int256BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt256BetweenAVX512(c.slice.Int256LLSlice(), c.match, c.match2, bits)
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
func BenchmarkMatchInt256BetweenAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256BetweenAVX2(a, MaxInt256.Rsh(2), MaxInt256.Rsh(1), bits)
            }
        })
    }
}

/*
func BenchmarkMatchInt256BetweenAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt256Slice(n.l, 1).Int256LLSlice()
        bits := make([]byte, bitFieldLen(a.Len()))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int256Size))
            for i := 0; i < B.N; i++ {
                matchInt256BetweenAVX512(a, MaxInt256.Rsh(2), MaxInt256.Rsh(1), bits)
            }
        })
    }
}
*/
