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

func TestMatchUint8EqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint8EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8EqualAVX2(c.slice, c.match, bits)
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

func TestMatchUint8EqualAVX512(T *testing.T) {
    if !useAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint8EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8EqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint8EqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8EqualAVX2(a, math.MaxUint8/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint8EqualAVX512(B *testing.B) {
    if !useAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8EqualAVX512(a, math.MaxUint8/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
//
func TestMatchUint8NotEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint8NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8NotEqualAVX2(c.slice, c.match, bits)
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

func TestMatchUint8NotEqualAVX512(T *testing.T) {
    if !useAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint8NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8NotEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint8NotEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8NotEqualAVX2(a, math.MaxUint8/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint8NotEqualAVX512(B *testing.B) {
    if !useAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8NotEqualAVX512(a, math.MaxUint8/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Testcases
//

func TestMatchUint8LessAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint8LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8LessThanAVX2(c.slice, c.match, bits)
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

func TestMatchUint8LessAVX512(T *testing.T) {
    if !useAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint8LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8LessThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint8LessAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8LessThanAVX2(a, math.MaxUint8/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint8LessAVX512(B *testing.B) {
    if !useAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8LessThanAVX512(a, math.MaxUint8/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//
func TestMatchUint8LessEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint8LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8LessThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchUint8LessEqualAVX512(T *testing.T) {
    if !useAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint8LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8LessThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint8LessEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8LessThanEqualAVX2(a, math.MaxUint8/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint8LessEqualAVX512(B *testing.B) {
    if !useAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8LessThanEqualAVX512(a, math.MaxUint8/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Testcases
//
func TestMatchUint8GreaterAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint8GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8GreaterThanAVX2(c.slice, c.match, bits)
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

func TestMatchUint8GreaterAVX512(T *testing.T) {
    if !useAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint8GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8GreaterThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint8GreaterAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8GreaterThanAVX2(a, math.MaxUint8/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint8GreaterAVX512(B *testing.B) {
    if !useAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8GreaterThanAVX512(a, math.MaxUint8/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
func TestMatchUint8GreaterEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint8GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8GreaterThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchUint8GreaterEqualAVX512(T *testing.T) {
    if !useAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint8GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8GreaterThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint8GreaterEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8GreaterThanEqualAVX2(a, math.MaxUint8/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint8GreaterEqualAVX512(B *testing.B) {
    if !useAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8GreaterThanEqualAVX512(a, math.MaxUint8/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Between Testcases
//
func TestMatchUint8BetweenAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint8BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8BetweenAVX2(c.slice, c.match, c.match2, bits)
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

func TestMatchUint8BetweenAVX512(T *testing.T) {
    if !useAVX512_BW {
        T.SkipNow()
    }
    for _, c := range uint8BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint8BetweenAVX512(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchUint8BetweenAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8BetweenAVX2(a, math.MaxUint8/4, math.MaxUint8/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint8BetweenAVX512(B *testing.B) {
    if !useAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint8Size))
            for i := 0; i < B.N; i++ {
                matchUint8BetweenAVX512(a, math.MaxUint8/4, math.MaxUint8/2, bits)
            }
        })
    }
}
