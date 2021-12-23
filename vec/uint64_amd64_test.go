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

func TestMatchUint64EqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint64EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64EqualAVX2(c.slice, c.match, bits)
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

func TestMatchUint64EqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range uint64EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64EqualAVX512(c.slice, c.match, bits)
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

func BenchmarkMatchUint64EqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64EqualAVX2(a, math.MaxUint64/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint64EqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64EqualAVX512(a, math.MaxUint64/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
//

func TestMatchUint64NotEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint64NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64NotEqualAVX2(c.slice, c.match, bits)
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

func TestMatchUint64NotEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range uint64NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64NotEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint64NotEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64NotEqualAVX2(a, math.MaxUint64/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint64NotEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64NotEqualAVX512(a, math.MaxUint64/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Testcases
//

func TestMatchUint64LessAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint64LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64LessThanAVX2(c.slice, c.match, bits)
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

func TestMatchUint64LessAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range uint64LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64LessThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint64LessAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64LessThanAVX2(a, math.MaxUint64/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint64LessAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64LessThanAVX512(a, math.MaxUint64/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//

func TestMatchUint64LessEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint64LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64LessThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchUint64LessEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range uint64LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64LessThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint64LessEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64LessThanEqualAVX2(a, math.MaxUint64/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint64LessEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64LessThanEqualAVX512(a, math.MaxUint64/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Testcases
//
func TestMatchUint64GreaterAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint64GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64GreaterThanAVX2(c.slice, c.match, bits)
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

func TestMatchUint64GreaterAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range uint64GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64GreaterThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint64GreaterAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64GreaterThanAVX2(a, math.MaxUint64/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint64GreaterAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64GreaterThanAVX512(a, math.MaxUint64/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

func TestMatchUint64GreaterEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint64GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64GreaterThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchUint64GreaterEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range uint64GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64GreaterThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint64GreaterEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64GreaterThanEqualAVX2(a, math.MaxUint64/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint64GreaterEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64GreaterThanEqualAVX512(a, math.MaxUint64/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Between Testcases
//
func TestMatchUint64BetweenAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range uint64BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64BetweenAVX2(c.slice, c.match, c.match2, bits)
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

func TestMatchUint64BetweenAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range uint64BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchUint64BetweenAVX512(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchUint64BetweenAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64BetweenAVX2(a, math.MaxUint64/4, math.MaxUint64/2, bits)
            }
        })
    }
}

func BenchmarkMatchUint64BetweenAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randUint64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Uint64Size))
            for i := 0; i < B.N; i++ {
                matchUint64BetweenAVX512(a, math.MaxUint64/4, math.MaxUint64/2, bits)
            }
        })
    }
}
