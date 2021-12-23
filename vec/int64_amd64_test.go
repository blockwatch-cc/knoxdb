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

func TestMatchInt64EqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range int64EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64EqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt64EqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range int64EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64EqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt64EqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64EqualAVX2(a, math.MaxInt64/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt64EqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64EqualAVX512(a, math.MaxInt64/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//

func TestMatchInt64NotEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range int64NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64NotEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt64NotEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range int64NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64NotEqualAVX512(c.slice, c.match, bits)
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

func BenchmarkMatchInt64NotEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64NotEqualAVX2(a, math.MaxInt64/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt64NotEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64NotEqualAVX512(a, math.MaxInt64/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Testcases
//

func TestMatchInt64LessAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range int64LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64LessThanAVX2(c.slice, c.match, bits)
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

func TestMatchInt64LessAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range int64LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64LessThanAVX512(c.slice, c.match, bits)
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

func BenchmarkMatchInt64LessAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64LessThanAVX2(a, math.MaxInt64/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt64LessAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64LessThanAVX512(a, math.MaxInt64/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//

func TestMatchInt64LessEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range int64LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64LessThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt64LessEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range int64LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64LessThanEqualAVX512(c.slice, c.match, bits)
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

func BenchmarkMatchInt64LessEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64LessThanEqualAVX2(a, math.MaxInt64/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt64LessEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64LessThanEqualAVX512(a, math.MaxInt64/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Testcases
//

func TestMatchInt64GreaterAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range int64GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64GreaterThanAVX2(c.slice, c.match, bits)
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

func TestMatchInt64GreaterAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range int64GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64GreaterThanAVX512(c.slice, c.match, bits)
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

func BenchmarkMatchInt64GreaterAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64GreaterThanAVX2(a, math.MaxInt64/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt64GreaterAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64GreaterThanAVX512(a, math.MaxInt64/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

func TestMatchInt64GreaterEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range int64GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64GreaterThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt64GreaterEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range int64GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64GreaterThanEqualAVX512(c.slice, c.match, bits)
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

func BenchmarkMatchInt64GreaterEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64GreaterThanEqualAVX2(a, math.MaxInt64/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt64GreaterEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64GreaterThanEqualAVX512(a, math.MaxInt64/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Between Testcases
//

func TestMatchInt64BetweenAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range int64BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64BetweenAVX2(c.slice, c.match, c.match2, bits)
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

func TestMatchInt64BetweenAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range int64BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt64BetweenAVX512(c.slice, c.match, c.match2, bits)
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

func BenchmarkMatchInt64BetweenAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64BetweenAVX2(a, math.MaxInt64/4, math.MaxInt64/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt64BetweenAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int64Size))
            for i := 0; i < B.N; i++ {
                matchInt64BetweenAVX512(a, math.MaxInt64/4, math.MaxInt64/2, bits)
            }
        })
    }
}
