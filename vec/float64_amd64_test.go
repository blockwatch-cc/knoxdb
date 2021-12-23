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

func TestMatchFloat64EqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range float64EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64EqualAVX2(c.slice, c.match, bits)
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

func TestMatchFloat64EqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range float64EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64EqualAVX512(c.slice, c.match, bits)
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

func BenchmarkMatchFloat64EqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64EqualAVX2(a, 0.5, bits)
            }
        })
    }
}

func BenchmarkMatchFloat64EqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64EqualAVX512(a, 0.5, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//

func TestMatchFloat64NotEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range float64NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64NotEqualAVX2(c.slice, c.match, bits)
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

func TestMatchFloat64NotEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range float64NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64NotEqualAVX512(c.slice, c.match, bits)
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

func BenchmarkMatchFloat64NotEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64NotEqualAVX2(a, 0.5, bits)
            }
        })
    }
}

func BenchmarkMatchFloat64NotEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64NotEqualAVX512(a, 0.5, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Testcases
//

func TestMatchFloat64LessAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range float64LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64LessThanAVX2(c.slice, c.match, bits)
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

func TestMatchFloat64LessAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range float64LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64LessThanAVX512(c.slice, c.match, bits)
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

func BenchmarkMatchFloat64LessAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64LessThanAVX2(a, 0.5, bits)
            }
        })
    }
}

func BenchmarkMatchFloat64LessAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64LessThanAVX512(a, 0.5, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//

func TestMatchFloat64LessEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range float64LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64LessThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchFloat64LessEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range float64LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64LessThanEqualAVX512(c.slice, c.match, bits)
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

func BenchmarkMatchFloat64LessEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64LessThanEqualAVX2(a, 0.5, bits)
            }
        })
    }
}

func BenchmarkMatchFloat64LessEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64LessThanEqualAVX512(a, 0.5, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Testcases
//

func TestMatchFloat64GreaterAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range float64GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64GreaterThanAVX2(c.slice, c.match, bits)
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

func TestMatchFloat64GreaterAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range float64GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64GreaterThanAVX512(c.slice, c.match, bits)
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

func BenchmarkMatchFloat64GreaterAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64GreaterThanAVX2(a, 0.5, bits)
            }
        })
    }
}

func BenchmarkMatchFloat64GreaterAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64GreaterThanAVX512(a, 0.5, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

func TestMatchFloat64GreaterEqualAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range float64GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64GreaterThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchFloat64GreaterEqualAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range float64GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64GreaterThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat64GreaterEqualGeneric(B *testing.B) {
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64GreaterThanEqualGeneric(a, 0.5, bits)
            }
        })
    }
}

func BenchmarkMatchFloat64GreaterEqualAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64GreaterThanEqualAVX2(a, 0.5, bits)
            }
        })
    }
}

func BenchmarkMatchFloat64GreaterEqualAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64GreaterThanEqualAVX512(a, 0.5, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Between Testcases
//
var float64BetweenCases = []Float64MatchTest{
    {
        name:   "l0",
        slice:  make([]float64, 0),
        match:  float64BetweenTestMatch_1,
        result: []byte{},
        count:  0,
    }, {
        name:   "nil",
        slice:  nil,
        match:  float64BetweenTestMatch_1,
        result: []byte{},
        count:  0,
    },
    CreateFloat64TestCase("vec1", float64TestSlice_0, float64BetweenTestMatch_0, float64BetweenTestMatch_0b, float64BetweenTestResult_0, 32),
    CreateFloat64TestCase("vec2", float64TestSlice_0, float64BetweenTestMatch_0, float64BetweenTestMatch_0b, float64BetweenTestResult_0, 64),
    CreateFloat64TestCase("l32", float64TestSlice_1, float64BetweenTestMatch_1, float64BetweenTestMatch_1b, float64BetweenTestResult_1, 32),
    CreateFloat64TestCase("l64", append(float64TestSlice_1, float64TestSlice_0...), float64BetweenTestMatch_1, float64BetweenTestMatch_1b,
        append(float64BetweenTestResult_1, float64BetweenTestResult_0...), 64),
    CreateFloat64TestCase("l128", append(float64TestSlice_1, float64TestSlice_0...), float64BetweenTestMatch_1, float64BetweenTestMatch_1b,
        append(float64BetweenTestResult_1, float64BetweenTestResult_0...), 128),
    CreateFloat64TestCase("l127", append(float64TestSlice_1, float64TestSlice_0...), float64BetweenTestMatch_1, float64BetweenTestMatch_1b,
        append(float64BetweenTestResult_1, float64BetweenTestResult_0...), 127),
    CreateFloat64TestCase("l63", float64TestSlice_1, float64BetweenTestMatch_1, float64BetweenTestMatch_1b, float64BetweenTestResult_1, 63),
    CreateFloat64TestCase("l31", float64TestSlice_1, float64BetweenTestMatch_1, float64BetweenTestMatch_1b, float64BetweenTestResult_1, 31),
    CreateFloat64TestCase("l23", float64TestSlice_1, float64BetweenTestMatch_1, float64BetweenTestMatch_1b, float64BetweenTestResult_1, 23),
    CreateFloat64TestCase("l15", float64TestSlice_1, float64BetweenTestMatch_1, float64BetweenTestMatch_1b, float64BetweenTestResult_1, 15),
    CreateFloat64TestCase("l7", float64TestSlice_1, float64BetweenTestMatch_1, float64BetweenTestMatch_1b, float64BetweenTestResult_1, 7),
    CreateFloat64TestCase("neg64", float64TestSlice_2, float64BetweenTestMatch_2, float64BetweenTestMatch_2b, float64BetweenTestResult_2, 64),
    CreateFloat64TestCase("neg32", float64TestSlice_2, float64BetweenTestMatch_2, float64BetweenTestMatch_2b, float64BetweenTestResult_2, 32),
    CreateFloat64TestCase("neg31", float64TestSlice_2, float64BetweenTestMatch_2, float64BetweenTestMatch_2b, float64BetweenTestResult_2, 31),
    CreateFloat64TestCase("ext64", float64TestSlice_3, float64BetweenTestMatch_3, float64BetweenTestMatch_3b, float64BetweenTestResult_3, 64),
    CreateFloat64TestCase("ext32", float64TestSlice_3, float64BetweenTestMatch_3, float64BetweenTestMatch_3b, float64BetweenTestResult_3, 32),
    CreateFloat64TestCase("ext31", float64TestSlice_3, float64BetweenTestMatch_3, float64BetweenTestMatch_3b, float64BetweenTestResult_3, 31),
    CreateFloat64TestCase("nan64", float64TestSlice_4, float64BetweenTestMatch_4, float64BetweenTestMatch_4b, float64BetweenTestResult_4, 64),
    CreateFloat64TestCase("nan32", float64TestSlice_4, float64BetweenTestMatch_4, float64BetweenTestMatch_4b, float64BetweenTestResult_4, 32),
    CreateFloat64TestCase("nan31", float64TestSlice_4, float64BetweenTestMatch_4, float64BetweenTestMatch_4b, float64BetweenTestResult_4, 31),
}

func TestMatchFloat64BetweenGeneric(T *testing.T) {
    for _, c := range float64BetweenCases {
        // pre-allocate the result slice
        bits := make([]byte, bitFieldLen(len(c.slice)))
        cnt := matchFloat64BetweenGeneric(c.slice, c.match, c.match2, bits)
        if got, want := len(bits), len(c.result); got != want {
            T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
        }
        if got, want := cnt, c.count; got != want {
            T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
        }
        if bytes.Compare(bits, c.result) != 0 {
            T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
        }
    }
}

func TestMatchFloat64BetweenAVX2(T *testing.T) {
    if !useAVX2 {
        T.SkipNow()
    }
    for _, c := range float64BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64BetweenAVX2(c.slice, c.match, c.match2, bits)
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

func TestMatchFloat64BetweenAVX512(T *testing.T) {
    if !useAVX512_F {
        T.SkipNow()
    }
    for _, c := range float64BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat64BetweenAVX512(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchFloat64BetweenGeneric(B *testing.B) {
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64BetweenGeneric(a, 0.25, 0.5, bits)
            }
        })
    }
}

func BenchmarkMatchFloat64BetweenAVX2(B *testing.B) {
    if !useAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64BetweenAVX2(a, 0.25, 0.5, bits)
            }
        })
    }
}

func BenchmarkMatchFloat64BetweenAVX512(B *testing.B) {
    if !useAVX512_F {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randFloat64Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Float64Size))
            for i := 0; i < B.N; i++ {
                matchFloat64BetweenAVX512(a, 0.25, 0.5, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------
// Float64 Slice
//
func TestFloat64SliceContains(T *testing.T) {
    // nil slice
    if Float64.Contains(nil, 1) {
        T.Errorf("nil slice cannot contain value")
    }

    // empty slice
    if Float64.Contains([]float64{}, 1) {
        T.Errorf("empty slice cannot contain value")
    }

    // 1-element slice positive
    if !Float64.Contains([]float64{1}, 1) {
        T.Errorf("1-element slice value not found")
    }

    // 1-element slice negative
    if Float64.Contains([]float64{1}, 2) {
        T.Errorf("1-element slice found wrong match")
    }

    // n-element slice positive first element
    if !Float64.Contains([]float64{1, 3, 5, 7, 11, 13}, 1) {
        T.Errorf("N-element first slice value not found")
    }

    // n-element slice positive middle element
    if !Float64.Contains([]float64{1, 3, 5, 7, 11, 13}, 5) {
        T.Errorf("N-element middle slice value not found")
    }

    // n-element slice positive last element
    if !Float64.Contains([]float64{1, 3, 5, 7, 11, 13}, 13) {
        T.Errorf("N-element last slice value not found")
    }

    // n-element slice negative before
    if Float64.Contains([]float64{1, 3, 5, 7, 11, 13}, 0) {
        T.Errorf("N-element before slice value wrong match")
    }

    // n-element slice negative middle
    if Float64.Contains([]float64{1, 3, 5, 7, 11, 13}, 2) {
        T.Errorf("N-element middle slice value wrong match")
    }

    // n-element slice negative after
    if Float64.Contains([]float64{1, 3, 5, 7, 11, 13}, 14) {
        T.Errorf("N-element after slice value wrong match")
    }
}

func BenchmarkFloat64SliceContains(B *testing.B) {
    cases := []int{10, 1000, 1000000}
    for _, n := range cases {
        B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
            a := Float64.Sort(randFloat64Slice(n, 1))
            B.ResetTimer()
            for i := 0; i < B.N; i++ {
                Float64.Contains(a, rand.Float64())
            }
        })
    }
    for _, n := range cases {
        B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
            a := Float64.Sort(randFloat64Slice(n, 1))
            B.ResetTimer()
            for i := 0; i < B.N; i++ {
                Float64.Contains(a, a[rand.Intn(len(a))])
            }
        })
    }
}

func TestFloat64SliceContainsRange(T *testing.T) {
    type VecTestRange struct {
        Name  string
        From  float64
        To    float64
        Match bool
    }

    type VecTestcase struct {
        Slice  []float64
        Ranges []VecTestRange
    }

    var tests = []VecTestcase{
        // nil slice
        VecTestcase{
            Slice: nil,
            Ranges: []VecTestRange{
                VecTestRange{Name: "X", From: 0, To: 2, Match: false},
            },
        },
        // empty slice
        VecTestcase{
            Slice: []float64{},
            Ranges: []VecTestRange{
                VecTestRange{Name: "X", From: 0, To: 2, Match: false},
            },
        },
        // 1-element slice
        VecTestcase{
            Slice: []float64{3},
            Ranges: []VecTestRange{
                VecTestRange{Name: "A", From: 0, To: 2, Match: false},   // Case A
                VecTestRange{Name: "B1", From: 1, To: 3, Match: true},   // Case B.1, D1
                VecTestRange{Name: "B3", From: 3, To: 4, Match: true},   // Case B.3, D3
                VecTestRange{Name: "E", From: 15, To: 16, Match: false}, // Case E
                VecTestRange{Name: "F", From: 1, To: 4, Match: true},    // Case F
            },
        },
        // 1-element slice, from == to
        VecTestcase{
            Slice: []float64{3},
            Ranges: []VecTestRange{
                VecTestRange{Name: "BCD", From: 3, To: 3, Match: true}, // Case B.3, C.1, D.1
            },
        },
        // N-element slice
        VecTestcase{
            Slice: []float64{3, 5, 7, 11, 13},
            Ranges: []VecTestRange{
                VecTestRange{Name: "A", From: 0, To: 2, Match: false},    // Case A
                VecTestRange{Name: "B1a", From: 1, To: 3, Match: true},   // Case B.1
                VecTestRange{Name: "B1b", From: 3, To: 3, Match: true},   // Case B.1
                VecTestRange{Name: "B2a", From: 1, To: 4, Match: true},   // Case B.2
                VecTestRange{Name: "B2b", From: 1, To: 5, Match: true},   // Case B.2
                VecTestRange{Name: "B3a", From: 3, To: 4, Match: true},   // Case B.3
                VecTestRange{Name: "B3b", From: 3, To: 5, Match: true},   // Case B.3
                VecTestRange{Name: "C1a", From: 4, To: 5, Match: true},   // Case C.1
                VecTestRange{Name: "C1b", From: 4, To: 6, Match: true},   // Case C.1
                VecTestRange{Name: "C1c", From: 4, To: 7, Match: true},   // Case C.1
                VecTestRange{Name: "C1d", From: 5, To: 5, Match: true},   // Case C.1
                VecTestRange{Name: "C2a", From: 8, To: 8, Match: false},  // Case C.2
                VecTestRange{Name: "C2b", From: 8, To: 10, Match: false}, // Case C.2
                VecTestRange{Name: "D1a", From: 11, To: 13, Match: true}, // Case D.1
                VecTestRange{Name: "D1b", From: 12, To: 13, Match: true}, // Case D.1
                VecTestRange{Name: "D2", From: 12, To: 14, Match: true},  // Case D.2
                VecTestRange{Name: "D3a", From: 13, To: 13, Match: true}, // Case D.3
                VecTestRange{Name: "D3b", From: 13, To: 14, Match: true}, // Case D.3
                VecTestRange{Name: "E", From: 15, To: 16, Match: false},  // Case E
                VecTestRange{Name: "Fa", From: 0, To: 16, Match: true},   // Case F
                VecTestRange{Name: "Fb", From: 0, To: 13, Match: true},   // Case F
                VecTestRange{Name: "Fc", From: 3, To: 13, Match: true},   // Case F
            },
        },
        // real-word testcase
        VecTestcase{
            Slice: []float64{
                699421, 1374016, 1692360, 1797909, 1809339,
                2552208, 2649552, 2740915, 2769610, 3043393,
            },
            Ranges: []VecTestRange{
                VecTestRange{Name: "1", From: 2785281, To: 2818048, Match: false},
                VecTestRange{Name: "2", From: 2818049, To: 2850816, Match: false},
                VecTestRange{Name: "3", From: 2850817, To: 2883584, Match: false},
                VecTestRange{Name: "4", From: 2883585, To: 2916352, Match: false},
                VecTestRange{Name: "5", From: 2916353, To: 2949120, Match: false},
                VecTestRange{Name: "6", From: 2949121, To: 2981888, Match: false},
                VecTestRange{Name: "7", From: 2981889, To: 3014656, Match: false},
                VecTestRange{Name: "8", From: 3014657, To: 3047424, Match: true},
            },
        },
    }

    for i, v := range tests {
        for _, r := range v.Ranges {
            if want, got := r.Match, Float64.ContainsRange(v.Slice, r.From, r.To); want != got {
                T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
            }
        }
    }
}

func BenchmarkFloat64SliceContainsRange(B *testing.B) {
    for _, n := range []int{10, 1000, 1000000} {
        B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
            a := Float64.Sort(randFloat64Slice(n, 1))
            B.ResetTimer()
            for i := 0; i < B.N; i++ {
                min, max := rand.Float64(), rand.Float64()
                if min > max {
                    min, max = max, min
                }
                Float64.ContainsRange(a, min, max)
            }
        })
    }
}
