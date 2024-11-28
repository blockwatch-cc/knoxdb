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
func TestMatchInt8EqualAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int8EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8EqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt8EqualAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int8EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8EqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt8EqualAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8EqualAVX2(a, math.MaxInt8/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt8EqualAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8EqualAVX512(a, math.MaxInt8/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//
func TestMatchInt8NotEqualAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int8NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8NotEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt8NotEqualAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int8NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8NotEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt8NotEqualAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8NotEqualAVX2(a, math.MaxInt8/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt8NotEqualAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8NotEqualAVX512(a, math.MaxInt8/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Testcases
//

func TestMatchInt8LessAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int8LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8LessThanAVX2(c.slice, c.match, bits)
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

func TestMatchInt8LessAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int8LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8LessThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt8LessAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8LessThanAVX2(a, math.MaxInt8/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt8LessAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8LessThanAVX512(a, math.MaxInt8/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//

func TestMatchInt8LessEqualAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int8LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8LessThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt8LessEqualAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int8LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8LessThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt8LessEqualAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8LessThanEqualAVX2(a, math.MaxInt8/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt8LessEqualAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8LessThanEqualAVX512(a, math.MaxInt8/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Testcases
//

func TestMatchInt8GreaterAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int8GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8GreaterThanAVX2(c.slice, c.match, bits)
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

func TestMatchInt8GreaterAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int8GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8GreaterThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt8GreaterAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8GreaterThanAVX2(a, math.MaxInt8/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt8GreaterAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8GreaterThanAVX512(a, math.MaxInt8/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

func TestMatchInt8GreaterEqualAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int8GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8GreaterThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt8GreaterEqualAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int8GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8GreaterThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt8GreaterEqualAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8GreaterThanEqualAVX2(a, math.MaxInt8/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt8GreaterEqualAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8GreaterThanEqualAVX512(a, math.MaxInt8/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Between Testcases
//

func TestMatchInt8BetweenAVX2(T *testing.T) {
    if !util.UseAVX2 {
        T.SkipNow()
    }
    for _, c := range int8BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8BetweenAVX2(c.slice, c.match, c.match2, bits)
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

func TestMatchInt8BetweenAVX512(T *testing.T) {
    if !util.UseAVX512_BW {
        T.SkipNow()
    }
    for _, c := range int8BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchInt8BetweenAVX512(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchInt8BetweenAVX2(B *testing.B) {
    if !util.UseAVX2 {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8BetweenAVX2(a, math.MaxInt8/4, math.MaxInt8/2, bits)
            }
        })
    }
}

func BenchmarkMatchInt8BetweenAVX512(B *testing.B) {
    if !util.UseAVX512_BW {
        B.SkipNow()
    }
    for _, n := range vecBenchmarkSizes {
        a := randInt8Slice(n.l, 1)
        bits := make([]byte, bitFieldLen(len(a)))
        B.Run(n.name, func(B *testing.B) {
            B.SetBytes(int64(n.l * Int8Size))
            for i := 0; i < B.N; i++ {
                matchInt8BetweenAVX512(a, math.MaxInt8/4, math.MaxInt8/2, bits)
            }
        })
    }
}
