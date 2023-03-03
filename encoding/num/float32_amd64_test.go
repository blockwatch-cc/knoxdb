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

func TestMatchFloat32EqualAVX2(T *testing.T) {
    for _, c := range float32EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32EqualAVX2(c.slice, c.match, bits)
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

func TestMatchFloat32EqualAVX512(T *testing.T) {
    if !util.UseAVX512_F {
        T.Skip("AVX512F not available. Skipping TestMatchFloat32EqualAVX512.")
    }
    for _, c := range float32EqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32EqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat32EqualAVX2(B *testing.B) {
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32EqualAVX2(a, math.MaxFloat32/2, bits)
            }
        })
    }
}

func BenchmarkMatchFloat32EqualAVX512(B *testing.B) {
    if !util.UseAVX512_F {
        B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32EqualAVX512.")
    }
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32EqualAVX512(a, math.MaxFloat32/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//
func TestMatchFloat32NotEqualAVX2(T *testing.T) {
    for _, c := range float32NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32NotEqualAVX2(c.slice, c.match, bits)
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

func TestMatchFloat32NotEqualAVX512(T *testing.T) {
    if !util.UseAVX512_F {
        T.Skip("AVX512F not available. Skipping TestMatchFloat32NotEqualAVX512.")
    }
    for _, c := range float32NotEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32NotEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat32NotEqualAVX2(B *testing.B) {
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32NotEqualAVX2(a, math.MaxFloat32/2, bits)
            }
        })
    }
}

func BenchmarkMatchFloat32NotEqualAVX512(B *testing.B) {
    if !util.UseAVX512_F {
        B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32NotEqualAVX512.")
    }
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32NotEqualAVX512(a, math.MaxFloat32/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Testcases
//
func TestMatchFloat32LessAVX2(T *testing.T) {
    for _, c := range float32LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32LessThanAVX2(c.slice, c.match, bits)
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

func TestMatchFloat32LessAVX512(T *testing.T) {
    if !util.UseAVX512_F {
        T.Skip("AVX512F not available. Skipping TestMatchFloat32LessAVX512.")
    }
    for _, c := range float32LessCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32LessThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat32LessAVX2(B *testing.B) {
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32LessThanAVX2(a, math.MaxFloat32/2, bits)
            }
        })
    }
}

func BenchmarkMatchFloat32LessAVX512(B *testing.B) {
    if !util.UseAVX512_F {
        B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32LessAVX512.")
    }
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32LessThanAVX512(a, math.MaxFloat32/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//
func TestMatchFloat32LessEqualAVX2(T *testing.T) {
    for _, c := range float32LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32LessThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchFloat32LessEqualAVX512(T *testing.T) {
    if !util.UseAVX512_F {
        T.Skip("AVX512F not available. Skipping TestMatchFloat32LessEqualAVX512.")
    }
    for _, c := range float32LessEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32LessThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat32LessEqualAVX2(B *testing.B) {
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32LessThanEqualAVX2(a, math.MaxFloat32/2, bits)
            }
        })
    }
}

func BenchmarkMatchFloat32LessEqualAVX512(B *testing.B) {
    if !util.UseAVX512_F {
        B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32LessEqualAVX512.")
    }
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32LessThanEqualAVX512(a, math.MaxFloat32/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Testcases
//
func TestMatchFloat32GreaterAVX2(T *testing.T) {
    for _, c := range float32GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32GreaterThanAVX2(c.slice, c.match, bits)
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

func TestMatchFloat32GreaterAVX512(T *testing.T) {
    if !util.UseAVX512_F {
        T.Skip("AVX512F not available. Skipping TestMatchFloat32GreaterAVX512.")
    }
    for _, c := range float32GreaterCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32GreaterThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat32GreaterAVX2(B *testing.B) {
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32GreaterThanAVX2(a, math.MaxFloat32/2, bits)
            }
        })
    }
}

func BenchmarkMatchFloat32GreaterAVX512(B *testing.B) {
    if !util.UseAVX512_F {
        B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32GreaterAVX512.")
    }
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32GreaterThanAVX512(a, math.MaxFloat32/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
func TestMatchFloat32GreaterEqualAVX2(T *testing.T) {
    for _, c := range float32GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32GreaterThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchFloat32GreaterEqualAVX512(T *testing.T) {
    if !util.UseAVX512_F {
        T.Skip("AVX512F not available. Skipping TestMatchFloat32GreaterEqualAVX512.")
    }
    for _, c := range float32GreaterEqualCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32GreaterThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat32GreaterEqualAVX2(B *testing.B) {
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32GreaterThanEqualAVX2(a, math.MaxFloat32/2, bits)
            }
        })
    }
}

func BenchmarkMatchFloat32GreaterEqualAVX512(B *testing.B) {
    if !util.UseAVX512_F {
        B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32GreaterEqualAVX512.")
    }
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32GreaterThanEqualAVX512(a, math.MaxFloat32/2, bits)
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Between Testcases
//
func TestMatchFloat32BetweenAVX2(T *testing.T) {
    for _, c := range float32BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32BetweenAVX2(c.slice, c.match, c.match2, bits)
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

func TestMatchFloat32BetweenAVX512(T *testing.T) {
    if !util.UseAVX512_F {
        T.Skip("AVX512F not available. Skipping TestMatchFloat32BetweenAVX512.")
    }
    for _, c := range float32BetweenCases {
        // pre-allocate the result slice and fill with poison
        l := bitFieldLen(len(c.slice))
        bits := make([]byte, l+32)
        for i, _ := range bits {
            bits[i] = 0xfa
        }
        bits = bits[:l]
        cnt := matchFloat32BetweenAVX512(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchFloat32BetweenAVX2(B *testing.B) {
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32BetweenAVX2(a, 5, math.MaxFloat32/2, bits)
            }
        })
    }
}

func BenchmarkMatchFloat32BetweenAVX512(B *testing.B) {
    if !util.UseAVX512_F {
        B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32BetweenAVX512.")
    }
    for _, n := range vecBenchmarkSizes {
        B.Run(n.name, func(B *testing.B) {
            a := randFloat32Slice(n.l, 1)
            bits := make([]byte, bitFieldLen(len(a)))
            B.ResetTimer()
            B.SetBytes(int64(n.l * Float32Size))
            for i := 0; i < B.N; i++ {
                matchFloat32BetweenAVX512(a, 5, 10, bits)
            }
        })
    }
}
