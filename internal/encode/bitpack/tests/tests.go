// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"fmt"
	"slices"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"

	"github.com/stretchr/testify/require"
)

type EncodeFunc[T types.Integer] func([]byte, []T, T, T) ([]byte, int)
type DecodeFunc[T types.Integer] func([]T, []byte, int, T) (int, error)
type DecodeIndex[T types.Integer] func(index int) T
type DecodeIndexFunc[T types.Integer] func(buf []byte, log2 int) DecodeIndex[T]
type CompareFunc func([]byte, int, uint64, int, *bitset.Bitset) *bitset.Bitset
type CompareFunc2 func([]byte, int, uint64, uint64, int, *bitset.Bitset) *bitset.Bitset

type TestCase[T types.Integer] struct {
	Name string
	Vals []T
	Gen  func() []T
}

func (c TestCase[T]) Data() []T {
	if c.Gen != nil {
		return c.Gen()
	}
	return c.Vals
}

func MakeTests[T types.Integer]() []TestCase[T] {
	return []TestCase[T]{
		{Name: "nil", Vals: nil},
		{Name: "empty", Vals: []T{}},
		{Name: "mixed", Vals: []T{7, 6, 127, 4, 3, 2, 1}},
		{Name: "outlier", Vals: []T{7, 6, types.MaxVal[T]() - 1, 4, 3, 2, 1}},
	}
}

var (
	TestSizes          = []int{1, 7, 15, 16, 128, 1024, 1025} // algorithm boundaries (8x loop unrolled, 7x tail)
	Chars     CharType = "abcdefgh"
	WarnSym            = map[bool]string{false: "!!!"}
)

type CharType string

func (c CharType) Get(i int) string {
	return string(c[i%8])
}

func EncodeTest[T types.Integer](t *testing.T, enc EncodeFunc[T], dec DecodeFunc[T]) {
	if enc == nil {
		enc = encode[T]
	}
	if dec == nil {
		dec = decode[T]
	}
	for _, n := range TestSizes {
		for w := range int(unsafe.Sizeof(T(0))*8) - 1 { // bit depths [0..62] + 1
			w++
			t.Run(fmt.Sprintf("%T/%d_bits/n_%d", T(0), w, n), func(t *testing.T) {
				src := tests.GenRndBits[T](n, w)
				minv, maxv := slices.Min(src), slices.Max(src)
				buf := make([]byte, len(src)*8)

				buf, log2 := enc(buf, src, minv, maxv)

				dst := make([]T, len(src))
				n, err := dec(dst, buf, log2, minv)
				require.NoError(t, err)

				if !testing.Short() {
					for i, v := range dst {
						t.Logf("Val %s=%x unpacked %x %s", Chars.Get(i), src[i], v, WarnSym[src[i] == v])
					}
				}

				if len(src) > 0 {
					require.Equal(t, src, dst[:n])
				}
			})
		}
	}

	// test patterns
	for _, test := range MakeTests[T]() {
		t.Run(fmt.Sprintf("%T/%s", T(0), test.Name), func(t *testing.T) {
			src := test.Data()
			var maxv T
			if len(src) > 0 {
				maxv = slices.Max(src)
			}
			buf := make([]byte, len(src)*8)

			// encode without min-FOR to be compatible with testcase data
			buf, log2 := enc(buf, slices.Clone(src), 0, maxv)
			dst := make([]T, len(src))
			n, err := dec(dst, buf, log2, 0)
			require.NoError(t, err)

			if len(src) > 0 {
				require.Equal(t, src, dst[:n])
			}
		})
	}
}

type GenFunc[T types.Integer] func(int, int) []T
type CompareCase[T types.Integer] struct {
	Name string
	Gen  GenFunc[T]
}

func MakeCompareCases[T types.Integer]() []CompareCase[T] {
	return []CompareCase[T]{
		{"one", func(n, w int) []T {
			x := 1
			if w == 0 {
				x = 0
			}
			return tests.GenConst(n, T(x))
		}},
		{"rnd", tests.GenRndBits[T]},
	}
}

var CompareSizes = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 23}

func CompareTest[T types.Integer](t *testing.T, cmp CompareFunc, mode types.FilterMode, enc EncodeFunc[T], dec DecodeIndexFunc[T]) {
	for _, sz := range CompareSizes {
		for _, c := range MakeCompareCases[T]() {
			for w := range 63 { // bit widths 1..63
				w++
				t.Run(fmt.Sprintf("%s/%d_bits/sz_%d", c.Name, w, sz), func(t *testing.T) {
					src := c.Gen(sz, w)
					minv, maxv := T(0), T(1<<w-1) // manually setting minv and maxv because the Gen sometimes doesnt produce the full range for the bitwidth
					buf := make([]byte, sz*8)
					buf, log2 := enc(buf, src, minv, maxv)
					require.Equal(t, w, log2, "bit width for generated data should be equal to compressed data bit width")

					bits := bitset.NewBitset(sz)

					// value exists
					val := src[sz/2]
					cmp(buf, w, uint64(val), sz, bits)
					ensureBits(t, buf, w, src, val, val, bits, mode, dec)
					bits.Zero()
					require.Equal(t, 0, bits.Count(), "cleared")

					if w > 1 {
						// value over bounds
						over := maxv + 1
						cmp(buf, w, uint64(over), sz, bits)
						ensureBits(t, buf, w, src, over, over, bits, mode, dec)
						bits.Zero()
						require.Equal(t, 0, bits.Count(), "cleared")

						// value under bounds
						under := minv
						if under > 0 {
							under--
						}
						cmp(buf, w, uint64(under), sz, bits)
						ensureBits(t, buf, w, src, under, under, bits, mode, dec)
						bits.Zero()
						require.Equal(t, 0, bits.Count(), "cleared")
					}
				})
			}
		}
	}
}

// range mode specific test with 2 values
func CompareTest2[T types.Integer](t *testing.T, cmp CompareFunc2, mode types.FilterMode, enc EncodeFunc[T], dec DecodeIndexFunc[T]) {
	for _, sz := range CompareSizes {
		for _, c := range MakeCompareCases[T]() {
			for w := range 63 { // bit widths 1..63
				w++
				t.Run(fmt.Sprintf("%s/%d_bits/sz_%d", c.Name, w, sz), func(t *testing.T) {
					src := c.Gen(sz, w)
					minv, maxv := T(0), T(1<<w-1) // slices.Min(src), slices.Max(src)
					buf := make([]byte, sz*8)
					buf, log2 := enc(buf, src, minv, maxv)
					require.Equal(t, w, log2, "bit width for generated data should be equal to compressed data bit width")

					bits := bitset.NewBitset(sz)

					// single value
					val := src[sz/2]
					cmp(buf, w, uint64(val), uint64(val), sz, bits)
					ensureBits(t, buf, w, src, val, val, bits, mode, dec)
					bits.Zero()

					// full range
					cmp(buf, w, uint64(minv), uint64(maxv), sz, bits)
					ensureBits(t, buf, w, src, minv, maxv, bits, mode, dec)
					bits.Zero()

					// partial range
					from, to := max(val/2, minv+1), min(val*2, maxv-1)
					if from > to {
						from, to = to, from
					}
					cmp(buf, w, uint64(from), uint64(to), sz, bits)
					ensureBits(t, buf, w, src, from, to, bits, mode, dec)
					bits.Zero()

					if w > 1 {
						// out of bounds (over)
						cmp(buf, w, uint64(maxv+1), uint64(maxv+1), sz, bits)
						ensureBits(t, buf, w, src, maxv+1, maxv+1, bits, mode, dec)
						bits.Zero()

						// out of bounds (under)
						if minv > 2 {
							cmp(buf, w, uint64(minv-1), uint64(minv-1), sz, bits)
							ensureBits(t, buf, w, src, minv-1, minv-1, bits, mode, dec)
							bits.Zero()
						}
					}
				})
			}
		}
	}
}

func ensureBits[T types.Integer](t *testing.T, buf []byte, log2 int, vals []T, val, val2 T, bits *bitset.Bitset, mode types.FilterMode, decoder DecodeIndexFunc[T]) {
	if !testing.Short() {
		dec := decoder(buf, log2)
		for i, v := range vals {
			t.Logf("Val %s=%x decoded %x", Chars.Get(i), v, dec(i))
		}
	}
	switch mode {
	case types.FilterModeEqual:
		for i, v := range vals {
			require.Equal(t, v == val, bits.IsSet(i), "bit=%d val=%d c=%d", i, v, val)
		}

	case types.FilterModeNotEqual:
		for i, v := range vals {
			require.Equal(t, v != val, bits.IsSet(i), "bit=%d val=%d c=%d", i, v, val)
		}

	case types.FilterModeLt:
		for i, v := range vals {
			require.Equal(t, v < val, bits.IsSet(i), "bit=%d val=%d c=%d", i, v, val)
		}

	case types.FilterModeLe:
		for i, v := range vals {
			require.Equal(t, v <= val, bits.IsSet(i), "bit=%d val=%d c=%d", i, v, val)
		}

	case types.FilterModeGt:
		for i, v := range vals {
			require.Equal(t, v > val, bits.IsSet(i), "bit=%d val=%d c=%d", i, v, val)
		}

	case types.FilterModeGe:
		for i, v := range vals {
			require.Equal(t, v >= val, bits.IsSet(i), "bit=%d val=%d c=%d", i, v, val)
		}

	case types.FilterModeRange:
		for i, v := range vals {
			require.Equal(t, v >= val && v <= val2, bits.IsSet(i), "bit=%d val=%d a=%d b=%d", i, v, val, val2)
		}
	}
}
