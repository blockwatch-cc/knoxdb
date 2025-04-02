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
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

type EncodeFunc[T types.Integer] func([]byte, []T, T, T) ([]byte, error)
type DecodeFunc[T types.Unsigned] func([]T, []byte) (int, error)
type CompareFunc func([]byte, uint64, *bitset.Bitset) *bitset.Bitset
type CompareFunc2 func([]byte, uint64, uint64, *bitset.Bitset) *bitset.Bitset

type TestCase[T types.Unsigned] struct {
	Name string
	Data []T
	Gen  func() []T
	Err  bool
}

func MakeTests[T types.Unsigned]() []TestCase[T] {
	width := unsafe.Sizeof(T(0))
	tests := []TestCase[T]{
		{Name: "nil", Data: nil},
		{Name: "empty", Data: []T{}},
		{Name: "mixed sizes", Data: []T{7, 6, 255, 4, 3, 2, 1}},
		{Name: "240 ones", Gen: ones[T](240)},
		{Name: "120 ones plus 5", Gen: func() []T {
			in := ones[T](240)()
			in[120] = 5
			return in
		}},
		{Name: "119 ones plus 5", Gen: func() []T {
			in := ones[T](240)()
			in[119] = 5
			return in
		}},
		{Name: "239 ones plus 5", Gen: func() []T {
			in := ones[T](241)()
			in[239] = 5
			return in
		}},
		{Name: "1 bit", Gen: bits[T](120, 1)},
		{Name: "2 bits", Gen: bits[T](120, 2)},
		{Name: "3 bits", Gen: bits[T](120, 3)},
		{Name: "4 bits", Gen: bits[T](120, 4)},
		{Name: "5 bits", Gen: bits[T](120, 5)},
		{Name: "6 bits", Gen: bits[T](120, 6)},
		{Name: "7 bits", Gen: bits[T](120, 7)},
		{Name: "8 bits", Gen: bits[T](120, 8)},
		{Name: "67", Data: slices.Repeat([]T{67}, 640)},
	}
	combi := TestCase[T]{
		Name: "combination",
		Gen: combine[T](
			bits[T](120, 1),
			bits[T](120, 2),
			bits[T](120, 3),
			bits[T](120, 4),
			bits[T](120, 5),
			bits[T](120, 6),
			bits[T](120, 7),
			bits[T](120, 8),
		)}

	if width > 1 {
		tests = append(tests, []TestCase[T]{
			{Name: "10 bits", Gen: bits[T](120, 10)},
			{Name: "12 bits", Gen: bits[T](120, 12)},
			{Name: "15 bits", Gen: bits[T](120, 15)},
		}...)
		combi.Gen = combine[T](
			bits[T](120, 1),
			bits[T](120, 2),
			bits[T](120, 3),
			bits[T](120, 4),
			bits[T](120, 5),
			bits[T](120, 6),
			bits[T](120, 7),
			bits[T](120, 8),
			bits[T](120, 10),
			bits[T](120, 12),
			bits[T](120, 15),
			bits[T](120, 16),
		)
	}

	if width > 2 {
		tests = append(tests, []TestCase[T]{
			{Name: "20 bits", Gen: bits[T](120, 20)},
			{Name: "30 bits", Gen: bits[T](120, 30)},
			{Name: "32 bits", Gen: bits[T](120, 32)},
		}...)
		combi.Gen = combine[T](
			bits[T](120, 1),
			bits[T](120, 2),
			bits[T](120, 3),
			bits[T](120, 4),
			bits[T](120, 5),
			bits[T](120, 6),
			bits[T](120, 7),
			bits[T](120, 8),
			bits[T](120, 10),
			bits[T](120, 12),
			bits[T](120, 15),
			bits[T](120, 20),
			bits[T](120, 30),
			bits[T](120, 32),
		)
	}
	if width > 4 {
		tests = append(tests, []TestCase[T]{
			{Name: "60 bits", Gen: bits[T](120, 60)},
			{
				Name: "too big",
				Data: util.ReinterpretSlice[uint64, T]([]uint64{7, 6, 2<<61 - 1, 4, 3, 2, 1}),
				Err:  true,
			},
		}...)

		combi.Gen = combine[T](
			bits[T](120, 1),
			bits[T](120, 2),
			bits[T](120, 3),
			bits[T](120, 4),
			bits[T](120, 5),
			bits[T](120, 6),
			bits[T](120, 7),
			bits[T](120, 8),
			bits[T](120, 10),
			bits[T](120, 12),
			bits[T](120, 15),
			bits[T](120, 20),
			bits[T](120, 30),
			bits[T](120, 60),
		)
	}

	return append(tests, combi)
}

func EncodeTest[T types.Unsigned](t *testing.T, enc EncodeFunc[T], dec DecodeFunc[T]) {
	for _, c := range MakeTests[T]() {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			in := c.Data
			if c.Gen != nil {
				in = c.Gen()
			}
			var _, maxv T
			if len(in) > 0 {
				_, maxv = slices.Min(in), slices.Max(in)
			}
			buf := make([]byte, len(in)*8)

			// encode without min-FOR to be compatible with testcase data
			// testing all selectors
			buf, err := enc(buf, slices.Clone(in), 0, maxv)
			if c.Err {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}

			dst := make([]T, len(in))
			n, err := dec(dst, buf)
			require.NoError(t, err)

			if len(in) > 0 {
				require.Equal(t, in, dst[:n])
			}
		})
	}
}

type CompareCase struct {
	Name string
	Gen  func(int) []uint64
}

var CompareCases = []CompareCase{
	{"one", func(n int) []uint64 { return tests.GenConst[uint64](n, 1) }},
	{"rnd", tests.GenRnd[uint64]},
}

var CompareSizes = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 30, 60, 120, 240, 1024}

func CompareTest(t *testing.T, enc EncodeFunc[uint64], cmp CompareFunc, mode types.FilterMode) {
	for _, sz := range CompareSizes {
		for _, c := range CompareCases {
			t.Run(fmt.Sprintf("%s/sz_%d", c.Name, sz), func(t *testing.T) {
				vals := c.Gen(sz)
				minv, maxv := slices.Min(vals), slices.Max(vals)
				buf, err := enc(make([]byte, sz*8), vals, 0, maxv) // sic! no MinFOR
				require.NoError(t, err)
				bits := bitset.NewBitset(sz)

				// value exists
				val := vals[len(vals)/2]
				cmp(buf, val, bits)
				ensureBits(t, vals, val, val, bits, mode)
				bits.Zero()
				require.Equal(t, 0, bits.Count(), "cleared")

				// value over bounds
				over := maxv + 1
				cmp(buf, over, bits)
				ensureBits(t, vals, over, over, bits, mode)
				bits.Zero()
				require.Equal(t, 0, bits.Count(), "cleared")

				// value under bounds
				under := minv
				if under > 0 {
					under--
				}
				cmp(buf, under, bits)
				ensureBits(t, vals, under, under, bits, mode)
				bits.Zero()
				require.Equal(t, 0, bits.Count(), "cleared")
			})
		}
	}
}

// range mode specific test with 2 values
func CompareTest2(t *testing.T, enc EncodeFunc[uint64], cmp CompareFunc2, mode types.FilterMode) {
	for _, sz := range CompareSizes {
		for _, c := range CompareCases {
			t.Run(fmt.Sprintf("%s/sz_%d", c.Name, sz), func(t *testing.T) {
				vals := c.Gen(sz)
				minv, maxv := slices.Min(vals), slices.Max(vals)
				buf, err := enc(make([]byte, sz*8), vals, 0, 1)
				require.NoError(t, err)
				bits := bitset.NewBitset(sz)

				// single value
				val := vals[len(vals)/2]
				cmp(buf, val, val, bits)
				ensureBits(t, vals, val, val, bits, mode)
				bits.Zero()

				// full range
				cmp(buf, minv, maxv, bits)
				ensureBits(t, vals, minv, maxv, bits, mode)
				bits.Zero()

				// partial range
				from, to := max(val/2, minv+1), min(val*2, maxv-1)
				cmp(buf, from, to, bits)
				ensureBits(t, vals, from, to, bits, mode)
				bits.Zero()

				// out of bounds (over)
				cmp(buf, maxv+1, maxv+2, bits)
				ensureBits(t, vals, maxv+1, maxv+2, bits, mode)
				bits.Zero()

				// out of bounds (under)
				if minv > 2 {
					cmp(buf, minv-2, minv-1, bits)
					ensureBits(t, vals, minv-2, minv-1, bits, mode)
					bits.Zero()
				}
			})
		}
	}
}

func ensureBits(t *testing.T, vals []uint64, val, val2 uint64, bits *bitset.Bitset, mode types.FilterMode) {
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
