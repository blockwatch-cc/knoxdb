// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package alp

import (
	"fmt"
	"math"
	"testing"
	"testing/quick"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/encode/bitpack"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/require"
)

type TestCase[T types.Float] struct {
	Name string
	Data []T
	NEx  int
}

func MakeTestcases[T types.Float]() []TestCase[T] {
	if unsafe.Sizeof(T(0)) == 8 {
		// float64 cases
		return []TestCase[T]{
			{"bw0", append(tests.GenConst[T](1024, 33554431.0), 0.0), 0},
			{"bw1", append(tests.GenConst[T](1023, 67108863.0), 0.0), 0},
			{"bw2", append(tests.GenConst[T](1023, 134217727.0), 0.0), 0},
			{"bw3", append(tests.GenConst[T](1023, 268435455.0), 0.0), 0},
			{"bw4", append(tests.GenConst[T](1023, 536870911.0), 0.0), 0},
			{"bw5", append(tests.GenConst[T](1023, 1073741823.0), 0.0), 0},
			{"bw6", append(tests.GenConst[T](1023, 2147483647.0), 0.0), 0},
			{"bw7", append(tests.GenConst[T](1023, 4294967295.0), 0.0), 0},
			{"bw8", append(tests.GenConst[T](1023, 8589934591.0), 0.0), 0},
			{"bw9", append(tests.GenConst[T](1023, 17179869183.0), 0.0), 0},
			{"bw10", append(tests.GenConst[T](1023, 34359738367.0), 0.0), 0},
			{"bw11", append(tests.GenConst[T](1023, 68719476735.0), 0.0), 0},
			{"bw12", append(tests.GenConst[T](1023, 137438953471.0), 0.0), 0},
			{"bw13", append(tests.GenConst[T](1023, 274877906943.0), 0.0), 0},
			{"bw14", append(tests.GenConst[T](1023, 549755813887.0), 0.0), 0},
			{"bw15", append(tests.GenConst[T](1023, 1099511627775.0), 0.0), 0},
			{"bw16", append(tests.GenConst[T](1023, 2199023255551.0), 0.0), 0},
			{"bw17", append(tests.GenConst[T](1023, 4398046511103.0), 0.0), 0},
			{"bw18", append(tests.GenConst[T](1023, 8796093022207.0), 0.0), 0},
			{"bw19", append(tests.GenConst[T](1023, 17592186044415.0), 0.0), 0},
			{"bw20", append(tests.GenConst[T](1023, 35184372088831.0), 0.0), 0},
			{"bw21", append(tests.GenConst[T](1023, 70368744177663.0), 0.0), 0},
			{"bw22", append(tests.GenConst[T](1023, 140737488355327.0), 0.0), 0},
			{"bw23", append(tests.GenConst[T](1023, 281474976710655.0), 0.0), 0},
			{"bw24", append(tests.GenConst[T](1023, 562949953421311.0), 0.0), 0},
			{"bw25", append(tests.GenConst[T](1023, 1125899906842623.0), 0.0), 0},
			{"bw26", append(tests.GenConst[T](1023, 2251799813685247.0), 0.0), 0},
			{"bw27", append(tests.GenConst[T](1023, 4503599627370495.0), 0.0), 0},
			{"bw28", append(tests.GenConst[T](1023, 9007199254740991.0), 0.0), 0},
			{"bw29", append(tests.GenConst[T](1023, 18014398509481983.0), 0.0), 0},
			{"bw30", append(tests.GenConst[T](1023, 36028797018963967.0), 0.0), 0},
			{"bw31", append(tests.GenConst[T](1023, 72057594037927935.0), 0.0), 0},
			{"bw32", append(tests.GenConst[T](1023, 144115188075855871.0), 0.0), 0},
			{"bw33", append(tests.GenConst[T](1023, 288230376151711743.0), 0.0), 0},
			{"bw34", append(tests.GenConst[T](1023, 576460752303423487.0), 0.0), 0},
			{"bw35", append(tests.GenConst[T](1023, 1152921504606846975.0), 0.0), 0},
			{"bw36", append(tests.GenConst[T](1023, 2305843009213693951.0), 0.0), 0},
			{"bw37", append(tests.GenConst[T](1023, 4611686018427387903.0), 0.0), 0},
			{"nan", []T{-1.0, 1.0, T(math.NaN()), -1.0, 1.0}, 1},
			// -0.0 is not an exception
			{"inf", []T{T(math.Inf(-1)), 1.0, 2.0, -1.0, T(math.Inf(1)), -0.0}, 2},
			// {"bw38", append(tests.GenConst[T](1023, 9223372036854775807.0), 0.0)},
			// {"bw39", append(tests.GenConst[T](1023, 18446744073709551615.0), 0.0)}
		}
	} else {
		// float32 cases
		return []TestCase[T]{
			{"bw0", tests.GenConst[T](1024, 0.0), 0},
			{"bw1", append(tests.GenConst[T](1023, 1.0), 0.0), 0},
			{"bw2", append(tests.GenConst[T](1023, 3.0), 0.0), 0},
			{"bw3", append(tests.GenConst[T](1023, 7.0), 0.0), 0},
			{"bw4", append(tests.GenConst[T](1023, 15.0), 0.0), 0},
			{"bw5", append(tests.GenConst[T](1023, 31.0), 0.0), 0},
			{"bw6", append(tests.GenConst[T](1023, 63.0), 0.0), 0},
			{"bw7", append(tests.GenConst[T](1023, 127.0), 0.0), 0},
			{"bw8", append(tests.GenConst[T](1023, 255.0), 0.0), 0},
			{"bw9", append(tests.GenConst[T](1023, 511.0), 0.0), 0},
			{"bw10", append(tests.GenConst[T](1023, 1023.0), 0.0), 0},
			{"bw11", append(tests.GenConst[T](1023, 2047.0), 0.0), 0},
			{"bw12", append(tests.GenConst[T](1023, 4095.0), 0.0), 0},
			{"bw13", append(tests.GenConst[T](1023, 8191.0), 0.0), 0},
			{"bw14", append(tests.GenConst[T](1023, 16383.0), 0.0), 0},
			{"bw15", append(tests.GenConst[T](1023, 32767.0), 0.0), 0},
			{"bw16", append(tests.GenConst[T](1023, 65535.0), 0.0), 0},
			{"bw17", append(tests.GenConst[T](1023, 131071.0), 0.0), 0},
			{"bw18", append(tests.GenConst[T](1023, 262143.0), 0.0), 0},
			{"bw19", append(tests.GenConst[T](1023, 524287.0), 0.0), 0},
			{"bw20", append(tests.GenConst[T](1023, 1048575.0), 0.0), 0},
			{"bw21", append(tests.GenConst[T](1023, 2097151.0), 0.0), 0},
			{"bw22", append(tests.GenConst[T](1023, 4194303.0), 0.0), 0},
			{"bw23", append(tests.GenConst[T](1023, 8388607.0), 0.0), 0},
			{"bw24", append(tests.GenConst[T](1023, 16777215.0), 0.0), 0},
			{"bw25", append(tests.GenConst[T](1023, 1235.64), 0.0), 0},
			{"bw26", append(tests.GenConst[T](1023, 10.23), 0.0), 0},
			{"nan", []T{-1.0, 1.0, T(math.NaN()), -1.0, 1.0}, 1},
			// -0.0 is not an exception
			{"inf", []T{T(math.Inf(-1)), 1.0, 2.0, -1.0, T(math.Inf(1)), T(-0.0)}, 2},
		}
	}
}

func TestAlp(t *testing.T) {
	AlpTest[float32, int32](t)
	AlpTest[float64, int64](t)
}

func TestFusedAlp(t *testing.T) {
	AlpFusedTest[float32, int32](t)
	AlpFusedTest[float64, int64](t)
}

func AlpTest[T Float, E Int](t *testing.T) {
	for _, c := range MakeTestcases[T]() {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			enc := NewEncoder[T, E]()
			a := Analyze[T, E](c.Data)
			res := enc.Encode(c.Data, a.Exp)
			require.Equal(t, len(c.Data), len(res.Encoded))
			require.Equal(t, c.NEx, len(res.PatchValues))
			dec := NewDecoder[T, E](a.Exp.F, a.Exp.E).
				WithExceptions(res.PatchValues, res.PatchIndices).
				WithSafeInt(res.IsSafeInt)
			dst := make([]T, len(c.Data))
			dec.Decode(dst, res.Encoded)
			for i, v := range c.Data {
				if math.IsNaN(float64(v)) {
					require.Equal(t, math.IsNaN(float64(v)), math.IsNaN(float64(dst[i])), "val %d: %v != %v", i, v, dst[i])
				} else {
					require.Equal(t, v, dst[i], "val %d int=%x exp=[%d,%d] want=%x have=%x", i,
						res.Encoded[i],
						a.Exp.E, a.Exp.F,
						math.Float64bits(float64(v)),
						math.Float64bits(float64(dst[i])),
					)
				}
			}
			res.Close()
		})
	}
}

func AlpFusedTest[T Float, E Int](t *testing.T) {
	for _, c := range MakeTestcases[T]() {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			enc := NewEncoder[T, E]()
			a := Analyze[T, E](c.Data)
			res := enc.Encode(c.Data, a.Exp)
			require.Equal(t, len(c.Data), len(res.Encoded))
			require.Equal(t, c.NEx, len(res.PatchValues))
			dec := NewDecoder[T, E](a.Exp.F, a.Exp.E).
				WithExceptions(res.PatchValues, res.PatchIndices)
			buf := make([]byte, len(c.Data)*16)
			buf, _ = bitpack.Encode(buf, res.Encoded, res.Min, res.Max)
			dst := make([]T, len(c.Data))
			dec.DecodeFused(dst, buf, types.Log2Range(res.Min, res.Max), res.Min)
			for i, v := range c.Data {
				if math.IsNaN(float64(v)) {
					require.Equal(t, math.IsNaN(float64(v)), math.IsNaN(float64(dst[i])), "val %d: %v != %v", i, v, dst[i])
				} else {
					require.Equal(t, v, dst[i], "val %d: %v != %v", i, v, dst[i])
				}
			}
			res.Close()
		})
	}
}

func TestAlpQuick(t *testing.T) {
	AlpTestQuick[float32, int32](t)
	AlpTestQuick[float64, int64](t)
}

func AlpTestQuick[T Float, E Int](t *testing.T) {
	t.Run(fmt.Sprintf("%T", T(0)), func(t *testing.T) {
		err := quick.Check(func(vals []T) bool {
			enc := NewEncoder[T, E]()
			a := Analyze[T, E](vals)
			res := enc.Encode(vals, a.Exp)
			require.Equal(t, len(vals), len(res.Encoded))
			dec := NewDecoder[T, E](a.Exp.F, a.Exp.E).
				WithExceptions(res.PatchValues, res.PatchIndices).
				WithSafeInt(res.IsSafeInt)
			dst := make([]T, len(vals))
			dec.Decode(dst, res.Encoded)
			for i, v := range vals {
				if math.IsNaN(float64(v)) {
					require.Equal(t, math.IsNaN(float64(v)), math.IsNaN(float64(dst[i])), "val %d: %v != %v", i, v, dst[i])
				} else {
					require.Equal(t, v, dst[i], "val %d: %v != %v", i, v, dst[i])
				}
			}
			res.Close()
			return true
		}, nil)
		require.NoError(t, err)
	})
}

func TestFiles(t *testing.T) {
	tests.EnsureDataFiles(t)
	sz := 1024
	enc := NewEncoder[float64, int64]()
	dst := make([]float64, sz)
	for _, c := range tests.MakeFileTests[float64](sz) {
		t.Run(c.Name, func(t *testing.T) {
			for {
				src, ok := c.Next()
				if !ok {
					break
				}
				a := Analyze[float64, int64](src)
				res := enc.Encode(src, a.Exp)
				require.Equal(t, len(src), len(res.Encoded))
				dec := NewDecoder[float64, int64](a.Exp.F, a.Exp.E).
					WithExceptions(res.PatchValues, res.PatchIndices).
					WithSafeInt(res.IsSafeInt)
				dec.Decode(dst, res.Encoded)
				require.Equal(t, src, dst[:len(src)])
				dec.Close()
				res.Close()
			}
		})
	}
}
