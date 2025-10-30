// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"flag"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"github.com/stretchr/testify/require"
)

var (
	ShowInfo   bool
	ShowValues bool

	CompareSizes = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 23, 128, 129}
	ItSizes      = []int{7, 8, 15, 16, 63, 64, 65, 127, 128, 129}

	constCase  = []int{1, 1, 1, 1, 1, 1}
	deltaCase  = []int{1, 2, 3, 4, 5, 6} // delta = 1
	runsCase   = []int{1, 1, 2, 2, 3, 3}
	dictCase   = []int{1, 50, 1, 50, 1, 50}
	edgeCase   = []int{1, 2, 2, 2, 2, 2}       // initial delta = 1, then 0
	negCase    = []int{-1, -2, -3, -4, -5, -6} // delta = -1
	sixtySeven = slices.Repeat([]int{67}, 640) // 640 equal values

	// float
	floatConstCase = []float64{1.25, 1.25, 1.25, 1.25, 1.25, 1.25}
	floatRunsCase  = []float64{1.5, 1.5, 2.35, 2.35, 3.60, 3.60}
	floatDictCase  = []float64{1.50, 50.45, 1.50, 50.45, 1.50, 50.45}
	floatAlpCase   = []float64{2.50, 540.4532, 1.5210, 50.4125, 1.5330, 50.4335}
	floatAlpRdCase = []float64{18446744073709551615.50, 18446744073709551615.4532, 18446744073709551615.5210, 18446744073709551615.4125, 18446744073709551615.5330, 18446744073709551615.4335}
)

func init() {
	flag.BoolVar(&ShowInfo, "info", false, "be more verbose")
	flag.BoolVar(&ShowValues, "detail", false, "show values")
}

type TestCase[T types.Number] struct {
	Name string
	Data []T
}

func MakeShortIntTests[T types.Integer](scheme int) []TestCase[T] {
	switch scheme {
	case 1: // TIntConstant:
		return []TestCase[T]{MakeIntTest[T]("const", 6, constCase...)}
	case 2: // TIntDelta:
		return []TestCase[T]{
			MakeIntTest[T]("delta", 6, deltaCase...),
			MakeIntTest[T]("negd", 6, negCase...),
		}
	}
	return []TestCase[T]{
		MakeIntTest[T]("const", 6, constCase...),
		MakeIntTest[T]("delta", 6, deltaCase...),
		MakeIntTest[T]("runs", 6, runsCase...),
		MakeIntTest[T]("dict", 6, dictCase...),
		MakeIntTest[T]("edge", 6, edgeCase...),
		MakeIntTest[T]("negd", 6, negCase...),
		MakeIntTest[T]("67", 40*16, sixtySeven...),
	}
}

func MakeIntTest[T types.Integer](s string, n int, data ...int) TestCase[T] {
	c := TestCase[T]{
		Name: s + "_" + strconv.Itoa(n),
		Data: make([]T, n),
	}
	if len(data) > 0 {
		for i := range n {
			c.Data[i] = T(data[i])
		}
	} else {
		c.Data = tests.GenRnd[T](n)
	}
	return c
}

func MakeIntTests[T types.Integer](n int) []TestCase[T] {
	name := strconv.Itoa(n)
	return []TestCase[T]{
		{"const_" + name, tests.GenConst[T](n, 42)},
		{"delta-_" + name, tests.GenSeq[T](n, 3)},
		{"delta+_" + name, tests.GenSeq[T](n, -3)},
		{"dups_" + name, tests.GenDups[T](n, n/10, -1)},
		{"runs_" + name, tests.GenRuns[T](n, 5, -1)},
		{"rand_" + name, tests.GenRnd[T](n)},
	}
}

func MakeShortFloatTests[T types.Float](scheme int) []TestCase[T] {
	if scheme == 10 {
		// TFloatConstant:
		return []TestCase[T]{MakeFloatTest[T]("const", 6, floatConstCase...)}
	}
	tests := []TestCase[T]{
		MakeFloatTest[T]("runs", 6, floatRunsCase...),
		MakeFloatTest[T]("dict", 6, floatDictCase...),
		MakeFloatTest[T]("alp", 6, floatAlpCase...),
	}
	if scheme == 4 {
		// TFloatAlpRd
		tests = append(tests, MakeFloatTest[T]("alprd", 6, floatAlpRdCase...))
	}
	return tests
}

func MakeFloatTest[T types.Float](s string, n int, data ...float64) TestCase[T] {
	c := TestCase[T]{
		Name: s + "_" + strconv.Itoa(n),
		Data: make([]T, n),
	}
	if len(data) > 0 {
		for i := 0; i < n; i++ {
			c.Data[i] = T(data[i])
		}
	} else {
		c.Data = tests.GenRnd[T](n)
	}
	return c
}

func MakeFloatTests[T types.Float](n int) []TestCase[T] {
	name := reflect.TypeOf(T(0)).String() + "_" + strconv.Itoa(n)
	return []TestCase[T]{
		{"const_" + name, tests.GenConst[T](n, 4.225)},
		{"dups_" + name, tests.GenDups[T](n, n/10, -1)},
		{"runs_" + name, tests.GenRuns[T](n, 5, -1)},
		{"rand_" + name, tests.GenRnd[T](n)},
	}
}

func EnsureBits[T types.Number](t *testing.T, vals []T, val, val2 T, bits *bitset.Bitset, set *xroar.Bitmap, mode types.FilterMode) {
	if ShowValues {
		for i, v := range vals {
			t.Logf("Val %d: %v", i, v)
		}
		t.Logf("Bitset %x", bits.Bytes())
	}
	minv, maxv := slices.Min(vals), slices.Max(vals)
	switch mode {
	case types.FilterModeEqual:
		for i, v := range vals {
			require.Equal(t, v == val, bits.Contains(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeNotEqual:
		for i, v := range vals {
			require.Equal(t, v != val, bits.Contains(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeLt:
		for i, v := range vals {
			require.Equal(t, v < val, bits.Contains(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeLe:
		for i, v := range vals {
			require.Equal(t, v <= val, bits.Contains(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeGt:
		for i, v := range vals {
			require.Equal(t, v > val, bits.Contains(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeGe:
		for i, v := range vals {
			require.Equal(t, v >= val, bits.Contains(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeRange:
		for i, v := range vals {
			require.Equal(t, v >= val && v <= val2, bits.Contains(i), "bit=%d val=%v %s [%v,%v] min=%v max=%v",
				i, v, mode, val, val2, minv, maxv)
		}

	case types.FilterModeIn:
		for i, v := range vals {
			require.Equal(t, set.Contains(uint64(v)), bits.Contains(i), "bit=%d min=%v max=%v val=%v %s %v",
				i, minv, maxv, v, mode, set.ToArray(nil))
		}

	case types.FilterModeNotIn:
		for i, v := range vals {
			require.Equal(t, !set.Contains(uint64(v)), bits.Contains(i), "bit=%d min=%v max=%v val=%v %s %v",
				i, minv, maxv, v, mode, set.ToArray(nil))
		}
	}
}
