// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
)

// test all combinations of delta container setups
func makeSignedDeltaCases[T types.Signed]() []DeltaContainer[T] {
	return []DeltaContainer[T]{
		DeltaContainer[T]{Delta: 3, For: 0, N: 10},   // +delta and no for
		DeltaContainer[T]{Delta: 4, For: 4, N: 10},   // +delta and +for
		DeltaContainer[T]{Delta: 4, For: -4, N: 10},  // +delta and -for
		DeltaContainer[T]{Delta: -3, For: 0, N: 10},  // -delta and no for
		DeltaContainer[T]{Delta: -4, For: 4, N: 10},  // -delta and +for
		DeltaContainer[T]{Delta: -4, For: -4, N: 10}, // -delta and -for
		DeltaContainer[T]{Delta: -4, For: 40, N: 10}, // -delta and high +for (all +values)
	}
}

func makeUnsignedDeltaCases[T types.Unsigned]() []DeltaContainer[T] {
	return []DeltaContainer[T]{
		DeltaContainer[T]{Delta: 3, For: 0, N: 10},
		DeltaContainer[T]{Delta: 4, For: 4, N: 10},
	}
}

func TestSignedDelta(t *testing.T) {
	testIntDelta[int8](t, makeSignedDeltaCases[int8]())
	testIntDelta[int16](t, makeSignedDeltaCases[int16]())
	testIntDelta[int32](t, makeSignedDeltaCases[int32]())
	testIntDelta[int64](t, makeSignedDeltaCases[int64]())
}

func TestUnsignedDelta(t *testing.T) {
	testIntDelta[uint8](t, makeUnsignedDeltaCases[uint8]())
	testIntDelta[uint16](t, makeUnsignedDeltaCases[uint16]())
	testIntDelta[uint32](t, makeUnsignedDeltaCases[uint32]())
	testIntDelta[uint64](t, makeUnsignedDeltaCases[uint64]())
}

// generate check sequence compatible with delta container settings
func genDelta[T types.Integer](c DeltaContainer[T]) []T {
	vals := make([]T, c.N)
	for i := range c.N {
		vals[i] = c.For + T(i)*c.Delta
	}
	return vals
}

func testIntDelta[T types.Integer](t *testing.T, cases []DeltaContainer[T]) {
	for _, c := range cases {
		src := genDelta(c)
		name := fmt.Sprintf("_%d_%d_%T", c.Delta, c.For, T(0))

		// equal
		t.Run("EQ"+name, func(t *testing.T) {
			testCompareFunc[T](t, c.MatchEqual, src, types.FilterModeEqual)
		})

		// not equal
		t.Run("NE"+name, func(t *testing.T) {
			testCompareFunc[T](t, c.MatchNotEqual, src, types.FilterModeNotEqual)
		})

		// less
		t.Run("LT"+name, func(t *testing.T) {
			testCompareFunc[T](t, c.MatchLess, src, types.FilterModeLt)
		})

		// less equal
		t.Run("LE"+name, func(t *testing.T) {
			testCompareFunc[T](t, c.MatchLessEqual, src, types.FilterModeLe)
		})

		// greater
		t.Run("GT"+name, func(t *testing.T) {
			testCompareFunc[T](t, c.MatchGreater, src, types.FilterModeGt)
		})

		// greater equal
		t.Run("GE"+name, func(t *testing.T) {
			testCompareFunc[T](t, c.MatchGreaterEqual, src, types.FilterModeGe)
		})

		// between
		t.Run("RG"+name, func(t *testing.T) {
			testCompareFunc2[T](t, c.MatchBetween, src, types.FilterModeRange)
		})

		// in set
		t.Run("IN"+name, func(t *testing.T) {
			testCompareFunc3[T](t, c.MatchInSet, src, types.FilterModeIn)
		})

		// not in set
		t.Run("NI"+name, func(t *testing.T) {
			testCompareFunc3[T](t, c.MatchNotInSet, src, types.FilterModeNotIn)
		})
	}
}
