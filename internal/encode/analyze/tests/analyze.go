// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/assert"
)

type AnalyzeFunc[T types.Integer] func([]T) (T, T, T, int)

type TestCase[T types.Integer] struct {
	Name     string
	Input    []T
	ExpMin   T
	ExpMax   T
	ExpDelta T
	ExpRuns  int
}

func MakeSignedTests[T types.Signed]() []TestCase[T] {
	return []TestCase[T]{
		{"Empty", []T{}, 0, 0, 0, 0},
		{"Single", []T{42}, 42, 42, 0, 1},
		{"Double", []T{5, 10}, 5, 10, 5, 2},
		{"Duplicate", []T{7, 7}, 7, 7, 0, 1},
		{"Four", []T{1, 2, 3, 4}, 1, 4, 1, 4},
		{"Five", []T{1, 1, 2, 2, 3}, 1, 3, 0, 3},
		{"Zeros", []T{0, 0, 0}, 0, 0, 0, 1},
		{"DeltaNoDups", []T{-1, 0, 1, 2}, -1, 2, 1, 4},
		{"Runs", []T{-1, -1, 5, 5, 1, 1}, -1, 5, 0, 3},
		{"Runs64", []T{
			1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4,
		}, 1, 4, 0, 4},
		{"Runs32", []T{
			1, 1, 1, 1, 1, 1, 1, 1,
			2, 2, 2, 2, 2, 2, 2, 2,
			3, 3, 3, 3, 3, 3, 3, 3,
			4, 4, 4, 4, 4, 4, 4, 4,
		}, 1, 4, 0, 4},
		{"Runs16", []T{
			1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
			4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
		}, 1, 4, 0, 4},
		{"Runs8", []T{
			1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
			4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
		}, 1, 4, 0, 4},
		{"DictFriendly", []T{-1, 1, 5, 1, -1, 1}, -1, 5, 0, 6},
		{"AllSame", []T{5, 5, 5, 5, 5}, 5, 5, 0, 1},
		{"Alternating", []T{1, 0, 1, 0, 1}, 0, 1, 0, 5},
		{"LargeDelta", []T{10, 20, 30, 40, 50}, 10, 50, 10, 5},
		{"Bounds", []T{
			types.MinVal[T](), 0, types.MaxVal[T](),
		},
			types.MinVal[T](), types.MaxVal[T](), 0, 3},
		{"Short", []T{1, 2, 3}, 1, 3, 1, 3},
		{"MixedRuns", []T{1, 1, 2, 2, 5, 8, 8}, 1, 8, 0, 4},
		{"Unaligned", []T{1, 2, 3, 4, 5, 6, 7}, 1, 7, 1, 7},
		{"Random", []T{3, 1, 4, 1, 5, 9, 2, 6, 5, 3}, 1, 9, 0, 10},
		{"NegDelta", []T{
			32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17,
			16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0,
		}, 0, 32, -1, 33},
		{"LongDelta", []T{
			-32, -31, -30, -29, -28, -27, -26, -25, -24, -23, -22, -21, -20, -19, -18, -17,
			-16, -15, -14, -13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1,
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
			16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
		}, -32, 32, 1, 65},
		{"LongRuns", []T{
			1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
			4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
		}, 1, 4, 0, 4},
		{"LastNoDelta", []T{
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
			17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 33,
		}, 0, 33, 0, 33},
		// 32 elements, exactly one vector, no boundary crossing
		{
			Name:     "SingleVector",
			Input:    append(tests.GenConst(16, T(1)), tests.GenConst(16, T(2))...),
			ExpMin:   1,
			ExpMax:   2,
			ExpDelta: 0,
			ExpRuns:  2,
		},
		// 33 elements, crosses boundary, transition at 32
		{
			Name:     "BoundaryTransition",
			Input:    append(tests.GenConst(32, T(1)), T(2)),
			ExpMin:   1,
			ExpMax:   2,
			ExpDelta: 0,
			ExpRuns:  2,
		},
		// 64 elements, two vectors, transition at 32
		{
			Name:     "TwoVectorsTransition",
			Input:    append(tests.GenConst(32, T(1)), tests.GenConst(32, T(2))...),
			ExpMin:   1,
			ExpMax:   2,
			ExpDelta: 0,
			ExpRuns:  2,
		},
		// 63 elements, transition just before boundary
		{
			Name:     "PreBoundaryTransition",
			Input:    append(tests.GenConst(31, T(1)), append([]T{T(2)}, tests.GenConst(31, T(2))...)...),
			ExpMin:   1,
			ExpMax:   2,
			ExpDelta: 0,
			ExpRuns:  2,
		},
		// Delta across boundary
		{
			Name:     "DeltaAcrossBoundary",
			Input:    tests.GenRange(T(0), T(65)),
			ExpMin:   0,
			ExpMax:   64,
			ExpDelta: 1,
			ExpRuns:  65,
		},
	}
}

func MakeUnsignedTests[T types.Unsigned]() []TestCase[T] {
	return []TestCase[T]{
		{"Empty", []T{}, 0, 0, 0, 0},
		{"Single", []T{42}, 42, 42, 0, 1},
		{"Double", []T{5, 10}, 5, 10, 5, 2},
		{"Duplicate", []T{7, 7}, 7, 7, 0, 1},
		{"Four", []T{1, 2, 3, 4}, 1, 4, 1, 4},
		{"Five", []T{1, 1, 2, 2, 3}, 1, 3, 0, 3},
		{"Zeros", []T{0, 0, 0}, 0, 0, 0, 1},
		{"DeltaNoDups", []T{0, 1, 2, 3}, 0, 3, 1, 4},
		{"Runs", []T{0, 0, 5, 5, 1, 1}, 0, 5, 0, 3},
		{"Runs64", []T{
			1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4,
		}, 1, 4, 0, 4},
		{"Runs32", []T{
			1, 1, 1, 1, 1, 1, 1, 1,
			2, 2, 2, 2, 2, 2, 2, 2,
			3, 3, 3, 3, 3, 3, 3, 3,
			4, 4, 4, 4, 4, 4, 4, 4,
		}, 1, 4, 0, 4},
		{"Runs16", []T{
			1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
			4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
		}, 1, 4, 0, 4},
		{"Runs8", []T{
			1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
			4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
		}, 1, 4, 0, 4},
		{"DictFriendly", []T{0, 1, 5, 1, 0, 1}, 0, 5, 0, 6},
		{"AllSame", []T{5, 5, 5, 5, 5}, 5, 5, 0, 1},
		{"Alternating", []T{1, 0, 1, 0, 1}, 0, 1, 0, 5},
		{"LargeDelta", []T{10, 20, 30, 40, 50}, 10, 50, 10, 5},
		{"Bounds", []T{
			types.MinVal[T](), 0, types.MaxVal[T](),
		}, types.MinVal[T](), types.MaxVal[T](), 0, 2},
		{"Short", []T{1, 2, 3}, 1, 3, 1, 3},
		{"MixedRuns", []T{1, 1, 2, 2, 5, 8, 8}, 1, 8, 0, 4},
		{"Unaligned", []T{1, 2, 3, 4, 5, 6, 7}, 1, 7, 1, 7},
		{"Random", []T{3, 1, 4, 1, 5, 9, 2, 6, 5, 3}, 1, 9, 0, 10},
		{"NegDelta", []T{
			32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17,
			16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0,
		}, 0, 32, types.MaxVal[T](), 33},
		{"LongDelta", []T{
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
			17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
			33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
			49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
		}, 0, 64, 1, 65},
		{"LongRuns", []T{
			1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
			4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
		}, 1, 4, 0, 4},
		{"LastNoDelta", []T{
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
			17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 33,
		}, 0, 33, 0, 33},
		// 32 elements, exactly one vector, no boundary crossing
		{
			Name:     "SingleVector",
			Input:    append(tests.GenConst(16, T(1)), tests.GenConst(16, T(2))...),
			ExpMin:   1,
			ExpMax:   2,
			ExpDelta: 0,
			ExpRuns:  2,
		},
		// 33 elements, crosses boundary, transition at 32
		{
			Name:     "BoundaryTransition",
			Input:    append(tests.GenConst(32, T(1)), T(2)),
			ExpMin:   1,
			ExpMax:   2,
			ExpDelta: 0,
			ExpRuns:  2,
		},
		// 64 elements, two vectors, transition at 32
		{
			Name:     "TwoVectorsTransition",
			Input:    append(tests.GenConst(32, T(1)), tests.GenConst(32, T(2))...),
			ExpMin:   1,
			ExpMax:   2,
			ExpDelta: 0,
			ExpRuns:  2,
		},
		// 63 elements, transition just before boundary
		{
			Name:     "PreBoundaryTransition",
			Input:    append(tests.GenConst(31, T(1)), append([]T{T(2)}, tests.GenConst(31, T(2))...)...),
			ExpMin:   1,
			ExpMax:   2,
			ExpDelta: 0,
			ExpRuns:  2,
		},
		// Delta across boundary
		{
			Name:     "DeltaAcrossBoundary",
			Input:    tests.GenRange(T(0), T(65)),
			ExpMin:   0,
			ExpMax:   64,
			ExpDelta: 1,
			ExpRuns:  65,
		},
	}
}

func AnalyzeTest[T types.Integer](t *testing.T, cases []TestCase[T], fn AnalyzeFunc[T]) {
	for _, tt := range cases {
		t.Run(tt.Name, func(t *testing.T) {
			minv, maxv, delta, numRuns := fn(tt.Input)
			assert.Equal(t, tt.ExpMin, minv, "min")
			assert.Equal(t, tt.ExpMax, maxv, "max")
			assert.Equal(t, tt.ExpDelta, delta, "delta")
			assert.Equal(t, tt.ExpRuns, numRuns, "num_runs")
		})
	}
}
