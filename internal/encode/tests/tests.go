// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"reflect"
	"strconv"

	"blockwatch.cc/knoxdb/internal/types"
)

var (
	constCase  = []int{1, 1, 1, 1, 1, 1}
	deltaCase  = []int{1, 2, 3, 4, 5, 6} // delta = 1
	runsCase   = []int{1, 1, 2, 2, 3, 3}
	dictCase   = []int{1, 50, 1, 50, 1, 50}
	edgeCase   = []int{1, 2, 2, 2, 2, 2}       // initial delta = 1, then 0
	negCase    = []int{-1, -2, -3, -4, -5, -6} // delta = -1
	sixtySeven = []int{                        // 640 equal values
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
	}

	// float
	floatConstCase = []float64{1.25, 1.25, 1.25, 1.25, 1.25, 1.25}
	floatRunsCase  = []float64{1.5, 1.5, 2.35, 2.35, 3.60, 3.60}
	floatDictCase  = []float64{1.50, 50.45, 1.50, 50.45, 1.50, 50.45}
)

type TestCase[T types.Number] struct {
	Name string
	Data []T
}

func MakeShortIntTests[T types.Integer](scheme int) []TestCase[T] {
	switch scheme {
	case 0: // TIntegerConstant:
		return []TestCase[T]{MakeIntTest[T]("const", 6, constCase...)}
	case 1: // TIntegerDelta:
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
		Name: s + "_" + reflect.TypeOf(T(0)).String() + "_" + strconv.Itoa(n),
		Data: make([]T, n),
	}
	if len(data) > 0 {
		for i := 0; i < n; i++ {
			c.Data[i] = T(data[i])
		}
	} else {
		c.Data = GenRnd[T](n)
	}
	return c
}

func MakeIntTests[T types.Integer](n int) []TestCase[T] {
	name := reflect.TypeOf(T(0)).String() + "_" + strconv.Itoa(n)
	return []TestCase[T]{
		{"const_" + name, GenConstInt[T](n, 42)},
		{"delta_" + name, GenSeq[T](n)},
		{"dups_" + name, GenDups[T](n, n/10)},
		{"runs_" + name, GenRuns[T](n, 5)},
		{"rand_" + name, GenRnd[T](n)},
	}
}

func MakeShortFloatTests[T types.Float](scheme int) []TestCase[T] {
	switch scheme {
	case 0: // TFloatConstant:
		return []TestCase[T]{MakeFloatTest[T]("const", 6, floatConstCase...)}
	}
	return []TestCase[T]{
		MakeFloatTest[T]("const", 6, floatConstCase...),
		MakeFloatTest[T]("runs", 6, floatRunsCase...),
		MakeFloatTest[T]("dict", 6, floatDictCase...),
		// MakeFloatTest[T]("alp", 6, alpCase...),
		// MakeFloatTest[T]("alprd", 6, alpRdCase...),
	}
}

func MakeFloatTest[T types.Float](s string, n int, data ...float64) TestCase[T] {
	c := TestCase[T]{
		Name: s + "_" + reflect.TypeOf(T(0)).String() + "_" + strconv.Itoa(n),
		Data: make([]T, n),
	}
	if len(data) > 0 {
		for i := 0; i < n; i++ {
			c.Data[i] = T(data[i])
		}
	} else {
		c.Data = GenRnd[T](n)
	}
	return c
}

func MakeFloatTests[T types.Float](n int) []TestCase[T] {
	name := reflect.TypeOf(T(0)).String() + "_" + strconv.Itoa(n)
	return []TestCase[T]{
		{"const_" + name, GenConstFloat[T](n)},
		{"dups_" + name, GenDups[T](n, n/10)},
		{"runs_" + name, GenRuns[T](n, 5)},
		{"rand_" + name, GenRnd[T](n)},
	}
}
