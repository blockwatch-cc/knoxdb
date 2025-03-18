// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"reflect"
	"strconv"

	"blockwatch.cc/knoxdb/internal/types"
)

type BenchmarkSize struct {
	Name string
	N    int
}

var BenchmarkSizes = []BenchmarkSize{
	{"1k", 1024},
	{"16k", 16 * 1024},
	{"64k", 64 * 1024},
}

type Benchmark struct {
	Name string
	Data []int64
}

var Benchmarks = []Benchmark{
	{"dups_1K", GenDups[int64](1024, 10)}, // 10% unique
	{"dups_16K", GenDups[int64](16*1024, 10)},
	{"dups_64K", GenDups[int64](64*1024, 10)},

	{"runs_1K", GenRuns[int64](1024, 10)}, // run length 10
	{"runs_16K", GenRuns[int64](16*1024, 10)},
	{"runs_64K", GenRuns[int64](64*1024, 10)},

	{"seq_1K", GenSequence[int64](1024)},
	{"seq_16K", GenSequence[int64](16 * 1024)},
	{"seq_64K", GenSequence[int64](64 * 1024)},
}

var (
	constCase = []int{1, 1, 1, 1, 1, 1}
	deltaCase = []int{1, 2, 3, 4, 5, 6} // delta = 1
	runsCase  = []int{1, 1, 2, 2, 3, 3}
	dictCase  = []int{1, 50, 1, 50, 1, 50}
	edgeCase  = []int{1, 2, 2, 2, 2, 2}       // initial delta = 1, then 0
	negCase   = []int{-1, -2, -3, -4, -5, -6} // delta = -1
)

type IntTestCase[T types.Integer] struct {
	Name string
	Data []T
}

func MakeShortIntTests[T types.Integer](scheme int) []IntTestCase[T] {
	switch scheme {
	case 0: // TIntegerConstant:
		return []IntTestCase[T]{MakeIntTest[T]("const", 6, constCase...)}
	case 1: // TIntegerDelta:
		return []IntTestCase[T]{
			MakeIntTest[T]("delta", 6, deltaCase...),
			MakeIntTest[T]("negd", 6, negCase...),
		}
	}
	return []IntTestCase[T]{
		MakeIntTest[T]("const", 6, constCase...),
		MakeIntTest[T]("delta", 6, deltaCase...),
		MakeIntTest[T]("runs", 6, runsCase...),
		MakeIntTest[T]("dict", 6, dictCase...),
		MakeIntTest[T]("edge", 6, edgeCase...),
		MakeIntTest[T]("negd", 6, negCase...),
	}
}

func MakeIntTest[T types.Integer](s string, n int, data ...int) IntTestCase[T] {
	c := IntTestCase[T]{
		Name: s + "_" + reflect.TypeOf(T(0)).String() + "_" + strconv.Itoa(n),
		Data: make([]T, n),
	}
	if len(data) > 0 {
		for i := 0; i < n; i++ {
			c.Data[i] = T(data[i])
		}
	} else {
		c.Data = GenRandom[T](n)
	}
	return c
}

func MakeIntTests[T types.Integer](n int) []IntTestCase[T] {
	name := reflect.TypeOf(T(0)).String() + "_" + strconv.Itoa(n)
	return []IntTestCase[T]{
		{"const_" + name, GenConst[T](n)},
		{"delta_" + name, GenSequence[T](n)},
		{"dups_" + name, GenDups[T](n, n/10)},
		{"runs_" + name, GenRuns[T](n, 5)},
		{"rand_" + name, GenRandom[T](n)},
	}
}
