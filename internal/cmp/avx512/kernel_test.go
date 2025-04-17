// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64
// +build amd64

package avx512

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

func requireAvx512(t testing.TB) {
	if !util.UseAVX512_F {
		t.Skip("AVX512F not available.")
	}
	if !util.UseAVX512_BW {
		t.Skip("AVX512BW not available.")
	}
}

func TestMatchEqualAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases(t, tests.Uint64EqualCases, Uint64Equal)
	tests.TestCases(t, tests.Uint32EqualCases, Uint32Equal)
	tests.TestCases(t, tests.Uint16EqualCases, Uint16Equal)
	tests.TestCases(t, tests.Uint8EqualCases, Uint8Equal)
	tests.TestCases(t, tests.Int64EqualCases, Int64Equal)
	tests.TestCases(t, tests.Int32EqualCases, Int32Equal)
	tests.TestCases(t, tests.Int16EqualCases, Int16Equal)
	tests.TestCases(t, tests.Int8EqualCases, Int8Equal)
	tests.TestCases(t, tests.Float64EqualCases, Float64Equal)
	tests.TestCases(t, tests.Float32EqualCases, Float32Equal)
}

func TestMatchNotEqualAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases(t, tests.Uint64NotEqualCases, Uint64NotEqual)
	tests.TestCases(t, tests.Uint32NotEqualCases, Uint32NotEqual)
	tests.TestCases(t, tests.Uint16NotEqualCases, Uint16NotEqual)
	tests.TestCases(t, tests.Uint8NotEqualCases, Uint8NotEqual)
	tests.TestCases(t, tests.Int64NotEqualCases, Int64NotEqual)
	tests.TestCases(t, tests.Int32NotEqualCases, Int32NotEqual)
	tests.TestCases(t, tests.Int16NotEqualCases, Int16NotEqual)
	tests.TestCases(t, tests.Int8NotEqualCases, Int8NotEqual)
	tests.TestCases(t, tests.Float64NotEqualCases, Float64NotEqual)
	tests.TestCases(t, tests.Float32NotEqualCases, Float32NotEqual)
}

func TestMatchLessAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases(t, tests.Uint64LessCases, Uint64Less)
	tests.TestCases(t, tests.Uint32LessCases, Uint32Less)
	tests.TestCases(t, tests.Uint16LessCases, Uint16Less)
	tests.TestCases(t, tests.Uint8LessCases, Uint8Less)
	tests.TestCases(t, tests.Int64LessCases, Int64Less)
	tests.TestCases(t, tests.Int32LessCases, Int32Less)
	tests.TestCases(t, tests.Int16LessCases, Int16Less)
	tests.TestCases(t, tests.Int8LessCases, Int8Less)
	tests.TestCases(t, tests.Float64LessCases, Float64Less)
	tests.TestCases(t, tests.Float32LessCases, Float32Less)
}

func TestMatchLessEqualAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases(t, tests.Uint64LessEqualCases, Uint64LessEqual)
	tests.TestCases(t, tests.Uint32LessEqualCases, Uint32LessEqual)
	tests.TestCases(t, tests.Uint16LessEqualCases, Uint16LessEqual)
	tests.TestCases(t, tests.Uint8LessEqualCases, Uint8LessEqual)
	tests.TestCases(t, tests.Int64LessEqualCases, Int64LessEqual)
	tests.TestCases(t, tests.Int32LessEqualCases, Int32LessEqual)
	tests.TestCases(t, tests.Int16LessEqualCases, Int16LessEqual)
	tests.TestCases(t, tests.Int8LessEqualCases, Int8LessEqual)
	tests.TestCases(t, tests.Float64LessEqualCases, Float64LessEqual)
	tests.TestCases(t, tests.Float32LessEqualCases, Float32LessEqual)
}

func TestMatchGreaterAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases(t, tests.Uint64GreaterCases, Uint64Greater)
	tests.TestCases(t, tests.Uint32GreaterCases, Uint32Greater)
	tests.TestCases(t, tests.Uint16GreaterCases, Uint16Greater)
	tests.TestCases(t, tests.Uint8GreaterCases, Uint8Greater)
	tests.TestCases(t, tests.Int64GreaterCases, Int64Greater)
	tests.TestCases(t, tests.Int32GreaterCases, Int32Greater)
	tests.TestCases(t, tests.Int16GreaterCases, Int16Greater)
	tests.TestCases(t, tests.Int8GreaterCases, Int8Greater)
	tests.TestCases(t, tests.Float64GreaterCases, Float64Greater)
	tests.TestCases(t, tests.Float32GreaterCases, Float32Greater)
}

func TestMatchGreaterEqualAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases(t, tests.Uint64GreaterEqualCases, Uint64GreaterEqual)
	tests.TestCases(t, tests.Uint32GreaterEqualCases, Uint32GreaterEqual)
	tests.TestCases(t, tests.Uint16GreaterEqualCases, Uint16GreaterEqual)
	tests.TestCases(t, tests.Uint8GreaterEqualCases, Uint8GreaterEqual)
	tests.TestCases(t, tests.Int64GreaterEqualCases, Int64GreaterEqual)
	tests.TestCases(t, tests.Int32GreaterEqualCases, Int32GreaterEqual)
	tests.TestCases(t, tests.Int16GreaterEqualCases, Int16GreaterEqual)
	tests.TestCases(t, tests.Int8GreaterEqualCases, Int8GreaterEqual)
	tests.TestCases(t, tests.Float64GreaterEqualCases, Float64GreaterEqual)
	tests.TestCases(t, tests.Float32GreaterEqualCases, Float32GreaterEqual)
}

func TestMatchBetweenAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases2(t, tests.Uint64BetweenCases, Uint64Between)
	tests.TestCases2(t, tests.Uint32BetweenCases, Uint32Between)
	tests.TestCases2(t, tests.Uint16BetweenCases, Uint16Between)
	tests.TestCases2(t, tests.Uint8BetweenCases, Uint8Between)
	tests.TestCases2(t, tests.Int64BetweenCases, Int64Between)
	tests.TestCases2(t, tests.Int32BetweenCases, Int32Between)
	tests.TestCases2(t, tests.Int16BetweenCases, Int16Between)
	tests.TestCases2(t, tests.Int8BetweenCases, Int8Between)
	tests.TestCases2(t, tests.Float64BetweenCases, Float64Between)
	tests.TestCases2(t, tests.Float32BetweenCases, Float32Between)
}

// ---------------------------------------------------
// Benchmarks
//

func BenchmarkMatchEqualAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases(b, Uint64Equal)
	tests.BenchCases(b, Uint32Equal)
	tests.BenchCases(b, Uint16Equal)
	tests.BenchCases(b, Uint8Equal)
	tests.BenchCases(b, Int64Equal)
	tests.BenchCases(b, Int32Equal)
	tests.BenchCases(b, Int16Equal)
	tests.BenchCases(b, Int8Equal)
	tests.BenchCases(b, Float64Equal)
	tests.BenchCases(b, Float32Equal)
}

func BenchmarkMatchNotEqualAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases(b, Uint64NotEqual)
	tests.BenchCases(b, Uint32NotEqual)
	tests.BenchCases(b, Uint16NotEqual)
	tests.BenchCases(b, Uint8NotEqual)
	tests.BenchCases(b, Int64NotEqual)
	tests.BenchCases(b, Int32NotEqual)
	tests.BenchCases(b, Int16NotEqual)
	tests.BenchCases(b, Int8NotEqual)
	tests.BenchCases(b, Float64NotEqual)
	tests.BenchCases(b, Float32NotEqual)
}

func BenchmarkMatchLessAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases(b, Uint64Less)
	tests.BenchCases(b, Uint32Less)
	tests.BenchCases(b, Uint16Less)
	tests.BenchCases(b, Uint8Less)
	tests.BenchCases(b, Int64Less)
	tests.BenchCases(b, Int32Less)
	tests.BenchCases(b, Int16Less)
	tests.BenchCases(b, Int8Less)
	tests.BenchCases(b, Float64Less)
	tests.BenchCases(b, Float32Less)
}

func BenchmarkMatchLessEqualAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases(b, Uint64LessEqual)
	tests.BenchCases(b, Uint32LessEqual)
	tests.BenchCases(b, Uint16LessEqual)
	tests.BenchCases(b, Uint8LessEqual)
	tests.BenchCases(b, Int64LessEqual)
	tests.BenchCases(b, Int32LessEqual)
	tests.BenchCases(b, Int16LessEqual)
	tests.BenchCases(b, Int8LessEqual)
	tests.BenchCases(b, Float64LessEqual)
	tests.BenchCases(b, Float32LessEqual)
}

func BenchmarkMatchGreaterAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases(b, Uint64Greater)
	tests.BenchCases(b, Uint32Greater)
	tests.BenchCases(b, Uint16Greater)
	tests.BenchCases(b, Uint8Greater)
	tests.BenchCases(b, Int64Greater)
	tests.BenchCases(b, Int32Greater)
	tests.BenchCases(b, Int16Greater)
	tests.BenchCases(b, Int8Greater)
	tests.BenchCases(b, Float64Greater)
	tests.BenchCases(b, Float32Greater)
}

func BenchmarkMatchGreaterEqualAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases(b, Uint64GreaterEqual)
	tests.BenchCases(b, Uint32GreaterEqual)
	tests.BenchCases(b, Uint16GreaterEqual)
	tests.BenchCases(b, Uint8GreaterEqual)
	tests.BenchCases(b, Int64GreaterEqual)
	tests.BenchCases(b, Int32GreaterEqual)
	tests.BenchCases(b, Int16GreaterEqual)
	tests.BenchCases(b, Int8GreaterEqual)
	tests.BenchCases(b, Float64GreaterEqual)
	tests.BenchCases(b, Float32GreaterEqual)
}

func BenchmarkMatchBetweenAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases2(b, Uint64Between)
	tests.BenchCases2(b, Uint32Between)
	tests.BenchCases2(b, Uint16Between)
	tests.BenchCases2(b, Uint8Between)
	tests.BenchCases2(b, Int64Between)
	tests.BenchCases2(b, Int32Between)
	tests.BenchCases2(b, Int16Between)
	tests.BenchCases2(b, Int8Between)
	tests.BenchCases2(b, Float64Between)
	tests.BenchCases2(b, Float32Between)
}
