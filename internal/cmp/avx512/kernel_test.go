// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx512

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

func TestMatchEqualAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases(t, tests.Uint64EqualCases, MatchUint64Equal)
	tests.TestCases(t, tests.Uint32EqualCases, MatchUint32Equal)
	tests.TestCases(t, tests.Uint16EqualCases, MatchUint16Equal)
	tests.TestCases(t, tests.Uint8EqualCases, MatchUint8Equal)
	tests.TestCases(t, tests.Int64EqualCases, MatchInt64Equal)
	tests.TestCases(t, tests.Int32EqualCases, MatchInt32Equal)
	tests.TestCases(t, tests.Int16EqualCases, MatchInt16Equal)
	tests.TestCases(t, tests.Int8EqualCases, MatchInt8Equal)
	tests.TestCases(t, tests.Float64EqualCases, MatchFloat64Equal)
	tests.TestCases(t, tests.Float32EqualCases, MatchFloat32Equal)
}

func TestMatchNotEqualAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases(t, tests.Uint64NotEqualCases, MatchUint64NotEqual)
	tests.TestCases(t, tests.Uint32NotEqualCases, MatchUint32NotEqual)
	tests.TestCases(t, tests.Uint16NotEqualCases, MatchUint16NotEqual)
	tests.TestCases(t, tests.Uint8NotEqualCases, MatchUint8NotEqual)
	tests.TestCases(t, tests.Int64NotEqualCases, MatchInt64NotEqual)
	tests.TestCases(t, tests.Int32NotEqualCases, MatchInt32NotEqual)
	tests.TestCases(t, tests.Int16NotEqualCases, MatchInt16NotEqual)
	tests.TestCases(t, tests.Int8NotEqualCases, MatchInt8NotEqual)
	tests.TestCases(t, tests.Float64NotEqualCases, MatchFloat64NotEqual)
	tests.TestCases(t, tests.Float32NotEqualCases, MatchFloat32NotEqual)
}

func TestMatchLessAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases(t, tests.Uint64LessCases, MatchUint64Less)
	tests.TestCases(t, tests.Uint32LessCases, MatchUint32Less)
	tests.TestCases(t, tests.Uint16LessCases, MatchUint16Less)
	tests.TestCases(t, tests.Uint8LessCases, MatchUint8Less)
	tests.TestCases(t, tests.Int64LessCases, MatchInt64Less)
	tests.TestCases(t, tests.Int32LessCases, MatchInt32Less)
	tests.TestCases(t, tests.Int16LessCases, MatchInt16Less)
	tests.TestCases(t, tests.Int8LessCases, MatchInt8Less)
	tests.TestCases(t, tests.Float64LessCases, MatchFloat64Less)
	tests.TestCases(t, tests.Float32LessCases, MatchFloat32Less)
}

func TestMatchLessEqualAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases(t, tests.Uint64LessEqualCases, MatchUint64LessEqual)
	tests.TestCases(t, tests.Uint32LessEqualCases, MatchUint32LessEqual)
	tests.TestCases(t, tests.Uint16LessEqualCases, MatchUint16LessEqual)
	tests.TestCases(t, tests.Uint8LessEqualCases, MatchUint8LessEqual)
	tests.TestCases(t, tests.Int64LessEqualCases, MatchInt64LessEqual)
	tests.TestCases(t, tests.Int32LessEqualCases, MatchInt32LessEqual)
	tests.TestCases(t, tests.Int16LessEqualCases, MatchInt16LessEqual)
	tests.TestCases(t, tests.Int8LessEqualCases, MatchInt8LessEqual)
	tests.TestCases(t, tests.Float64LessEqualCases, MatchFloat64LessEqual)
	tests.TestCases(t, tests.Float32LessEqualCases, MatchFloat32LessEqual)
}

func TestMatchGreaterAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases(t, tests.Uint64GreaterCases, MatchUint64Greater)
	tests.TestCases(t, tests.Uint32GreaterCases, MatchUint32Greater)
	tests.TestCases(t, tests.Uint16GreaterCases, MatchUint16Greater)
	tests.TestCases(t, tests.Uint8GreaterCases, MatchUint8Greater)
	tests.TestCases(t, tests.Int64GreaterCases, MatchInt64Greater)
	tests.TestCases(t, tests.Int32GreaterCases, MatchInt32Greater)
	tests.TestCases(t, tests.Int16GreaterCases, MatchInt16Greater)
	tests.TestCases(t, tests.Int8GreaterCases, MatchInt8Greater)
	tests.TestCases(t, tests.Float64GreaterCases, MatchFloat64Greater)
	tests.TestCases(t, tests.Float32GreaterCases, MatchFloat32Greater)
}

func TestMatchGreaterEqualAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases(t, tests.Uint64GreaterEqualCases, MatchUint64GreaterEqual)
	tests.TestCases(t, tests.Uint32GreaterEqualCases, MatchUint32GreaterEqual)
	tests.TestCases(t, tests.Uint16GreaterEqualCases, MatchUint16GreaterEqual)
	tests.TestCases(t, tests.Uint8GreaterEqualCases, MatchUint8GreaterEqual)
	tests.TestCases(t, tests.Int64GreaterEqualCases, MatchInt64GreaterEqual)
	tests.TestCases(t, tests.Int32GreaterEqualCases, MatchInt32GreaterEqual)
	tests.TestCases(t, tests.Int16GreaterEqualCases, MatchInt16GreaterEqual)
	tests.TestCases(t, tests.Int8GreaterEqualCases, MatchInt8GreaterEqual)
	tests.TestCases(t, tests.Float64GreaterEqualCases, MatchFloat64GreaterEqual)
	tests.TestCases(t, tests.Float32GreaterEqualCases, MatchFloat32GreaterEqual)
}

func TestMatchBetweenAVX512(t *testing.T) {
	requireAvx512(t)
	tests.TestCases2(t, tests.Uint64BetweenCases, MatchUint64Between)
	tests.TestCases2(t, tests.Uint32BetweenCases, MatchUint32Between)
	tests.TestCases2(t, tests.Uint16BetweenCases, MatchUint16Between)
	tests.TestCases2(t, tests.Uint8BetweenCases, MatchUint8Between)
	tests.TestCases2(t, tests.Int64BetweenCases, MatchInt64Between)
	tests.TestCases2(t, tests.Int32BetweenCases, MatchInt32Between)
	tests.TestCases2(t, tests.Int16BetweenCases, MatchInt16Between)
	tests.TestCases2(t, tests.Int8BetweenCases, MatchInt8Between)
	tests.TestCases2(t, tests.Float64BetweenCases, MatchFloat64Between)
	tests.TestCases2(t, tests.Float32BetweenCases, MatchFloat32Between)
}

// ---------------------------------------------------
// Benchmarks
//

func BenchmarkMatchEqualAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases(b, MatchUint64Equal)
	tests.BenchCases(b, MatchUint32Equal)
	tests.BenchCases(b, MatchUint16Equal)
	tests.BenchCases(b, MatchUint8Equal)
	tests.BenchCases(b, MatchInt64Equal)
	tests.BenchCases(b, MatchInt32Equal)
	tests.BenchCases(b, MatchInt16Equal)
	tests.BenchCases(b, MatchInt8Equal)
	tests.BenchCases(b, MatchFloat64Equal)
	tests.BenchCases(b, MatchFloat32Equal)
}

func BenchmarkMatchNotEqualAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases(b, MatchUint64NotEqual)
	tests.BenchCases(b, MatchUint32NotEqual)
	tests.BenchCases(b, MatchUint16NotEqual)
	tests.BenchCases(b, MatchUint8NotEqual)
	tests.BenchCases(b, MatchInt64NotEqual)
	tests.BenchCases(b, MatchInt32NotEqual)
	tests.BenchCases(b, MatchInt16NotEqual)
	tests.BenchCases(b, MatchInt8NotEqual)
	tests.BenchCases(b, MatchFloat64NotEqual)
	tests.BenchCases(b, MatchFloat32NotEqual)
}

func BenchmarkMatchLessAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases(b, MatchUint64Less)
	tests.BenchCases(b, MatchUint32Less)
	tests.BenchCases(b, MatchUint16Less)
	tests.BenchCases(b, MatchUint8Less)
	tests.BenchCases(b, MatchInt64Less)
	tests.BenchCases(b, MatchInt32Less)
	tests.BenchCases(b, MatchInt16Less)
	tests.BenchCases(b, MatchInt8Less)
	tests.BenchCases(b, MatchFloat64Less)
	tests.BenchCases(b, MatchFloat32Less)
}

func BenchmarkMatchLessEqualAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases(b, MatchUint64LessEqual)
	tests.BenchCases(b, MatchUint32LessEqual)
	tests.BenchCases(b, MatchUint16LessEqual)
	tests.BenchCases(b, MatchUint8LessEqual)
	tests.BenchCases(b, MatchInt64LessEqual)
	tests.BenchCases(b, MatchInt32LessEqual)
	tests.BenchCases(b, MatchInt16LessEqual)
	tests.BenchCases(b, MatchInt8LessEqual)
	tests.BenchCases(b, MatchFloat64LessEqual)
	tests.BenchCases(b, MatchFloat32LessEqual)
}

func BenchmarkMatchGreaterAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases(b, MatchUint64Greater)
	tests.BenchCases(b, MatchUint32Greater)
	tests.BenchCases(b, MatchUint16Greater)
	tests.BenchCases(b, MatchUint8Greater)
	tests.BenchCases(b, MatchInt64Greater)
	tests.BenchCases(b, MatchInt32Greater)
	tests.BenchCases(b, MatchInt16Greater)
	tests.BenchCases(b, MatchInt8Greater)
	tests.BenchCases(b, MatchFloat64Greater)
	tests.BenchCases(b, MatchFloat32Greater)
}

func BenchmarkMatchGreaterEqualAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases(b, MatchUint64GreaterEqual)
	tests.BenchCases(b, MatchUint32GreaterEqual)
	tests.BenchCases(b, MatchUint16GreaterEqual)
	tests.BenchCases(b, MatchUint8GreaterEqual)
	tests.BenchCases(b, MatchInt64GreaterEqual)
	tests.BenchCases(b, MatchInt32GreaterEqual)
	tests.BenchCases(b, MatchInt16GreaterEqual)
	tests.BenchCases(b, MatchInt8GreaterEqual)
	tests.BenchCases(b, MatchFloat64GreaterEqual)
	tests.BenchCases(b, MatchFloat32GreaterEqual)
}

func BenchmarkMatchBetweenAVX512(b *testing.B) {
	requireAvx512(b)
	tests.BenchCases2(b, MatchUint64Between)
	tests.BenchCases2(b, MatchUint32Between)
	tests.BenchCases2(b, MatchUint16Between)
	tests.BenchCases2(b, MatchUint8Between)
	tests.BenchCases2(b, MatchInt64Between)
	tests.BenchCases2(b, MatchInt32Between)
	tests.BenchCases2(b, MatchInt16Between)
	tests.BenchCases2(b, MatchInt8Between)
	tests.BenchCases2(b, MatchFloat64Between)
	tests.BenchCases2(b, MatchFloat32Between)
}
