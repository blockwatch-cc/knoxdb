// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -------------------------------------------
// Tests
//

func TestMatchEqual(t *testing.T) {
	tests.TestCases(t, tests.Int64EqualCases, MatchEqual[int64])
	tests.TestCases(t, tests.Int32EqualCases, MatchEqual[int32])
	tests.TestCases(t, tests.Int16EqualCases, MatchEqual[int16])
	tests.TestCases(t, tests.Int8EqualCases, MatchEqual[int8])
	tests.TestCases(t, tests.Uint64EqualCases, MatchEqual[uint64])
	tests.TestCases(t, tests.Uint32EqualCases, MatchEqual[uint32])
	tests.TestCases(t, tests.Uint16EqualCases, MatchEqual[uint16])
	tests.TestCases(t, tests.Uint8EqualCases, MatchEqual[uint8])
	tests.TestCases(t, tests.Float64EqualCases, MatchFloatEqual[float64])
	tests.TestCases(t, tests.Float32EqualCases, MatchFloatEqual[float32])
}

func TestMatchNotEqual(t *testing.T) {
	tests.TestCases(t, tests.Int64NotEqualCases, MatchNotEqual[int64])
	tests.TestCases(t, tests.Int32NotEqualCases, MatchNotEqual[int32])
	tests.TestCases(t, tests.Int16NotEqualCases, MatchNotEqual[int16])
	tests.TestCases(t, tests.Int8NotEqualCases, MatchNotEqual[int8])
	tests.TestCases(t, tests.Uint64NotEqualCases, MatchNotEqual[uint64])
	tests.TestCases(t, tests.Uint32NotEqualCases, MatchNotEqual[uint32])
	tests.TestCases(t, tests.Uint16NotEqualCases, MatchNotEqual[uint16])
	tests.TestCases(t, tests.Uint8NotEqualCases, MatchNotEqual[uint8])
	tests.TestCases(t, tests.Float64NotEqualCases, MatchFloatNotEqual[float64])
	tests.TestCases(t, tests.Float32NotEqualCases, MatchFloatNotEqual[float32])
}

func TestMatchLess(t *testing.T) {
	tests.TestCases(t, tests.Int64LessCases, MatchLess[int64])
	tests.TestCases(t, tests.Int32LessCases, MatchLess[int32])
	tests.TestCases(t, tests.Int16LessCases, MatchLess[int16])
	tests.TestCases(t, tests.Int8LessCases, MatchLess[int8])
	tests.TestCases(t, tests.Uint64LessCases, MatchLess[uint64])
	tests.TestCases(t, tests.Uint32LessCases, MatchLess[uint32])
	tests.TestCases(t, tests.Uint16LessCases, MatchLess[uint16])
	tests.TestCases(t, tests.Uint8LessCases, MatchLess[uint8])
	tests.TestCases(t, tests.Float64LessCases, MatchFloatLess[float64])
	tests.TestCases(t, tests.Float32LessCases, MatchFloatLess[float32])
}

func TestMatchLessEqual(t *testing.T) {
	tests.TestCases(t, tests.Int64LessEqualCases, MatchLessEqual[int64])
	tests.TestCases(t, tests.Int32LessEqualCases, MatchLessEqual[int32])
	tests.TestCases(t, tests.Int16LessEqualCases, MatchLessEqual[int16])
	tests.TestCases(t, tests.Int8LessEqualCases, MatchLessEqual[int8])
	tests.TestCases(t, tests.Uint64LessEqualCases, MatchLessEqual[uint64])
	tests.TestCases(t, tests.Uint32LessEqualCases, MatchLessEqual[uint32])
	tests.TestCases(t, tests.Uint16LessEqualCases, MatchLessEqual[uint16])
	tests.TestCases(t, tests.Uint8LessEqualCases, MatchLessEqual[uint8])
	tests.TestCases(t, tests.Float64LessEqualCases, MatchFloatLessEqual[float64])
	tests.TestCases(t, tests.Float32LessEqualCases, MatchFloatLessEqual[float32])
}

func TestMatchGreater(t *testing.T) {
	tests.TestCases(t, tests.Int64GreaterCases, MatchGreater[int64])
	tests.TestCases(t, tests.Int32GreaterCases, MatchGreater[int32])
	tests.TestCases(t, tests.Int16GreaterCases, MatchGreater[int16])
	tests.TestCases(t, tests.Int8GreaterCases, MatchGreater[int8])
	tests.TestCases(t, tests.Uint64GreaterCases, MatchGreater[uint64])
	tests.TestCases(t, tests.Uint32GreaterCases, MatchGreater[uint32])
	tests.TestCases(t, tests.Uint16GreaterCases, MatchGreater[uint16])
	tests.TestCases(t, tests.Uint8GreaterCases, MatchGreater[uint8])
	tests.TestCases(t, tests.Float64GreaterCases, MatchFloatGreater[float64])
	tests.TestCases(t, tests.Float32GreaterCases, MatchFloatGreater[float32])
}

func TestMatchGreaterEqual(t *testing.T) {
	tests.TestCases(t, tests.Int64GreaterEqualCases, MatchGreaterEqual[int64])
	tests.TestCases(t, tests.Int32GreaterEqualCases, MatchGreaterEqual[int32])
	tests.TestCases(t, tests.Int16GreaterEqualCases, MatchGreaterEqual[int16])
	tests.TestCases(t, tests.Int8GreaterEqualCases, MatchGreaterEqual[int8])
	tests.TestCases(t, tests.Uint64GreaterEqualCases, MatchGreaterEqual[uint64])
	tests.TestCases(t, tests.Uint32GreaterEqualCases, MatchGreaterEqual[uint32])
	tests.TestCases(t, tests.Uint16GreaterEqualCases, MatchGreaterEqual[uint16])
	tests.TestCases(t, tests.Uint8GreaterEqualCases, MatchGreaterEqual[uint8])
	tests.TestCases(t, tests.Float64GreaterEqualCases, MatchFloatGreaterEqual[float64])
	tests.TestCases(t, tests.Float32GreaterEqualCases, MatchFloatGreaterEqual[float32])
}

func TestMatchBetween(t *testing.T) {
	tests.TestCases2(t, tests.Int64BetweenCases, MatchBetweenSigned[int64])
	tests.TestCases2(t, tests.Int32BetweenCases, MatchBetweenSigned[int32])
	tests.TestCases2(t, tests.Int16BetweenCases, MatchBetweenSigned[int16])
	tests.TestCases2(t, tests.Int8BetweenCases, MatchBetweenSigned[int8])
	tests.TestCases2(t, tests.Uint64BetweenCases, MatchBetweenUnsigned[uint64])
	tests.TestCases2(t, tests.Uint32BetweenCases, MatchBetweenUnsigned[uint32])
	tests.TestCases2(t, tests.Uint16BetweenCases, MatchBetweenUnsigned[uint16])
	tests.TestCases2(t, tests.Uint8BetweenCases, MatchBetweenUnsigned[uint8])
	tests.TestCases2(t, tests.Float64BetweenCases, MatchFloatBetween[float64])
	tests.TestCases2(t, tests.Float32BetweenCases, MatchFloatBetween[float32])
}

// -------------------------------------------
// Benchmarks
//

func BenchmarkMatchEqual(b *testing.B) {
	tests.BenchCases(b, MatchEqual[int64])
	tests.BenchCases(b, MatchEqual[int32])
	tests.BenchCases(b, MatchEqual[int16])
	tests.BenchCases(b, MatchEqual[int8])
	tests.BenchCases(b, MatchEqual[uint64])
	tests.BenchCases(b, MatchEqual[uint32])
	tests.BenchCases(b, MatchEqual[uint16])
	tests.BenchCases(b, MatchEqual[uint8])
	tests.BenchCases(b, MatchFloatEqual[float64])
	tests.BenchCases(b, MatchFloatEqual[float32])
}

func BenchmarkMatchNotEqual(b *testing.B) {
	tests.BenchCases(b, MatchNotEqual[int64])
	tests.BenchCases(b, MatchNotEqual[int32])
	tests.BenchCases(b, MatchNotEqual[int16])
	tests.BenchCases(b, MatchNotEqual[int8])
	tests.BenchCases(b, MatchNotEqual[uint64])
	tests.BenchCases(b, MatchNotEqual[uint32])
	tests.BenchCases(b, MatchNotEqual[uint16])
	tests.BenchCases(b, MatchNotEqual[uint8])
	tests.BenchCases(b, MatchFloatNotEqual[float64])
	tests.BenchCases(b, MatchFloatNotEqual[float32])
}

func BenchmarkMatchLess(b *testing.B) {
	tests.BenchCases(b, MatchLess[int64])
	tests.BenchCases(b, MatchLess[int32])
	tests.BenchCases(b, MatchLess[int16])
	tests.BenchCases(b, MatchLess[int8])
	tests.BenchCases(b, MatchLess[uint64])
	tests.BenchCases(b, MatchLess[uint32])
	tests.BenchCases(b, MatchLess[uint16])
	tests.BenchCases(b, MatchLess[uint8])
	tests.BenchCases(b, MatchFloatLess[float64])
	tests.BenchCases(b, MatchFloatLess[float32])
}

func BenchmarkMatchLessEqual(b *testing.B) {
	tests.BenchCases(b, MatchLessEqual[int64])
	tests.BenchCases(b, MatchLessEqual[int32])
	tests.BenchCases(b, MatchLessEqual[int16])
	tests.BenchCases(b, MatchLessEqual[int8])
	tests.BenchCases(b, MatchLessEqual[uint64])
	tests.BenchCases(b, MatchLessEqual[uint32])
	tests.BenchCases(b, MatchLessEqual[uint16])
	tests.BenchCases(b, MatchLessEqual[uint8])
	tests.BenchCases(b, MatchFloatLessEqual[float64])
	tests.BenchCases(b, MatchFloatLessEqual[float32])
}

func BenchmarkMatchGreater(b *testing.B) {
	tests.BenchCases(b, MatchGreater[int64])
	tests.BenchCases(b, MatchGreater[int32])
	tests.BenchCases(b, MatchGreater[int16])
	tests.BenchCases(b, MatchGreater[int8])
	tests.BenchCases(b, MatchGreater[uint64])
	tests.BenchCases(b, MatchGreater[uint32])
	tests.BenchCases(b, MatchGreater[uint16])
	tests.BenchCases(b, MatchGreater[uint8])
	tests.BenchCases(b, MatchFloatGreater[float64])
	tests.BenchCases(b, MatchFloatGreater[float32])
}

func BenchmarkMatchGreaterEqual(b *testing.B) {
	tests.BenchCases(b, MatchGreaterEqual[int64])
	tests.BenchCases(b, MatchGreaterEqual[int32])
	tests.BenchCases(b, MatchGreaterEqual[int16])
	tests.BenchCases(b, MatchGreaterEqual[int8])
	tests.BenchCases(b, MatchGreaterEqual[uint64])
	tests.BenchCases(b, MatchGreaterEqual[uint32])
	tests.BenchCases(b, MatchGreaterEqual[uint16])
	tests.BenchCases(b, MatchGreaterEqual[uint8])
	tests.BenchCases(b, MatchFloatGreaterEqual[float64])
	tests.BenchCases(b, MatchFloatGreaterEqual[float32])
}

func BenchmarkMatchBeetwee(b *testing.B) {
	tests.BenchCases2(b, MatchBetweenSigned[int64])
	tests.BenchCases2(b, MatchBetweenSigned[int32])
	tests.BenchCases2(b, MatchBetweenSigned[int16])
	tests.BenchCases2(b, MatchBetweenSigned[int8])
	tests.BenchCases2(b, MatchBetweenUnsigned[uint64])
	tests.BenchCases2(b, MatchBetweenUnsigned[uint32])
	tests.BenchCases2(b, MatchBetweenUnsigned[uint16])
	tests.BenchCases2(b, MatchBetweenUnsigned[uint8])
	tests.BenchCases2(b, MatchFloatBetween[float64])
	tests.BenchCases2(b, MatchFloatBetween[float32])
}
