// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -------------------------------------------
// Tests
//

func TestMatchEqual(t *testing.T) {
	tests.TestCases(t, tests.Int64EqualCases, cmp_eq[int64])
	tests.TestCases(t, tests.Int32EqualCases, cmp_eq[int32])
	tests.TestCases(t, tests.Int16EqualCases, cmp_eq[int16])
	tests.TestCases(t, tests.Int8EqualCases, cmp_eq[int8])
	tests.TestCases(t, tests.Uint64EqualCases, cmp_eq[uint64])
	tests.TestCases(t, tests.Uint32EqualCases, cmp_eq[uint32])
	tests.TestCases(t, tests.Uint16EqualCases, cmp_eq[uint16])
	tests.TestCases(t, tests.Uint8EqualCases, cmp_eq[uint8])
	tests.TestCases(t, tests.Float64EqualCases, cmp_eq_f[float64])
	tests.TestCases(t, tests.Float32EqualCases, cmp_eq_f[float32])
}

func TestMatchNotEqual(t *testing.T) {
	tests.TestCases(t, tests.Int64NotEqualCases, cmp_ne[int64])
	tests.TestCases(t, tests.Int32NotEqualCases, cmp_ne[int32])
	tests.TestCases(t, tests.Int16NotEqualCases, cmp_ne[int16])
	tests.TestCases(t, tests.Int8NotEqualCases, cmp_ne[int8])
	tests.TestCases(t, tests.Uint64NotEqualCases, cmp_ne[uint64])
	tests.TestCases(t, tests.Uint32NotEqualCases, cmp_ne[uint32])
	tests.TestCases(t, tests.Uint16NotEqualCases, cmp_ne[uint16])
	tests.TestCases(t, tests.Uint8NotEqualCases, cmp_ne[uint8])
	tests.TestCases(t, tests.Float64NotEqualCases, cmp_ne_f[float64])
	tests.TestCases(t, tests.Float32NotEqualCases, cmp_ne_f[float32])
}

func TestMatchLess(t *testing.T) {
	tests.TestCases(t, tests.Int64LessCases, cmp_lt[int64])
	tests.TestCases(t, tests.Int32LessCases, cmp_lt[int32])
	tests.TestCases(t, tests.Int16LessCases, cmp_lt[int16])
	tests.TestCases(t, tests.Int8LessCases, cmp_lt[int8])
	tests.TestCases(t, tests.Uint64LessCases, cmp_lt[uint64])
	tests.TestCases(t, tests.Uint32LessCases, cmp_lt[uint32])
	tests.TestCases(t, tests.Uint16LessCases, cmp_lt[uint16])
	tests.TestCases(t, tests.Uint8LessCases, cmp_lt[uint8])
	tests.TestCases(t, tests.Float64LessCases, cmp_lt_f[float64])
	tests.TestCases(t, tests.Float32LessCases, cmp_lt_f[float32])
}

func TestMatchLessEqual(t *testing.T) {
	tests.TestCases(t, tests.Int64LessEqualCases, cmp_le[int64])
	tests.TestCases(t, tests.Int32LessEqualCases, cmp_le[int32])
	tests.TestCases(t, tests.Int16LessEqualCases, cmp_le[int16])
	tests.TestCases(t, tests.Int8LessEqualCases, cmp_le[int8])
	tests.TestCases(t, tests.Uint64LessEqualCases, cmp_le[uint64])
	tests.TestCases(t, tests.Uint32LessEqualCases, cmp_le[uint32])
	tests.TestCases(t, tests.Uint16LessEqualCases, cmp_le[uint16])
	tests.TestCases(t, tests.Uint8LessEqualCases, cmp_le[uint8])
	tests.TestCases(t, tests.Float64LessEqualCases, cmp_le_f[float64])
	tests.TestCases(t, tests.Float32LessEqualCases, cmp_le_f[float32])
}

func TestMatchGreater(t *testing.T) {
	tests.TestCases(t, tests.Int64GreaterCases, cmp_gt[int64])
	tests.TestCases(t, tests.Int32GreaterCases, cmp_gt[int32])
	tests.TestCases(t, tests.Int16GreaterCases, cmp_gt[int16])
	tests.TestCases(t, tests.Int8GreaterCases, cmp_gt[int8])
	tests.TestCases(t, tests.Uint64GreaterCases, cmp_gt[uint64])
	tests.TestCases(t, tests.Uint32GreaterCases, cmp_gt[uint32])
	tests.TestCases(t, tests.Uint16GreaterCases, cmp_gt[uint16])
	tests.TestCases(t, tests.Uint8GreaterCases, cmp_gt[uint8])
	tests.TestCases(t, tests.Float64GreaterCases, cmp_gt_f[float64])
	tests.TestCases(t, tests.Float32GreaterCases, cmp_gt_f[float32])
}

func TestMatchGreaterEqual(t *testing.T) {
	tests.TestCases(t, tests.Int64GreaterEqualCases, cmp_ge[int64])
	tests.TestCases(t, tests.Int32GreaterEqualCases, cmp_ge[int32])
	tests.TestCases(t, tests.Int16GreaterEqualCases, cmp_ge[int16])
	tests.TestCases(t, tests.Int8GreaterEqualCases, cmp_ge[int8])
	tests.TestCases(t, tests.Uint64GreaterEqualCases, cmp_ge[uint64])
	tests.TestCases(t, tests.Uint32GreaterEqualCases, cmp_ge[uint32])
	tests.TestCases(t, tests.Uint16GreaterEqualCases, cmp_ge[uint16])
	tests.TestCases(t, tests.Uint8GreaterEqualCases, cmp_ge[uint8])
	tests.TestCases(t, tests.Float64GreaterEqualCases, cmp_ge_f[float64])
	tests.TestCases(t, tests.Float32GreaterEqualCases, cmp_ge_f[float32])
}

func TestMatchBetween(t *testing.T) {
	tests.TestCases2(t, tests.Int64BetweenCases, cmp_bs[int64])
	tests.TestCases2(t, tests.Int32BetweenCases, cmp_bs[int32])
	tests.TestCases2(t, tests.Int16BetweenCases, cmp_bs[int16])
	tests.TestCases2(t, tests.Int8BetweenCases, cmp_bs[int8])
	tests.TestCases2(t, tests.Uint64BetweenCases, cmp_bu[uint64])
	tests.TestCases2(t, tests.Uint32BetweenCases, cmp_bu[uint32])
	tests.TestCases2(t, tests.Uint16BetweenCases, cmp_bu[uint16])
	tests.TestCases2(t, tests.Uint8BetweenCases, cmp_bu[uint8])
	tests.TestCases2(t, tests.Float64BetweenCases, cmp_bw_f[float64])
	tests.TestCases2(t, tests.Float32BetweenCases, cmp_bw_f[float32])
}

// -------------------------------------------
// Benchmarks
//

func BenchmarkMatchEqual(b *testing.B) {
	tests.BenchCases(b, cmp_eq[int64])
	tests.BenchCases(b, cmp_eq[int32])
	tests.BenchCases(b, cmp_eq[int16])
	tests.BenchCases(b, cmp_eq[int8])
	tests.BenchCases(b, cmp_eq[uint64])
	tests.BenchCases(b, cmp_eq[uint32])
	tests.BenchCases(b, cmp_eq[uint16])
	tests.BenchCases(b, cmp_eq[uint8])
	tests.BenchCases(b, cmp_eq_f[float64])
	tests.BenchCases(b, cmp_eq_f[float32])
}

func BenchmarkMatchNotEqual(b *testing.B) {
	tests.BenchCases(b, cmp_ne[int64])
	tests.BenchCases(b, cmp_ne[int32])
	tests.BenchCases(b, cmp_ne[int16])
	tests.BenchCases(b, cmp_ne[int8])
	tests.BenchCases(b, cmp_ne[uint64])
	tests.BenchCases(b, cmp_ne[uint32])
	tests.BenchCases(b, cmp_ne[uint16])
	tests.BenchCases(b, cmp_ne[uint8])
	tests.BenchCases(b, cmp_ne_f[float64])
	tests.BenchCases(b, cmp_ne_f[float32])
}

func BenchmarkMatchLess(b *testing.B) {
	tests.BenchCases(b, cmp_lt[int64])
	tests.BenchCases(b, cmp_lt[int32])
	tests.BenchCases(b, cmp_lt[int16])
	tests.BenchCases(b, cmp_lt[int8])
	tests.BenchCases(b, cmp_lt[uint64])
	tests.BenchCases(b, cmp_lt[uint32])
	tests.BenchCases(b, cmp_lt[uint16])
	tests.BenchCases(b, cmp_lt[uint8])
	tests.BenchCases(b, cmp_lt_f[float64])
	tests.BenchCases(b, cmp_lt_f[float32])
}

func BenchmarkMatchLessEqual(b *testing.B) {
	tests.BenchCases(b, cmp_le[int64])
	tests.BenchCases(b, cmp_le[int32])
	tests.BenchCases(b, cmp_le[int16])
	tests.BenchCases(b, cmp_le[int8])
	tests.BenchCases(b, cmp_le[uint64])
	tests.BenchCases(b, cmp_le[uint32])
	tests.BenchCases(b, cmp_le[uint16])
	tests.BenchCases(b, cmp_le[uint8])
	tests.BenchCases(b, cmp_le_f[float64])
	tests.BenchCases(b, cmp_le_f[float32])
}

func BenchmarkMatchGreater(b *testing.B) {
	tests.BenchCases(b, cmp_gt[int64])
	tests.BenchCases(b, cmp_gt[int32])
	tests.BenchCases(b, cmp_gt[int16])
	tests.BenchCases(b, cmp_gt[int8])
	tests.BenchCases(b, cmp_gt[uint64])
	tests.BenchCases(b, cmp_gt[uint32])
	tests.BenchCases(b, cmp_gt[uint16])
	tests.BenchCases(b, cmp_gt[uint8])
	tests.BenchCases(b, cmp_gt_f[float64])
	tests.BenchCases(b, cmp_gt_f[float32])
}

func BenchmarkMatchGreaterEqual(b *testing.B) {
	tests.BenchCases(b, cmp_ge[int64])
	tests.BenchCases(b, cmp_ge[int32])
	tests.BenchCases(b, cmp_ge[int16])
	tests.BenchCases(b, cmp_ge[int8])
	tests.BenchCases(b, cmp_ge[uint64])
	tests.BenchCases(b, cmp_ge[uint32])
	tests.BenchCases(b, cmp_ge[uint16])
	tests.BenchCases(b, cmp_ge[uint8])
	tests.BenchCases(b, cmp_ge_f[float64])
	tests.BenchCases(b, cmp_ge_f[float32])
}

func BenchmarkMatchBeetwee(b *testing.B) {
	tests.BenchCases2(b, cmp_bs[int64])
	tests.BenchCases2(b, cmp_bs[int32])
	tests.BenchCases2(b, cmp_bs[int16])
	tests.BenchCases2(b, cmp_bs[int8])
	tests.BenchCases2(b, cmp_bu[uint64])
	tests.BenchCases2(b, cmp_bu[uint32])
	tests.BenchCases2(b, cmp_bu[uint16])
	tests.BenchCases2(b, cmp_bu[uint8])
	tests.BenchCases2(b, cmp_bw_f[float64])
	tests.BenchCases2(b, cmp_bw_f[float32])
}
