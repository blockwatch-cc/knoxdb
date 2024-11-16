// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx512

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt64EqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[int64](t, tests.Int64EqualCases, MatchInt64Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt64EqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[int64](b, MatchInt64Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt64NotEqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[int64](t, tests.Int64NotEqualCases, MatchInt64NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt64NotEqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[int64](b, MatchInt64NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt64LessAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[int64](t, tests.Int64LessCases, MatchInt64Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt64LessAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[int64](b, MatchInt64Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt64LessEqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[int64](t, tests.Int64LessEqualCases, MatchInt64LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt64LessEqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[int64](b, MatchInt64LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt64GreaterAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[int64](t, tests.Int64GreaterCases, MatchInt64Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt64GreaterAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[int64](b, MatchInt64Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt64GreaterEqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[int64](t, tests.Int64GreaterEqualCases, MatchInt64GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt64GreaterEqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[int64](b, MatchInt64GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt64BetweenAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases2[int64](t, tests.Int64BetweenCases, MatchInt64Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt64BetweenAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases2[int64](b, MatchInt64Between)
}
