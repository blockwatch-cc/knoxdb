// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchFloat64EqualAVX2(t *testing.T) {
	tests.TestCases[float64](t, tests.Float64EqualCases, MatchFloat64Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchFloat64EqualAVX2(b *testing.B) {
	tests.BenchCases[float64](b, MatchFloat64Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchFloat64NotEqualAVX2(t *testing.T) {
	tests.TestCases[float64](t, tests.Float64NotEqualCases, MatchFloat64NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchFloat64NotEqualAVX2(b *testing.B) {
	tests.BenchCases[float64](b, MatchFloat64NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchFloat64LessAVX2(t *testing.T) {
	tests.TestCases[float64](t, tests.Float64LessCases, MatchFloat64Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchFloat64LessAVX2(b *testing.B) {
	tests.BenchCases[float64](b, MatchFloat64Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchFloat64LessEqualAVX2(t *testing.T) {
	tests.TestCases[float64](t, tests.Float64LessEqualCases, MatchFloat64LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchFloat64LessEqualAVX2(b *testing.B) {
	tests.BenchCases[float64](b, MatchFloat64LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchFloat64GreaterAVX2(t *testing.T) {
	tests.TestCases[float64](t, tests.Float64GreaterCases, MatchFloat64Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchFloat64GreaterAVX2(b *testing.B) {
	tests.BenchCases[float64](b, MatchFloat64Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchFloat64GreaterEqualAVX2(t *testing.T) {
	tests.TestCases[float64](t, tests.Float64GreaterEqualCases, MatchFloat64GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchFloat64GreaterEqualAVX2(b *testing.B) {
	tests.BenchCases[float64](b, MatchFloat64GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchFloat64BetweenAVX2(t *testing.T) {
	tests.TestCases2[float64](t, tests.Float64BetweenCases, MatchFloat64Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchFloat64BetweenAVX2(b *testing.B) {
	tests.BenchCases2[float64](b, MatchFloat64Between)
}
