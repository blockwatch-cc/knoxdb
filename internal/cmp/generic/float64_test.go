// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchFloat64Equal(t *testing.T) {
	tests.TestCases[float64](t, tests.Float64EqualCases, MatchFloatEqual[float64])
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchFloat64Equal(b *testing.B) {
	tests.BenchCases[float64](b, MatchFloatEqual[float64])
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchFloat64NotEqual(t *testing.T) {
	tests.TestCases[float64](t, tests.Float64NotEqualCases, MatchFloatNotEqual[float64])
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchFloat64NotEqual(b *testing.B) {
	tests.BenchCases[float64](b, MatchFloatNotEqual[float64])
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchFloat64Less(t *testing.T) {
	tests.TestCases[float64](t, tests.Float64LessCases, MatchFloatLess[float64])
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchFloat64Less(b *testing.B) {
	tests.BenchCases[float64](b, MatchFloatLess[float64])
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchFloat64LessEqual(t *testing.T) {
	tests.TestCases[float64](t, tests.Float64LessEqualCases, MatchFloatLessEqual[float64])
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchFloat64LessEqual(b *testing.B) {
	tests.BenchCases[float64](b, MatchFloatLessEqual[float64])
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchFloat64Greater(t *testing.T) {
	tests.TestCases[float64](t, tests.Float64GreaterCases, MatchFloatGreater[float64])
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchFloat64Greater(b *testing.B) {
	tests.BenchCases[float64](b, MatchFloatGreater[float64])
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchFloat64GreaterEqual(t *testing.T) {
	tests.TestCases[float64](t, tests.Float64GreaterEqualCases, MatchFloatGreaterEqual[float64])
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchFloat64GreaterEqual(b *testing.B) {
	tests.BenchCases[float64](b, MatchFloatGreaterEqual[float64])
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchFloat64Between(t *testing.T) {
	tests.TestCases2[float64](t, tests.Float64BetweenCases, MatchFloatBetween[float64])
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchFloat64Between(b *testing.B) {
	tests.BenchCases2[float64](b, MatchFloatBetween[float64])
}
