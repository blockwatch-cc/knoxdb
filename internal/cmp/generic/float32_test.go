// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchFloat32Equal(t *testing.T) {
	tests.TestCases[float32](t, tests.Float32EqualCases, MatchFloatEqual[float32])
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchFloat32Equal(b *testing.B) {
	tests.BenchCases[float32](b, MatchFloatEqual[float32])
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchFloat32NotEqual(t *testing.T) {
	tests.TestCases[float32](t, tests.Float32NotEqualCases, MatchFloatNotEqual[float32])
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchFloat32NotEqual(b *testing.B) {
	tests.BenchCases[float32](b, MatchFloatNotEqual[float32])
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchFloat32Less(t *testing.T) {
	tests.TestCases[float32](t, tests.Float32LessCases, MatchFloatLess[float32])
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchFloat32Less(b *testing.B) {
	tests.BenchCases[float32](b, MatchFloatLess[float32])
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchFloat32LessEqual(t *testing.T) {
	tests.TestCases[float32](t, tests.Float32LessEqualCases, MatchFloatLessEqual[float32])
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchFloat32LessEqual(b *testing.B) {
	tests.BenchCases[float32](b, MatchFloatLessEqual[float32])
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchFloat32Greater(t *testing.T) {
	tests.TestCases[float32](t, tests.Float32GreaterCases, MatchFloatGreater[float32])
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchFloat32Greater(b *testing.B) {
	tests.BenchCases[float32](b, MatchFloatGreater[float32])
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchFloat32GreaterEqual(t *testing.T) {
	tests.TestCases[float32](t, tests.Float32GreaterEqualCases, MatchFloatGreaterEqual[float32])
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchFloat32GreaterEqual(b *testing.B) {
	tests.BenchCases[float32](b, MatchFloatGreaterEqual[float32])
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchFloat32Between(t *testing.T) {
	tests.TestCases2[float32](t, tests.Float32BetweenCases, MatchFloatBetween[float32])
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchFloat32Between(b *testing.B) {
	tests.BenchCases2[float32](b, MatchFloatBetween[float32])
}
