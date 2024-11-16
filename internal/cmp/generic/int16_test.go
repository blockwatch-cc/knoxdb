// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt16Equal(t *testing.T) {
	tests.TestCases[int16](t, tests.Int16EqualCases, MatchEqual[int16])
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt16Equal(b *testing.B) {
	tests.BenchCases[int16](b, MatchEqual[int16])
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt16NotEqual(t *testing.T) {
	tests.TestCases[int16](t, tests.Int16NotEqualCases, MatchNotEqual[int16])
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt16NotEqual(b *testing.B) {
	tests.BenchCases[int16](b, MatchNotEqual[int16])
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt16Less(t *testing.T) {
	tests.TestCases[int16](t, tests.Int16LessCases, MatchLess[int16])
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt16Less(b *testing.B) {
	tests.BenchCases[int16](b, MatchLess[int16])
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt16LessEqual(t *testing.T) {
	tests.TestCases[int16](t, tests.Int16LessEqualCases, MatchLessEqual[int16])
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt16LessEqual(b *testing.B) {
	tests.BenchCases[int16](b, MatchLessEqual[int16])
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt16Greater(t *testing.T) {
	tests.TestCases[int16](t, tests.Int16GreaterCases, MatchGreater[int16])
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt16Greater(b *testing.B) {
	tests.BenchCases[int16](b, MatchGreater[int16])
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt16GreaterEqual(t *testing.T) {
	tests.TestCases[int16](t, tests.Int16GreaterEqualCases, MatchGreaterEqual[int16])
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt16GreaterEqual(b *testing.B) {
	tests.BenchCases[int16](b, MatchGreaterEqual[int16])
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt16Between(t *testing.T) {
	tests.TestCases2[int16](t, tests.Int16BetweenCases, MatchBetween[int16])
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt16Between(b *testing.B) {
	tests.BenchCases2[int16](b, MatchBetween[int16])
}
