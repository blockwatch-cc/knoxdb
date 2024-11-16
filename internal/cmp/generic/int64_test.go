// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt64Equal(t *testing.T) {
	tests.TestCases[int64](t, tests.Int64EqualCases, MatchEqual[int64])
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt64Equal(b *testing.B) {
	tests.BenchCases[int64](b, MatchEqual[int64])
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt64NotEqual(t *testing.T) {
	tests.TestCases[int64](t, tests.Int64NotEqualCases, MatchNotEqual[int64])
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt64NotEqual(b *testing.B) {
	tests.BenchCases[int64](b, MatchNotEqual[int64])
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt64Less(t *testing.T) {
	tests.TestCases[int64](t, tests.Int64LessCases, MatchLess[int64])
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt64Less(b *testing.B) {
	tests.BenchCases[int64](b, MatchLess[int64])
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt64LessEqual(t *testing.T) {
	tests.TestCases[int64](t, tests.Int64LessEqualCases, MatchLessEqual[int64])
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt64LessEqual(b *testing.B) {
	tests.BenchCases[int64](b, MatchLessEqual[int64])
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt64Greater(t *testing.T) {
	tests.TestCases[int64](t, tests.Int64GreaterCases, MatchGreater[int64])
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt64Greater(b *testing.B) {
	tests.BenchCases[int64](b, MatchGreater[int64])
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt64GreaterEqual(t *testing.T) {
	tests.TestCases[int64](t, tests.Int64GreaterEqualCases, MatchGreaterEqual[int64])
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt64GreaterEqual(b *testing.B) {
	tests.BenchCases[int64](b, MatchGreaterEqual[int64])
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt64Between(t *testing.T) {
	tests.TestCases2[int64](t, tests.Int64BetweenCases, MatchBetween[int64])
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt64Between(b *testing.B) {
	tests.BenchCases2[int64](b, MatchBetween[int64])
}
