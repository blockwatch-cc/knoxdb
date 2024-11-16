// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt32Equal(t *testing.T) {
	tests.TestCases[int32](t, tests.Int32EqualCases, MatchEqual[int32])
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt32Equal(b *testing.B) {
	tests.BenchCases[int32](b, MatchEqual[int32])
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt32NotEqual(t *testing.T) {
	tests.TestCases[int32](t, tests.Int32NotEqualCases, MatchNotEqual[int32])
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt32NotEqual(b *testing.B) {
	tests.BenchCases[int32](b, MatchNotEqual[int32])
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt32Less(t *testing.T) {
	tests.TestCases[int32](t, tests.Int32LessCases, MatchLess[int32])
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt32Less(b *testing.B) {
	tests.BenchCases[int32](b, MatchLess[int32])
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt32LessEqual(t *testing.T) {
	tests.TestCases[int32](t, tests.Int32LessEqualCases, MatchLessEqual[int32])
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt32LessEqual(b *testing.B) {
	tests.BenchCases[int32](b, MatchLessEqual[int32])
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt32Greater(t *testing.T) {
	tests.TestCases[int32](t, tests.Int32GreaterCases, MatchGreater[int32])
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt32Greater(b *testing.B) {
	tests.BenchCases[int32](b, MatchGreater[int32])
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt32GreaterEqual(t *testing.T) {
	tests.TestCases[int32](t, tests.Int32GreaterEqualCases, MatchGreaterEqual[int32])
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt32GreaterEqual(b *testing.B) {
	tests.BenchCases[int32](b, MatchGreaterEqual[int32])
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt32Between(t *testing.T) {
	tests.TestCases2[int32](t, tests.Int32BetweenCases, MatchBetween[int32])
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt32Between(b *testing.B) {
	tests.BenchCases2[int32](b, MatchBetween[int32])
}
