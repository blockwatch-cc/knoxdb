// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt8Equal(t *testing.T) {
	tests.TestCases[int8](t, tests.Int8EqualCases, MatchEqual[int8])
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt8Equal(b *testing.B) {
	tests.BenchCases[int8](b, MatchEqual[int8])
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt8NotEqual(t *testing.T) {
	tests.TestCases[int8](t, tests.Int8NotEqualCases, MatchNotEqual[int8])
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt8NotEqual(b *testing.B) {
	tests.BenchCases[int8](b, MatchNotEqual[int8])
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt8Less(t *testing.T) {
	tests.TestCases[int8](t, tests.Int8LessCases, MatchLess[int8])
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt8Less(b *testing.B) {
	tests.BenchCases[int8](b, MatchLess[int8])
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt8LessEqual(t *testing.T) {
	tests.TestCases[int8](t, tests.Int8LessEqualCases, MatchLessEqual[int8])
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt8LessEqual(b *testing.B) {
	tests.BenchCases[int8](b, MatchLessEqual[int8])
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt8Greater(t *testing.T) {
	tests.TestCases[int8](t, tests.Int8GreaterCases, MatchGreater[int8])
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt8Greater(b *testing.B) {
	tests.BenchCases[int8](b, MatchGreater[int8])
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt8GreaterEqual(t *testing.T) {
	tests.TestCases[int8](t, tests.Int8GreaterEqualCases, MatchGreaterEqual[int8])
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt8GreaterEqual(b *testing.B) {
	tests.BenchCases[int8](b, MatchGreaterEqual[int8])
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt8Between(t *testing.T) {
	tests.TestCases2[int8](t, tests.Int8BetweenCases, MatchBetween[int8])
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt8Between(b *testing.B) {
	tests.BenchCases2[int8](b, MatchBetween[int8])
}
