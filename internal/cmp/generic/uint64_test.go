// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchUint64Equal(t *testing.T) {
	tests.TestCases[uint64](t, tests.Uint64EqualCases, MatchEqual[uint64])
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchUint64Equal(b *testing.B) {
	tests.BenchCases[uint64](b, MatchEqual[uint64])
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchUint64NotEqual(t *testing.T) {
	tests.TestCases[uint64](t, tests.Uint64NotEqualCases, MatchNotEqual[uint64])
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchUint64NotEqual(b *testing.B) {
	tests.BenchCases[uint64](b, MatchNotEqual[uint64])
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchUint64Less(t *testing.T) {
	tests.TestCases[uint64](t, tests.Uint64LessCases, MatchLess[uint64])
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchUint64Less(b *testing.B) {
	tests.BenchCases[uint64](b, MatchLess[uint64])
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint64LessEqual(t *testing.T) {
	tests.TestCases[uint64](t, tests.Uint64LessEqualCases, MatchLessEqual[uint64])
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchUint64LessEqual(b *testing.B) {
	tests.BenchCases[uint64](b, MatchLessEqual[uint64])
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint64Greater(t *testing.T) {
	tests.TestCases[uint64](t, tests.Uint64GreaterCases, MatchGreater[uint64])
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchUint64Greater(b *testing.B) {
	tests.BenchCases[uint64](b, MatchGreater[uint64])
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint64GreaterEqual(t *testing.T) {
	tests.TestCases[uint64](t, tests.Uint64GreaterEqualCases, MatchGreaterEqual[uint64])
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchUint64GreaterEqual(b *testing.B) {
	tests.BenchCases[uint64](b, MatchGreaterEqual[uint64])
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchUint64Between(t *testing.T) {
	tests.TestCases2[uint64](t, tests.Uint64BetweenCases, MatchBetween[uint64])
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchUint64Between(b *testing.B) {
	tests.BenchCases2[uint64](b, MatchBetween[uint64])
}
