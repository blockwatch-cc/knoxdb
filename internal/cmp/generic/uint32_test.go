// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchUint32Equal(t *testing.T) {
	tests.TestCases[uint32](t, tests.Uint32EqualCases, MatchEqual[uint32])
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchUint32Equal(b *testing.B) {
	tests.BenchCases[uint32](b, MatchEqual[uint32])
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchUint32NotEqual(t *testing.T) {
	tests.TestCases[uint32](t, tests.Uint32NotEqualCases, MatchNotEqual[uint32])
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchUint32NotEqual(b *testing.B) {
	tests.BenchCases[uint32](b, MatchNotEqual[uint32])
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchUint32Less(t *testing.T) {
	tests.TestCases[uint32](t, tests.Uint32LessCases, MatchLess[uint32])
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchUint32Less(b *testing.B) {
	tests.BenchCases[uint32](b, MatchLess[uint32])
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint32LessEqual(t *testing.T) {
	tests.TestCases[uint32](t, tests.Uint32LessEqualCases, MatchLessEqual[uint32])
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchUint32LessEqual(b *testing.B) {
	tests.BenchCases[uint32](b, MatchLessEqual[uint32])
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint32Greater(t *testing.T) {
	tests.TestCases[uint32](t, tests.Uint32GreaterCases, MatchGreater[uint32])
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchUint32Greater(b *testing.B) {
	tests.BenchCases[uint32](b, MatchGreater[uint32])
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint32GreaterEqual(t *testing.T) {
	tests.TestCases[uint32](t, tests.Uint32GreaterEqualCases, MatchGreaterEqual[uint32])
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchUint32GreaterEqual(b *testing.B) {
	tests.BenchCases[uint32](b, MatchGreaterEqual[uint32])
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchUint32Between(t *testing.T) {
	tests.TestCases2[uint32](t, tests.Uint32BetweenCases, MatchBetween[uint32])
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchUint32Between(b *testing.B) {
	tests.BenchCases2[uint32](b, MatchBetween[uint32])
}
