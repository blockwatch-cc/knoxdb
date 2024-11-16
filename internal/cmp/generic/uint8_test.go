// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchUint8Equal(t *testing.T) {
	tests.TestCases[uint8](t, tests.Uint8EqualCases, MatchEqual[uint8])
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchUint8Equal(b *testing.B) {
	tests.BenchCases[uint8](b, MatchEqual[uint8])
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchUint8NotEqual(t *testing.T) {
	tests.TestCases[uint8](t, tests.Uint8NotEqualCases, MatchNotEqual[uint8])
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchUint8NotEqual(b *testing.B) {
	tests.BenchCases[uint8](b, MatchNotEqual[uint8])
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchUint8Less(t *testing.T) {
	tests.TestCases[uint8](t, tests.Uint8LessCases, MatchLess[uint8])
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchUint8Less(b *testing.B) {
	tests.BenchCases[uint8](b, MatchLess[uint8])
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint8LessEqual(t *testing.T) {
	tests.TestCases[uint8](t, tests.Uint8LessEqualCases, MatchLessEqual[uint8])
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchUint8LessEqual(b *testing.B) {
	tests.BenchCases[uint8](b, MatchLessEqual[uint8])
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint8Greater(t *testing.T) {
	tests.TestCases[uint8](t, tests.Uint8GreaterCases, MatchGreater[uint8])
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchUint8Greater(b *testing.B) {
	tests.BenchCases[uint8](b, MatchGreater[uint8])
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint8GreaterEqual(t *testing.T) {
	tests.TestCases[uint8](t, tests.Uint8GreaterEqualCases, MatchGreaterEqual[uint8])
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchUint8GreaterEqual(b *testing.B) {
	tests.BenchCases[uint8](b, MatchGreaterEqual[uint8])
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchUint8Between(t *testing.T) {
	tests.TestCases2[uint8](t, tests.Uint8BetweenCases, MatchBetween[uint8])
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchUint8Between(b *testing.B) {
	tests.BenchCases2[uint8](b, MatchBetween[uint8])
}
