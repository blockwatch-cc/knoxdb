// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchUint16Equal(t *testing.T) {
	tests.TestCases[uint16](t, tests.Uint16EqualCases, MatchEqual[uint16])
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchUint16Equal(b *testing.B) {
	tests.BenchCases[uint16](b, MatchEqual[uint16])
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchUint16NotEqual(t *testing.T) {
	tests.TestCases[uint16](t, tests.Uint16NotEqualCases, MatchNotEqual[uint16])
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchUint16NotEqual(b *testing.B) {
	tests.BenchCases[uint16](b, MatchNotEqual[uint16])
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchUint16Less(t *testing.T) {
	tests.TestCases[uint16](t, tests.Uint16LessCases, MatchLess[uint16])
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchUint16Less(b *testing.B) {
	tests.BenchCases[uint16](b, MatchLess[uint16])
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint16LessEqual(t *testing.T) {
	tests.TestCases[uint16](t, tests.Uint16LessEqualCases, MatchLessEqual[uint16])
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchUint16LessEqual(b *testing.B) {
	tests.BenchCases[uint16](b, MatchLessEqual[uint16])
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint16Greater(t *testing.T) {
	tests.TestCases[uint16](t, tests.Uint16GreaterCases, MatchGreater[uint16])
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchUint16Greater(b *testing.B) {
	tests.BenchCases[uint16](b, MatchGreater[uint16])
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint16GreaterEqual(t *testing.T) {
	tests.TestCases[uint16](t, tests.Uint16GreaterEqualCases, MatchGreaterEqual[uint16])
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchUint16GreaterEqual(b *testing.B) {
	tests.BenchCases[uint16](b, MatchGreaterEqual[uint16])
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchUint16Between(t *testing.T) {
	tests.TestCases2[uint16](t, tests.Uint16BetweenCases, MatchBetween[uint16])
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchUint16Between(b *testing.B) {
	tests.BenchCases2[uint16](b, MatchBetween[uint16])
}
