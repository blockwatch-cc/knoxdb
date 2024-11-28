// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt128Equal(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128EqualCases, MatchInt128Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt128Equal(b *testing.B) {
	tests.BenchInt128Cases(b, MatchInt128Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt128NotEqual(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128NotEqualCases, MatchInt128NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt128NotEqual(b *testing.B) {
	tests.BenchInt128Cases(b, MatchInt128NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt128Less(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128LessCases, MatchInt128Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt128Less(b *testing.B) {
	tests.BenchInt128Cases(b, MatchInt128Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt128LessEqual(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128LessEqualCases, MatchInt128LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt128LessEqual(b *testing.B) {
	tests.BenchInt128Cases(b, MatchInt128LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt128Greater(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128GreaterCases, MatchInt128Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt128Greater(b *testing.B) {
	tests.BenchInt128Cases(b, MatchInt128Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt128GreaterEqual(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128GreaterEqualCases, MatchInt128GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt128GreaterEqual(b *testing.B) {
	tests.BenchInt128Cases(b, MatchInt128GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt128Between(t *testing.T) {
	tests.TestInt128Cases2(t, tests.Int128BetweenCases, MatchInt128Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt128Between(b *testing.B) {
	tests.BenchInt128Cases2(b, MatchInt128Between)
}
