// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt128EqualAVX2(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128EqualCases, MatchInt128Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt128EqualAVX2(b *testing.B) {
	tests.BenchInt128Cases(b, MatchInt128Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt128NotEqualAVX2(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128NotEqualCases, MatchInt128NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt128NotEqualAVX2(b *testing.B) {
	tests.BenchInt128Cases(b, MatchInt128NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt128LessAVX2(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128LessCases, MatchInt128Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt128LessAVX2(b *testing.B) {
	tests.BenchInt128Cases(b, MatchInt128Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt128LessEqualAVX2(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128LessEqualCases, MatchInt128LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt128LessEqualAVX2(b *testing.B) {
	tests.BenchInt128Cases(b, MatchInt128LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt128GreaterAVX2(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128GreaterCases, MatchInt128Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt128GreaterAVX2(b *testing.B) {
	tests.BenchInt128Cases(b, MatchInt128Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt128GreaterEqualAVX2(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128GreaterEqualCases, MatchInt128GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt128GreaterEqualAVX2(b *testing.B) {
	tests.BenchInt128Cases(b, MatchInt128GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt128BetweenAVX2(t *testing.T) {
	tests.TestInt128Cases2(t, tests.Int128BetweenCases, MatchInt128Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt128BetweenAVX2(b *testing.B) {
	tests.BenchInt128Cases2(b, MatchInt128Between)
}
