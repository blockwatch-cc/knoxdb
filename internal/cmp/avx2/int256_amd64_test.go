// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt256EqualAVX2(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256EqualCases, MatchInt256Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt256EqualAVX2(b *testing.B) {
	tests.BenchInt256Cases(b, MatchInt256Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt256NotEqualAVX2(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256NotEqualCases, MatchInt256NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt256NotEqualAVX2(b *testing.B) {
	tests.BenchInt256Cases(b, MatchInt256NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt256LessAVX2(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256LessCases, MatchInt256Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt256LessAVX2(b *testing.B) {
	tests.BenchInt256Cases(b, MatchInt256Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt256LessEqualAVX2(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256LessEqualCases, MatchInt256LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt256LessEqualAVX2(b *testing.B) {
	tests.BenchInt256Cases(b, MatchInt256LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt256GreaterAVX2(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256GreaterCases, MatchInt256Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt256GreaterAVX2(b *testing.B) {
	tests.BenchInt256Cases(b, MatchInt256Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt256GreaterEqualAVX2(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256GreaterEqualCases, MatchInt256GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt256GreaterEqualAVX2(b *testing.B) {
	tests.BenchInt256Cases(b, MatchInt256GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt256BetweenAVX2(t *testing.T) {
	tests.TestInt256Cases2(t, tests.Int256BetweenCases, MatchInt256Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt256BetweenAVX2(b *testing.B) {
	tests.BenchInt256Cases2(b, MatchInt256Between)
}
