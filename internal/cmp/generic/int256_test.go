// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt256Equal(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256EqualCases, MatchInt256Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt256Equal(b *testing.B) {
	tests.BenchInt256Cases(b, MatchInt256Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt256NotEqual(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256NotEqualCases, MatchInt256NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt256NotEqual(b *testing.B) {
	tests.BenchInt256Cases(b, MatchInt256NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt256Less(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256LessCases, MatchInt256Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt256Less(b *testing.B) {
	tests.BenchInt256Cases(b, MatchInt256Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt256LessEqual(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256LessEqualCases, MatchInt256LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt256LessEqual(b *testing.B) {
	tests.BenchInt256Cases(b, MatchInt256LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt256Greater(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256GreaterCases, MatchInt256Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt256Greater(b *testing.B) {
	tests.BenchInt256Cases(b, MatchInt256Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt256GreaterEqual(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256GreaterEqualCases, MatchInt256GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt256GreaterEqual(b *testing.B) {
	tests.BenchInt256Cases(b, MatchInt256GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt256Between(t *testing.T) {
	tests.TestInt256Cases2(t, tests.Int256BetweenCases, MatchInt256Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt256Between(b *testing.B) {
	tests.BenchInt256Cases2(b, MatchInt256Between)
}
