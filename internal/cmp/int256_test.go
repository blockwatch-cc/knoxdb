// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt256Equal(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256EqualCases, cmp_i256_eq)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt256Equal(b *testing.B) {
	tests.BenchInt256Cases(b, cmp_i256_eq)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt256NotEqual(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256NotEqualCases, cmp_i256_ne)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt256NotEqual(b *testing.B) {
	tests.BenchInt256Cases(b, cmp_i256_ne)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt256Less(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256LessCases, cmp_i256_lt)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt256Less(b *testing.B) {
	tests.BenchInt256Cases(b, cmp_i256_lt)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt256LessEqual(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256LessEqualCases, cmp_i256_le)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt256LessEqual(b *testing.B) {
	tests.BenchInt256Cases(b, cmp_i256_le)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt256Greater(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256GreaterCases, cmp_i256_gt)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt256Greater(b *testing.B) {
	tests.BenchInt256Cases(b, cmp_i256_gt)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt256GreaterEqual(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256GreaterEqualCases, cmp_i256_ge)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt256GreaterEqual(b *testing.B) {
	tests.BenchInt256Cases(b, cmp_i256_ge)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt256Between(t *testing.T) {
	tests.TestInt256Cases2(t, tests.Int256BetweenCases, cmp_i256_bw)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt256Between(b *testing.B) {
	tests.BenchInt256Cases2(b, cmp_i256_bw)
}
