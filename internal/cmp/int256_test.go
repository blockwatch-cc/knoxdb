// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestInt256Equal(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256EqualCases, cmp_i256_eq)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkInt256Equal(b *testing.B) {
	tests.BenchInt256Cases(b, cmp_i256_eq)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestInt256NotEqual(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256NotEqualCases, cmp_i256_ne)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkInt256NotEqual(b *testing.B) {
	tests.BenchInt256Cases(b, cmp_i256_ne)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestInt256Less(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256LessCases, cmp_i256_lt)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkInt256Less(b *testing.B) {
	tests.BenchInt256Cases(b, cmp_i256_lt)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestInt256LessEqual(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256LessEqualCases, cmp_i256_le)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkInt256LessEqual(b *testing.B) {
	tests.BenchInt256Cases(b, cmp_i256_le)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestInt256Greater(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256GreaterCases, cmp_i256_gt)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkInt256Greater(b *testing.B) {
	tests.BenchInt256Cases(b, cmp_i256_gt)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestInt256GreaterEqual(t *testing.T) {
	tests.TestInt256Cases(t, tests.Int256GreaterEqualCases, cmp_i256_ge)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkInt256GreaterEqual(b *testing.B) {
	tests.BenchInt256Cases(b, cmp_i256_ge)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestInt256Between(t *testing.T) {
	tests.TestInt256Cases2(t, tests.Int256BetweenCases, cmp_i256_bw)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkInt256Between(b *testing.B) {
	tests.BenchInt256Cases2(b, cmp_i256_bw)
}
