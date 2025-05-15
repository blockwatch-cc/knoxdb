// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestInt128Equal(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128EqualCases, cmp_i128_eq)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkInt128Equal(b *testing.B) {
	tests.BenchInt128Cases(b, cmp_i128_eq)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestInt128NotEqual(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128NotEqualCases, cmp_i128_ne)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkInt128NotEqual(b *testing.B) {
	tests.BenchInt128Cases(b, cmp_i128_ne)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestInt128Less(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128LessCases, cmp_i128_lt)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkInt128Less(b *testing.B) {
	tests.BenchInt128Cases(b, cmp_i128_lt)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestInt128LessEqual(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128LessEqualCases, cmp_i128_le)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkInt128LessEqual(b *testing.B) {
	tests.BenchInt128Cases(b, cmp_i128_le)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestInt128Greater(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128GreaterCases, cmp_i128_gt)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkInt128Greater(b *testing.B) {
	tests.BenchInt128Cases(b, cmp_i128_gt)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestInt128GreaterEqual(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128GreaterEqualCases, cmp_i128_ge)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkInt128GreaterEqual(b *testing.B) {
	tests.BenchInt128Cases(b, cmp_i128_ge)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestInt128Between(t *testing.T) {
	tests.TestInt128Cases2(t, tests.Int128BetweenCases, cmp_i128_bw)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkInt128Between(b *testing.B) {
	tests.BenchInt128Cases2(b, cmp_i128_bw)
}
