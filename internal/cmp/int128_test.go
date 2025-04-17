// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt128Equal(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128EqualCases, cmp_i128_eq)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt128Equal(b *testing.B) {
	tests.BenchInt128Cases(b, cmp_i128_eq)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt128NotEqual(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128NotEqualCases, cmp_i128_ne)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt128NotEqual(b *testing.B) {
	tests.BenchInt128Cases(b, cmp_i128_ne)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt128Less(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128LessCases, cmp_i128_lt)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt128Less(b *testing.B) {
	tests.BenchInt128Cases(b, cmp_i128_lt)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt128LessEqual(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128LessEqualCases, cmp_i128_le)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt128LessEqual(b *testing.B) {
	tests.BenchInt128Cases(b, cmp_i128_le)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt128Greater(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128GreaterCases, cmp_i128_gt)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt128Greater(b *testing.B) {
	tests.BenchInt128Cases(b, cmp_i128_gt)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt128GreaterEqual(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128GreaterEqualCases, cmp_i128_ge)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt128GreaterEqual(b *testing.B) {
	tests.BenchInt128Cases(b, cmp_i128_ge)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt128Between(t *testing.T) {
	tests.TestInt128Cases2(t, tests.Int128BetweenCases, cmp_i128_bw)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt128Between(b *testing.B) {
	tests.BenchInt128Cases2(b, cmp_i128_bw)
}
