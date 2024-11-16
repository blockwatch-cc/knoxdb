// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt32EqualAVX2(t *testing.T) {
	tests.TestCases[int32](t, tests.Int32EqualCases, MatchInt32Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt32EqualAVX2(b *testing.B) {
	tests.BenchCases[int32](b, MatchInt32Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt32NotEqualAVX2(t *testing.T) {
	tests.TestCases[int32](t, tests.Int32NotEqualCases, MatchInt32NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt32NotEqualAVX2(b *testing.B) {
	tests.BenchCases[int32](b, MatchInt32NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt32LessAVX2(t *testing.T) {
	tests.TestCases[int32](t, tests.Int32LessCases, MatchInt32Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt32LessAVX2(b *testing.B) {
	tests.BenchCases[int32](b, MatchInt32Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt32LessEqualAVX2(t *testing.T) {
	tests.TestCases[int32](t, tests.Int32LessEqualCases, MatchInt32LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt32LessEqualAVX2(b *testing.B) {
	tests.BenchCases[int32](b, MatchInt32LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt32GreaterAVX2(t *testing.T) {
	tests.TestCases[int32](t, tests.Int32GreaterCases, MatchInt32Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt32GreaterAVX2(b *testing.B) {
	tests.BenchCases[int32](b, MatchInt32Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt32GreaterEqualAVX2(t *testing.T) {
	tests.TestCases[int32](t, tests.Int32GreaterEqualCases, MatchInt32GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt32GreaterEqualAVX2(b *testing.B) {
	tests.BenchCases[int32](b, MatchInt32GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt32BetweenAVX2(t *testing.T) {
	tests.TestCases2[int32](t, tests.Int32BetweenCases, MatchInt32Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt32BetweenAVX2(b *testing.B) {
	tests.BenchCases2[int32](b, MatchInt32Between)
}
