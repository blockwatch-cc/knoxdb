// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt16EqualAVX2(t *testing.T) {
	tests.TestCases[int16](t, tests.Int16EqualCases, MatchInt16Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt16EqualAVX2(b *testing.B) {
	tests.BenchCases[int16](b, MatchInt16Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt16NotEqualAVX2(t *testing.T) {
	tests.TestCases[int16](t, tests.Int16NotEqualCases, MatchInt16NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt16NotEqualAVX2(b *testing.B) {
	tests.BenchCases[int16](b, MatchInt16NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt16LessAVX2(t *testing.T) {
	tests.TestCases[int16](t, tests.Int16LessCases, MatchInt16Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt16LessAVX2(b *testing.B) {
	tests.BenchCases[int16](b, MatchInt16Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt16LessEqualAVX2(t *testing.T) {
	tests.TestCases[int16](t, tests.Int16LessEqualCases, MatchInt16LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt16LessEqualAVX2(b *testing.B) {
	tests.BenchCases[int16](b, MatchInt16LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt16GreaterAVX2(t *testing.T) {
	tests.TestCases[int16](t, tests.Int16GreaterCases, MatchInt16Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt16GreaterAVX2(b *testing.B) {
	tests.BenchCases[int16](b, MatchInt16Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt16GreaterEqualAVX2(t *testing.T) {
	tests.TestCases[int16](t, tests.Int16GreaterEqualCases, MatchInt16GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt16GreaterEqualAVX2(b *testing.B) {
	tests.BenchCases[int16](b, MatchInt16GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt16BetweenAVX2(t *testing.T) {
	tests.TestCases2[int16](t, tests.Int16BetweenCases, MatchInt16Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt16BetweenAVX2(b *testing.B) {
	tests.BenchCases2[int16](b, MatchInt16Between)
}
