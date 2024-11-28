// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt8EqualAVX2(t *testing.T) {
	tests.TestCases[int8](t, tests.Int8EqualCases, MatchInt8Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt8EqualAVX2(b *testing.B) {
	tests.BenchCases[int8](b, MatchInt8Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt8NotEqualAVX2(t *testing.T) {
	tests.TestCases[int8](t, tests.Int8NotEqualCases, MatchInt8NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt8NotEqualAVX2(b *testing.B) {
	tests.BenchCases[int8](b, MatchInt8NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt8LessAVX2(t *testing.T) {
	tests.TestCases[int8](t, tests.Int8LessCases, MatchInt8Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt8LessAVX2(b *testing.B) {
	tests.BenchCases[int8](b, MatchInt8Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt8LessEqualAVX2(t *testing.T) {
	tests.TestCases[int8](t, tests.Int8LessEqualCases, MatchInt8LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt8LessEqualAVX2(b *testing.B) {
	tests.BenchCases[int8](b, MatchInt8LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt8GreaterAVX2(t *testing.T) {
	tests.TestCases[int8](t, tests.Int8GreaterCases, MatchInt8Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt8GreaterAVX2(b *testing.B) {
	tests.BenchCases[int8](b, MatchInt8Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt8GreaterEqualAVX2(t *testing.T) {
	tests.TestCases[int8](t, tests.Int8GreaterEqualCases, MatchInt8GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt8GreaterEqualAVX2(b *testing.B) {
	tests.BenchCases[int8](b, MatchInt8GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt8BetweenAVX2(t *testing.T) {
	tests.TestCases2[int8](t, tests.Int8BetweenCases, MatchInt8Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt8BetweenAVX2(b *testing.B) {
	tests.BenchCases2[int8](b, MatchInt8Between)
}
