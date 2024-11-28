// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx512

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchUint8EqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[uint8](t, tests.Uint8EqualCases, MatchUint8Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchUint8EqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[uint8](b, MatchUint8Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchUint8NotEqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[uint8](t, tests.Uint8NotEqualCases, MatchUint8NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchUint8NotEqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[uint8](b, MatchUint8NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchUint8LessAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[uint8](t, tests.Uint8LessCases, MatchUint8Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchUint8LessAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[uint8](b, MatchUint8Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint8LessEqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[uint8](t, tests.Uint8LessEqualCases, MatchUint8LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchUint8LessEqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[uint8](b, MatchUint8LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint8GreaterAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[uint8](t, tests.Uint8GreaterCases, MatchUint8Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchUint8GreaterAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[uint8](b, MatchUint8Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint8GreaterEqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[uint8](t, tests.Uint8GreaterEqualCases, MatchUint8GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchUint8GreaterEqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[uint8](b, MatchUint8GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchUint8BetweenAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases2[uint8](t, tests.Uint8BetweenCases, MatchUint8Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchUint8BetweenAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases2[uint8](b, MatchUint8Between)
}
