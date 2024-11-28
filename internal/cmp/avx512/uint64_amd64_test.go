// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx512

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchUint64EqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[uint64](t, tests.Uint64EqualCases, MatchUint64Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchUint64EqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[uint64](b, MatchUint64Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchUint64NotEqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[uint64](t, tests.Uint64NotEqualCases, MatchUint64NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchUint64NotEqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[uint64](b, MatchUint64NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchUint64LessAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[uint64](t, tests.Uint64LessCases, MatchUint64Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchUint64LessAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[uint64](b, MatchUint64Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint64LessEqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[uint64](t, tests.Uint64LessEqualCases, MatchUint64LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchUint64LessEqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[uint64](b, MatchUint64LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint64GreaterAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[uint64](t, tests.Uint64GreaterCases, MatchUint64Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchUint64GreaterAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[uint64](b, MatchUint64Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint64GreaterEqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[uint64](t, tests.Uint64GreaterEqualCases, MatchUint64GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchUint64GreaterEqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[uint64](b, MatchUint64GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchUint64BetweenAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases2[uint64](t, tests.Uint64BetweenCases, MatchUint64Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchUint64BetweenAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases2[uint64](b, MatchUint64Between)
}
