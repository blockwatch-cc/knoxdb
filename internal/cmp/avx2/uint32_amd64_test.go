// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchUint32EqualAVX2(t *testing.T) {
	tests.TestCases[uint32](t, tests.Uint32EqualCases, MatchUint32Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchUint32EqualAVX2(b *testing.B) {
	tests.BenchCases[uint32](b, MatchUint32Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchUint32NotEqualAVX2(t *testing.T) {
	tests.TestCases[uint32](t, tests.Uint32NotEqualCases, MatchUint32NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchUint32NotEqualAVX2(b *testing.B) {
	tests.BenchCases[uint32](b, MatchUint32NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchUint32LessAVX2(t *testing.T) {
	tests.TestCases[uint32](t, tests.Uint32LessCases, MatchUint32Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchUint32LessAVX2(b *testing.B) {
	tests.BenchCases[uint32](b, MatchUint32Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint32LessEqualAVX2(t *testing.T) {
	tests.TestCases[uint32](t, tests.Uint32LessEqualCases, MatchUint32LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchUint32LessEqualAVX2(b *testing.B) {
	tests.BenchCases[uint32](b, MatchUint32LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint32GreaterAVX2(t *testing.T) {
	tests.TestCases[uint32](t, tests.Uint32GreaterCases, MatchUint32Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchUint32GreaterAVX2(b *testing.B) {
	tests.BenchCases[uint32](b, MatchUint32Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint32GreaterEqualAVX2(t *testing.T) {
	tests.TestCases[uint32](t, tests.Uint32GreaterEqualCases, MatchUint32GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchUint32GreaterEqualAVX2(b *testing.B) {
	tests.BenchCases[uint32](b, MatchUint32GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchUint32BetweenAVX2(t *testing.T) {
	tests.TestCases2[uint32](t, tests.Uint32BetweenCases, MatchUint32Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchUint32BetweenAVX2(b *testing.B) {
	tests.BenchCases2[uint32](b, MatchUint32Between)
}
