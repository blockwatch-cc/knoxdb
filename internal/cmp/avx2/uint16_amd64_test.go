// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchUint16EqualAVX2(t *testing.T) {
	tests.TestCases[uint16](t, tests.Uint16EqualCases, MatchUint16Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchUint16EqualAVX2(b *testing.B) {
	tests.BenchCases[uint16](b, MatchUint16Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchUint16NotEqualAVX2(t *testing.T) {
	tests.TestCases[uint16](t, tests.Uint16NotEqualCases, MatchUint16NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchUint16NotEqualAVX2(b *testing.B) {
	tests.BenchCases[uint16](b, MatchUint16NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchUint16LessAVX2(t *testing.T) {
	tests.TestCases[uint16](t, tests.Uint16LessCases, MatchUint16Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchUint16LessAVX2(b *testing.B) {
	tests.BenchCases[uint16](b, MatchUint16Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchUint16LessEqualAVX2(t *testing.T) {
	tests.TestCases[uint16](t, tests.Uint16LessEqualCases, MatchUint16LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchUint16LessEqualAVX2(b *testing.B) {
	tests.BenchCases[uint16](b, MatchUint16LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchUint16GreaterAVX2(t *testing.T) {
	tests.TestCases[uint16](t, tests.Uint16GreaterCases, MatchUint16Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchUint16GreaterAVX2(b *testing.B) {
	tests.BenchCases[uint16](b, MatchUint16Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchUint16GreaterEqualAVX2(t *testing.T) {
	tests.TestCases[uint16](t, tests.Uint16GreaterEqualCases, MatchUint16GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchUint16GreaterEqualAVX2(b *testing.B) {
	tests.BenchCases[uint16](b, MatchUint16GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchUint16BetweenAVX2(t *testing.T) {
	tests.TestCases2[uint16](t, tests.Uint16BetweenCases, MatchUint16Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchUint16BetweenAVX2(b *testing.B) {
	tests.BenchCases2[uint16](b, MatchUint16Between)
}
