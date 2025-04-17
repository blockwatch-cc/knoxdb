// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchInt128EqualAVX2(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128EqualCases, Int128Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchInt128EqualAVX2(b *testing.B) {
	tests.BenchInt128Cases(b, Int128Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchInt128NotEqualAVX2(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128NotEqualCases, Int128NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchInt128NotEqualAVX2(b *testing.B) {
	tests.BenchInt128Cases(b, Int128NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchInt128LessAVX2(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128LessCases, Int128Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchInt128LessAVX2(b *testing.B) {
	tests.BenchInt128Cases(b, Int128Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchInt128LessEqualAVX2(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128LessEqualCases, Int128LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchInt128LessEqualAVX2(b *testing.B) {
	tests.BenchInt128Cases(b, Int128LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchInt128GreaterAVX2(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128GreaterCases, Int128Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchInt128GreaterAVX2(b *testing.B) {
	tests.BenchInt128Cases(b, Int128Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchInt128GreaterEqualAVX2(t *testing.T) {
	tests.TestInt128Cases(t, tests.Int128GreaterEqualCases, Int128GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchInt128GreaterEqualAVX2(b *testing.B) {
	tests.BenchInt128Cases(b, Int128GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchInt128BetweenAVX2(t *testing.T) {
	tests.TestInt128Cases2(t, tests.Int128BetweenCases, Int128Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchInt128BetweenAVX2(b *testing.B) {
	tests.BenchInt128Cases2(b, Int128Between)
}
