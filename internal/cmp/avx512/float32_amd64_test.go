// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package avx512

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchFloat32EqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[float32](t, tests.Float32EqualCases, MatchFloat32Equal)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchFloat32EqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[float32](b, MatchFloat32Equal)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchFloat32NotEqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[float32](t, tests.Float32NotEqualCases, MatchFloat32NotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchFloat32NotEqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[float32](b, MatchFloat32NotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchFloat32LessAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[float32](t, tests.Float32LessCases, MatchFloat32Less)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchFloat32LessAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[float32](b, MatchFloat32Less)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchFloat32LessEqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[float32](t, tests.Float32LessEqualCases, MatchFloat32LessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchFloat32LessEqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[float32](b, MatchFloat32LessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchFloat32GreaterAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[float32](t, tests.Float32GreaterCases, MatchFloat32Greater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchFloat32GreaterAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[float32](b, MatchFloat32Greater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchFloat32GreaterEqualAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases[float32](t, tests.Float32GreaterEqualCases, MatchFloat32GreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchFloat32GreaterEqualAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases[float32](b, MatchFloat32GreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchFloat32BetweenAVX2(t *testing.T) {
	requireAvx512F(t)
	tests.TestCases2[float32](t, tests.Float32BetweenCases, MatchFloat32Between)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchFloat32BetweenAVX2(b *testing.B) {
	requireAvx512F(b)
	tests.BenchCases2[float32](b, MatchFloat32Between)
}
