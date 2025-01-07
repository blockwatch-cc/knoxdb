// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchBytesEqual(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesEqualCases, MatchBytesEqual)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchBytesEqual(b *testing.B) {
	tests.BenchBytesCases(b, MatchBytesEqual)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchBytesNotEqual(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesNotEqualCases, MatchBytesNotEqual)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchBytesNotEqual(b *testing.B) {
	tests.BenchBytesCases(b, MatchBytesNotEqual)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchBytesLess(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesLessCases, MatchBytesLess)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchBytesLess(b *testing.B) {
	tests.BenchBytesCases(b, MatchBytesLess)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchBytesLessEqual(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesLessEqualCases, MatchBytesLessEqual)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchBytesLessEqual(b *testing.B) {
	tests.BenchBytesCases(b, MatchBytesLessEqual)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchBytesGreater(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesGreaterCases, MatchBytesGreater)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchBytesGreater(b *testing.B) {
	tests.BenchBytesCases(b, MatchBytesGreater)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchBytesGreaterEqual(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesGreaterEqualCases, MatchBytesGreaterEqual)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchBytesGreaterEqual(b *testing.B) {
	tests.BenchBytesCases(b, MatchBytesGreaterEqual)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchBytesBetween(t *testing.T) {
	tests.TestBytesCases2(t, tests.BytesBetweenCases, MatchBytesBetween)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchBytesBetween(b *testing.B) {
	tests.BenchBytesCases2(b, MatchBytesBetween)
}
