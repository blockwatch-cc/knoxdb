// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestMatchBytesEqual(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesEqualCases, cmp_bytes_eq)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkMatchBytesEqual(b *testing.B) {
	tests.BenchBytesCases(b, cmp_bytes_eq)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestMatchBytesNotEqual(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesNotEqualCases, cmp_bytes_ne)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkMatchBytesNotEqual(b *testing.B) {
	tests.BenchBytesCases(b, cmp_bytes_ne)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestMatchBytesLess(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesLessCases, cmp_bytes_lt)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkMatchBytesLess(b *testing.B) {
	tests.BenchBytesCases(b, cmp_bytes_lt)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestMatchBytesLessEqual(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesLessEqualCases, cmp_bytes_le)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkMatchBytesLessEqual(b *testing.B) {
	tests.BenchBytesCases(b, cmp_bytes_le)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestMatchBytesGreater(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesGreaterCases, cmp_bytes_gt)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkMatchBytesGreater(b *testing.B) {
	tests.BenchBytesCases(b, cmp_bytes_gt)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestMatchBytesGreaterEqual(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesGreaterEqualCases, cmp_bytes_ge)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkMatchBytesGreaterEqual(b *testing.B) {
	tests.BenchBytesCases(b, cmp_bytes_ge)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestMatchBytesBetween(t *testing.T) {
	tests.TestBytesCases2(t, tests.BytesBetweenCases, cmp_bytes_bw)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkMatchBytesBetween(b *testing.B) {
	tests.BenchBytesCases2(b, cmp_bytes_bw)
}
