// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cmp/tests"
)

// -----------------------------------------------------------------------------
// Equal Testcases
func TestBytesEqual(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesEqualCases, cmp_bytes_eq)
}

// -----------------------------------------------------------------------------
// Equal benchmarks
func BenchmarkBytesEqual(b *testing.B) {
	tests.BenchBytesCases(b, cmp_bytes_eq)
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
func TestBytesNotEqual(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesNotEqualCases, cmp_bytes_ne)
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
func BenchmarkBytesNotEqual(b *testing.B) {
	tests.BenchBytesCases(b, cmp_bytes_ne)
}

// -----------------------------------------------------------------------------
// Less Testcases
func TestBytesLess(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesLessCases, cmp_bytes_lt)
}

// -----------------------------------------------------------------------------
// Less benchmarks
func BenchmarkBytesLess(b *testing.B) {
	tests.BenchBytesCases(b, cmp_bytes_lt)
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
func TestBytesLessEqual(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesLessEqualCases, cmp_bytes_le)
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
func BenchmarkBytesLessEqual(b *testing.B) {
	tests.BenchBytesCases(b, cmp_bytes_le)
}

// -----------------------------------------------------------------------------
// Greater Testcases
func TestBytesGreater(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesGreaterCases, cmp_bytes_gt)
}

// -----------------------------------------------------------------------------
// Greater benchmarks
func BenchmarkBytesGreater(b *testing.B) {
	tests.BenchBytesCases(b, cmp_bytes_gt)
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
func TestBytesGreaterEqual(t *testing.T) {
	tests.TestBytesCases(t, tests.BytesGreaterEqualCases, cmp_bytes_ge)
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
func BenchmarkBytesGreaterEqual(b *testing.B) {
	tests.BenchBytesCases(b, cmp_bytes_ge)
}

// -----------------------------------------------------------------------------
// Between Testcases
func TestBytesBetween(t *testing.T) {
	tests.TestBytesCases2(t, tests.BytesBetweenCases, cmp_bytes_bw)
}

// -----------------------------------------------------------------------------
// Between benchmarks
func BenchmarkBytesBetween(b *testing.B) {
	tests.BenchBytesCases2(b, cmp_bytes_bw)
}
