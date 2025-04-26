// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package bitpack

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/bitpack/tests"
	"blockwatch.cc/knoxdb/internal/types"
)

// -------------------------------
// Tests

func TestCmpEqual(t *testing.T) {
	tests.CompareTest[uint64](t, Equal, types.FilterModeEqual, Encode)
}

func TestCmpNotEqual(t *testing.T) {
	tests.CompareTest[uint64](t, NotEqual, types.FilterModeNotEqual, Encode)
}

func TestCmpLess(t *testing.T) {
	tests.CompareTest[uint64](t, Less, types.FilterModeLt, Encode)
}

func TestCmpLessEqual(t *testing.T) {
	tests.CompareTest[uint64](t, LessEqual, types.FilterModeLe, Encode)
}

func TestCmpGreater(t *testing.T) {
	tests.CompareTest[uint64](t, Greater, types.FilterModeGt, Encode)
}

func TestCmpGreaterEqual(t *testing.T) {
	tests.CompareTest[uint64](t, GreaterEqual, types.FilterModeGe, Encode)
}

func TestCmpBetween(t *testing.T) {
	tests.CompareTest2[uint64](t, Between, types.FilterModeRange, Encode)
}

// -------------------------------
// Benchmarks
//

// equal
func BenchmarkCmpEqual(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], Equal)
	tests.CompareBenchmark(b, Encode[uint32], Equal)
	tests.CompareBenchmark(b, Encode[uint16], Equal)
	tests.CompareBenchmark(b, Encode[uint8], Equal)
}

// not equal
func BenchmarkCmpNotEqual(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], NotEqual)
	tests.CompareBenchmark(b, Encode[uint32], NotEqual)
	tests.CompareBenchmark(b, Encode[uint16], NotEqual)
	tests.CompareBenchmark(b, Encode[uint8], NotEqual)
}

// less
func BenchmarkCmpLess(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], Less)
	tests.CompareBenchmark(b, Encode[uint32], Less)
	tests.CompareBenchmark(b, Encode[uint16], Less)
	tests.CompareBenchmark(b, Encode[uint8], Less)
}

// less equal
func BenchmarkCmpLessEqual(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], LessEqual)
	tests.CompareBenchmark(b, Encode[uint32], LessEqual)
	tests.CompareBenchmark(b, Encode[uint16], LessEqual)
	tests.CompareBenchmark(b, Encode[uint8], LessEqual)
}

// greater
func BenchmarkCmpGreater(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], Greater)
	tests.CompareBenchmark(b, Encode[uint32], Greater)
	tests.CompareBenchmark(b, Encode[uint16], Greater)
	tests.CompareBenchmark(b, Encode[uint8], Greater)
}

// greater equal
func BenchmarkCmpGreaterEqual(b *testing.B) {
	tests.CompareBenchmark(b, Encode[uint64], GreaterEqual)
	tests.CompareBenchmark(b, Encode[uint32], GreaterEqual)
	tests.CompareBenchmark(b, Encode[uint16], GreaterEqual)
	tests.CompareBenchmark(b, Encode[uint8], GreaterEqual)
}

// between
func BenchmarkCmpBetween(b *testing.B) {
	tests.CompareBenchmark2(b, Encode[uint64], Between)
	tests.CompareBenchmark2(b, Encode[uint32], Between)
	tests.CompareBenchmark2(b, Encode[uint16], Between)
	tests.CompareBenchmark2(b, Encode[uint8], Between)
}
