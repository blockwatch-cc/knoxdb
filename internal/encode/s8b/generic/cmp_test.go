// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	stests "blockwatch.cc/knoxdb/internal/encode/s8b/tests"
	"blockwatch.cc/knoxdb/internal/types"
)

// -------------------------------
// Tests
//

func TestCmpEqual(t *testing.T) {
	stests.CompareTest(t, Encode[uint64], Decode[uint64], Equal, types.FilterModeEqual)
	stests.CompareTest(t, Encode[uint32], Decode[uint64], Equal, types.FilterModeEqual)
	stests.CompareTest(t, Encode[uint16], Decode[uint64], Equal, types.FilterModeEqual)
	stests.CompareTest(t, Encode[uint8], Decode[uint64], Equal, types.FilterModeEqual)
	stests.CompareTest(t, Encode[int64], Decode[uint64], Equal, types.FilterModeEqual)
	stests.CompareTest(t, Encode[int32], Decode[uint64], Equal, types.FilterModeEqual)
	stests.CompareTest(t, Encode[int16], Decode[uint64], Equal, types.FilterModeEqual)
	stests.CompareTest(t, Encode[int8], Decode[uint64], Equal, types.FilterModeEqual)
}

func TestCmpNotEqual(t *testing.T) {
	stests.CompareTest(t, Encode[uint64], Decode[uint64], NotEqual, types.FilterModeNotEqual)
	stests.CompareTest(t, Encode[uint32], Decode[uint64], NotEqual, types.FilterModeNotEqual)
	stests.CompareTest(t, Encode[uint16], Decode[uint64], NotEqual, types.FilterModeNotEqual)
	stests.CompareTest(t, Encode[uint8], Decode[uint64], NotEqual, types.FilterModeNotEqual)
	stests.CompareTest(t, Encode[int64], Decode[uint64], NotEqual, types.FilterModeNotEqual)
	stests.CompareTest(t, Encode[int32], Decode[uint64], NotEqual, types.FilterModeNotEqual)
	stests.CompareTest(t, Encode[int16], Decode[uint64], NotEqual, types.FilterModeNotEqual)
	stests.CompareTest(t, Encode[int8], Decode[uint64], NotEqual, types.FilterModeNotEqual)
}

func TestCmpLess(t *testing.T) {
	stests.CompareTest(t, Encode[uint64], Decode[uint64], Less, types.FilterModeLt)
	stests.CompareTest(t, Encode[uint32], Decode[uint64], Less, types.FilterModeLt)
	stests.CompareTest(t, Encode[uint16], Decode[uint64], Less, types.FilterModeLt)
	stests.CompareTest(t, Encode[uint8], Decode[uint64], Less, types.FilterModeLt)
	stests.CompareTest(t, Encode[int64], Decode[uint64], Less, types.FilterModeLt)
	stests.CompareTest(t, Encode[int32], Decode[uint64], Less, types.FilterModeLt)
	stests.CompareTest(t, Encode[int16], Decode[uint64], Less, types.FilterModeLt)
	stests.CompareTest(t, Encode[int8], Decode[uint64], Less, types.FilterModeLt)
}

func TestCmpLessEqual(t *testing.T) {
	stests.CompareTest(t, Encode[uint64], Decode[uint64], LessEqual, types.FilterModeLe)
	stests.CompareTest(t, Encode[uint32], Decode[uint64], LessEqual, types.FilterModeLe)
	stests.CompareTest(t, Encode[uint16], Decode[uint64], LessEqual, types.FilterModeLe)
	stests.CompareTest(t, Encode[uint8], Decode[uint64], LessEqual, types.FilterModeLe)
	stests.CompareTest(t, Encode[int64], Decode[uint64], LessEqual, types.FilterModeLe)
	stests.CompareTest(t, Encode[int32], Decode[uint64], LessEqual, types.FilterModeLe)
	stests.CompareTest(t, Encode[int16], Decode[uint64], LessEqual, types.FilterModeLe)
	stests.CompareTest(t, Encode[int8], Decode[uint64], LessEqual, types.FilterModeLe)
}

func TestCmpGreater(t *testing.T) {
	stests.CompareTest(t, Encode[uint64], Decode[uint64], Greater, types.FilterModeGt)
	stests.CompareTest(t, Encode[uint32], Decode[uint64], Greater, types.FilterModeGt)
	stests.CompareTest(t, Encode[uint16], Decode[uint64], Greater, types.FilterModeGt)
	stests.CompareTest(t, Encode[uint8], Decode[uint64], Greater, types.FilterModeGt)
	stests.CompareTest(t, Encode[int64], Decode[uint64], Greater, types.FilterModeGt)
	stests.CompareTest(t, Encode[int32], Decode[uint64], Greater, types.FilterModeGt)
	stests.CompareTest(t, Encode[int16], Decode[uint64], Greater, types.FilterModeGt)
	stests.CompareTest(t, Encode[int8], Decode[uint64], Greater, types.FilterModeGt)
}

func TestCmpGreaterEqual(t *testing.T) {
	stests.CompareTest(t, Encode[uint64], Decode[uint64], GreaterEqual, types.FilterModeGe)
	stests.CompareTest(t, Encode[uint32], Decode[uint64], GreaterEqual, types.FilterModeGe)
	stests.CompareTest(t, Encode[uint16], Decode[uint64], GreaterEqual, types.FilterModeGe)
	stests.CompareTest(t, Encode[uint8], Decode[uint64], GreaterEqual, types.FilterModeGe)
	stests.CompareTest(t, Encode[int64], Decode[uint64], GreaterEqual, types.FilterModeGe)
	stests.CompareTest(t, Encode[int32], Decode[uint64], GreaterEqual, types.FilterModeGe)
	stests.CompareTest(t, Encode[int16], Decode[uint64], GreaterEqual, types.FilterModeGe)
	stests.CompareTest(t, Encode[int8], Decode[uint64], GreaterEqual, types.FilterModeGe)
}

func TestCmpBetween(t *testing.T) {
	stests.CompareTest2(t, Encode[uint64], Decode[uint64], Between, types.FilterModeRange)
	stests.CompareTest2(t, Encode[uint32], Decode[uint64], Between, types.FilterModeRange)
	stests.CompareTest2(t, Encode[uint16], Decode[uint64], Between, types.FilterModeRange)
	stests.CompareTest2(t, Encode[uint8], Decode[uint64], Between, types.FilterModeRange)
	stests.CompareTest2(t, Encode[int64], Decode[uint64], Between, types.FilterModeRange)
	stests.CompareTest2(t, Encode[int32], Decode[uint64], Between, types.FilterModeRange)
	stests.CompareTest2(t, Encode[int16], Decode[uint64], Between, types.FilterModeRange)
	stests.CompareTest2(t, Encode[int8], Decode[uint64], Between, types.FilterModeRange)
}

// -------------------------------
// Benchmarks
//

// equal
func BenchmarkFusionCmpEqual(b *testing.B) {
	stests.CompareBenchmark(b, Encode[uint64], Equal)
	stests.CompareBenchmark(b, Encode[uint32], Equal)
	stests.CompareBenchmark(b, Encode[uint16], Equal)
	stests.CompareBenchmark(b, Encode[uint8], Equal)
}

// not equal
func BenchmarkFusionCmpNotEqual(b *testing.B) {
	stests.CompareBenchmark(b, Encode[uint64], NotEqual)
	stests.CompareBenchmark(b, Encode[uint32], NotEqual)
	stests.CompareBenchmark(b, Encode[uint16], NotEqual)
	stests.CompareBenchmark(b, Encode[uint8], NotEqual)
}

// less
func BenchmarkFusionCmpLess(b *testing.B) {
	stests.CompareBenchmark(b, Encode[uint64], Less)
	stests.CompareBenchmark(b, Encode[uint32], Less)
	stests.CompareBenchmark(b, Encode[uint16], Less)
	stests.CompareBenchmark(b, Encode[uint8], Less)
}

// less equal
func BenchmarkFusionCmpLessEqual(b *testing.B) {
	stests.CompareBenchmark(b, Encode[uint64], LessEqual)
	stests.CompareBenchmark(b, Encode[uint32], LessEqual)
	stests.CompareBenchmark(b, Encode[uint16], LessEqual)
	stests.CompareBenchmark(b, Encode[uint8], LessEqual)
}

// greater
func BenchmarkFusionCmpGreater(b *testing.B) {
	stests.CompareBenchmark(b, Encode[uint64], Greater)
	stests.CompareBenchmark(b, Encode[uint32], Greater)
	stests.CompareBenchmark(b, Encode[uint16], Greater)
	stests.CompareBenchmark(b, Encode[uint8], Greater)
}

// greater equal
func BenchmarkFusionCmpGreaterEqual(b *testing.B) {
	stests.CompareBenchmark(b, Encode[uint64], GreaterEqual)
	stests.CompareBenchmark(b, Encode[uint32], GreaterEqual)
	stests.CompareBenchmark(b, Encode[uint16], GreaterEqual)
	stests.CompareBenchmark(b, Encode[uint8], GreaterEqual)
}

// between
func BenchmarkFusionCmpBetween(b *testing.B) {
	stests.CompareBenchmark2(b, Encode[uint64], Between)
	stests.CompareBenchmark2(b, Encode[uint32], Between)
	stests.CompareBenchmark2(b, Encode[uint16], Between)
	stests.CompareBenchmark2(b, Encode[uint8], Between)
}
