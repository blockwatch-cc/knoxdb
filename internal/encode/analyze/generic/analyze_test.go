// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/analyze/tests"
)

func TestAnalyzeInt64(t *testing.T) {
	tests.AnalyzeTest[int64](t, tests.MakeSignedTests[int64](), Analyze[int64])
}

func TestAnalyzeUint64(t *testing.T) {
	tests.AnalyzeTest[uint64](t, tests.MakeUnsignedTests[uint64](), Analyze[uint64])
}

func TestAnalyzeInt32(t *testing.T) {
	tests.AnalyzeTest[int32](t, tests.MakeSignedTests[int32](), Analyze[int32])
}

func TestAnalyzeUint32(t *testing.T) {
	tests.AnalyzeTest[uint32](t, tests.MakeUnsignedTests[uint32](), Analyze[uint32])
}

func TestAnalyzeInt16(t *testing.T) {
	tests.AnalyzeTest[int16](t, tests.MakeSignedTests[int16](), Analyze[int16])
}

func TestAnalyzeUint16(t *testing.T) {
	tests.AnalyzeTest[uint16](t, tests.MakeUnsignedTests[uint16](), Analyze[uint16])
}

func TestAnalyzeInt8(t *testing.T) {
	tests.AnalyzeTest[int8](t, tests.MakeSignedTests[int8](), Analyze[int8])
}

func TestAnalyzeUint8(t *testing.T) {
	tests.AnalyzeTest[uint8](t, tests.MakeUnsignedTests[uint8](), Analyze[uint8])
}

func BenchmarkAnalyzeInt64(b *testing.B) {
	tests.AnalyzeBenchmark[int64](b, Analyze[int64])
}

func BenchmarkAnalyzeUint64(b *testing.B) {
	tests.AnalyzeBenchmark[uint64](b, Analyze[uint64])
}

func BenchmarkAnalyzeInt32(b *testing.B) {
	tests.AnalyzeBenchmark[int32](b, Analyze[int32])
}

func BenchmarkAnalyzeUint32(b *testing.B) {
	tests.AnalyzeBenchmark[uint32](b, Analyze[uint32])
}

func BenchmarkAnalyzeInt16(b *testing.B) {
	tests.AnalyzeBenchmark[int16](b, Analyze[int16])
}

func BenchmarkAnalyzeUint16(b *testing.B) {
	tests.AnalyzeBenchmark[uint16](b, Analyze[uint16])
}

func BenchmarkAnalyzeInt8(b *testing.B) {
	tests.AnalyzeBenchmark[int8](b, Analyze[int8])
}

func BenchmarkAnalyzeUint8(b *testing.B) {
	tests.AnalyzeBenchmark[uint8](b, Analyze[uint8])
}
