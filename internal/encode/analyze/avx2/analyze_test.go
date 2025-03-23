// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/analyze/tests"
)

func TestAnalyzeInt64(t *testing.T) {
	tests.AnalyzeTest[int64](t, tests.MakeSignedTests[int64](), AnalyzeInt64)
}

func TestAnalyzeUint64(t *testing.T) {
	tests.AnalyzeTest[uint64](t, tests.MakeUnsignedTests[uint64](), AnalyzeUint64)
}

func TestAnalyzeInt32(t *testing.T) {
	tests.AnalyzeTest[int32](t, tests.MakeSignedTests[int32](), AnalyzeInt32)
}

func TestAnalyzeUint32(t *testing.T) {
	tests.AnalyzeTest[uint32](t, tests.MakeUnsignedTests[uint32](), AnalyzeUint32)
}

func TestAnalyzeInt16(t *testing.T) {
	tests.AnalyzeTest[int16](t, tests.MakeSignedTests[int16](), AnalyzeInt16)
}

func TestAnalyzeUint16(t *testing.T) {
	tests.AnalyzeTest[uint16](t, tests.MakeUnsignedTests[uint16](), AnalyzeUint16)
}

func TestAnalyzeInt8(t *testing.T) {
	tests.AnalyzeTest[int8](t, tests.MakeSignedTests[int8](), AnalyzeInt8)
}

func TestAnalyzeUint8(t *testing.T) {
	tests.AnalyzeTest[uint8](t, tests.MakeUnsignedTests[uint8](), AnalyzeUint8)
}

func BenchmarkAnalyzeInt64(b *testing.B) {
	tests.AnalyzeBenchmark[int64](b, AnalyzeInt64)
}

func BenchmarkAnalyzeUint64(b *testing.B) {
	tests.AnalyzeBenchmark[uint64](b, AnalyzeUint64)
}

func BenchmarkAnalyzeInt32(b *testing.B) {
	tests.AnalyzeBenchmark[int32](b, AnalyzeInt32)
}

func BenchmarkAnalyzeUint32(b *testing.B) {
	tests.AnalyzeBenchmark[uint32](b, AnalyzeUint32)
}

func BenchmarkAnalyzeInt16(b *testing.B) {
	tests.AnalyzeBenchmark[int16](b, AnalyzeInt16)
}

func BenchmarkAnalyzeUint16(b *testing.B) {
	tests.AnalyzeBenchmark[uint16](b, AnalyzeUint16)
}

func BenchmarkAnalyzeInt8(b *testing.B) {
	tests.AnalyzeBenchmark[int8](b, AnalyzeInt8)
}

func BenchmarkAnalyzeUint8(b *testing.B) {
	tests.AnalyzeBenchmark[uint8](b, AnalyzeUint8)
}
