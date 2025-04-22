// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64
// +build !amd64

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/analyze/tests"
)

func TestAnalyze(t *testing.T) {
	tests.AnalyzeTest(t, tests.MakeUnsignedTests[uint64](), AnalyzeUint64)
	tests.AnalyzeTest(t, tests.MakeUnsignedTests[uint32](), AnalyzeUint32)
	tests.AnalyzeTest(t, tests.MakeUnsignedTests[uint16](), AnalyzeUint16)
	tests.AnalyzeTest(t, tests.MakeUnsignedTests[uint8](), AnalyzeUint8)

	tests.AnalyzeTest(t, tests.MakeSignedTests[int64](), AnalyzeInt64)
	tests.AnalyzeTest(t, tests.MakeSignedTests[int32](), AnalyzeInt32)
	tests.AnalyzeTest(t, tests.MakeSignedTests[int16](), AnalyzeInt16)
	tests.AnalyzeTest(t, tests.MakeSignedTests[int8](), AnalyzeInt8)

	tests.AnalyzeFloatTest(t, tests.MakeFloatTests[float64](), AnalyzeFloat64)
	tests.AnalyzeFloatTest(t, tests.MakeFloatTests[float32](), AnalyzeFloat32)
}

func BenchmarkAnalyze(b *testing.B) {
	tests.AnalyzeBenchmark(b, AnalyzeUint64)
	tests.AnalyzeBenchmark(b, AnalyzeUint32)
	tests.AnalyzeBenchmark(b, AnalyzeUint16)
	tests.AnalyzeBenchmark(b, AnalyzeUint8)

	tests.AnalyzeBenchmark(b, AnalyzeInt64)
	tests.AnalyzeBenchmark(b, AnalyzeInt32)
	tests.AnalyzeBenchmark(b, AnalyzeInt16)
	tests.AnalyzeBenchmark(b, AnalyzeInt8)

	tests.AnalyzeFloatBenchmark(b, AnalyzeFloat64)
	tests.AnalyzeFloatBenchmark(b, AnalyzeFloat32)
}
