// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/analyze/tests"
)

func TestAnalyze(t *testing.T) {
	tests.AnalyzeTest(t, tests.MakeUnsignedTests[uint64](), Analyze)
	tests.AnalyzeTest(t, tests.MakeUnsignedTests[uint32](), Analyze)
	tests.AnalyzeTest(t, tests.MakeUnsignedTests[uint16](), Analyze)
	tests.AnalyzeTest(t, tests.MakeUnsignedTests[uint8](), Analyze)

	tests.AnalyzeTest(t, tests.MakeSignedTests[int64](), Analyze)
	tests.AnalyzeTest(t, tests.MakeSignedTests[int32](), Analyze)
	tests.AnalyzeTest(t, tests.MakeSignedTests[int16](), Analyze)
	tests.AnalyzeTest(t, tests.MakeSignedTests[int8](), Analyze)

	tests.AnalyzeFloatTest(t, tests.MakeFloatTests[float64](), AnalyzeFloat)
	tests.AnalyzeFloatTest(t, tests.MakeFloatTests[float32](), AnalyzeFloat)
}

func BenchmarkAnalyze(b *testing.B) {
	tests.AnalyzeBenchmark(b, Analyze[uint64])
	tests.AnalyzeBenchmark(b, Analyze[uint32])
	tests.AnalyzeBenchmark(b, Analyze[uint16])
	tests.AnalyzeBenchmark(b, Analyze[uint8])

	tests.AnalyzeBenchmark(b, Analyze[int64])
	tests.AnalyzeBenchmark(b, Analyze[int32])
	tests.AnalyzeBenchmark(b, Analyze[int16])
	tests.AnalyzeBenchmark(b, Analyze[int8])

	tests.AnalyzeFloatBenchmark(b, AnalyzeFloat[float64])
	tests.AnalyzeFloatBenchmark(b, AnalyzeFloat[float32])
}
