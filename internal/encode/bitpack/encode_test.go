// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc
package bitpack

import (
	"testing"

	bptest "blockwatch.cc/knoxdb/internal/encode/bitpack/tests"
)

func TestEncode(t *testing.T) {
	bptest.EncodeTest(t, Encode[int8], Decode)
	bptest.EncodeTest(t, Encode[uint8], Decode)
	bptest.EncodeTest(t, Encode[int16], Decode)
	bptest.EncodeTest(t, Encode[uint16], Decode)
	bptest.EncodeTest(t, Encode[int32], Decode)
	bptest.EncodeTest(t, Encode[uint32], Decode)
	bptest.EncodeTest(t, Encode[int64], Decode)
	bptest.EncodeTest(t, Encode[uint64], Decode)
}

// func BenchmarkEncode10(b *testing.B) {
// 	n := 1 << 16
// 	src := tests.GenRndBits[uint32](n, 10)
// 	minv, maxv := slices.Min(src), slices.Max(src)
// 	buf := make([]byte, EstimateSize(10, n))
// 	b.SetBytes(int64(n * 4))
// 	for b.Loop() {
// 		Encode(buf, src, minv, maxv)
// 	}
// 	b.ReportMetric(float64(n*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
// }

// func BenchmarkDecode10(b *testing.B) {
// 	n := 1 << 16
// 	src := tests.GenRndBits[uint32](n, 10)
// 	minv, maxv := slices.Min(src), slices.Max(src)
// 	buf := make([]byte, EstimateSize(10, n))
// 	dst := make([]uint32, n)
// 	Encode(buf, src, minv, maxv)
// 	b.SetBytes(int64(n * 4))
// 	for b.Loop() {
// 		Decode(dst, buf, 10, minv)
// 	}
// 	b.ReportMetric(float64(n*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
// }

func BenchmarkEncode(b *testing.B) {
	bptest.EncodeBenchmark(b, Encode[uint8])
	bptest.EncodeBenchmark(b, Encode[uint16])
	bptest.EncodeBenchmark(b, Encode[uint32])
	bptest.EncodeBenchmark(b, Encode[uint64])
}

func BenchmarkDecode(b *testing.B) {
	bptest.DecodeBenchmark(b, Encode[uint8], Decode[uint8])
	bptest.DecodeBenchmark(b, Encode[uint16], Decode[uint16])
	bptest.DecodeBenchmark(b, Encode[uint32], Decode[uint32])
	bptest.DecodeBenchmark(b, Encode[uint64], Decode[uint64])
}
