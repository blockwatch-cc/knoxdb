// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package pack

import (
	"testing"

	bptest "blockwatch.cc/knoxdb/internal/encode/bitpack/tests"
	"blockwatch.cc/knoxdb/internal/tests"
)

func TestEncode(t *testing.T) {
	bptest.EncodeTest(t, Encode[uint8], Decode)
	bptest.EncodeTest(t, Encode[uint16], Decode)
	bptest.EncodeTest(t, Encode[uint32], Decode)
	bptest.EncodeTest(t, Encode[uint64], Decode)
}

func BenchmarkEncodeConst(b *testing.B) {
	val := tests.GenConst[uint32](1<<16, 21)
	buf := make([]byte, 8*len(val))
	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		Encode(buf, val, 21, 42)
	}
}

func BenchmarkEncodeRnd(b *testing.B) {
	val := tests.GenRnd[uint32](1 << 16)
	buf := make([]byte, 4*len(val))
	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		Encode(buf, val, 21, 42)
	}
}

func BenchmarkEncode(b *testing.B) {
	bptest.EncodeBenchmark(b, Encode[uint8])
	bptest.EncodeBenchmark(b, Encode[uint16])
	bptest.EncodeBenchmark(b, Encode[uint32])
	bptest.EncodeBenchmark(b, Encode[uint64])
}

func BenchmarkDecodeConst(b *testing.B) {
	bptest.DecodeBenchmark(b, Encode[uint8], Decode[uint8])
	bptest.DecodeBenchmark(b, Encode[uint16], Decode[uint16])
	bptest.DecodeBenchmark(b, Encode[uint32], Decode[uint32])
	bptest.DecodeBenchmark(b, Encode[uint64], Decode[uint64])
}
