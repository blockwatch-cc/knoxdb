// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/bitpack/tests"
)

// -------------------------------
// Tests
//

func TestEncode(t *testing.T) {
	tests.EncodeTest(t, Encode[uint64], nil)
	tests.EncodeTest(t, Encode[uint32], nil)
	tests.EncodeTest(t, Encode[uint16], nil)
	tests.EncodeTest(t, Encode[uint8], nil)
	tests.EncodeTest(t, Encode[int64], nil)
	tests.EncodeTest(t, Encode[int32], nil)
	tests.EncodeTest(t, Encode[int16], nil)
	tests.EncodeTest(t, Encode[int8], nil)
}

func TestDecode(t *testing.T) {
	tests.EncodeTest(t, nil, Decode[uint64])
	tests.EncodeTest(t, nil, Decode[uint32])
	tests.EncodeTest(t, nil, Decode[uint16])
	tests.EncodeTest(t, nil, Decode[uint8])
	tests.EncodeTest(t, nil, Decode[int64])
	tests.EncodeTest(t, nil, Decode[int32])
	tests.EncodeTest(t, nil, Decode[int16])
	tests.EncodeTest(t, nil, Decode[int8])
}

// -------------------------------
// Benchmarks
//

func BenchmarkEncode(b *testing.B) {
	tests.EncodeBenchmark(b, Encode[uint64])
	tests.EncodeBenchmark(b, Encode[uint32])
	tests.EncodeBenchmark(b, Encode[uint16])
	tests.EncodeBenchmark(b, Encode[uint8])
}

func BenchmarkDecode(b *testing.B) {
	tests.DecodeBenchmark(b, nil, Decode[uint64])
	tests.DecodeBenchmark(b, nil, Decode[uint32])
	tests.DecodeBenchmark(b, nil, Decode[uint16])
	tests.DecodeBenchmark(b, nil, Decode[uint8])
}
