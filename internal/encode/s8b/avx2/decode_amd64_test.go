// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/s8b/generic"
	"blockwatch.cc/knoxdb/internal/encode/s8b/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

func TestDecode(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}
	tests.EncodeTest[uint64](t, generic.Encode[uint64], DecodeUint64)
	tests.EncodeTest[uint32](t, generic.Encode[uint32], DecodeUint32)
	tests.EncodeTest[uint16](t, generic.Encode[uint16], DecodeUint16)
	tests.EncodeTest[uint8](t, generic.Encode[uint8], DecodeUint8)
}

func BenchmarkDecode(b *testing.B) {
	tests.DecodeBenchmark[uint64](b, generic.Encode[uint64], DecodeUint64)
	tests.DecodeBenchmark[uint32](b, generic.Encode[uint32], DecodeUint32)
	tests.DecodeBenchmark[uint16](b, generic.Encode[uint16], DecodeUint16)
	tests.DecodeBenchmark[uint8](b, generic.Encode[uint8], DecodeUint8)
}
