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

func TestDecodeUint64(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}
	tests.EncodeTest[uint64](t, generic.Encode[uint64], DecodeUint64)
}

func TestDecodeUint32(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}
	tests.EncodeTest[uint32](t, generic.Encode[uint32], DecodeUint32)
}

func TestDecodeUint16(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}
	tests.EncodeTest[uint16](t, generic.Encode[uint16], DecodeUint16)
}

func TestDecodeUint8(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}
	tests.EncodeTest[uint8](t, generic.Encode[uint8], DecodeUint8)
}

func BenchmarkDecodeUint64(b *testing.B) {
	tests.DecodeBenchmark[uint64](b, generic.Encode[uint64], DecodeUint64)
}

func BenchmarkDecodeUint32(b *testing.B) {
	tests.DecodeBenchmark[uint32](b, generic.Encode[uint32], DecodeUint32)
}

func BenchmarkDecodeUint16(b *testing.B) {
	tests.DecodeBenchmark[uint16](b, generic.Encode[uint16], DecodeUint16)
}

func BenchmarkDecodeUint8(b *testing.B) {
	tests.DecodeBenchmark[uint8](b, generic.Encode[uint8], DecodeUint8)
}
