// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx512

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cpu"
	"blockwatch.cc/knoxdb/internal/encode/s8b/generic"
	"blockwatch.cc/knoxdb/internal/encode/s8b/tests"
)

func TestDecodeUint64(t *testing.T) {
	if !cpu.UseAVX512_F {
		t.Skip()
	}
	tests.EncodeTest[uint64](t, generic.Encode[uint64], DecodeUint64)
}

func BenchmarkDecodeUint64(b *testing.B) {
	if !cpu.UseAVX512_F {
		b.Skip()
	}
	tests.DecodeBenchmark[uint64](b, generic.Encode[uint64], DecodeUint64)
}
