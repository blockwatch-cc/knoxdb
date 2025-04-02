// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// nolint
package avx512

import (
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
)

func bitmask(i int) byte {
	return byte(1 << uint(i&0x7))
}

func bytemask(size int) byte {
	return byte(0xff >> (7 - uint(size-1)&0x7) & 0xff)
}

func bitFieldLen(n int) int {
	return roundUpPow2(n, 8) >> 3
}

func roundUpPow2(n int, pow2 int) int {
	return (n + (pow2 - 1)) & ^(pow2 - 1)
}

func requireAvx512(t testing.TB) {
	if !util.UseAVX512_F {
		t.Skip("AVX512F not available.")
	}
	if !util.UseAVX512_BW {
		t.Skip("AVX512BW not available.")
	}
}
