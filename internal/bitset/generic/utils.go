// Copyright (c) 2020-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

var (
	BitFieldLen = bitFieldLen
	Bytemask    = bytemask
	RoundUpPow2 = roundUpPow2
)

func bitFieldLen(n int) int {
	return roundUpPow2(n, 8) >> 3
}

func bytemask(size int) byte {
	return byte(0xff >> (7 - uint(size-1)&0x7) & 0xff)
}

func roundUpPow2(n int, pow2 int) int {
	return (n + (pow2 - 1)) & ^(pow2 - 1)
}

var Bitmask = [8]byte{0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80}
