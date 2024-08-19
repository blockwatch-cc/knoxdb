// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx2

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
