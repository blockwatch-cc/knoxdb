// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

func ensureBitfieldSize(bits *Bitset, srcsize int) *Bitset {
	if bits == nil {
		bits = NewBitset(srcsize)
	} else {
		bits.Grow(srcsize)
	}
	return bits
}

func bitFieldLen(n int) int {
	return roundUpPow2(n, 8) >> 3
}

func bytemask(size int) byte {
	return byte(0xff >> (7 - uint(size-1)&0x7) & 0xff)
}

func bitmask(i int) byte {
	return byte(1 << uint(i&0x7))
}

func roundUpPow2(n int, pow2 int) int {
	return (n + (pow2 - 1)) & ^(pow2 - 1)
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
