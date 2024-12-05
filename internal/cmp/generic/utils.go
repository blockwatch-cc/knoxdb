// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

func bytemask(size int) byte {
	return byte(0xff >> (7 - uint(size-1)&0x7) & 0xff)
}

// func bitFieldLen(n int) int {
// 	return roundUpPow2(n, 8) >> 3
// }

// func roundUpPow2(n int, pow2 int) int {
// 	return (n + (pow2 - 1)) & ^(pow2 - 1)
// }
