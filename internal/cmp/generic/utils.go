// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

func bytemask(size int) byte {
	return byte(0xff >> (7 - uint(size-1)&0x7) & 0xff)
}

func b2u(b bool) (x byte) {
	if b {
		x = 1
	}
	return
}
