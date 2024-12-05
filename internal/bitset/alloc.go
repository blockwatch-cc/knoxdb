// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitset

// alloc without arena for very small sized bitsets
func makeBitset(size int) *Bitset {
	return &Bitset{
		buf:  make([]byte, bitFieldLen(size)),
		cnt:  0,
		size: size,
	}
}

// Note Go 1.12 has built-in clear
func clear(b []byte) {
	if len(b) == 0 {
		return
	}
	b[0] = 0
	for bp := 1; bp < len(b); bp *= 2 {
		copy(b[bp:], b[:bp])
	}
}
