// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

// log2up returns the log2 of the number that is the next highest power of 2.
func log2up(v int) int {
	for i := 0; i < 63; i++ {
		if 1<<i >= v {
			return i
		}
	}
	return 0
}

// packs integer value as n-bit packed integer into buf to position index
// This is a write-once, read-only datastructure. Assumes buffer is zeroed
// at start, does not support value overwrite once written.
func pack(buf []byte, index, value, log2 int) {
	// shift
	shift := (64 - log2) & 7 * (index + 1) & 7

	// mast & shift value
	val := uint64(value&((1<<log2)-1)) << shift

	// output position
	pos := (index * log2) >> 3

	// most significant byte
	msb := (log2 + shift - 1) >> 3

	// patch into position
	for i := msb; i >= 0; i-- {
		buf[pos+i] |= byte(val)
		val >>= 8
	}
}

// unpacks integer value from n-bit packed int at position index in buf
func unpack(buf []byte, index, log2 int) int {
	// output shift
	shift := (64 - log2) & 7 * (index + 1) & 7

	// input position
	pos := (index * log2) >> 3

	// most significant byte
	msb := (log2 + shift - 1) >> 3

	var val int
	for i := 0; i <= msb; i++ {
		val <<= 8
		val += int(buf[pos+i])
	}

	// shift and mask output
	return (val >> shift) & ((1 << log2) - 1)
}
