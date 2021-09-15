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

// packs integer value as log2-bit integer into buf
func pack(buf []byte, index, value, log2 int) {
	// mask
	value = value & ((1 << log2) - 1)

	// shift
	shift := (64 - log2) & 7 * (index + 1) & 7
	val := uint64(value) << shift

	// output position
	pos := (index * log2) >> 3

	// most significant byte
	msb := (log2+shift+7)>>3 - 1
	for i := msb; i >= 0; i-- {
		buf[pos+i] |= byte(val)
		val >>= 8
	}
}

// unpacks integer value as log2-bit integer from buf
func unpack(buf []byte, index, log2 int) int {
	// output shift
	shift := (64 - log2) & 7 * (index + 1) & 7

	// output mask
	mask := ((1 << log2) - 1)

	// input position
	pos := (index * log2) >> 3

	// most significant byte
	msb := (log2+shift+7)>>3 - 1

	var val int
	for i := 0; i <= msb; i++ {
		val <<= 8
		val += int(buf[pos+i])
	}
	return (val >> shift) & mask
}
