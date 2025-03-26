// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitpack

import (
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/types"
)

var (
	BE = binary.BigEndian
)

// Pack packs integer value as n-bit packed integer into buf to position index
// This is a write-once, read-only datastructure. Assumes buffer is zeroed
// at start, does not support value overwrite once written.
func Pack(buf []byte, index, log2 int, value uint64) {
	// shift
	shift := (64 - log2) & 7 * (index + 1) & 7

	// mask & shift value
	val := (value & ((1 << log2) - 1)) << shift

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

// Packer retuns a pack function locked to a specific bit width. Use it
// for slightly faster performance when packing many values at once.
func Packer(log2 int) func(buf []byte, index int, value uint64) {
	mask := uint64((1 << log2) - 1)
	shift1 := (64 - log2) & 7
	return func(buf []byte, index int, value uint64) {
		// shift
		shift := shift1 * (index + 1) & 7

		// mask & shift value
		val := (value & mask) << shift

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
}

// Unpack unpacks integer value from n-bit packed int at position index in buf
func Unpack(buf []byte, index, log2 int) uint64 {
	// output shift
	shift := (64 - log2) & 7 * (index + 1) & 7

	// input position
	pos := (index * log2) >> 3

	// most significant byte
	msb := (log2 + shift - 1) >> 3

	var val uint64
	for i := 0; i <= msb; i++ {
		val <<= 8
		val += uint64(buf[pos+i])
	}

	// shift and mask output
	return (val >> shift) & ((1 << log2) - 1)
}

// Unpacker retuns an unpack function locked to a specific bit width. Use it
// for slightly faster performance when unpacking many values at once.
func Unpacker(log2 int) func(buf []byte, index int) uint64 {
	mask := uint64((1 << log2) - 1)
	shift1 := (64 - log2) & 7
	return func(buf []byte, index int) uint64 {
		// output shift
		shift := shift1 * (index + 1) & 7

		// input position
		pos := (index * log2) >> 3

		// most significant byte
		msb := (log2 + shift - 1) >> 3

		var val uint64
		for i := 0; i <= msb; i++ {
			val <<= 8
			val += uint64(buf[pos+i])
		}

		// shift and mask output
		return (val >> shift) & mask
	}
}

// PackVec packs a vector of unsigned values of type uint8, uint16, uint32 or
// uint64 into buffer at bit width log2.
func PackVec[T types.Unsigned](buf []byte, vals []T, log2 int) {
	pack := Packer(log2)
	for i, v := range vals {
		pack(buf, i, uint64(v))
	}
}

// UnpackVec unpacks a vector of len(vals) unsigned values of type uint8,
// uint16, uint32 or uint64 into vals at bit width log2. Vals must be
// allocated and have the desired length. If buf contains less values the
// function panics.
func UnpackVec[T types.Unsigned](buf []byte, vals []T, log2 int) {
	unpack := Unpacker(log2)
	for i := range vals {
		vals[i] = T(unpack(buf, i))
	}
}
