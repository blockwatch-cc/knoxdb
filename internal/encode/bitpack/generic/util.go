// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/bitset"
)

type Bitset = bitset.Bitset

var (
	BE = binary.BigEndian
)

// u16be reads short bit-packed uint16 values and shifts them like regular
// big endian u16 values to be compatible with shifts in bitpacked compare.
func u16be(buf []byte) uint16 {
	if len(buf) == 1 {
		return uint16(buf[0]) << 8
	}
	return BE.Uint16(buf)
}

// u32be reads short bit-packed uint32 values and shifts them like regular
// big endian u32 values to be compatible with shifts in bitpacked compare.
func u32be(buf []byte) uint32 {
	switch len(buf) {
	case 2:
		return uint32(buf[0])<<24 | uint32(buf[1])<<16
	case 3:
		return uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8
	}
	return BE.Uint32(buf)
}

// u64be reads short bit-packed uint64 values and shifts them like regular
// big endian u64 values to be compatible with shifts in bitpacked compare.
func u64be(buf []byte) uint64 {
	switch len(buf) {
	case 4:
		return uint64(buf[0])<<56 | uint64(buf[1])<<48 | uint64(buf[2])<<40 |
			uint64(buf[3])<<32
	case 5:
		return uint64(buf[0])<<56 | uint64(buf[1])<<48 | uint64(buf[2])<<40 |
			uint64(buf[3])<<32 | uint64(buf[4])<<24
	case 6:
		return uint64(buf[0])<<56 | uint64(buf[1])<<48 | uint64(buf[2])<<40 |
			uint64(buf[3])<<32 | uint64(buf[4])<<24 | uint64(buf[5])<<16
	case 7:
		return uint64(buf[0])<<56 | uint64(buf[1])<<48 | uint64(buf[2])<<40 |
			uint64(buf[3])<<32 | uint64(buf[4])<<24 | uint64(buf[5])<<16 |
			uint64(buf[6])<<8
	}
	return BE.Uint64(buf)
}

// Bool2Byte - compiler optimized to 1 opcode CSET
// See issue 6011. https://tip.golang.org/src/cmd/compile/internal/ssa/phiopt.go
func b2b(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
