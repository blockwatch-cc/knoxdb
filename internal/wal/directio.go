// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// Source modified from https://github.com/ncw/directio
// MIT Copyright (C) 2012 by Nick Craig-Wood http://www.craig-wood.com/nick/

package wal

import (
	"fmt"
	"unsafe"
)

// alignment returns alignment of the block in memory
// with reference to AlignSize
//
// Can't check alignment of a zero sized block as &block[0] is invalid
func alignment(block []byte, sz int) int {
	return int(uintptr(unsafe.Pointer(&block[0])) & uintptr(sz-1))
}

// isAligned checks wether passed byte slice is aligned
func isAligned(block []byte) bool {
	return alignment(block, alignSize) == 0
}

func isAlignedLen(l int) bool {
	return l&(alignSize-1) == 0
}

// makeAligned returns []byte of size sz aligned to a multiple
// of alignSize in memory (must be power of two)
func makeAligned(sz int) []byte {
	block := make([]byte, sz+alignSize)
	if alignSize == 0 {
		return block
	}
	a := alignment(block, alignSize)
	offset := 0
	if a != 0 {
		offset = alignSize - a
	}
	block = block[offset : offset+sz]
	// Can't check alignment of a zero sized block
	if sz != 0 {
		if !isAligned(block) {
			panic(fmt.Errorf("failed to align block"))
		}
	}
	return block
}
