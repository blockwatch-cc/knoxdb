// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"bytes"

	"blockwatch.cc/knoxdb/internal/bitset/generic"
)

var poison = []byte{0xfa}

var BitFieldLen = generic.BitFieldLen

func MakePoison(sz int) []byte {
	return bytes.Repeat(poison, sz)
}

// allocate the result bitset and fill all with poison
func MakeBitsPoison(sz int) []byte {
	l := BitFieldLen(sz)
	bits := make([]byte, l+32)
	for i := range 32 {
		bits[l+i] = 0xfa
	}
	bits = bits[:l]
	return bits
}

// allocate the result bitset and fill padding with poison
func MakeBitsAndMaskPoisonTail(sz, tail int, maskBits []byte) ([]byte, []byte) {
	l := BitFieldLen(sz)
	bits := make([]byte, l+tail)
	var mask []byte
	if len(maskBits) > 0 && maskBits[0] != 0xff && maskBits[0] != 0 {
		mask = bytes.Repeat(maskBits, l/len(maskBits))
	}
	for i := range tail {
		bits[l+i] = 0xfa
	}
	bits = bits[:l]
	return bits, mask
}

// allocate the result bitset and fill all with poison
func MakeBitsAndMaskPoison(sz int, maskBits []byte) ([]byte, []byte) {
	l := BitFieldLen(sz)
	bits := make([]byte, l+32)
	var mask []byte
	if len(maskBits) > 0 && maskBits[0] != 0xff && maskBits[0] != 0 {
		mask = bytes.Repeat(maskBits, l/len(maskBits))
	}
	for i := range 32 {
		bits[l+i] = 0xfa
	}
	bits = bits[:l]
	return bits, mask
}
