// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"bytes"
)

var (
	poison  = []byte{0xfa}
	maskAll = []byte{0xff}
)

func MakePoison(sz int) []byte {
	return bytes.Repeat(poison, sz)
}

// allocate the result bitset and fill all with poison
func MakeBitsPoison(sz int) []byte {
	l := bitFieldLen(sz)
	bits := make([]byte, l+32)
	for i := range 32 {
		bits[l+i] = 0xfa
	}
	bits = bits[:l]
	return bits
}

// allocate the result bitset and fill padding with poison
func MakeBitsAndMaskPoisonTail(sz, tail int, maskBits []byte) ([]byte, []byte) {
	l := bitFieldLen(sz)
	bits := make([]byte, l+tail)
	mask := bytes.Repeat(maskBits, l/len(maskBits))
	for i := range tail {
		bits[l+i] = 0xfa
	}
	bits = bits[:l]
	return bits, mask
}

// allocate the result bitset and fill all with poison
func MakeBitsAndMaskPoison(sz int, maskBits []byte) ([]byte, []byte) {
	l := bitFieldLen(sz)
	bits := make([]byte, l+32)
	mask := bytes.Repeat(maskBits, l/len(maskBits))
	for i := range 32 {
		bits[l+i] = 0xfa
	}
	bits = bits[:l]
	return bits, mask
}

func bitFieldLen(n int) int {
	return roundUpPow2(n, 8) >> 3
}

func roundUpPow2(n int, pow2 int) int {
	return (n + (pow2 - 1)) & ^(pow2 - 1)
}
