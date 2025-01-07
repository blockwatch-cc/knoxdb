// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"bytes"

	"blockwatch.cc/knoxdb/pkg/util"
)

func bitFieldLen(n int) int {
	return roundUpPow2(n, 8) >> 3
}

func roundUpPow2(n int, pow2 int) int {
	return (n + (pow2 - 1)) & ^(pow2 - 1)
}

var (
	poison  = []byte{0xfa}
	maskAll = []byte{0xff}
)

func MakePoison(sz int) []byte {
	return bytes.Repeat(poison, sz)
}

// allocate the result bitset and fill padding with poison
func MakeBitsAndMaskPoisonTail(sz, tail int, maskBits []byte) ([]byte, []byte) {
	l := bitFieldLen(sz)
	bits := make([]byte, l+tail)
	mask := bytes.Repeat(maskBits, l/len(maskBits))
	for i := 0; i < tail; i++ {
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
	for i := 0; i < 32; i++ {
		bits[l+i] = 0xfa
	}
	bits = bits[:l]
	return bits, mask
}

func randSlice[T Number](sz int) []T {
	var (
		t T
		s any
	)
	switch any(t).(type) {
	case float32:
		s = any(util.RandFloats[float32](sz))
	case float64:
		s = any(util.RandFloats[float64](sz))
	case int64:
		s = any(util.RandInts[int64](sz))
	case int32:
		s = any(util.RandInts[int32](sz))
	case int16:
		s = any(util.RandInts[int16](sz))
	case int8:
		s = any(util.RandInts[int8](sz))
	case uint64:
		s = any(util.RandUints[uint64](sz))
	case uint32:
		s = any(util.RandUints[uint32](sz))
	case uint16:
		s = any(util.RandUints[uint16](sz))
	case uint8:
		s = any(util.RandUints[uint8](sz))
	}
	return s.([]T)
}
