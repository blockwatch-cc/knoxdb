// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Test-usage only

package tests

import (
	"fmt"
	"math"
	"math/bits"

	"blockwatch.cc/knoxdb/pkg/util"
)

func Popcount(buf []byte) int {
	var cnt int
	for _, c := range buf {
		cnt += bits.OnesCount8(c)
	}
	return cnt
}

func F(s string, args ...interface{}) string {
	return fmt.Sprintf(s, args...)
}

func FillBitset(buf []byte, size int, val byte) []byte {
	if len(buf) == 0 {
		buf = make([]byte, bitFieldLen(size))
	}
	buf[0] = val
	for bp := 1; bp < len(buf); bp *= 2 {
		copy(buf[bp:], buf[:bp])
	}
	buf[len(buf)-1] &= bytemask(size)
	return buf
}

func FillBitsetSaw(buf []byte, size int) []byte {
	if len(buf) == 0 {
		buf = make([]byte, bitFieldLen(size))
	}
	// generate the first sawtooth
	for i := 0; i < 256 && i < len(buf); i++ {
		buf[i] = byte(i)
	}
	// concat again and again, we make it one shorter to avoid a symetric vector
	for bp := 256; bp < len(buf); bp = 2*bp - 1 {
		copy(buf[bp:], buf[:bp])
	}
	buf[len(buf)-1] &= bytemask(size)
	return buf
}

func FillBitsetRand(buf []byte, size int, dense float64) []byte {
	if len(buf) == 0 {
		buf = make([]byte, bitFieldLen(size))
	} else {
		for i := range buf {
			buf[i] = 0
		}
	}
	appbitcount := int(math.Ceil(dense * float64(size)))
	for ccount := 0; ccount < appbitcount; {
		bit := util.RandIntn(size)
		bef := buf[bit/8]
		aft := bef | 0x01<<(bit%8)
		if bef != aft {
			ccount++
		}
		buf[bit/8] = aft
	}
	if appbitcount != Popcount(buf) {
		panic("fillBitsetRand: wrong number of bits")
	}

	return buf
}

func bitFieldLen(n int) int {
	return roundUpPow2(n, 8) >> 3
}

func bytemask(size int) byte {
	return byte(0xff >> (7 - uint(size-1)&0x7) & 0xff)
}

func roundUpPow2(n int, pow2 int) int {
	return (n + (pow2 - 1)) & ^(pow2 - 1)
}
