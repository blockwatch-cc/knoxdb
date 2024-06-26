// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"math/rand"
	"strconv"
	"testing"
)

const bitpackBufSize = 32768

// create new buf
func makeBitpackBuf(n int) []byte {
	return make([]byte, n)
}

// create new buf filled with poison
func makeBitpackBufPoison(n int) []byte {
	buf := makeBitpackBuf(n)
	buf[0] = 0xFA
	for bp := 1; bp < len(buf); bp *= 2 {
		copy(buf[bp:], buf[:bp])
	}
	return buf
}

func TestBitPack(t *testing.T) {
	rand.Seed(1337)
	for i := 4; i < 25; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			buf := makeBitpackBuf(bitpackBufSize)
			vals := make([]int, bitpackBufSize*8/i)
			for k := 0; k < bitpackBufSize*8/i; k++ {
				val := int(rand.Int31n((1 << i) - 1))
				pack(buf, k, val, i)
				vals[k] = val
			}
			for k := 0; k < bitpackBufSize*8/i; k++ {
				uval := unpack(buf, k, i)
				if vals[k] != uval {
					t.Errorf("Mismatch: %d, %08b -> %d, %08b", vals[k], vals[k], uval, uval)
					t.FailNow()
				}
			}
		})
	}
}

func TestBitPackMsb(t *testing.T) {
	rand.Seed(1337)
	for i := 4; i < 25; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			buf := makeBitpackBuf(bitpackBufSize)
			val := 1 << (i - 1)
			for k := 0; k < bitpackBufSize*8/i; k++ {
				pack(buf, k, val, i)
			}
			for k := 0; k < bitpackBufSize*8/i; k++ {
				uval := unpack(buf, k, i)
				if val != uval {
					t.Errorf("Mismatch: %d, %08b -> %d, %08b", val, val, uval, uval)
					t.FailNow()
				}
			}
		})
	}
}

func BenchmarkBitPack(b *testing.B) {
	buf := makeBitpackBuf(bitpackBufSize)
	for d := 10; d < 17; d++ {
		b.Run(strconv.Itoa(d), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pack(buf, i%d, i, d)
			}
		})
	}
}

func BenchmarkBitUnpack(b *testing.B) {
	buf := makeBitpackBufPoison(bitpackBufSize)
	for d := 10; d < 17; d++ {
		b.Run(strconv.Itoa(d), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				unpack(buf, i%d, d)
			}
		})
	}
}
