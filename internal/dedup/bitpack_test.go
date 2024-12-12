// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"strconv"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
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
	for i := 4; i < 25; i++ {
		buf := makeBitpackBuf(bitpackBufSize)
		vals := make([]int, bitpackBufSize*8/i)
		for k := 0; k < bitpackBufSize*8/i; k++ {
			val := int(util.RandInt32n((1 << i) - 1))
			pack(buf, k, val, i)
			vals[k] = val
		}
		for k := 0; k < bitpackBufSize*8/i; k++ {
			uval := unpack(buf, k, i)
			assert.Equalf(t, vals[k], uval, "%d: Mismatch: %d, %08b -> %d, %08b", i, vals[k], vals[k], uval, uval)
		}
	}
}

func TestBitPackMsb(t *testing.T) {
	for i := 4; i < 25; i++ {
		buf := makeBitpackBuf(bitpackBufSize)
		val := 1 << (i - 1)
		for k := 0; k < bitpackBufSize*8/i; k++ {
			pack(buf, k, val, i)
		}
		for k := 0; k < bitpackBufSize*8/i; k++ {
			uval := unpack(buf, k, i)
			assert.Equalf(t, val, uval, "%d: Mismatch: %d, %08b -> %d, %08b", i, val, val, uval, uval)
		}
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
