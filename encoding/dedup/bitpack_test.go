// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestBitPack(t *testing.T) {
	rand.Seed(1337)
	for i := 4; i < 25; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			buf := make([]byte, 8192)
			for k := 0; k < 8192*8/i; k++ {
				val := int(rand.Int31n(1<<i - 1))
				pack(buf, k, val, i)
				uval := unpack(buf, k, i)
				if val != uval {
					t.Errorf("Mismatch: %d, %08b -> %d, %08b", val, val, uval, uval)
				}
			}
		})
	}
}

func BenchmarkBitPack(b *testing.B) {
	buf := make([]byte, 8192)
	for d := 4; d < 25; d++ {
		b.Run(strconv.Itoa(d), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pack(buf, i%d, i, d)
			}
		})
	}
}
