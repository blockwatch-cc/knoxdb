// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package vec

import (
	"math/rand"
	"testing"
)

func TestRoundUpPow2(T *testing.T) {
	for i := uint(1); i <= 31; i++ {
		x := 1<<i + rand.Intn(1<<(i-1))
		v := roundUpPow2(x, 1<<i)
		if v&((1<<i)-1) > 0 {
			T.Errorf("%16x is not multiple of 2^%d (src=%x)", v, i+1, x)
		}
	}
}

func BenchmarkRoundUpPow2(B *testing.B) {
	for i := 0; i < B.N; i++ {
		roundUpPow2(i, 1<<6)
	}
}
