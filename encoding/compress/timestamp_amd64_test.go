// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package compress

import (
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/util"
)

func BenchmarkDeltaScaleDecodeTimeAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	mod := uint64(1000000000)
	for _, n := range benchmarkSizes {
		a := make([]uint64, n.l)
		for i := 0; i < n.l; i++ {
			a[i] = uint64(rand.Intn(10000))
		}
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				deltaScaleDecodeTimeAVX2(a, mod)
			}
		})
	}
}
