// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore

package hashprobe

import (
	"unsafe"
)

//go:noescape
func radixSortAVX2(ptr unsafe.Pointer, len int)

func Sort64[T Integer](vs []T, shift int) {
	if len(vs) < 64 {
		for i := 0; i < len(vs); i++ {
			for j := i; j > 0 && vs[j-1] > vs[j]; j-- {
				vs[j-1], vs[j] = vs[j], vs[j-1]
			}
		}
		return
	}
	radixSortAVX2(unsafe.Pointer(&vs[0]), len(vs))
}
