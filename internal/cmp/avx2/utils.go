// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package avx2

import (
	"unsafe"
)

func bitFieldLen(n int) int {
	return roundUpPow2(n, 8) >> 3
}

func roundUpPow2(n int, pow2 int) int {
	return (n + (pow2 - 1)) & ^(pow2 - 1)
}

//go:linkname memclrNoHeapPointers runtime.memclrNoHeapPointers
func memclrNoHeapPointers(p unsafe.Pointer, n uintptr)

func memclr(b []byte) {
	if len(b) == 0 {
		return
	}
	p := unsafe.Pointer(&b[0])
	memclrNoHeapPointers(p, uintptr(len(b)))
}
