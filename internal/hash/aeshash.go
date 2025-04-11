// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hash

import (
	"unsafe"
)

//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

func AesHash(buf []byte, seed uint64) uint64 {
	return uint64(memhash(unsafe.Pointer(&buf[0]), uintptr(seed), uintptr(len(buf))))
}
