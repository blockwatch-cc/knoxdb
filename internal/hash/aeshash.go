// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hash

import (
	"unsafe"
)

//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHash uses AES hash CPU instruction on amd64 and arm64.
func MemHash(buf []byte, seed uint64) uint64 {
	return uint64(memhash(unsafe.Pointer(&buf[0]), uintptr(seed), uintptr(len(buf))))
}
