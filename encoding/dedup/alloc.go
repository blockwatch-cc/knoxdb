// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"unsafe"
)

type Allocator struct{}

func (a Allocator) Alloc(sz int) any {
	return NewByteArray(sz)
}

func (a Allocator) AllocPtr(sz int) unsafe.Pointer {
	// b is interface!
	b := NewByteArray(sz)
	return unsafe.Pointer(&b)
}

func (a Allocator) Free(val any) {
	dd, ok := val.(ByteArray)
	if ok {
		dd.Release()
	}
}

func (a Allocator) FreePtr(ptr unsafe.Pointer) {
	if ptr == nil {
		return
	}
	b := *(*ByteArray)(ptr)
	b.Release()
}
