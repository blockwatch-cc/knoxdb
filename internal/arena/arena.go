// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package arena

import (
	"unsafe"
)

// arena allocators
const (
	AllocTime = iota
	AllocInt64
	AllocInt32
	AllocInt16
	AllocInt8
	AllocUint64
	AllocUint32
	AllocUint16
	AllocUint8
	AllocFloat64
	AllocFloat32
	AllocBytes
	AllocBytesSlice
)

var _arena = newArena()

func Alloc(typ int, sz int) any {
	return _arena.Alloc(typ, sz)
}

func AllocPtr(typ int, sz int) unsafe.Pointer {
	return _arena.AllocPtr(typ, sz)
}

func Free(typ int, val any) {
	_arena.Free(typ, val)
}

func FreePtr(typ int, ptr unsafe.Pointer) {
	_arena.FreePtr(typ, ptr)
}

type arena struct {
	alloc [17]Allocator
}

func newArena() *arena {
	return &arena{
		alloc: [17]Allocator{
			newAllocator[int64](),   // BlockTime
			newAllocator[int64](),   // BlockInt64
			newAllocator[int32](),   // BlockInt32
			newAllocator[int16](),   // BlockInt16
			newAllocator[int8](),    // BlockInt8
			newAllocator[uint64](),  // BlockUint64
			newAllocator[uint32](),  // BlockUint32
			newAllocator[uint16](),  // BlockUint16
			newAllocator[uint8](),   // BlockUint8
			newAllocator[float64](), // BlockFloat64
			newAllocator[float32](), // BlockFloat32
			newAllocator[byte](),    // BlockBytes !! unused in block, used in bitset
			newAllocator[[]byte](),  // extra: used in dedup
		},
	}
}

func (a *arena) Alloc(typ int, sz int) any {
	return a.alloc[typ].Alloc(sz)
}

func (a *arena) AllocPtr(typ int, sz int) unsafe.Pointer {
	return a.alloc[typ].AllocPtr(sz)
}

func (a *arena) Free(typ int, val any) {
	if val == nil {
		return
	}
	a.alloc[typ].Free(val)
}

func (a *arena) FreePtr(typ int, ptr unsafe.Pointer) {
	if ptr == nil {
		return
	}
	a.alloc[typ].FreePtr(ptr)
}
