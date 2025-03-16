// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package arena

import (
	"unsafe"
)

type Integer interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8
}

type Number interface {
	Integer | float64 | float32
}

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

func AllocT[T Integer](sz int) []T {
	switch any(T(0)).(type) {
	case int64:
		return _arena.Alloc(AllocInt64, sz).([]T)
	case int32:
		return _arena.Alloc(AllocInt32, sz).([]T)
	case int16:
		return _arena.Alloc(AllocInt16, sz).([]T)
	case int8:
		return _arena.Alloc(AllocInt8, sz).([]T)
	case uint64:
		return _arena.Alloc(AllocUint64, sz).([]T)
	case uint32:
		return _arena.Alloc(AllocUint32, sz).([]T)
	case uint16:
		return _arena.Alloc(AllocUint16, sz).([]T)
	case uint8:
		return _arena.Alloc(AllocUint8, sz).([]T)
	case float32:
		return _arena.Alloc(AllocFloat32, sz).([]T)
	case float64:
		return _arena.Alloc(AllocFloat64, sz).([]T)
	default:
		return nil
	}
}

func Free(typ int, val any) {
	_arena.Free(typ, val)
}

func FreePtr(typ int, ptr unsafe.Pointer) {
	_arena.FreePtr(typ, ptr)
}

func FreeT[T Integer](val []T) {
	switch any(T(0)).(type) {
	case int64:
		_arena.Free(AllocInt64, val)
	case int32:
		_arena.Free(AllocInt32, val)
	case int16:
		_arena.Free(AllocInt16, val)
	case int8:
		_arena.Free(AllocInt8, val)
	case uint64:
		_arena.Free(AllocUint64, val)
	case uint32:
		_arena.Free(AllocUint32, val)
	case uint16:
		_arena.Free(AllocUint16, val)
	case uint8:
		_arena.Free(AllocUint8, val)
	case float32:
		_arena.Free(AllocFloat32, val)
	case float64:
		_arena.Free(AllocFloat64, val)
	}
}

type arena struct {
	alloc [13]Allocator
}

func newArena() *arena {
	return &arena{
		alloc: [13]Allocator{
			newAllocator[int64](),
			newAllocator[int64](),
			newAllocator[int32](),
			newAllocator[int16](),
			newAllocator[int8](),
			newAllocator[uint64](),
			newAllocator[uint32](),
			newAllocator[uint16](),
			newAllocator[uint8](),
			newAllocator[float64](),
			newAllocator[float32](),
			newAllocator[byte](),
			newAllocator[[]byte](),
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
