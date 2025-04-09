package main

import (
	"math/bits"
	"sync"
)

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
	default:
		return nil
	}
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

func Free(typ int, val any) {
	_arena.Free(typ, val)
}

func Alloc(typ int, sz int) any {
	return _arena.Alloc(typ, sz)
}

var _arena = newArena()

type arena struct {
	alloc [13]Allocator
}

func (a *arena) Alloc(typ int, sz int) any {
	return a.alloc[typ].Alloc(sz)
}

func (a *arena) Free(typ int, val any) {
	if val == nil {
		return
	}
	a.alloc[typ].Free(val)
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

type Allocator interface {
	Alloc(int) any
	Free(any)
}

// 1k (10) .. 128k (17) .. 32M (25) = 16 sync.Pools
type allocator[T any] struct {
	pools [16]*sync.Pool
}

const (
	minAllocClass = 10
	maxAllocClass = 25
)

func newAllocator[T any]() *allocator[T] {
	a := &allocator[T]{}
	for i := range a.pools {
		sz := 1 << (minAllocClass + i)
		a.pools[i] = &sync.Pool{
			New: func() any { return make([]T, 0, sz) },
		}
	}
	return a
}

func (a *allocator[T]) Alloc(sz int) any {
	class := 63 - bits.LeadingZeros(uint(sz))
	if bits.OnesCount(uint(sz)) > 1 {
		class++
	}
	if class < minAllocClass || class > maxAllocClass {
		return make([]T, 0, sz)
	}
	idx := class - minAllocClass
	return a.pools[idx].Get()
}

func (a *allocator[T]) Free(val any) {
	slice, ok := val.([]T)
	if !ok {
		return
	}
	sz := cap(slice)

	// don't recycle out of bounds or non-power of 2 slices
	class := 63 - bits.LeadingZeros(uint(sz))
	if class < minAllocClass || class > maxAllocClass || bits.OnesCount(uint(sz)) > 1 {
		return
	}
	idx := class - minAllocClass

	// nolint:staticcheck
	a.pools[idx].Put(slice[:0])
}
