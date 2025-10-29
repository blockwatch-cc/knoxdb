// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package arena

type Integer interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8
}

type Number interface {
	Integer | float64 | float32
}

// arena allocators
const (
	allocTypeInt64 = iota
	allocTypeInt32
	allocTypeInt16
	allocTypeInt8
	allocTypeUint64
	allocTypeUint32
	allocTypeUint16
	allocTypeUint8
	allocTypeFloat64
	allocTypeFloat32
	allocTypeBytesSlice
)

var _arena = newArena()

func AllocInt64(sz int) []int64 {
	return _arena.Alloc(allocTypeInt64, sz).([]int64)
}

func AllocInt32(sz int) []int32 {
	return _arena.Alloc(allocTypeInt32, sz).([]int32)
}

func AllocInt16(sz int) []int16 {
	return _arena.Alloc(allocTypeInt16, sz).([]int16)
}

func AllocInt8(sz int) []int8 {
	return _arena.Alloc(allocTypeInt8, sz).([]int8)
}

func AllocUint64(sz int) []uint64 {
	return _arena.Alloc(allocTypeUint64, sz).([]uint64)
}

func AllocUint32(sz int) []uint32 {
	return _arena.Alloc(allocTypeUint32, sz).([]uint32)
}

func AllocUint16(sz int) []uint16 {
	return _arena.Alloc(allocTypeUint16, sz).([]uint16)
}

func AllocUint8(sz int) []uint8 {
	return _arena.Alloc(allocTypeUint8, sz).([]uint8)
}

func AllocFloat64(sz int) []float64 {
	return _arena.Alloc(allocTypeFloat64, sz).([]float64)
}

func AllocFloat32(sz int) []float32 {
	return _arena.Alloc(allocTypeFloat32, sz).([]float32)
}

func AllocBytes(sz int) []byte {
	return _arena.Alloc(allocTypeUint8, sz).([]byte)
}

func AllocByteSlice(sz int) [][]byte {
	return _arena.Alloc(allocTypeBytesSlice, sz).([][]byte)
}

func Alloc[T Number | []byte](sz int) []T {
	var t T
	switch any(t).(type) {
	case int64:
		return _arena.Alloc(allocTypeInt64, sz).([]T)
	case int32:
		return _arena.Alloc(allocTypeInt32, sz).([]T)
	case int16:
		return _arena.Alloc(allocTypeInt16, sz).([]T)
	case int8:
		return _arena.Alloc(allocTypeInt8, sz).([]T)
	case uint64:
		return _arena.Alloc(allocTypeUint64, sz).([]T)
	case uint32:
		return _arena.Alloc(allocTypeUint32, sz).([]T)
	case uint16:
		return _arena.Alloc(allocTypeUint16, sz).([]T)
	case uint8: // == byte
		return _arena.Alloc(allocTypeUint8, sz).([]T)
	case float32:
		return _arena.Alloc(allocTypeFloat32, sz).([]T)
	case float64:
		return _arena.Alloc(allocTypeFloat64, sz).([]T)
	case []byte: // slice of bytes
		return _arena.Alloc(allocTypeBytesSlice, sz).([]T)
	default:
		return nil
	}
}

func Free[T Number | []byte](val []T) {
	if val == nil {
		return
	}
	var t T
	switch any(t).(type) {
	case int64:
		_arena.Free(allocTypeInt64, val[:0], cap(val))
	case int32:
		_arena.Free(allocTypeInt32, val[:0], cap(val))
	case int16:
		_arena.Free(allocTypeInt16, val[:0], cap(val))
	case int8:
		_arena.Free(allocTypeInt8, val[:0], cap(val))
	case uint64:
		_arena.Free(allocTypeUint64, val[:0], cap(val))
	case uint32:
		_arena.Free(allocTypeUint32, val[:0], cap(val))
	case uint16:
		_arena.Free(allocTypeUint16, val[:0], cap(val))
	case uint8: // == byte
		_arena.Free(allocTypeUint8, val[:0], cap(val))
	case float32:
		_arena.Free(allocTypeFloat32, val[:0], cap(val))
	case float64:
		_arena.Free(allocTypeFloat64, val[:0], cap(val))
	case []byte: // slice of bytes
		_arena.Free(allocTypeBytesSlice, val[:0], cap(val))
	}
}

func Clone[T Number | []byte](val []T) []T {
	clone := Alloc[T](len(val))[:len(val)]
	copy(clone, val)
	return clone
}

// TODO: we could allocate []byte (and [][]byte) only and unsafe-cast
// to user types, this would safe 12x8 sync pools
type arena struct {
	alloc [11]Allocator
}

func newArena() *arena {
	return &arena{
		alloc: [11]Allocator{
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
			newAllocator[[]byte](),
		},
	}
}

func (a *arena) Alloc(typ int, sz int) any {
	return a.alloc[typ].Alloc(sz)
}

func (a *arena) Free(typ int, val any, sz int) {
	a.alloc[typ].Free(val, sz)
}
