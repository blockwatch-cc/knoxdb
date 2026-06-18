// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package arena

type Allocator interface {
	Alloc(int) []byte
	Free([]byte)
}

var _alloc = newGoAllocator()

func Alloc[T FixedSizeType](sz int) []T {
	if sz <= 0 {
		return nil
	}
	return FromBytes[T](_alloc.Alloc(SizeFor[T]() * sz))[:0]
}

func Free[T FixedSizeType](val []T) {
	if val == nil {
		return
	}
	_alloc.Free(ToBytes(val))
}
