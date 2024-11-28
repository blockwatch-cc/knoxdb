// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package arena

import (
	"math/bits"
	"sync"
	"unsafe"
)

type Allocator interface {
	Alloc(int) any
	Free(any)
	AllocPtr(int) unsafe.Pointer
	FreePtr(unsafe.Pointer)
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
	a.pools[idx].Put(slice[:0])
}

func (a *allocator[T]) AllocPtr(sz int) unsafe.Pointer {
	v := a.Alloc(sz).([]T)
	return unsafe.Pointer(&v)
}

func (a *allocator[T]) FreePtr(ptr unsafe.Pointer) {
	v := *(*[]T)(ptr)
	a.Free(v)
}

type nullAllocator struct{}

func (_ nullAllocator) Alloc(_ int) any               { return nil }
func (_ nullAllocator) AllocPtr(_ int) unsafe.Pointer { return nil }
func (_ nullAllocator) Free(_ any)                    {}
func (_ nullAllocator) FreePtr(_ unsafe.Pointer)      {}
