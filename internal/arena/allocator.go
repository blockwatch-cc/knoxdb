// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package arena

import (
	"math/bits"
	"sync"
)

type Allocator interface {
	Alloc(int) any
	Free(any, int)
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

func (a *allocator[T]) Free(val any, sz int) {
	// don't recycle out of bounds or non-power of 2 slices
	class := 63 - bits.LeadingZeros(uint(sz))
	if class < minAllocClass || class > maxAllocClass || bits.OnesCount(uint(sz)) > 1 {
		return
	}
	idx := class - minAllocClass
	a.pools[idx].Put(val)
}
