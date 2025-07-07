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

// 1k (10) .. 128k (17) = 8 sync.Pools
type allocator[T any] struct {
	pools [8]*sync.Pool
}

const (
	minAllocClass = 10
	maxAllocClass = 17
)

func newAllocator[T any]() *allocator[T] {
	return &allocator[T]{}
}

func (a *allocator[T]) pool(class int) *sync.Pool {
	idx := class - minAllocClass
	if a.pools[idx] == nil {
		sz := 1 << class
		a.pools[idx] = &sync.Pool{
			New: func() any { return make([]T, 0, sz) },
		}
	}
	return a.pools[idx]
}

func (a *allocator[T]) Alloc(sz int) any {
	class := 63 - bits.LeadingZeros(uint(sz))
	if bits.OnesCount(uint(sz)) > 1 {
		class++
	}
	if class < minAllocClass || class > maxAllocClass {
		return make([]T, 0, max(sz, 8))
	}
	return a.pool(class).Get()
}

func (a *allocator[T]) Free(val any, sz int) {
	// don't recycle out of bounds or non-power of 2 slices
	class := 63 - bits.LeadingZeros(uint(sz))
	if class < minAllocClass || class > maxAllocClass || bits.OnesCount(uint(sz)) > 1 {
		return
	}
	a.pool(class).Put(val)
}
