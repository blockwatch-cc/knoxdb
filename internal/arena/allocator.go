// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
//go:build !with_assert

package arena

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

type Allocator interface {
	Alloc(int) any
	Free(any, int)
}

// 1k (10) .. 128k (17) = 8 sync.Pools
type allocator[T any] struct {
	pools [8]atomic.Pointer[sync.Pool]
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
	p := a.pools[idx].Load()
	if p == nil {
		sz := 1 << class
		p = &sync.Pool{
			New: func() any { return make([]T, 0, sz) },
		}
		a.pools[idx].Store(p)
	}
	return p
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
