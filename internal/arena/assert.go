// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
//go:build with_assert

package arena

import (
	"fmt"
	"math/bits"
	"sync"
	"unsafe"
)

// counting allocator with assertion

type Allocator interface {
	Alloc(int) any
	Free(any, int)
}

// 1k (10) .. 128k (17) = 8 sync.Pools
type allocator[T any] struct {
	mu    sync.Mutex
	pools [8]*sync.Pool
	track map[uintptr]int
}

const (
	minAllocClass = 10
	maxAllocClass = 17
)

func newAllocator[T any]() *allocator[T] {
	return &allocator[T]{track: make(map[uintptr]int)}
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

	val := a.pool(class).Get()
	s := val.([]T)[:1]
	ptr := uintptr(unsafe.Pointer(&s[0]))
	a.mu.Lock()
	a.track[ptr] = 1
	a.mu.Unlock()
	return val
}

func (a *allocator[T]) Free(val any, sz int) {
	// don't recycle out of bounds or non-power of 2 slices
	class := 63 - bits.LeadingZeros(uint(sz))
	if class < minAllocClass || class > maxAllocClass || bits.OnesCount(uint(sz)) > 1 {
		return
	}

	s := val.([]T)[:1]
	ptr := uintptr(unsafe.Pointer(&s[0]))
	a.mu.Lock()
	a.track[ptr]++
	cnt := a.track[ptr]
	a.mu.Unlock()

	if cnt == 0 {
		panic(fmt.Errorf("free without alloc for %T %p", s[0], &s[0]))
	}
	if cnt > 2 {
		panic(fmt.Errorf("double free for %T %p", s[0], &s[0]))
	}

	a.pool(class).Put(val)
}
