// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
//go:build !with_assert

package arena

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

const (
	minAllocClass = 7                                 // 128 byte
	maxAllocClass = 20                                // 1MB
	numClasses    = maxAllocClass - minAllocClass + 1 // 14 pools
)

type goAllocator struct {
	pools [numClasses]atomic.Pointer[sync.Pool]
}

func newGoAllocator() Allocator {
	return &goAllocator{}
}

func (a *goAllocator) Alloc(sz int) []byte {
	class := 63 - bits.LeadingZeros(uint(sz))
	if bits.OnesCount(uint(sz)) > 1 {
		class++
	}
	if class > maxAllocClass {
		return make([]byte, sz)
	}
	if class < minAllocClass {
		class = minAllocClass
	}
	return (*a.pool(class).Get().(*[]byte))[:sz]
}

func (a *goAllocator) Free(val []byte) {
	// don't recycle out of bounds or non-power of 2 slices
	sz := uint(cap(val))
	class := 63 - bits.LeadingZeros(sz)
	if class < minAllocClass || class > maxAllocClass || bits.OnesCount(sz) > 1 {
		return
	}
	a.pool(class).Put(&val)
}

// lazy allocate sync pools
func (a *goAllocator) pool(class int) *sync.Pool {
	idx := class - minAllocClass
	p := a.pools[idx].Load()
	if p == nil {
		sz := 1 << class
		p = &sync.Pool{
			New: func() any {
				buf := make([]byte, 0, sz)
				return &buf
			},
		}
		a.pools[idx].Store(p)
	}
	return p
}
