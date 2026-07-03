// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
//go:build with_assert

package arena

import (
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
)

const (
	minAllocClass = 7                                 // 128 byte
	maxAllocClass = 23                                // 16 MB
	numClasses    = maxAllocClass - minAllocClass + 1 // 17 pools
)

// counting allocator with assertion
type countAllocator struct {
	mu    sync.Mutex
	pools [numClasses]atomic.Pointer[sync.Pool]
	track map[*byte]int
}

func newGoAllocator() Allocator {
	return &countAllocator{track: make(map[*byte]int)}
}

func (a *countAllocator) Alloc(sz int) []byte {
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
	buf := (*a.pool(class).Get().(*[]byte))[:sz]
	a.mu.Lock()
	a.track[&buf[0]] = 1
	a.mu.Unlock()
	return buf
}

func (a *countAllocator) Free(val []byte) {
	// don't recycle out of bounds or non-power of 2 slices
	sz := uint(cap(val))
	class := 63 - bits.LeadingZeros(sz)
	if class < minAllocClass || class > maxAllocClass || bits.OnesCount(sz) > 1 {
		return
	}

	ptr := &val[:1][0]
	a.mu.Lock()
	a.track[ptr]++
	cnt := a.track[ptr]
	a.mu.Unlock()

	if cnt == 0 {
		panic(fmt.Errorf("free without alloc for %p", ptr))
	}
	if cnt > 2 {
		panic(fmt.Errorf("double free for %p", ptr))
	}

	a.pool(class).Put(&val)
}

func (a *countAllocator) pool(class int) *sync.Pool {
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
