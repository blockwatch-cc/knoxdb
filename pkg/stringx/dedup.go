// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stringx

import (
	"bytes"
	"fmt"
	"sync"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	dedupPool = sync.Pool{
		New: func() any {
			return &DedupPool{hash: make(map[uint64]int)}
		},
	}

	// ensure we implement required interfaces
	_ types.StringAccessor = (*DedupPool)(nil)
)

// DedupPool is a memory efficient pool for variable length strings
// which only stores unique values. This has the potential to massively
// reduce memory consumption when a set contains many duplicates.
// Like StringPool the DedupPool suffers from higher access costs
// due to non-inlinable Get() and range iterator functions.
type DedupPool struct {
	*StringPool
	hash  map[uint64]int // hash -> pos
	oflow []oval         // hash collision overflow
}

type oval struct {
	hash uint64
	pos  int
}

func NewDedupPool(n int) *DedupPool {
	p := dedupPool.Get().(*DedupPool)
	p.StringPool = NewStringPoolSize(n, StringPoolDefaultSize/8)
	return p
}

func (p *DedupPool) Close() {
	p.StringPool.Close()
	p.StringPool = nil
	p.Clear()
	dedupPool.Put(p)
}

func (p *DedupPool) Clear() {
	p.StringPool.Clear()
	clear(p.hash)
	if p.oflow != nil {
		p.oflow = p.oflow[:0]
	}
}

func (p *DedupPool) HeapSize() int {
	return p.StringPool.HeapSize() + len(p.hash)*16 + len(p.oflow)*16 + 24
}

func (p *DedupPool) Append(val []byte) int {
	hash := xxhash.Sum64(val)
	n, ok := p.hash[hash]
	if !ok {
		p.hash[hash] = len(p.ptr)
		return p.StringPool.Append(val)
	}
	if n != 0xFFFFFFFF {
		// check if the new string matches the one we already have at pos n
		if bytes.Equal(p.Get(n), val) {
			p.ptr = append(p.ptr, p.ptr[n])
			return len(p.ptr) - 1
		}
		// no match means the string is new and hashes collide
		p.oflow = append(p.oflow, oval{
			hash: hash,
			pos:  n,
		}, oval{
			hash: hash,
			pos:  len(p.ptr),
		})
		p.hash[hash] = 0xFFFFFFFF
		return p.StringPool.Append(val)
	} else {
		// we already have at least 2 overflow entries for this hash
		for _, v := range p.oflow {
			if v.hash != hash {
				continue
			}
			// check if we have seen the string before
			if bytes.Equal(p.Get(v.pos), val) {
				p.ptr = append(p.ptr, p.ptr[v.pos])
				return len(p.ptr) - 1
			}
		}
		// add another entry
		p.oflow = append(p.oflow, oval{
			hash: hash,
			pos:  len(p.ptr),
		})
		// add the new string
		return p.StringPool.Append(val)
	}
}

func (p *DedupPool) AppendMany(vals ...[]byte) int {
	if len(vals) == 0 {
		return -1
	}
	n := len(p.ptr)
	for _, v := range vals {
		p.Append(v)
	}
	return n
}

func (p *DedupPool) AppendString(val string) int {
	return p.Append(util.UnsafeGetBytes(val))
}

func (p *DedupPool) AppendManyStrings(vals ...string) int {
	if len(vals) == 0 {
		return -1
	}
	n := len(p.ptr)
	for _, v := range vals {
		p.Append(util.UnsafeGetBytes(v))
	}
	return n
}

func (p *DedupPool) Set(i int, val []byte) {
	// not supported
	panic(fmt.Errorf("dedup-pool: set is unsupported"))
}

func (p *DedupPool) Delete(i, j int) {
	// not supported
	panic(fmt.Errorf("dedup-pool: delete is unsupported"))
}
