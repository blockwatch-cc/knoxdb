// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitset

import (
	"iter"
	"sync"
)

// Values returns a sequence that ranges over all bits returning
// whether they are set or not.
func (s *Bitset) Values() iter.Seq[bool] {
	return func(yield func(bool) bool) {
		var i int
		for _, b := range s.buf {
			for k := range 8 {
				if i > s.size || !yield(b&bitmask[k] > 0) {
					return
				}
				i++
			}
		}
	}
}

func (s *Bitset) Select(sel []uint32) iter.Seq[bool] {
	return func(yield func(bool) bool) {
		for _, i := range sel {
			if !yield(s.Contains(int(i))) {
				return
			}
		}
	}
}

var chunkFactory = sync.Pool{
	New: func() any { return new(ChunkIterator) },
}

// ChunkIterator is a convenience helper to iterate through all
// values of a bitset
type ChunkIterator struct {
	idx  [CHUNK_SIZE]bool
	set  *Bitset
	base int
	same bool
}

func (s *Bitset) Chunks() BitmapIterator {
	it := chunkFactory.Get().(*ChunkIterator)
	it.set = s
	it.base = 0
	it.same = false
	_ = s.Count()

	switch {
	case s.All():
		for i := range it.idx {
			it.idx[i] = true
		}
		it.same = true
	case s.None():
		clear(it.idx[:])
		it.same = true
	}

	return it
}

func (it *ChunkIterator) Len() int {
	return it.set.size
}

func (it *ChunkIterator) Value(i int) bool {
	return it.set.Get(i)
}

func (it *ChunkIterator) Seek(n int) bool {
	if n < 0 || n >= it.set.size {
		it.base = it.set.size
		return false
	}
	it.base = n
	return true
}

func (it *ChunkIterator) Skip() int {
	n := min(CHUNK_SIZE, it.set.size-it.base)
	it.base += n
	return n
}

func (it *ChunkIterator) Next() (*[CHUNK_SIZE]bool, int) {
	if it.base >= it.set.size {
		return nil, 0
	}
	n := min(it.set.size-it.base, CHUNK_SIZE)

	// update when not all values are the same
	if !it.same {
		for i := range n {
			it.idx[i] = (it.set.buf[it.base>>3] & bitmask[it.base&7]) > 0
			it.base++
		}
	} else {
		it.base += n
	}

	return &it.idx, n
}

func (c *ChunkIterator) Close() {
	c.set = nil
	chunkFactory.Put(c)
}

func (c *ChunkIterator) All() iter.Seq2[int, bool] {
	return func(yield func(int, bool) bool) {
		var i int
		for v := range c.set.Values() {
			if !yield(i, v) {
				return
			}
			i++
		}
	}
}

func (c *ChunkIterator) Values() iter.Seq[bool] {
	return c.set.Values()
}

func (c *ChunkIterator) Select(sel []uint32) iter.Seq[bool] {
	return c.set.Select(sel)
}
