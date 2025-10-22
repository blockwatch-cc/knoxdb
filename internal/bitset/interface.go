// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitset

import (
	"iter"
)

type BitmapMatcher interface {
	MatchEqual(val bool, bits, mask *Bitset)
	MatchNotEqual(val bool, bits, mask *Bitset)
	MatchLess(val bool, bits, mask *Bitset)
	MatchLessEqual(val bool, bits, mask *Bitset)
	MatchGreater(val bool, bits, mask *Bitset)
	MatchGreaterEqual(val bool, bits, mask *Bitset)
	MatchBetween(a, b bool, bits, mask *Bitset)
}

type BitmapWriter interface {
	Append(bool) int
	Set(int)
	Unset(int)
	Delete(int, int)
	Clear()
	Cap() int
}

type BitmapReader interface {
	Len() int
	Size() int
	Get(int) bool
	AppendTo(*Bitset, []uint32)
	Iterator() iter.Seq[int]
	Chunks() BitmapIterator
	Cmp(i, j int) int
	Any() bool // max
	All() bool // min
	None() bool
}

type BitmapAccessor interface {
	BitmapReader
	BitmapWriter
	Matcher() BitmapMatcher
	Writer() *Bitset
	Close()
}

type BitmapIterator interface {
	// Returns CHUNK_SIZE or less indices with set bits in the bitset.
	// When exchausted returns nil and false.
	Next() ([]int, bool)

	// Close releases pointers and allows for efficient re-use
	// of iterators. Users are encouraged to call Close after use
	// to reduce allocations and GC overhead.
	Close()
}
