// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitset

import (
	"iter"
)

const CHUNK_SIZE = 128

type BitmapMatcher interface {
	MatchEqual(val bool, bits, mask *Bitset)
	MatchNotEqual(val bool, bits, mask *Bitset)
	MatchLess(val bool, bits, mask *Bitset)
	MatchLessEqual(val bool, bits, mask *Bitset)
	MatchGreater(val bool, bits, mask *Bitset)
	MatchGreaterEqual(val bool, bits, mask *Bitset)
	MatchBetween(a, b bool, bits, mask *Bitset)
	MatchInSet(s any, bits, mask *Bitset)
	MatchNotInSet(s any, bits, mask *Bitset)
}

type BitmapWriter interface {
	Append(bool) int
	Reserve(int)
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
	Ones() iter.Seq[int]
	Values() iter.Seq[bool]
	Chunks() BitmapIterator
	Cmp(i, j int) int
	All() bool  // min
	Some() bool // max
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
	// Returns the total number of elements in this array.
	Len() int

	// Returns an element at position n or zero when out of bounds.
	// Implicitly seeks and decodes the chunk containing n.
	Value(int) bool

	// Seeks to position n rounded by CHUNK_SIZE and decodes
	// the relevant chunk. Compatible with Next and Value.
	Seek(int) bool

	// Decodes and returns the next chunk at CHUNK_SIZE boundaries
	// and the number of valid elements in the chunk. Past EOF
	// returns nil and zero n.
	Next() (*[CHUNK_SIZE]bool, int)

	// Skips a chunk efficiently without decoding data and returns
	// the number of elements skipped or zero when at EOF. Users may
	// call skip repeatedly before requesting data from Next.
	Skip() int

	// Close releases pointers and allows for efficient re-use
	// of iterators. Users are encouraged to call Close after use
	// to reduce allocations and GC overhead.
	Close()

	// All iterates over all index/value pairs in the array.
	All() iter.Seq2[int, bool]

	// Values iterates over all values in the array in order.
	Values() iter.Seq[bool]

	// Select iterates over selected positions in the array.
	Select([]uint32) iter.Seq[bool]
}

// BitmapViewer is an interface for chunk-based iterators that
// return positions of set bits in the bitset.
type BitmapViewer interface {
	// Returns CHUNK_SIZE or less indices with set bits in the bitset.
	// When exhausted returns nil and false.
	Next() ([]int, bool)

	// Close releases pointers and allows for efficient re-use
	// of iterators. Users are encouraged to call Close after use
	// to reduce allocations and GC overhead.
	Close()
}
