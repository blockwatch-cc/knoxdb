// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"iter"
)

type StringMatcher interface {
	MatchEqual(val []byte, bits, mask *Bitset)
	MatchNotEqual(val []byte, bits, mask *Bitset)
	MatchLess(val []byte, bits, mask *Bitset)
	MatchLessEqual(val []byte, bits, mask *Bitset)
	MatchGreater(val []byte, bits, mask *Bitset)
	MatchGreaterEqual(val []byte, bits, mask *Bitset)
	MatchBetween(a, b []byte, bits, mask *Bitset)
	MatchInSet(s any, bits, mask *Bitset)
	MatchNotInSet(s any, bits, mask *Bitset)
}

type StringWriter interface {
	Append([]byte) int
	Reserve(int)
	Set(int, []byte)
	Delete(int, int)
	Clear()
	Cap() int
}

type StringReader interface {
	Len() int
	Size() int
	Get(int) []byte
	AppendTo(StringWriter, []uint32)
	All() iter.Seq2[int, []byte]
	Values() iter.Seq[[]byte]
	Chunks() StringIterator
	MinMax() ([]byte, []byte)
	Cmp(i, j int) int
	Min() []byte
	Max() []byte
}

type StringAccessor interface {
	StringReader
	StringWriter
	Matcher() StringMatcher
	Close()
}

type StringIterator interface {
	// Returns the total number of elements in this vector.
	Len() int

	// Returns an element at position n or zero when out of bounds.
	// Implicitly seeks and decodes the chunk containing n.
	Value(int) []byte

	// Seeks to position n rounded by CHUNK_SIZE and decodes
	// the relevant chunk. Compatible with Next and Get.
	Seek(int) bool

	// Decodes and returns the next chunk at CHUNK_SIZE boundaries
	// and the number of valid elements in the chunk. Past EOF
	// returns nil and zero n.
	Next() (*[CHUNK_SIZE][]byte, int)

	// Skips a chunk efficiently without decoding data and returns
	// the number of elements skipped or zero when at EOF. Users may
	// call skip repeatedly before requesting data from Next.
	Skip() int

	// All iterates over all index/value pairs in the array.
	All() iter.Seq2[int, []byte]

	// Values iterates over all values in the array in order.
	Values() iter.Seq[[]byte]

	// Select iterates over selected positions in the array.
	// The selection may be sorted or unsorted, however sorted
	// selections may experience far better performance on
	// compressed arrays.
	Select([]uint32) iter.Seq[[]byte]

	// Close releases pointers and allows for efficient re-use
	// of iterators. Users are encouraged to call Close after use
	// to reduce allocations and GC overhead.
	Close()
}
