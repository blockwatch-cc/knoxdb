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
}

type StringWriter interface {
	Append([]byte)
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
	Iterator() iter.Seq2[int, []byte]
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
	Get(int) []byte

	// Seeks to position n rounded by CHUNK_SIZE and decodes
	// the relevant chunk. Compatible with NextChunk and Get.
	Seek(int) bool

	// Decodes and returns the next chunk at CHUNK_SIZE boundaries
	// and the number of valid elements in the chunk. Past EOF
	// returns nil and zero n.
	NextChunk() (*[CHUNK_SIZE][]byte, int)

	// Skips a chunk efficiently without decoding data and returns
	// the number of elements skipped or zero when at EOF. Users may
	// call skip repeatedly before requesting data from NextChunk.
	SkipChunk() int

	// Close releases pointers and allows for efficient re-use
	// of iterators. Users are encouraged to call Close after use
	// to reduce allocations and GC overhead.
	Close()
}
