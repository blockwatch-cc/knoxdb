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
}

type StringReader interface {
	Len() int
	Size() int
	Get(int) []byte
	AppendTo(StringWriter, []uint32)
	Iterator() iter.Seq2[int, []byte]
}

type StringAccessor interface {
	StringReader
	StringWriter
	// Chunks() VectorIterator[E]
	// Slice() []E
	Matcher() StringMatcher
	MinMax() ([]byte, []byte)
	Cmp(i, j int) int
}
