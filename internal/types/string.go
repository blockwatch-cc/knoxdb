// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"iter"

	"blockwatch.cc/knoxdb/pkg/util"
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

type StringSetter = util.StringSetter

type StringAccessor interface {
	Len() int
	Size() int
	Get(int) []byte
	Iterator() iter.Seq[[]byte]
	AppendTo(StringSetter, []uint32)
}
