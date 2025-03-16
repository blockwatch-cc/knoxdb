// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TIntegerRaw
type RawContainer[T types.Integer] struct {
	Values []T
	sz     int
}

func (c *RawContainer[T]) Type() IntegerContainerType {
	return TIntegerRaw
}

func (c *RawContainer[T]) Len() int {
	return len(c.Values)
}

func (c *RawContainer[T]) MaxSize() int {
	return 1 + num.MaxVarintLen32 + c.sz*len(c.Values)
}

func (c *RawContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntegerRaw))
	dst = num.AppendUvarint(dst, uint64(c.sz*len(c.Values)))
	// if cpu.IsBigEndian {
	//  // TODO: flip byte order
	// }
	return append(dst, util.ToByteSlice(c.Values)...)
}

func (c *RawContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntegerRaw) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	buf = buf[n:]
	c.Values = util.FromByteSlice[T](buf[:int(v)])
	return buf[int(v):], nil
}

func (c *RawContainer[T]) Get(n int) T {
	return c.Values[n]
}

func (c *RawContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	for _, v := range sel {
		dst = append(dst, c.Values[v])
	}
	return dst
}

func (c *RawContainer[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	c.Values = vals
	c.sz = ctx.PhyBits / 8
	return c
}

func (c *RawContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *RawContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *RawContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *RawContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *RawContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *RawContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *RawContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *RawContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *RawContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}
