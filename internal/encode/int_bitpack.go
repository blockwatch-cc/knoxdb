// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"blockwatch.cc/knoxdb/internal/dedup"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// TIntegerBitpacked
type BitpackContainer[T types.Integer] struct {
	For    T
	Packed []byte
	Log2   int
	N      int
}

func (c *BitpackContainer[T]) Type() IntegerContainerType {
	return TIntegerBitpacked
}

func (c *BitpackContainer[T]) Len() int {
	return c.N
}

func (c *BitpackContainer[T]) MaxSize() int {
	// Typ (1) + FOR (varint) + log2 (1) + n (varint) + bits (variable)
	return 2 + 2*num.MaxVarintLen64 + len(c.Packed)
}

func (c *BitpackContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntegerBitpacked))
	dst = num.AppendUvarint(dst, uint64(c.For))
	dst = num.AppendUvarint(dst, uint64(c.Log2))
	dst = num.AppendUvarint(dst, uint64(c.N))
	return append(dst, c.Packed...)
}

func (c *BitpackContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntegerBitpacked) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	c.For = T(v)
	buf = buf[n:]
	v, n = num.Uvarint(buf)
	c.Log2 = int(v)
	buf = buf[n:]
	v, n = num.Uvarint(buf)
	c.N = int(v)
	buf = buf[n:]

	// reference next sz bytes as bitpacked data
	sz := c.Log2*c.N/8 + 1
	c.Packed = buf[:sz]
	return buf[sz:], nil
}

func (c *BitpackContainer[T]) Get(n int) T {
	return T(dedup.Unpack(c.Packed, n, c.Log2)) + c.For
}

func (c *BitpackContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	for _, v := range sel {
		dst = append(dst, c.Get(int(v)))
	}
	return dst
}

func (c *BitpackContainer[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	c.Packed = make([]byte, ctx.UseBits*len(vals)/8+1)
	c.Log2 = ctx.UseBits
	c.N = len(vals)
	c.For = ctx.Min
	for i, v := range vals {
		dedup.Pack(c.Packed, i, int(v-c.For), c.Log2)
	}
	return c
}

func (c *BitpackContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *BitpackContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *BitpackContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *BitpackContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *BitpackContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *BitpackContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *BitpackContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *BitpackContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *BitpackContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}
