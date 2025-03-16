// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// TIntegerDelta
type DeltaContainer[T types.Integer] struct {
	Delta T
	For   T
	N     int
}

func (c *DeltaContainer[T]) Type() IntegerContainerType {
	return TIntegerDelta
}

func (c *DeltaContainer[T]) Len() int {
	return c.N
}

func (c *DeltaContainer[T]) MaxSize() int {
	return 1 + 2*num.MaxVarintLen64 + num.MaxVarintLen32
}

func (c *DeltaContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntegerDelta))
	dst = num.AppendUvarint(dst, uint64(c.For))
	dst = num.AppendUvarint(dst, uint64(c.Delta))
	return num.AppendUvarint(dst, uint64(c.N))
}

func (c *DeltaContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntegerDelta) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	c.For = T(v)
	buf = buf[n:]
	v, n = num.Uvarint(buf)
	c.Delta = T(v)
	buf = buf[n:]
	v, n = num.Uvarint(buf)
	c.N = int(v)
	return buf[n:], nil
}

func (c *DeltaContainer[T]) Get(n int) T {
	return c.Delta*T(n) + c.For
}

func (c *DeltaContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	for _, v := range sel {
		dst = append(dst, c.Delta*T(v)+c.For)
	}
	return dst
}

func (c *DeltaContainer[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	c.For = vals[0]
	c.Delta = ctx.Delta
	c.N = len(vals)
	return c
}

func (c *DeltaContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DeltaContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DeltaContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DeltaContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DeltaContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DeltaContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DeltaContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DeltaContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *DeltaContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}
