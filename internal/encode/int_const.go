// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// TIntegerConstant
type ConstContainer[T types.Integer] struct {
	Val T
	N   int
}

func (c *ConstContainer[T]) Type() IntegerContainerType {
	return TIntegerConstant
}

func (c *ConstContainer[T]) Len() int {
	return c.N
}

func (c *ConstContainer[T]) MaxSize() int {
	return 1 + num.MaxVarintLen64 + num.MaxVarintLen32
}

func (c *ConstContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntegerConstant))
	dst = num.AppendUvarint(dst, uint64(c.Val))
	return num.AppendUvarint(dst, uint64(c.N))
}

func (c *ConstContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntegerConstant) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	c.Val = T(v)
	buf = buf[n:]
	v, n = num.Uvarint(buf)
	c.N = int(v)
	return buf[n:], nil
}

func (c *ConstContainer[T]) Get(_ int) T {
	return c.Val
}

func (c *ConstContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	for range sel {
		dst = append(dst, c.Val)
	}
	return dst
}

func (c *ConstContainer[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	c.Val = ctx.Min
	c.N = len(vals)
	return c
}

func (c *ConstContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *ConstContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *ConstContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *ConstContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *ConstContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *ConstContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *ConstContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *ConstContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *ConstContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}
