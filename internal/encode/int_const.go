// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"sync"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// TIntegerConstant
type ConstContainer[T types.Integer] struct {
	Val T
	N   int
}

func (c *ConstContainer[T]) Close() {
	putConstContainer[T](c)
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

type ConstFactory struct {
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
}

func newConstContainer[T types.Integer]() IntegerContainer[T] {
	switch (any(T(0))).(type) {
	case int64:
		return constFactory.i64Pool.Get().(IntegerContainer[T])
	case int32:
		return constFactory.i32Pool.Get().(IntegerContainer[T])
	case int16:
		return constFactory.i16Pool.Get().(IntegerContainer[T])
	case int8:
		return constFactory.i8Pool.Get().(IntegerContainer[T])
	case uint64:
		return constFactory.u64Pool.Get().(IntegerContainer[T])
	case uint32:
		return constFactory.u32Pool.Get().(IntegerContainer[T])
	case uint16:
		return constFactory.u16Pool.Get().(IntegerContainer[T])
	case uint8:
		return constFactory.u8Pool.Get().(IntegerContainer[T])
	default:
		return nil
	}
}

func putConstContainer[T types.Integer](c IntegerContainer[T]) {
	switch (any(T(0))).(type) {
	case int64:
		constFactory.i64Pool.Put(c)
	case int32:
		constFactory.i32Pool.Put(c)
	case int16:
		constFactory.i16Pool.Put(c)
	case int8:
		constFactory.i8Pool.Put(c)
	case uint64:
		constFactory.u64Pool.Put(c)
	case uint32:
		constFactory.u32Pool.Put(c)
	case uint16:
		constFactory.u16Pool.Put(c)
	case uint8:
		constFactory.u8Pool.Put(c)
	}
}

var constFactory = ConstFactory{
	i64Pool: sync.Pool{
		New: func() any { return new(ConstContainer[int64]) },
	},
	i32Pool: sync.Pool{
		New: func() any { return new(ConstContainer[int32]) },
	},
	i16Pool: sync.Pool{
		New: func() any { return new(ConstContainer[int16]) },
	},
	i8Pool: sync.Pool{
		New: func() any { return new(ConstContainer[int8]) },
	},
	u64Pool: sync.Pool{
		New: func() any { return new(ConstContainer[uint64]) },
	},
	u32Pool: sync.Pool{
		New: func() any { return new(ConstContainer[uint32]) },
	},
	u16Pool: sync.Pool{
		New: func() any { return new(ConstContainer[uint16]) },
	},
	u8Pool: sync.Pool{
		New: func() any { return new(ConstContainer[uint8]) },
	},
}
