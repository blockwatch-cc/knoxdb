// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"sync"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// TIntegerDelta
type DeltaContainer[T types.Integer] struct {
	Delta T
	For   T
	N     int
}

func (c *DeltaContainer[T]) Close() {
	putDeltaContainer[T](c)
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

type DeltaFactory struct {
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
}

func newDeltaContainer[T types.Integer]() IntegerContainer[T] {
	switch any(T(0)).(type) {
	case int64:
		return deltaFactory.i64Pool.Get().(IntegerContainer[T])
	case int32:
		return deltaFactory.i32Pool.Get().(IntegerContainer[T])
	case int16:
		return deltaFactory.i16Pool.Get().(IntegerContainer[T])
	case int8:
		return deltaFactory.i8Pool.Get().(IntegerContainer[T])
	case uint64:
		return deltaFactory.u64Pool.Get().(IntegerContainer[T])
	case uint32:
		return deltaFactory.u32Pool.Get().(IntegerContainer[T])
	case uint16:
		return deltaFactory.u16Pool.Get().(IntegerContainer[T])
	case uint8:
		return deltaFactory.u8Pool.Get().(IntegerContainer[T])
	default:
		return nil
	}
}

func putDeltaContainer[T types.Integer](c IntegerContainer[T]) {
	switch any(T(0)).(type) {
	case int64:
		deltaFactory.i64Pool.Put(c)
	case int32:
		deltaFactory.i32Pool.Put(c)
	case int16:
		deltaFactory.i16Pool.Put(c)
	case int8:
		deltaFactory.i8Pool.Put(c)
	case uint64:
		deltaFactory.u64Pool.Put(c)
	case uint32:
		deltaFactory.u32Pool.Put(c)
	case uint16:
		deltaFactory.u16Pool.Put(c)
	case uint8:
		deltaFactory.u8Pool.Put(c)
	}
}

var deltaFactory = DeltaFactory{
	i64Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[int64]) },
	},
	i32Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[int32]) },
	},
	i16Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[int16]) },
	},
	i8Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[int8]) },
	},
	u64Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[uint64]) },
	},
	u32Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[uint32]) },
	},
	u16Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[uint16]) },
	},
	u8Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[uint8]) },
	},
}
