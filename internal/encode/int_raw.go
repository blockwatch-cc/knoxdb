// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"sync"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TIntegerRaw
type RawContainer[T types.Integer] struct {
	Values []T
	sz     int
}

func (c *RawContainer[T]) Close() {
	c.Values = nil
	putRawContainer[T](c)
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

type RawFactory struct {
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
}

func newRawContainer[T types.Integer]() IntegerContainer[T] {
	switch any(T(0)).(type) {
	case int64:
		return rawFactory.i64Pool.Get().(IntegerContainer[T])
	case int32:
		return rawFactory.i32Pool.Get().(IntegerContainer[T])
	case int16:
		return rawFactory.i16Pool.Get().(IntegerContainer[T])
	case int8:
		return rawFactory.i8Pool.Get().(IntegerContainer[T])
	case uint64:
		return rawFactory.u64Pool.Get().(IntegerContainer[T])
	case uint32:
		return rawFactory.u32Pool.Get().(IntegerContainer[T])
	case uint16:
		return rawFactory.u16Pool.Get().(IntegerContainer[T])
	case uint8:
		return rawFactory.u8Pool.Get().(IntegerContainer[T])
	default:
		return nil
	}
}

func putRawContainer[T types.Integer](c IntegerContainer[T]) {
	switch (any(T(0))).(type) {
	case int64:
		rawFactory.i64Pool.Put(c)
	case int32:
		rawFactory.i32Pool.Put(c)
	case int16:
		rawFactory.i16Pool.Put(c)
	case int8:
		rawFactory.i8Pool.Put(c)
	case uint64:
		rawFactory.u64Pool.Put(c)
	case uint32:
		rawFactory.u32Pool.Put(c)
	case uint16:
		rawFactory.u16Pool.Put(c)
	case uint8:
		rawFactory.u8Pool.Put(c)
	}
}

var rawFactory = RawFactory{
	i64Pool: sync.Pool{
		New: func() any { return new(RawContainer[int64]) },
	},
	i32Pool: sync.Pool{
		New: func() any { return new(RawContainer[int32]) },
	},
	i16Pool: sync.Pool{
		New: func() any { return new(RawContainer[int16]) },
	},
	i8Pool: sync.Pool{
		New: func() any { return new(RawContainer[int8]) },
	},
	u64Pool: sync.Pool{
		New: func() any { return new(RawContainer[uint64]) },
	},
	u32Pool: sync.Pool{
		New: func() any { return new(RawContainer[uint32]) },
	},
	u16Pool: sync.Pool{
		New: func() any { return new(RawContainer[uint16]) },
	},
	u8Pool: sync.Pool{
		New: func() any { return new(RawContainer[uint8]) },
	},
}
