// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sync"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
)

// TIntConstant
type ConstContainer[T types.Integer] struct {
	Val T
	N   int
}

func (c *ConstContainer[T]) Info() string {
	return fmt.Sprintf("Const(%s)_[n=%d]", TypeName[T](), c.N)
}

func (c *ConstContainer[T]) Close() {
	putConstContainer[T](c)
}

func (c *ConstContainer[T]) Type() ContainerType {
	return TIntConstant
}

func (c *ConstContainer[T]) Len() int {
	return c.N
}

func (c *ConstContainer[T]) Size() int {
	return 1 + num.UvarintLen(c.Val) + num.UvarintLen(c.N)
}

func (c *ConstContainer[T]) Iterator() NumberIterator[T] {
	return NewConstIterator(c.Val, c.N)
}

func (c *ConstContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntConstant))
	dst = num.AppendUvarint(dst, uint64(c.Val))
	return num.AppendUvarint(dst, uint64(c.N))
}

func (c *ConstContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntConstant) {
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

func (c *ConstContainer[T]) AppendTo(dst []T, sel []uint32) []T {
	n := c.N
	if sel != nil {
		n = len(sel)
	}
	dst = dst[:n]
	var i int
	for range n / 16 {
		dst[i] = c.Val
		dst[i+1] = c.Val
		dst[i+2] = c.Val
		dst[i+3] = c.Val
		dst[i+4] = c.Val
		dst[i+5] = c.Val
		dst[i+6] = c.Val
		dst[i+7] = c.Val
		dst[i+8] = c.Val
		dst[i+9] = c.Val
		dst[i+10] = c.Val
		dst[i+11] = c.Val
		dst[i+12] = c.Val
		dst[i+13] = c.Val
		dst[i+14] = c.Val
		dst[i+15] = c.Val
		i += 16
	}
	for i < n {
		dst[i] = c.Val
		i++
	}
	return dst
}

func (c *ConstContainer[T]) Encode(ctx *Context[T], vals []T) NumberContainer[T] {
	c.Val = ctx.Min
	c.N = len(vals)
	return c
}

func (c *ConstContainer[T]) MatchEqual(val T, bits, _ *Bitset) {
	if c.Val == val {
		bits.One()
	}
}

func (c *ConstContainer[T]) MatchNotEqual(val T, bits, _ *Bitset) {
	if c.Val != val {
		bits.One()
	}
}

func (c *ConstContainer[T]) MatchLess(val T, bits, _ *Bitset) {
	if c.Val < val {
		bits.One()
	}
}

func (c *ConstContainer[T]) MatchLessEqual(val T, bits, _ *Bitset) {
	if c.Val <= val {
		bits.One()
	}
}

func (c *ConstContainer[T]) MatchGreater(val T, bits, _ *Bitset) {
	if c.Val > val {
		bits.One()
	}
}

func (c *ConstContainer[T]) MatchGreaterEqual(val T, bits, _ *Bitset) {
	if c.Val >= val {
		bits.One()
	}
}

func (c *ConstContainer[T]) MatchBetween(a, b T, bits, _ *Bitset) {
	if c.Val >= a && c.Val <= b {
		bits.One()
	}
}

func (c *ConstContainer[T]) MatchInSet(s any, bits, _ *Bitset) {
	set := s.(*xroar.Bitmap)
	if set.Contains(uint64(c.Val)) {
		bits.One()
	}
}

func (c *ConstContainer[T]) MatchNotInSet(s any, bits, _ *Bitset) {
	set := s.(*xroar.Bitmap)
	if !set.Contains(uint64(c.Val)) {
		bits.One()
	}
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

func newConstContainer[T types.Integer]() NumberContainer[T] {
	switch any(T(0)).(type) {
	case int64:
		return constFactory.i64Pool.Get().(NumberContainer[T])
	case int32:
		return constFactory.i32Pool.Get().(NumberContainer[T])
	case int16:
		return constFactory.i16Pool.Get().(NumberContainer[T])
	case int8:
		return constFactory.i8Pool.Get().(NumberContainer[T])
	case uint64:
		return constFactory.u64Pool.Get().(NumberContainer[T])
	case uint32:
		return constFactory.u32Pool.Get().(NumberContainer[T])
	case uint16:
		return constFactory.u16Pool.Get().(NumberContainer[T])
	case uint8:
		return constFactory.u8Pool.Get().(NumberContainer[T])
	default:
		return nil
	}
}

func putConstContainer[T types.Integer](c NumberContainer[T]) {
	switch any(T(0)).(type) {
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
