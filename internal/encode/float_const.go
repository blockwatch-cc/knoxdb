// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"sync"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// TFloatConstant
type FloatConstContainer[T types.Float] struct {
	Val T
	N   int
}

func (c *FloatConstContainer[T]) Close() {
	putFloatConstContainer(c)
}

func (c *FloatConstContainer[T]) Type() FloatContainerType {
	return TFloatConstant
}

func (c *FloatConstContainer[T]) Len() int {
	return c.N
}

func (c *FloatConstContainer[T]) MaxSize() int {
	return 1 + size[float64]() + num.MaxVarintLen32
}

func (c *FloatConstContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatConstant))
	dst = storeFloat(dst, c.Val)
	return num.AppendUvarint(dst, uint64(c.N))
}

func (c *FloatConstContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatConstant) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	c.Val, buf = loadFloat[T](buf)
	v, n := num.Uvarint(buf)
	c.N = int(v)
	return buf[n:], nil
}

func (c *FloatConstContainer[T]) Get(_ int) T {
	return c.Val
}

func (c *FloatConstContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	for range sel {
		dst = append(dst, c.Val)
	}
	return dst
}

func (c *FloatConstContainer[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	c.Val = ctx.Min
	c.N = len(vals)
	return c
}

func (c *FloatConstContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatConstContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatConstContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatConstContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatConstContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatConstContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatConstContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatConstContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *FloatConstContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

type FloatConstFactory struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
}

func newFloatConstContainer[T types.Float]() FloatContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatConstFactory.f64Pool.Get().(FloatContainer[T])
	case float32:
		return floatConstFactory.f32Pool.Get().(FloatContainer[T])
	default:
		return nil
	}
}

func putFloatConstContainer[T types.Float](c FloatContainer[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatConstFactory.f64Pool.Put(c)
	case float32:
		floatConstFactory.f32Pool.Put(c)
	}
}

var floatConstFactory = FloatConstFactory{
	f64Pool: sync.Pool{
		New: func() any { return new(FloatConstContainer[float64]) },
	},
	f32Pool: sync.Pool{
		New: func() any { return new(FloatConstContainer[float32]) },
	},
}
