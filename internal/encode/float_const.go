// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sync"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TFloatConstant
type FloatConstContainer[T types.Float] struct {
	Val T
	N   int
}

func (c *FloatConstContainer[T]) Info() string {
	return fmt.Sprintf("Const(%s)_[n=%d]", TypeName[T](), c.N)
}

func (c *FloatConstContainer[T]) Close() {
	putFloatConstContainer(c)
}

func (c *FloatConstContainer[T]) Type() ContainerType {
	return TFloatConstant
}

func (c *FloatConstContainer[T]) Len() int {
	return c.N
}

func (c *FloatConstContainer[T]) Size() int {
	return 1 + util.SizeOf[T]() + num.UvarintLen(uint64(c.N))
}

func (c *FloatConstContainer[T]) Iterator() NumberIterator[T] {
	return NewConstIterator(c.Val, c.N)
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

func (c *FloatConstContainer[T]) AppendTo(dst []T, sel []uint32) []T {
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

func (c *FloatConstContainer[T]) Encode(ctx *Context[T], vals []T) NumberContainer[T] {
	c.Val = vals[0]
	c.N = len(vals)
	return c
}

func (c *FloatConstContainer[T]) MatchEqual(val T, bits, _ *Bitset) {
	if c.Val == val {
		bits.One()
	}
}

func (c *FloatConstContainer[T]) MatchNotEqual(val T, bits, _ *Bitset) {
	if c.Val != val {
		bits.One()
	}
}

func (c *FloatConstContainer[T]) MatchLess(val T, bits, _ *Bitset) {
	if c.Val < val {
		bits.One()
	}
}

func (c *FloatConstContainer[T]) MatchLessEqual(val T, bits, _ *Bitset) {
	if c.Val <= val {
		bits.One()
	}
}

func (c *FloatConstContainer[T]) MatchGreater(val T, bits, _ *Bitset) {
	if c.Val > val {
		bits.One()
	}
}

func (c *FloatConstContainer[T]) MatchGreaterEqual(val T, bits, _ *Bitset) {
	if c.Val >= val {
		bits.One()
	}
}

func (c *FloatConstContainer[T]) MatchBetween(a, b T, bits, _ *Bitset) {
	if c.Val >= a && c.Val <= b {
		bits.One()
	}
}

// N.A.
func (c *FloatConstContainer[T]) MatchInSet(_ any, _, _ *Bitset)    {}
func (c *FloatConstContainer[T]) MatchNotInSet(_ any, _, _ *Bitset) {}

type FloatConstFactory struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
}

func newFloatConstContainer[T types.Float]() NumberContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatConstFactory.f64Pool.Get().(NumberContainer[T])
	case float32:
		return floatConstFactory.f32Pool.Get().(NumberContainer[T])
	default:
		return nil
	}
}

func putFloatConstContainer[T types.Float](c NumberContainer[T]) {
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
