// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sync"

	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TFloatRaw
type FloatRawContainer[T types.Float] struct {
	Values []T
	typ    types.BlockType
}

func (c *FloatRawContainer[T]) Info() string {
	return fmt.Sprintf("Raw(%s)_[n=%d]", TypeName[T](), len(c.Values))
}

func (c *FloatRawContainer[T]) Close() {
	c.Values = nil
	putFloatRawContainer(c)
}

func (c *FloatRawContainer[T]) Type() FloatContainerType {
	return TFloatRaw
}

func (c *FloatRawContainer[T]) Len() int {
	return len(c.Values)
}

func (c *FloatRawContainer[T]) MaxSize() int {
	return 1 + num.MaxVarintLen32 + SizeOf[T]()*len(c.Values)
}

func (c *FloatRawContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatRaw))
	dst = num.AppendUvarint(dst, uint64(SizeOf[T]()*len(c.Values)))
	return append(dst, util.ToByteSlice(c.Values)...)
}

func (c *FloatRawContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatRaw) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	buf = buf[n:]
	c.Values = util.FromByteSlice[T](buf[:int(v)])
	c.typ = BlockType[T]()
	return buf[int(v):], nil
}

func (c *FloatRawContainer[T]) Get(n int) T {
	return c.Values[n]
}

func (c *FloatRawContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	if sel == nil {
		dst = append(dst, c.Values...)
	} else {
		for _, v := range sel {
			dst = append(dst, c.Values[v])
		}
	}
	return dst
}

func (c *FloatRawContainer[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	c.Values = vals
	c.typ = BlockType[T]()
	return c
}

func (c *FloatRawContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		return cmp.MatchFloat64Equal(f64, float64(val), bits, mask)
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		return cmp.MatchFloat32Equal(f32, float32(val), bits, mask)
	}
	return bits
}

func (c *FloatRawContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		return cmp.MatchFloat64NotEqual(f64, float64(val), bits, mask)
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		return cmp.MatchFloat32NotEqual(f32, float32(val), bits, mask)
	}
	return bits
}

func (c *FloatRawContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		return cmp.MatchFloat64Less(f64, float64(val), bits, mask)
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		return cmp.MatchFloat32Less(f32, float32(val), bits, mask)
	}
	return bits
}

func (c *FloatRawContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		return cmp.MatchFloat64LessEqual(f64, float64(val), bits, mask)
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		return cmp.MatchFloat32LessEqual(f32, float32(val), bits, mask)
	}
	return bits
}

func (c *FloatRawContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		return cmp.MatchFloat64Greater(f64, float64(val), bits, mask)
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		return cmp.MatchFloat32Greater(f32, float32(val), bits, mask)
	}
	return bits
}

func (c *FloatRawContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		return cmp.MatchFloat64GreaterEqual(f64, float64(val), bits, mask)
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		return cmp.MatchFloat32GreaterEqual(f32, float32(val), bits, mask)
	}
	return bits
}

func (c *FloatRawContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		return cmp.MatchFloat64Between(f64, float64(a), float64(b), bits, mask)
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		return cmp.MatchFloat32Between(f32, float32(a), float32(b), bits, mask)
	}
	return bits
}

func (c *FloatRawContainer[T]) MatchSet(_ any, bits, _ *Bitset) *Bitset {
	// N.A.
	return bits
}

func (c *FloatRawContainer[T]) MatchNotSet(_ any, bits, _ *Bitset) *Bitset {
	// N.A.
	return bits
}

type FloatRawFactory struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
}

func newFloatRawContainer[T types.Float]() FloatContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatRawFactory.f64Pool.Get().(FloatContainer[T])
	case float32:
		return floatRawFactory.f32Pool.Get().(FloatContainer[T])
	default:
		return nil
	}
}

func putFloatRawContainer[T types.Float](c FloatContainer[T]) {
	switch (any(T(0))).(type) {
	case float64:
		floatRawFactory.f64Pool.Put(c)
	case float32:
		floatRawFactory.f32Pool.Put(c)
	}
}

var floatRawFactory = FloatRawFactory{
	f64Pool: sync.Pool{
		New: func() any { return new(FloatRawContainer[float64]) },
	},
	f32Pool: sync.Pool{
		New: func() any { return new(FloatRawContainer[float32]) },
	},
}
