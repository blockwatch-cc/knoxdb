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

func (c *FloatRawContainer[T]) Size() int {
	return 1 + num.UvarintLen(uint64(util.SizeOf[T]()*len(c.Values))) +
		util.SizeOf[T]()*len(c.Values)
}

func (c *FloatRawContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatRaw))
	dst = num.AppendUvarint(dst, uint64(util.SizeOf[T]()*len(c.Values)))
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

// func (c *FloatRawContainer[T]) DecodeChunk(dst *[CHUNK_SIZE]T, ofs int) {
// 	copy(dst[:], c.Values[ofs:])
// }

func (c *FloatRawContainer[T]) MatchEqual(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64Equal(f64, float64(val), bits.Bytes())

	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32Equal(f32, float32(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *FloatRawContainer[T]) MatchNotEqual(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64NotEqual(f64, float64(val), bits.Bytes())
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32NotEqual(f32, float32(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *FloatRawContainer[T]) MatchLess(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64Less(f64, float64(val), bits.Bytes())
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32Less(f32, float32(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *FloatRawContainer[T]) MatchLessEqual(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64LessEqual(f64, float64(val), bits.Bytes())
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32LessEqual(f32, float32(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *FloatRawContainer[T]) MatchGreater(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64Greater(f64, float64(val), bits.Bytes())
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32Greater(f32, float32(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *FloatRawContainer[T]) MatchGreaterEqual(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64GreaterEqual(f64, float64(val), bits.Bytes())
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32GreaterEqual(f32, float32(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *FloatRawContainer[T]) MatchBetween(a, b T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := util.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64Between(f64, float64(a), float64(b), bits.Bytes())
	case types.BlockFloat32:
		f32 := util.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32Between(f32, float32(a), float32(b), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

// N.A.
func (c *FloatRawContainer[T]) MatchInSet(_ any, bits, _ *Bitset)    {}
func (c *FloatRawContainer[T]) MatchNotInSet(_ any, bits, _ *Bitset) {}

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

// TODO
func (c *FloatRawContainer[T]) Iterator() Iterator[T] {
	return nil
}
