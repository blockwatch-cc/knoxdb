// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	stdcmp "cmp"
	"encoding/binary"
	"fmt"
	"iter"
	"slices"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/types"
)

// ensure we implement required interfaces
var (
	_ types.NumberAccessor[float64] = (*FloatRawContainer[float64])(nil)
	_ NumberContainer[float64]      = (*FloatRawContainer[float64])(nil)
)

// TFloatRaw
type FloatRawContainer[T types.Float] struct {
	readOnlyContainer[T]
	Values []T
	typ    types.BlockType
}

func (c *FloatRawContainer[T]) Info() string {
	return fmt.Sprintf("Raw(%s)_[n=%d]", TypeName[T](), len(c.Values))
}

func (c *FloatRawContainer[T]) Close() {
	arena.Free(c.Values)
	c.Values = nil
	putFloatRawContainer(c)
}

func (c *FloatRawContainer[T]) Type() ContainerType {
	return TFloatRaw
}

func (c *FloatRawContainer[T]) Len() int {
	return len(c.Values)
}

func (c *FloatRawContainer[T]) Size() int {
	return 1 + UvarintLen(uint64(arena.SizeFor[T]()*len(c.Values))) +
		arena.SizeFor[T]()*len(c.Values)
}

func (c *FloatRawContainer[T]) Matcher() types.NumberMatcher[T] {
	return c
}

func (c *FloatRawContainer[T]) Chunks() types.NumberIterator[T] {
	return NewRawIterator(c.Values)
}

func (c *FloatRawContainer[T]) Iterator() iter.Seq2[int, T] {
	return func(fn func(int, T) bool) {
		for i, v := range c.Values {
			if !fn(i, v) {
				return
			}
		}
	}
}

func (c *FloatRawContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatRaw))
	dst = binary.AppendUvarint(dst, uint64(arena.SizeFor[T]()*len(c.Values)))
	return append(dst, arena.ToBytes(c.Values)...)
}

func (c *FloatRawContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatRaw) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := binary.Uvarint(buf)
	buf = buf[n:]
	c.Values = arena.FromBytes[T](buf[:int(v)])
	c.typ = AsBlockType[T]()
	return buf[int(v):], nil
}

func (c *FloatRawContainer[T]) Get(n int) T {
	return c.Values[n]
}

func (c *FloatRawContainer[T]) AppendTo(dst []T, sel []uint32) []T {
	if sel == nil {
		dst = append(dst, c.Values...)
	} else {
		for _, v := range sel {
			dst = append(dst, c.Values[v])
		}
	}
	return dst
}

func (c *FloatRawContainer[T]) Encode(_ *Context[T], vals []T) NumberContainer[T] {
	c.Values = slices.Clone(vals)
	c.typ = AsBlockType[T]()
	return c
}

func (c *FloatRawContainer[T]) Cmp(i, j int) int {
	return stdcmp.Compare(c.Get(i), c.Get(j))
}

func (c *FloatRawContainer[T]) MatchEqual(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := arena.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64Equal(f64, float64(val), bits.Bytes())

	case types.BlockFloat32:
		f32 := arena.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32Equal(f32, float32(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *FloatRawContainer[T]) MatchNotEqual(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := arena.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64NotEqual(f64, float64(val), bits.Bytes())
	case types.BlockFloat32:
		f32 := arena.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32NotEqual(f32, float32(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *FloatRawContainer[T]) MatchLess(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := arena.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64Less(f64, float64(val), bits.Bytes())
	case types.BlockFloat32:
		f32 := arena.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32Less(f32, float32(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *FloatRawContainer[T]) MatchLessEqual(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := arena.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64LessEqual(f64, float64(val), bits.Bytes())
	case types.BlockFloat32:
		f32 := arena.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32LessEqual(f32, float32(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *FloatRawContainer[T]) MatchGreater(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := arena.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64Greater(f64, float64(val), bits.Bytes())
	case types.BlockFloat32:
		f32 := arena.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32Greater(f32, float32(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *FloatRawContainer[T]) MatchGreaterEqual(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := arena.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64GreaterEqual(f64, float64(val), bits.Bytes())
	case types.BlockFloat32:
		f32 := arena.ReinterpretSlice[T, float32](c.Values)
		n = cmp.Float32GreaterEqual(f32, float32(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *FloatRawContainer[T]) MatchBetween(a, b T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockFloat64:
		f64 := arena.ReinterpretSlice[T, float64](c.Values)
		n = cmp.Float64Between(f64, float64(a), float64(b), bits.Bytes())
	case types.BlockFloat32:
		f32 := arena.ReinterpretSlice[T, float32](c.Values)
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

func newFloatRawContainer[T types.Float]() NumberContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatRawFactory.f64Pool.Get().(NumberContainer[T])
	case float32:
		return floatRawFactory.f32Pool.Get().(NumberContainer[T])
	default:
		return nil
	}
}

func putFloatRawContainer[T types.Float](c NumberContainer[T]) {
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
