// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/encode/s8b"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TIntegerSimple8
type Simple8Container[T types.Integer] struct {
	For      T
	Packed   []byte
	Unpacked []T // TODO: fusion kernels ok, iterator WIP
	free     bool
	typ      types.BlockType
}

func (c *Simple8Container[T]) Info() string {
	return fmt.Sprintf("S8(%s)_[n=%d]", TypeName[T](), c.Len())
}

func (c *Simple8Container[T]) Close() {
	if c.free {
		arena.Free(arena.AllocBytes, c.Packed)
		c.free = false
	}
	c.Packed = nil
	if c.Unpacked != nil {
		// FIXME: returns uint to int pools (problem?)
		arena.FreeT(c.Unpacked)
		c.Unpacked = nil
	}
	putSimple8Container[T](c)
}

func (c *Simple8Container[T]) Type() IntegerContainerType {
	return TIntegerSimple8
}

func (c *Simple8Container[T]) Len() int {
	if c.Unpacked == nil {
		c.decodeAll()
	}
	return len(c.Unpacked)
}

func (c *Simple8Container[T]) MaxSize() int {
	return 1 + num.UvarintLen(uint64(c.For)) + num.UvarintLen(uint64(len(c.Packed))) +
		len(c.Packed)
}

func (c *Simple8Container[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntegerSimple8))
	dst = num.AppendUvarint(dst, uint64(c.For))
	dst = num.AppendUvarint(dst, uint64(len(c.Packed)))
	dst = append(dst, c.Packed...)
	return dst
}

func (c *Simple8Container[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntegerSimple8) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	c.For = T(v)
	buf = buf[n:]
	v, n = num.Uvarint(buf)
	buf = buf[n:]
	c.Packed = buf[:int(v)]
	c.free = false
	c.typ = BlockType[T]()
	return buf[int(v):], nil
}

func (c *Simple8Container[T]) Get(n int) T {
	if c.Unpacked == nil {
		c.decodeAll()
	}
	return c.Unpacked[n] + c.For
}

func (c *Simple8Container[T]) AppendTo(sel []uint32, dst []T) []T {
	if c.Unpacked == nil {
		c.decodeAll()
	}
	if sel == nil {
		dst = append(dst, c.Unpacked...)
	} else {
		for _, v := range sel {
			dst = append(dst, c.Unpacked[int(v)]+c.For)
		}
	}
	return dst
}

func (c *Simple8Container[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	c.For = ctx.Min
	c.typ = BlockType[T]()

	sz := s8b.EstimateMaxSize(len(vals), ctx.Min, ctx.Max) * 8
	buf := arena.Alloc(arena.AllocBytes, sz).([]byte)[:sz]
	var err error
	switch c.typ {
	case types.BlockInt64:
		v := util.ReinterpretSlice[T, int64](vals)
		buf, err = s8b.EncodeInt64(buf, v, int64(ctx.Min), int64(ctx.Max))
	case types.BlockUint64:
		v := util.ReinterpretSlice[T, uint64](vals)
		buf, err = s8b.EncodeUint64(buf, v, uint64(ctx.Min), uint64(ctx.Max))
	case types.BlockInt32:
		v := util.ReinterpretSlice[T, int32](vals)
		buf, err = s8b.EncodeInt32(buf, v, int32(ctx.Min), int32(ctx.Max))
	case types.BlockUint32:
		v := util.ReinterpretSlice[T, uint32](vals)
		buf, err = s8b.EncodeUint32(buf, v, uint32(ctx.Min), uint32(ctx.Max))
	case types.BlockInt16:
		v := util.ReinterpretSlice[T, int16](vals)
		buf, err = s8b.EncodeInt16(buf, v, int16(ctx.Min), int16(ctx.Max))
	case types.BlockUint16:
		v := util.ReinterpretSlice[T, uint16](vals)
		buf, err = s8b.EncodeUint16(buf, v, uint16(ctx.Min), uint16(ctx.Max))
	case types.BlockInt8:
		v := util.ReinterpretSlice[T, int8](vals)
		buf, err = s8b.EncodeInt8(buf, v, int8(ctx.Min), int8(ctx.Max))
	case types.BlockUint8:
		v := util.ReinterpretSlice[T, uint8](vals)
		buf, err = s8b.EncodeUint8(buf, v, uint8(ctx.Min), uint8(ctx.Max))
	}
	if err != nil {
		panic(err)
	}
	c.Packed = buf
	c.free = true

	return c
}

func (c *Simple8Container[T]) decodeAll() {
	n := s8b.CountValues(c.Packed)
	if n < 0 {
		panic(fmt.Errorf("simple8 corrupted data"))
	}
	var err error
	switch c.typ {
	case types.BlockInt64, types.BlockUint64:
		u64 := arena.AllocT[uint64](n)[:n]
		n, err = s8b.DecodeUint64(u64, c.Packed)
		c.Unpacked = util.ReinterpretSlice[uint64, T](u64[:n])
	case types.BlockInt32, types.BlockUint32:
		u32 := arena.AllocT[uint32](n)[:n]
		n, err = s8b.DecodeUint32(u32, c.Packed)
		c.Unpacked = util.ReinterpretSlice[uint32, T](u32[:n])
	case types.BlockInt16, types.BlockUint16:
		u16 := arena.AllocT[uint16](n)[:n]
		n, err = s8b.DecodeUint16(u16, c.Packed)
		c.Unpacked = util.ReinterpretSlice[uint16, T](u16[:n])
	case types.BlockInt8, types.BlockUint8:
		u8 := arena.AllocT[uint8](n)[:n]
		n, err = s8b.DecodeUint8(u8, c.Packed)
		c.Unpacked = util.ReinterpretSlice[uint8, T](u8[:n])
	}
	if err != nil {
		panic(err)
	}
}

func (c *Simple8Container[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	// Note: Fusion kernel may be slower based on data type and contents
	// return s8b.Equal(c.Packed, uint64(val), bits)

	// need unpacked data
	if c.Unpacked == nil {
		c.decodeAll()
	}

	// sanity checks of value range
	if val < c.For {
		return bits
	}
	val -= c.For

	// use type-based matcher
	switch c.typ {
	case types.BlockInt64, types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Unpacked)
		return cmp.MatchUint64Equal(u64, uint64(val), bits, mask)

	case types.BlockInt32, types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Unpacked)
		return cmp.MatchUint32Equal(u32, uint32(val), bits, mask)

	case types.BlockInt16, types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Unpacked)
		return cmp.MatchUint16Equal(u16, uint16(val), bits, mask)

	case types.BlockInt8, types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Unpacked)
		return cmp.MatchUint8Equal(u8, uint8(val), bits, mask)
	}
	return bits
}

func (c *Simple8Container[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	// Note: Fusion kernel may be slower based on data type and contents
	// return s8b.NotEqual(c.Packed, uint64(val), bits)

	// need unpacked data
	if c.Unpacked == nil {
		c.decodeAll()
	}

	// sanity checks of value range
	if val < c.For {
		return bits.One()
	}
	val -= c.For

	// use type-based matcher
	switch c.typ {
	case types.BlockInt64, types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Unpacked)
		return cmp.MatchUint64NotEqual(u64, uint64(val), bits, mask)

	case types.BlockInt32, types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Unpacked)
		return cmp.MatchUint32NotEqual(u32, uint32(val), bits, mask)

	case types.BlockInt16, types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Unpacked)
		return cmp.MatchUint16NotEqual(u16, uint16(val), bits, mask)

	case types.BlockInt8, types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Unpacked)
		return cmp.MatchUint8NotEqual(u8, uint8(val), bits, mask)
	}
	return bits

}

func (c *Simple8Container[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	// Note: Fusion kernel may be slower based on data type and contents
	// return s8b.Less(c.Packed, uint64(val), bits)

	// need unpacked data
	if c.Unpacked == nil {
		c.decodeAll()
	}

	// sanity checks of value range
	if val < c.For {
		return bits
	}
	val -= c.For

	// use type-based matcher
	switch c.typ {
	case types.BlockInt64, types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Unpacked)
		return cmp.MatchUint64Less(u64, uint64(val), bits, mask)

	case types.BlockInt32, types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Unpacked)
		return cmp.MatchUint32Less(u32, uint32(val), bits, mask)

	case types.BlockInt16, types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Unpacked)
		return cmp.MatchUint16Less(u16, uint16(val), bits, mask)

	case types.BlockInt8, types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Unpacked)
		return cmp.MatchUint8Less(u8, uint8(val), bits, mask)
	}
	return bits
}

func (c *Simple8Container[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	// Note: Fusion kernel may be slower based on data type and contents
	// return s8b.LessEqual(c.Packed, uint64(val), bits)

	// need unpacked data
	if c.Unpacked == nil {
		c.decodeAll()
	}

	// sanity checks of value range
	if val < c.For {
		return bits
	}
	val -= c.For

	// use type-based matcher
	switch c.typ {
	case types.BlockInt64, types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Unpacked)
		return cmp.MatchUint64LessEqual(u64, uint64(val), bits, mask)

	case types.BlockInt32, types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Unpacked)
		return cmp.MatchUint32LessEqual(u32, uint32(val), bits, mask)

	case types.BlockInt16, types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Unpacked)
		return cmp.MatchUint16LessEqual(u16, uint16(val), bits, mask)

	case types.BlockInt8, types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Unpacked)
		return cmp.MatchUint8LessEqual(u8, uint8(val), bits, mask)
	}
	return bits
}

func (c *Simple8Container[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	// Note: Fusion kernel may be slower based on data type and contents
	// return s8b.Greater(c.Packed, uint64(val), bits)

	// need unpacked data
	if c.Unpacked == nil {
		c.decodeAll()
	}

	// sanity checks of value range
	if val < c.For {
		return bits.One()
	}
	val -= c.For

	// use type-based matcher
	switch c.typ {
	case types.BlockInt64, types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Unpacked)
		return cmp.MatchUint64Greater(u64, uint64(val), bits, mask)

	case types.BlockInt32, types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Unpacked)
		return cmp.MatchUint32Greater(u32, uint32(val), bits, mask)

	case types.BlockInt16, types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Unpacked)
		return cmp.MatchUint16Greater(u16, uint16(val), bits, mask)

	case types.BlockInt8, types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Unpacked)
		return cmp.MatchUint8Greater(u8, uint8(val), bits, mask)
	}
	return bits
}

func (c *Simple8Container[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	// Note: Fusion kernel may be slower based on data type and contents
	// return s8b.GreaterEqual(c.Packed, uint64(val), bits)

	// need unpacked data
	if c.Unpacked == nil {
		c.decodeAll()
	}

	// sanity checks of value range
	if val < c.For {
		val = c.For
	}
	val -= c.For

	// use type-based matcher
	switch c.typ {
	case types.BlockInt64, types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Unpacked)
		return cmp.MatchUint64GreaterEqual(u64, uint64(val), bits, mask)

	case types.BlockInt32, types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Unpacked)
		return cmp.MatchUint32GreaterEqual(u32, uint32(val), bits, mask)

	case types.BlockInt16, types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Unpacked)
		return cmp.MatchUint16GreaterEqual(u16, uint16(val), bits, mask)

	case types.BlockInt8, types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Unpacked)
		return cmp.MatchUint8GreaterEqual(u8, uint8(val), bits, mask)
	}
	return bits
}

func (c *Simple8Container[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	// Note: Fusion kernel may be slower based on data type and contents
	// return s8b.Between(c.Packed, uint64(a), uint64(b), bits)

	// need unpacked data
	if c.Unpacked == nil {
		c.decodeAll()
	}

	// sanity checks of value range
	if b < c.For {
		return bits
	}
	if a < c.For {
		a = c.For
	}

	// ensure overflow free calculations
	a = T(uint64(a - c.For))
	b = T(uint64(b - c.For))

	// use type-based matcher, after min-FOR all values can be treated as
	// unsigned
	switch c.typ {
	case types.BlockUint64, types.BlockInt64:
		u64 := util.ReinterpretSlice[T, uint64](c.Unpacked)
		return cmp.MatchUint64Between(u64, uint64(a), uint64(b), bits, mask)

	case types.BlockUint32, types.BlockInt32:
		u32 := util.ReinterpretSlice[T, uint32](c.Unpacked)
		return cmp.MatchUint32Between(u32, uint32(a), uint32(b), bits, mask)

	case types.BlockUint16, types.BlockInt16:
		u16 := util.ReinterpretSlice[T, uint16](c.Unpacked)
		return cmp.MatchUint16Between(u16, uint16(a), uint16(b), bits, mask)

	case types.BlockUint8, types.BlockInt8:
		u8 := util.ReinterpretSlice[T, uint8](c.Unpacked)
		return cmp.MatchUint8Between(u8, uint8(a), uint8(b), bits, mask)
	}
	return bits
}

func (c *Simple8Container[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	if c.Unpacked == nil {
		c.decodeAll()
	}

	set := s.(*xroar.Bitmap)
	if mask != nil {
		// only process values from mask
		u32 := arena.AllocT[uint32](mask.Count())
		for _, k := range mask.Indexes(u32) {
			i := int(k)
			if set.Contains(uint64(c.Unpacked[i] + c.For)) {
				bits.Set(i)
			}
		}
		arena.FreeT(u32)
	} else {
		for i, v := range c.Unpacked {
			if set.Contains(uint64(v + c.For)) {
				bits.Set(i)
			}
		}
	}

	return bits
}

func (c *Simple8Container[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	if c.Unpacked == nil {
		c.decodeAll()
	}

	set := s.(*xroar.Bitmap)
	if mask != nil {
		// only process values from mask
		u32 := arena.AllocT[uint32](mask.Count())
		for _, k := range mask.Indexes(u32) {
			i := int(k)
			if !set.Contains(uint64(c.Unpacked[i] + c.For)) {
				bits.Set(i)
			}
		}
		arena.FreeT(u32)
	} else {
		for i, v := range c.Unpacked {
			if !set.Contains(uint64(v + c.For)) {
				bits.Set(i)
			}
		}
	}

	return bits
}

type Simple8Factory struct {
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
}

func newSimple8Container[T types.Integer]() IntegerContainer[T] {
	switch any(T(0)).(type) {
	case int64:
		return simple8Factory.i64Pool.Get().(IntegerContainer[T])
	case int32:
		return simple8Factory.i32Pool.Get().(IntegerContainer[T])
	case int16:
		return simple8Factory.i16Pool.Get().(IntegerContainer[T])
	case int8:
		return simple8Factory.i8Pool.Get().(IntegerContainer[T])
	case uint64:
		return simple8Factory.u64Pool.Get().(IntegerContainer[T])
	case uint32:
		return simple8Factory.u32Pool.Get().(IntegerContainer[T])
	case uint16:
		return simple8Factory.u16Pool.Get().(IntegerContainer[T])
	case uint8:
		return simple8Factory.u8Pool.Get().(IntegerContainer[T])
	default:
		return nil
	}
}

func putSimple8Container[T types.Integer](c IntegerContainer[T]) {
	switch any(T(0)).(type) {
	case int64:
		simple8Factory.i64Pool.Put(c)
	case int32:
		simple8Factory.i32Pool.Put(c)
	case int16:
		simple8Factory.i16Pool.Put(c)
	case int8:
		simple8Factory.i8Pool.Put(c)
	case uint64:
		simple8Factory.u64Pool.Put(c)
	case uint32:
		simple8Factory.u32Pool.Put(c)
	case uint16:
		simple8Factory.u16Pool.Put(c)
	case uint8:
		simple8Factory.u8Pool.Put(c)
	}
}

var simple8Factory = Simple8Factory{
	i64Pool: sync.Pool{
		New: func() any { return new(Simple8Container[int64]) },
	},
	i32Pool: sync.Pool{
		New: func() any { return new(Simple8Container[int32]) },
	},
	i16Pool: sync.Pool{
		New: func() any { return new(Simple8Container[int16]) },
	},
	i8Pool: sync.Pool{
		New: func() any { return new(Simple8Container[int8]) },
	},
	u64Pool: sync.Pool{
		New: func() any { return new(Simple8Container[uint64]) },
	},
	u32Pool: sync.Pool{
		New: func() any { return new(Simple8Container[uint32]) },
	},
	u16Pool: sync.Pool{
		New: func() any { return new(Simple8Container[uint16]) },
	},
	u8Pool: sync.Pool{
		New: func() any { return new(Simple8Container[uint8]) },
	},
}
