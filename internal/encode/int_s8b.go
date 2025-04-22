// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/s8b"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TIntegerSimple8
type Simple8Container[T types.Integer] struct {
	For    T
	Packed []byte
	N      int
	it     *s8b.Iterator[T]
	free   bool
}

func (c *Simple8Container[T]) Info() string {
	return fmt.Sprintf("S8(%s)_[n=%d]", TypeName[T](), c.Len())
}

func (c *Simple8Container[T]) Close() {
	if c.free {
		arena.Free(c.Packed)
		c.free = false
	}
	c.Packed = nil
	if c.it != nil {
		c.it.Close()
		c.it = nil
	}
	putSimple8Container[T](c)
}

func (c *Simple8Container[T]) Type() IntegerContainerType {
	return TIntegerSimple8
}

func (c *Simple8Container[T]) Len() int {
	return c.N
}

func (c *Simple8Container[T]) Size() int {
	return 1 + num.UvarintLen(uint64(c.For)) + num.UvarintLen(uint64(len(c.Packed))) +
		len(c.Packed)
}

func (c *Simple8Container[T]) Iterator() Iterator[T] {
	return s8b.NewIterator[T](c.Packed, c.N, c.For)
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
	c.N = s8b.CountValues(c.Packed)
	return buf[int(v):], nil
}

func (c *Simple8Container[T]) Get(n int) T {
	if c.it == nil {
		c.it = s8b.NewIterator[T](c.Packed, c.N, c.For)
	}
	return c.it.Get(n)
}

func (c *Simple8Container[T]) AppendTo(sel []uint32, dst []T) []T {
	it := c.Iterator()
	if sel == nil {
		// TODO: support FOR in s8b decoder/iterator
		for {
			vals, n := it.NextChunk()
			if n == 0 {
				break
			}
			for _, v := range vals[:n] {
				dst = append(dst, v)
			}
		}
	} else {
		for _, v := range sel {
			dst = append(dst, it.Get(int(v)))
		}
	}
	it.Close()
	return dst
}

func (c *Simple8Container[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	sz := s8b.EstimateMaxSize(len(vals), ctx.Min, ctx.Max) * 8
	buf := arena.AllocBytes(sz)[:sz]
	var err error
	switch any(T(0)).(type) {
	case uint64:
		v := util.ReinterpretSlice[T, uint64](vals)
		buf, err = s8b.EncodeUint64(buf, v, uint64(ctx.Min), uint64(ctx.Max))
	case uint32:
		v := util.ReinterpretSlice[T, uint32](vals)
		buf, err = s8b.EncodeUint32(buf, v, uint32(ctx.Min), uint32(ctx.Max))
	case uint16:
		v := util.ReinterpretSlice[T, uint16](vals)
		buf, err = s8b.EncodeUint16(buf, v, uint16(ctx.Min), uint16(ctx.Max))
	case uint8:
		v := util.ReinterpretSlice[T, uint8](vals)
		buf, err = s8b.EncodeUint8(buf, v, uint8(ctx.Min), uint8(ctx.Max))
	case int64:
		v := util.ReinterpretSlice[T, int64](vals)
		buf, err = s8b.EncodeInt64(buf, v, int64(ctx.Min), int64(ctx.Max))
	case int32:
		v := util.ReinterpretSlice[T, int32](vals)
		buf, err = s8b.EncodeInt32(buf, v, int32(ctx.Min), int32(ctx.Max))
	case int16:
		v := util.ReinterpretSlice[T, int16](vals)
		buf, err = s8b.EncodeInt16(buf, v, int16(ctx.Min), int16(ctx.Max))
	case int8:
		v := util.ReinterpretSlice[T, int8](vals)
		buf, err = s8b.EncodeInt8(buf, v, int8(ctx.Min), int8(ctx.Max))
	}
	if err != nil {
		panic(err)
	}
	c.Packed = buf
	c.free = true
	c.For = ctx.Min
	c.N = len(vals)

	return c
}

func (c *Simple8Container[T]) MatchEqual(val T, bits, _ *Bitset) {
	// sanity checks of value range
	if val < c.For {
		return
	}

	// use Fusion kernel, safely subtract FOR
	s8b.Equal(c.Packed, uint64(val)-uint64(c.For), bits)
}

func (c *Simple8Container[T]) MatchNotEqual(val T, bits, _ *Bitset) {
	// sanity checks of value range
	if val < c.For {
		bits.One()
		return
	}

	// use Fusion kernel, safely subtract FOR
	s8b.NotEqual(c.Packed, uint64(val)-uint64(c.For), bits)
}

func (c *Simple8Container[T]) MatchLess(val T, bits, mask *Bitset) {
	// sanity checks of value range
	if val < c.For {
		return
	}

	// use Fusion kernel, safely subtract FOR
	s8b.Less(c.Packed, uint64(val)-uint64(c.For), bits)
}

func (c *Simple8Container[T]) MatchLessEqual(val T, bits, mask *Bitset) {
	// sanity checks of value range
	if val < c.For {
		return
	}

	// use Fusion kernel, safely subtract FOR
	s8b.LessEqual(c.Packed, uint64(val)-uint64(c.For), bits)
}

func (c *Simple8Container[T]) MatchGreater(val T, bits, mask *Bitset) {
	// sanity checks of value range
	if val < c.For {
		bits.One()
		return
	}

	// use Fusion kernel, safely subtract FOR
	s8b.Greater(c.Packed, uint64(val)-uint64(c.For), bits)
}

func (c *Simple8Container[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	// sanity checks of value range
	if val < c.For {
		val = c.For
	}

	// use Fusion kernel, safely subtract FOR
	s8b.GreaterEqual(c.Packed, uint64(val)-uint64(c.For), bits)
}

func (c *Simple8Container[T]) MatchBetween(a, b T, bits, mask *Bitset) {
	// sanity checks of value range
	if b < c.For {
		return
	}
	if a < c.For {
		a = c.For
	}

	// use Fusion kernel, safely subtract FOR
	s8b.Between(c.Packed, uint64(a)-uint64(c.For), uint64(b)-uint64(c.For), bits)
}

func (c *Simple8Container[T]) MatchInSet(s any, bits, mask *Bitset) {
	it := c.Iterator()
	set := s.(*xroar.Bitmap)
	if mask != nil {
		// only process values from mask
		u32 := arena.Alloc[uint32](mask.Count())
		for _, k := range mask.Indexes(u32) {
			i := int(k)
			it.Seek(i)
			val, _ := it.Next()
			if set.Contains(uint64(val)) {
				bits.Set(i)
			}
		}
		arena.Free(u32)
	} else {
		var i int
		for {
			vals, n := it.NextChunk()
			if n == 0 {
				break
			}
			for _, v := range vals[:n] {
				if set.Contains(uint64(v)) {
					bits.Set(i)
				}
				i++
			}
		}
	}
	it.Close()
}

func (c *Simple8Container[T]) MatchNotInSet(s any, bits, mask *Bitset) {
	it := c.Iterator()
	set := s.(*xroar.Bitmap)
	if mask != nil {
		// only process values from mask
		u32 := arena.Alloc[uint32](mask.Count())
		for _, k := range mask.Indexes(u32) {
			i := int(k)
			it.Seek(i)
			val, _ := it.Next()
			if !set.Contains(uint64(val)) {
				bits.Set(i)
			}
		}
		arena.Free(u32)
	} else {
		var i int
		for {
			vals, n := it.NextChunk()
			if n == 0 {
				break
			}
			for _, v := range vals[:n] {
				if !set.Contains(uint64(v)) {
					bits.Set(i)
				}
				i++
			}
		}
	}
	it.Close()
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

	i64ItPool sync.Pool
	i32ItPool sync.Pool
	i16ItPool sync.Pool
	i8ItPool  sync.Pool
	u64ItPool sync.Pool
	u32ItPool sync.Pool
	u16ItPool sync.Pool
	u8ItPool  sync.Pool
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
