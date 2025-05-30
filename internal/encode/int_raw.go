// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"iter"
	"slices"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// ensure we implement required interfaces
var (
	_ types.NumberAccessor[int64] = (*RawContainer[int64])(nil)
	_ NumberContainer[int64]      = (*RawContainer[int64])(nil)
)

// TIntRaw
type RawContainer[T types.Integer] struct {
	readOnlyContainer[T]
	Values []T
	sz     int
	typ    types.BlockType
}

func (c *RawContainer[T]) Info() string {
	return fmt.Sprintf("Raw(%s)_[n=%d]", TypeName[T](), len(c.Values))
}

func (c *RawContainer[T]) Close() {
	c.Values = nil
	putRawContainer[T](c)
}

func (c *RawContainer[T]) Type() ContainerType {
	return TIntRaw
}

func (c *RawContainer[T]) Len() int {
	return len(c.Values)
}

func (c *RawContainer[T]) Size() int {
	return 1 + num.UvarintLen(len(c.Values)) + c.sz*len(c.Values)
}

func (c *RawContainer[T]) Matcher() types.NumberMatcher[T] {
	return c
}

func (c *RawContainer[T]) Chunks() types.NumberIterator[T] {
	return NewRawIterator(c.Values)
}

func (c *RawContainer[T]) Iterator() iter.Seq2[int, T] {
	return func(fn func(int, T) bool) {
		for i, v := range c.Values {
			if !fn(i, v) {
				return
			}
		}
	}
}

func (c *RawContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntRaw))
	dst = num.AppendUvarint(dst, uint64(len(c.Values)))
	// if cpu.IsBigEndian {
	//  // TODO: flip byte order
	// }
	return append(dst, util.ToByteSlice(c.Values)...)
}

func (c *RawContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntRaw) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	buf = buf[n:]
	c.sz = util.SizeOf[T]()
	c.typ = BlockType[T]()
	n = int(v) * c.sz
	c.Values = util.FromByteSlice[T](buf[:n])
	return buf[n:], nil
}

func (c *RawContainer[T]) Get(n int) T {
	return c.Values[n]
}

func (c *RawContainer[T]) AppendTo(dst []T, sel []uint32) []T {
	if sel == nil {
		dst = append(dst, c.Values...)
	} else {
		for _, v := range sel {
			dst = append(dst, c.Values[v])
		}
	}
	return dst
}

func (c *RawContainer[T]) Encode(ctx *Context[T], vals []T) NumberContainer[T] {
	c.Values = slices.Clone(vals)
	c.sz = util.SizeOf[T]()
	c.typ = BlockType[T]()
	return c
}

func (c *RawContainer[T]) Cmp(i, j int) int {
	return util.Cmp(c.Values[i], c.Values[j])
}

func (c *RawContainer[T]) MatchEqual(val T, bits, _ *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockInt64:
		i64 := util.ReinterpretSlice[T, int64](c.Values)
		n = cmp.Int64Equal(i64, int64(val), bits.Bytes())
	case types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Values)
		n = cmp.Uint64Equal(u64, uint64(val), bits.Bytes())
	case types.BlockInt32:
		i32 := util.ReinterpretSlice[T, int32](c.Values)
		n = cmp.Int32Equal(i32, int32(val), bits.Bytes())
	case types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Values)
		n = cmp.Uint32Equal(u32, uint32(val), bits.Bytes())
	case types.BlockInt16:
		i16 := util.ReinterpretSlice[T, int16](c.Values)
		n = cmp.Int16Equal(i16, int16(val), bits.Bytes())
	case types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Values)
		n = cmp.Uint16Equal(u16, uint16(val), bits.Bytes())
	case types.BlockInt8:
		i8 := util.ReinterpretSlice[T, int8](c.Values)
		n = cmp.Int8Equal(i8, int8(val), bits.Bytes())
	case types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Values)
		n = cmp.Uint8Equal(u8, uint8(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *RawContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockInt64:
		i64 := util.ReinterpretSlice[T, int64](c.Values)
		n = cmp.Int64NotEqual(i64, int64(val), bits.Bytes())
	case types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Values)
		n = cmp.Uint64NotEqual(u64, uint64(val), bits.Bytes())
	case types.BlockInt32:
		i32 := util.ReinterpretSlice[T, int32](c.Values)
		n = cmp.Int32NotEqual(i32, int32(val), bits.Bytes())
	case types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Values)
		n = cmp.Uint32NotEqual(u32, uint32(val), bits.Bytes())
	case types.BlockInt16:
		i16 := util.ReinterpretSlice[T, int16](c.Values)
		n = cmp.Int16NotEqual(i16, int16(val), bits.Bytes())
	case types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Values)
		n = cmp.Uint16NotEqual(u16, uint16(val), bits.Bytes())
	case types.BlockInt8:
		i8 := util.ReinterpretSlice[T, int8](c.Values)
		n = cmp.Int8NotEqual(i8, int8(val), bits.Bytes())
	case types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Values)
		n = cmp.Uint8NotEqual(u8, uint8(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *RawContainer[T]) MatchLess(val T, bits, mask *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockInt64:
		i64 := util.ReinterpretSlice[T, int64](c.Values)
		n = cmp.Int64Less(i64, int64(val), bits.Bytes())
	case types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Values)
		n = cmp.Uint64Less(u64, uint64(val), bits.Bytes())
	case types.BlockInt32:
		i32 := util.ReinterpretSlice[T, int32](c.Values)
		n = cmp.Int32Less(i32, int32(val), bits.Bytes())
	case types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Values)
		n = cmp.Uint32Less(u32, uint32(val), bits.Bytes())
	case types.BlockInt16:
		i16 := util.ReinterpretSlice[T, int16](c.Values)
		n = cmp.Int16Less(i16, int16(val), bits.Bytes())
	case types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Values)
		n = cmp.Uint16Less(u16, uint16(val), bits.Bytes())
	case types.BlockInt8:
		i8 := util.ReinterpretSlice[T, int8](c.Values)
		n = cmp.Int8Less(i8, int8(val), bits.Bytes())
	case types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Values)
		n = cmp.Uint8Less(u8, uint8(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *RawContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockInt64:
		i64 := util.ReinterpretSlice[T, int64](c.Values)
		n = cmp.Int64LessEqual(i64, int64(val), bits.Bytes())
	case types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Values)
		n = cmp.Uint64LessEqual(u64, uint64(val), bits.Bytes())
	case types.BlockInt32:
		i32 := util.ReinterpretSlice[T, int32](c.Values)
		n = cmp.Int32LessEqual(i32, int32(val), bits.Bytes())
	case types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Values)
		n = cmp.Uint32LessEqual(u32, uint32(val), bits.Bytes())
	case types.BlockInt16:
		i16 := util.ReinterpretSlice[T, int16](c.Values)
		n = cmp.Int16LessEqual(i16, int16(val), bits.Bytes())
	case types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Values)
		n = cmp.Uint16LessEqual(u16, uint16(val), bits.Bytes())
	case types.BlockInt8:
		i8 := util.ReinterpretSlice[T, int8](c.Values)
		n = cmp.Int8LessEqual(i8, int8(val), bits.Bytes())
	case types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Values)
		n = cmp.Uint8LessEqual(u8, uint8(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *RawContainer[T]) MatchGreater(val T, bits, mask *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockInt64:
		i64 := util.ReinterpretSlice[T, int64](c.Values)
		n = cmp.Int64Greater(i64, int64(val), bits.Bytes())
	case types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Values)
		n = cmp.Uint64Greater(u64, uint64(val), bits.Bytes())
	case types.BlockInt32:
		i32 := util.ReinterpretSlice[T, int32](c.Values)
		n = cmp.Int32Greater(i32, int32(val), bits.Bytes())
	case types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Values)
		n = cmp.Uint32Greater(u32, uint32(val), bits.Bytes())
	case types.BlockInt16:
		i16 := util.ReinterpretSlice[T, int16](c.Values)
		n = cmp.Int16Greater(i16, int16(val), bits.Bytes())
	case types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Values)
		n = cmp.Uint16Greater(u16, uint16(val), bits.Bytes())
	case types.BlockInt8:
		i8 := util.ReinterpretSlice[T, int8](c.Values)
		n = cmp.Int8Greater(i8, int8(val), bits.Bytes())
	case types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Values)
		n = cmp.Uint8Greater(u8, uint8(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *RawContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockInt64:
		i64 := util.ReinterpretSlice[T, int64](c.Values)
		n = cmp.Int64GreaterEqual(i64, int64(val), bits.Bytes())
	case types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Values)
		n = cmp.Uint64GreaterEqual(u64, uint64(val), bits.Bytes())
	case types.BlockInt32:
		i32 := util.ReinterpretSlice[T, int32](c.Values)
		n = cmp.Int32GreaterEqual(i32, int32(val), bits.Bytes())
	case types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Values)
		n = cmp.Uint32GreaterEqual(u32, uint32(val), bits.Bytes())
	case types.BlockInt16:
		i16 := util.ReinterpretSlice[T, int16](c.Values)
		n = cmp.Int16GreaterEqual(i16, int16(val), bits.Bytes())
	case types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Values)
		n = cmp.Uint16GreaterEqual(u16, uint16(val), bits.Bytes())
	case types.BlockInt8:
		i8 := util.ReinterpretSlice[T, int8](c.Values)
		n = cmp.Int8GreaterEqual(i8, int8(val), bits.Bytes())
	case types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Values)
		n = cmp.Uint8GreaterEqual(u8, uint8(val), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *RawContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) {
	var n int64
	switch c.typ {
	case types.BlockInt64:
		i64 := util.ReinterpretSlice[T, int64](c.Values)
		n = cmp.Int64Between(i64, int64(a), int64(b), bits.Bytes())
	case types.BlockUint64:
		u64 := util.ReinterpretSlice[T, uint64](c.Values)
		n = cmp.Uint64Between(u64, uint64(a), uint64(b), bits.Bytes())
	case types.BlockInt32:
		i32 := util.ReinterpretSlice[T, int32](c.Values)
		n = cmp.Int32Between(i32, int32(a), int32(b), bits.Bytes())
	case types.BlockUint32:
		u32 := util.ReinterpretSlice[T, uint32](c.Values)
		n = cmp.Uint32Between(u32, uint32(a), uint32(b), bits.Bytes())
	case types.BlockInt16:
		i16 := util.ReinterpretSlice[T, int16](c.Values)
		n = cmp.Int16Between(i16, int16(a), int16(b), bits.Bytes())
	case types.BlockUint16:
		u16 := util.ReinterpretSlice[T, uint16](c.Values)
		n = cmp.Uint16Between(u16, uint16(a), uint16(b), bits.Bytes())
	case types.BlockInt8:
		i8 := util.ReinterpretSlice[T, int8](c.Values)
		n = cmp.Int8Between(i8, int8(a), int8(b), bits.Bytes())
	case types.BlockUint8:
		u8 := util.ReinterpretSlice[T, uint8](c.Values)
		n = cmp.Uint8Between(u8, uint8(a), uint8(b), bits.Bytes())
	}
	bits.ResetCount(int(n))
}

func (c *RawContainer[T]) MatchInSet(s any, bits, mask *Bitset) {
	set := s.(*xroar.Bitmap)
	if mask != nil {
		// only process values from mask
		u32 := arena.Alloc[uint32](mask.Count())
		for _, k := range mask.Indexes(u32) {
			i := int(k)
			if set.Contains(uint64(c.Values[i])) {
				bits.Set(i)
			}
		}
		arena.Free(u32)
	} else {
		for i, v := range c.Values {
			if set.Contains(uint64(v)) {
				bits.Set(i)
			}
		}
	}
}

func (c *RawContainer[T]) MatchNotInSet(s any, bits, mask *Bitset) {
	set := s.(*xroar.Bitmap)
	if mask != nil {
		// only process values from mask
		u32 := arena.Alloc[uint32](mask.Count())
		for _, k := range mask.Indexes(u32) {
			i := int(k)
			if !set.Contains(uint64(c.Values[i])) {
				bits.Set(i)
			}
		}
		arena.Free(u32)
	} else {
		for i, v := range c.Values {
			if !set.Contains(uint64(v)) {
				bits.Set(i)
			}
		}
	}
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

func newRawContainer[T types.Integer]() NumberContainer[T] {
	switch any(T(0)).(type) {
	case int64:
		return rawFactory.i64Pool.Get().(NumberContainer[T])
	case int32:
		return rawFactory.i32Pool.Get().(NumberContainer[T])
	case int16:
		return rawFactory.i16Pool.Get().(NumberContainer[T])
	case int8:
		return rawFactory.i8Pool.Get().(NumberContainer[T])
	case uint64:
		return rawFactory.u64Pool.Get().(NumberContainer[T])
	case uint32:
		return rawFactory.u32Pool.Get().(NumberContainer[T])
	case uint16:
		return rawFactory.u16Pool.Get().(NumberContainer[T])
	case uint8:
		return rawFactory.u8Pool.Get().(NumberContainer[T])
	default:
		return nil
	}
}

func putRawContainer[T types.Integer](c NumberContainer[T]) {
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
	i64Pool: sync.Pool{New: func() any { return new(RawContainer[int64]) }},
	i32Pool: sync.Pool{New: func() any { return new(RawContainer[int32]) }},
	i16Pool: sync.Pool{New: func() any { return new(RawContainer[int16]) }},
	i8Pool:  sync.Pool{New: func() any { return new(RawContainer[int8]) }},
	u64Pool: sync.Pool{New: func() any { return new(RawContainer[uint64]) }},
	u32Pool: sync.Pool{New: func() any { return new(RawContainer[uint32]) }},
	u16Pool: sync.Pool{New: func() any { return new(RawContainer[uint16]) }},
	u8Pool:  sync.Pool{New: func() any { return new(RawContainer[uint8]) }},
}
