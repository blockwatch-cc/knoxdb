// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/bitpack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TIntegerBitpacked
type BitpackContainer[T types.Integer] struct {
	Packed []byte
	Log2   int
	N      int
	For    T
	free   bool
	unpack bitpack.UnpackFunc
}

func (c *BitpackContainer[T]) Close() {
	if c.free {
		arena.Free(arena.AllocBytes, c.Packed)
	}
	c.Packed = nil
	c.free = false
	c.unpack = nil
	putBitpackContainer[T](c)
}

func (c *BitpackContainer[T]) Type() IntegerContainerType {
	return TIntegerBitpacked
}

func (c *BitpackContainer[T]) Len() int {
	return c.N
}

func (c *BitpackContainer[T]) MaxSize() int {
	// Typ (1) + FOR (varint) + log2 (1) + n (varint) + bits (variable)
	return 2 + 2*num.MaxVarintLen64 + len(c.Packed)
}

func (c *BitpackContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntegerBitpacked))
	dst = num.AppendUvarint(dst, uint64(c.For))
	dst = num.AppendUvarint(dst, uint64(c.Log2))
	dst = num.AppendUvarint(dst, uint64(c.N))
	return append(dst, c.Packed...)
}

func (c *BitpackContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntegerBitpacked) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	c.For = T(v)
	buf = buf[n:]
	v, n = num.Uvarint(buf)
	c.Log2 = int(v)
	buf = buf[n:]
	v, n = num.Uvarint(buf)
	c.N = int(v)
	buf = buf[n:]

	// init unpacker func
	c.unpack = bitpack.Unpacker(c.Log2)

	// reference next sz bytes as bitpacked data
	sz := bitpack.EstimateMaxSize(c.Log2, c.N)
	c.Packed = buf[:sz]
	return buf[sz:], nil
}

func (c *BitpackContainer[T]) Get(n int) T {
	return T(c.unpack(c.Packed, n)) + c.For
}

func (c *BitpackContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	for _, v := range sel {
		dst = append(dst, T(c.unpack(c.Packed, int(v)))+c.For)
	}
	return dst
}

func (c *BitpackContainer[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	sz := bitpack.EstimateMaxSize(ctx.UseBits, len(vals))
	c.Packed = arena.Alloc(arena.AllocBytes, sz).([]byte)[:sz]
	c.free = true
	c.Log2 = ctx.UseBits
	c.N = len(vals)
	c.For = ctx.Min
	c.unpack = bitpack.Unpacker(c.Log2)

	var err error
	switch BlockType[T]() {
	case types.BlockInt64:
		v := util.ReinterpretSlice[T, int64](vals)
		_, _, err = bitpack.EncodeInt64(c.Packed, v, int64(ctx.Min), int64(ctx.Max))
	case types.BlockUint64:
		v := util.ReinterpretSlice[T, uint64](vals)
		_, _, err = bitpack.EncodeUint64(c.Packed, v, uint64(ctx.Min), uint64(ctx.Max))
	case types.BlockInt32:
		v := util.ReinterpretSlice[T, int32](vals)
		_, _, err = bitpack.EncodeInt32(c.Packed, v, int32(ctx.Min), int32(ctx.Max))
	case types.BlockUint32:
		v := util.ReinterpretSlice[T, uint32](vals)
		_, _, err = bitpack.EncodeUint32(c.Packed, v, uint32(ctx.Min), uint32(ctx.Max))
	case types.BlockInt16:
		v := util.ReinterpretSlice[T, int16](vals)
		_, _, err = bitpack.EncodeInt16(c.Packed, v, int16(ctx.Min), int16(ctx.Max))
	case types.BlockUint16:
		v := util.ReinterpretSlice[T, uint16](vals)
		_, _, err = bitpack.EncodeUint16(c.Packed, v, uint16(ctx.Min), uint16(ctx.Max))
	case types.BlockInt8:
		v := util.ReinterpretSlice[T, int8](vals)
		_, _, err = bitpack.EncodeInt8(c.Packed, v, int8(ctx.Min), int8(ctx.Max))
	case types.BlockUint8:
		v := util.ReinterpretSlice[T, uint8](vals)
		_, _, err = bitpack.EncodeUint8(c.Packed, v, uint8(ctx.Min), uint8(ctx.Max))
	}
	if err != nil {
		panic(err)
	}

	return c
}

func (c *BitpackContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	// convert val to MinFOR reference, prevent wrapping
	if val < c.For {
		return bits
	}
	val -= c.For

	// call bitpack cmp function for width
	return bitpack.Equal(c.Packed, c.Log2, uint64(val), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	// convert val to MinFOR reference, prevent wrapping
	if val < c.For {
		return bits.One()
	}
	val -= c.For

	// call bitpack cmp function for width
	return bitpack.NotEqual(c.Packed, c.Log2, uint64(val), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	// convert val to MinFOR reference, prevent wrapping
	if val < c.For {
		return bits
	}
	val -= c.For

	// call bitpack cmp function for width
	return bitpack.Less(c.Packed, c.Log2, uint64(val), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	// convert val to MinFOR reference, prevent wrapping
	if val < c.For {
		return bits
	}
	val -= c.For

	// call bitpack cmp function for width
	return bitpack.LessEqual(c.Packed, c.Log2, uint64(val), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	// convert val to MinFOR reference, prevent wrapping
	if val < c.For {
		return bits.One()
	}
	val -= c.For

	// call bitpack cmp function for width
	return bitpack.Greater(c.Packed, c.Log2, uint64(val), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	// convert val to MinFOR reference, prevent wrapping
	if val < c.For {
		return bits.One()
	}
	val -= c.For

	// call bitpack cmp function for width
	return bitpack.GreaterEqual(c.Packed, c.Log2, uint64(val), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	// convert val to MinFOR reference, prevent wrapping
	if b < c.For {
		return bits
	}
	if a < c.For {
		a = c.For
	}
	a -= c.For
	b -= c.For

	// call bitpack cmp function for width
	return bitpack.Between(c.Packed, c.Log2, uint64(a), uint64(b), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// TODO: performance: iterator or decode all?
	set := s.(*xroar.Bitmap)
	if mask != nil {
		// only process values from mask
		u32 := arena.AllocT[uint32](mask.Count())
		for _, k := range mask.Indexes(u32) {
			i := int(k)
			if set.Contains(uint64(T(c.unpack(c.Packed, i)) + c.For)) {
				bits.Set(i)
			}
		}
		arena.FreeT(u32)
	} else {
		for i := range c.Len() {
			if set.Contains(uint64(T(c.unpack(c.Packed, i)) + c.For)) {
				bits.Set(i)
			}
		}
	}
	return bits
}

func (c *BitpackContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// TODO: performance: iterator or decode all?
	set := s.(*xroar.Bitmap)
	if mask != nil {
		// only process values from mask
		u32 := arena.AllocT[uint32](mask.Count())
		for _, k := range mask.Indexes(u32) {
			i := int(k)
			if !set.Contains(uint64(T(c.unpack(c.Packed, i)) + c.For)) {
				bits.Set(i)
			}
		}
		arena.FreeT(u32)
	} else {
		for i := range c.Len() {
			if !set.Contains(uint64(T(c.unpack(c.Packed, i)) + c.For)) {
				bits.Set(i)
			}
		}
	}
	return bits
}

type BitpackFactory struct {
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
}

func newBitpackContainer[T types.Integer]() IntegerContainer[T] {
	switch any(T(0)).(type) {
	case int64:
		return bitpackFactory.i64Pool.Get().(IntegerContainer[T])
	case int32:
		return bitpackFactory.i32Pool.Get().(IntegerContainer[T])
	case int16:
		return bitpackFactory.i16Pool.Get().(IntegerContainer[T])
	case int8:
		return bitpackFactory.i8Pool.Get().(IntegerContainer[T])
	case uint64:
		return bitpackFactory.u64Pool.Get().(IntegerContainer[T])
	case uint32:
		return bitpackFactory.u32Pool.Get().(IntegerContainer[T])
	case uint16:
		return bitpackFactory.u16Pool.Get().(IntegerContainer[T])
	case uint8:
		return bitpackFactory.u8Pool.Get().(IntegerContainer[T])
	default:
		return nil
	}
}

func putBitpackContainer[T types.Integer](c IntegerContainer[T]) {
	switch any(T(0)).(type) {
	case int64:
		bitpackFactory.i64Pool.Put(c)
	case int32:
		bitpackFactory.i32Pool.Put(c)
	case int16:
		bitpackFactory.i16Pool.Put(c)
	case int8:
		bitpackFactory.i8Pool.Put(c)
	case uint64:
		bitpackFactory.u64Pool.Put(c)
	case uint32:
		bitpackFactory.u32Pool.Put(c)
	case uint16:
		bitpackFactory.u16Pool.Put(c)
	case uint8:
		bitpackFactory.u8Pool.Put(c)
	}
}

var bitpackFactory = BitpackFactory{
	i64Pool: sync.Pool{
		New: func() any { return new(BitpackContainer[int64]) },
	},
	i32Pool: sync.Pool{
		New: func() any { return new(BitpackContainer[int32]) },
	},
	i16Pool: sync.Pool{
		New: func() any { return new(BitpackContainer[int16]) },
	},
	i8Pool: sync.Pool{
		New: func() any { return new(BitpackContainer[int8]) },
	},
	u64Pool: sync.Pool{
		New: func() any { return new(BitpackContainer[uint64]) },
	},
	u32Pool: sync.Pool{
		New: func() any { return new(BitpackContainer[uint32]) },
	},
	u16Pool: sync.Pool{
		New: func() any { return new(BitpackContainer[uint16]) },
	},
	u8Pool: sync.Pool{
		New: func() any { return new(BitpackContainer[uint8]) },
	},
}
