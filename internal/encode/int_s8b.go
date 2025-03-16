// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/s8b"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TIntegerSimple8
type Simple8Container[T types.Integer] struct {
	For      T
	Packed   []byte
	Unpacked []T // TODO: we could walk selectors manually without copy
	free     bool
}

func (c *Simple8Container[T]) Close() {
	if c.free {
		arena.Free(arena.AllocUint64, util.FromByteSlice[uint64](c.Packed))
	}
	c.Packed = nil
	c.free = false
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
	return 1 + 2*num.MaxVarintLen64 + len(c.Packed)
}

func (c *Simple8Container[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntegerSimple8))
	dst = num.AppendUvarint(dst, uint64(c.For))
	dst = num.AppendUvarint(dst, uint64(len(c.Packed)))
	return append(dst, c.Packed...)
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
	for _, v := range sel {
		dst = append(dst, c.Unpacked[int(v)]+c.For)
	}
	return dst
}

func (c *Simple8Container[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	c.For = ctx.Min

	// s8b encoder works in-place on a u64 slice; consider overflows when ctx.Min is close to
	// signed int[8|16|32|64]-min
	u64 := arena.Alloc(arena.AllocUint64, len(vals)).([]uint64)[:len(vals)]
	for64 := uint64(c.For)
	for i, v := range vals {
		u64[i] = uint64(v) - for64
	}

	// encode and cast result slice
	// enc := s8b.NewEncoder()
	// enc.SetValues(u64)
	// var err error
	// c.Packed, err = enc.Bytes()
	// if err != nil {
	// 	panic(err)
	// }

	// encode reusing src buffer
	var err error
	u64, err = s8b.EncodeUint64(u64)
	if err != nil {
		panic(err)
	}
	c.Packed = util.ToByteSlice(u64)
	c.free = true
	// fmt.Printf("s8 %d vals => %d bytes\n", len(vals), len(c.Packed))

	return c
}

func (c *Simple8Container[T]) decodeAll() error {
	n, err := s8b.CountValues(c.Packed)
	if err != nil {
		return err
	}
	switch int(unsafe.Sizeof(c.For)) {
	case 8:
		u64 := arena.AllocT[uint64](n)[:n]
		n, err = s8b.DecodeUint64(u64, c.Packed)
		c.Unpacked = util.ReinterpretSlice[uint64, T](u64[:n])
	case 4:
		u32 := arena.AllocT[uint32](n)[:n]
		n, err = s8b.DecodeUint32(u32, c.Packed)
		c.Unpacked = util.ReinterpretSlice[uint32, T](u32[:n])
	case 2:
		u16 := arena.AllocT[uint16](n)[:n]
		n, err = s8b.DecodeUint16(u16, c.Packed)
		c.Unpacked = util.ReinterpretSlice[uint16, T](u16[:n])
	case 1:
		u8 := arena.AllocT[uint8](n)[:n]
		n, err = s8b.DecodeUint8(u8, c.Packed)
		c.Unpacked = util.ReinterpretSlice[uint8, T](u8[:n])
	}
	return err
}

func (c *Simple8Container[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *Simple8Container[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *Simple8Container[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *Simple8Container[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *Simple8Container[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *Simple8Container[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *Simple8Container[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *Simple8Container[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *Simple8Container[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
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
	switch (any(T(0))).(type) {
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
	switch (any(T(0))).(type) {
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
