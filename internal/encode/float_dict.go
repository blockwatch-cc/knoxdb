// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"slices"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
)

// TFloatDictionary
type FloatDictionaryContainer[T types.Float] struct {
	Values FloatContainer[T]
	Codes  IntegerContainer[uint16]
}

func (c *FloatDictionaryContainer[T]) Close() {
	c.Values.Close()
	c.Codes.Close()
	c.Values = nil
	c.Codes = nil
	putFloatDictionaryContainer(c)
}

func (c *FloatDictionaryContainer[T]) Type() FloatContainerType {
	return TFloatDictionary
}

func (c *FloatDictionaryContainer[T]) Len() int {
	return c.Codes.Len()
}

func (c *FloatDictionaryContainer[T]) MaxSize() int {
	return 1 + c.Values.MaxSize() + c.Codes.MaxSize()
}

func (c *FloatDictionaryContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatDictionary))
	dst = c.Values.Store(dst)
	return c.Codes.Store(dst)
}

func (c *FloatDictionaryContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatDictionary) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	// alloc and decode values child container
	c.Values = NewFloat[T](FloatContainerType(buf[0]))
	var err error
	buf, err = c.Values.Load(buf)
	if err != nil {
		return buf, err
	}

	// alloc and decode ends child container
	c.Codes = NewInt[uint16](IntegerContainerType(buf[0]))
	return c.Codes.Load(buf)
}

func (c *FloatDictionaryContainer[T]) Get(n int) T {
	return c.Values.Get(int(c.Codes.Get(n)))
}

func (c *FloatDictionaryContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	for _, v := range sel {
		dst = append(dst, c.Get(int(v)))
	}
	return dst
}

func (c *FloatDictionaryContainer[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	// construct dictionary and encode vals
	dict, codes := dictEncodeFloatMap(ctx, vals)

	// encode child containers
	vctx := AnalyzeFloat(dict, false)
	c.Values = EncodeFloat(vctx, dict, lvl-1)
	vctx.Close()
	if c.Values.Type() != TFloatRaw {
		arena.FreeT(dict)
	}

	cctx := AnalyzeInt(codes, false)
	c.Codes = EncodeInt(cctx, codes, lvl-1)
	cctx.Close()
	if c.Codes.Type() != TIntegerRaw {
		arena.Free(arena.AllocUint16, codes)
	}

	return c
}

func dictEncodeFloatMap[T types.Float](ctx *FloatContext[T], vals []T) ([]T, []uint16) {
	// construct unique values map
	if ctx.UniqueMap == nil {
		ctx.UniqueMap = make(map[T]uint16, ctx.NumUnique)
		ctx.buildUniqueMap(vals)
	}

	// construct dict from unique values
	dict := arena.AllocT[T](len(ctx.UniqueMap))
	for v := range ctx.UniqueMap {
		dict = append(dict, v)
	}

	// sort dict
	slices.Sort(dict)

	// remap dict codes to original values
	for i, v := range dict {
		ctx.UniqueMap[v] = uint16(i)
	}

	// translate values to codes
	codes := arena.Alloc(arena.AllocUint16, len(vals)).([]uint16)[:0]
	for _, v := range vals {
		codes = append(codes, ctx.UniqueMap[v])
	}

	return dict, codes
}

func (c *FloatDictionaryContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatDictionaryContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatDictionaryContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatDictionaryContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatDictionaryContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatDictionaryContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatDictionaryContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatDictionaryContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *FloatDictionaryContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

type FloatDictionaryFactory struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
}

func newFloatDictionaryContainer[T types.Float]() FloatContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatDictionaryFactory.f64Pool.Get().(FloatContainer[T])
	case float32:
		return floatDictionaryFactory.f32Pool.Get().(FloatContainer[T])
	default:
		return nil
	}
}

func putFloatDictionaryContainer[T types.Float](c FloatContainer[T]) {
	switch (any(T(0))).(type) {
	case float64:
		floatDictionaryFactory.f64Pool.Put(c)
	case float32:
		floatDictionaryFactory.f32Pool.Put(c)
	}
}

var floatDictionaryFactory = FloatDictionaryFactory{
	f64Pool: sync.Pool{
		New: func() any { return new(FloatDictionaryContainer[float64]) },
	},
	f32Pool: sync.Pool{
		New: func() any { return new(FloatDictionaryContainer[float32]) },
	},
}
