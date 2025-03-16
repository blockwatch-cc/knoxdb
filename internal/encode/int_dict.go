// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"slices"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// TIntegerDictionary
type DictionaryContainer[T types.Integer] struct {
	For    T
	Values IntegerContainer[T]
	Codes  IntegerContainer[uint16]
}

func (c *DictionaryContainer[T]) Close() {
	c.Values.Close()
	c.Codes.Close()
	c.Values = nil
	c.Codes = nil
	putDictionaryContainer[T](c)
}

func (c *DictionaryContainer[T]) Type() IntegerContainerType {
	return TIntegerDictionary
}

func (c *DictionaryContainer[T]) Len() int {
	return c.Codes.Len()
}

func (c *DictionaryContainer[T]) MaxSize() int {
	return 1 + num.MaxVarintLen64 + c.Values.MaxSize() + c.Codes.MaxSize()
}

func (c *DictionaryContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntegerDictionary))
	dst = num.AppendUvarint(dst, uint64(c.For))
	dst = c.Values.Store(dst)
	return c.Codes.Store(dst)
}

func (c *DictionaryContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntegerDictionary) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	c.For = T(v)
	buf = buf[n:]

	// alloc and decode values child container
	c.Values = NewInt[T](IntegerContainerType(buf[0]))
	var err error
	buf, err = c.Values.Load(buf)
	if err != nil {
		return buf, err
	}

	// alloc and decode ends child container
	c.Codes = NewInt[uint16](IntegerContainerType(buf[0]))
	return c.Codes.Load(buf)
}

func (c *DictionaryContainer[T]) Get(n int) T {
	return c.Values.Get(int(c.Codes.Get(n))) + c.For
}

func (c *DictionaryContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	for _, v := range sel {
		dst = append(dst, c.Get(int(v)))
	}
	return dst
}

func (c *DictionaryContainer[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	// init FOR
	c.For = ctx.Min

	// construct unique values map (if not done during analysis)
	// unique := ctx.Unique
	if ctx.Unique == nil {
		ctx.Unique = make(map[T]uint16, ctx.NumUnique)
	}
	if len(ctx.Unique) == 0 {
		for _, v := range vals {
			ctx.Unique[v] = 0
		}
	}

	// construct dict from unique values (apply FOR)
	// dict := make([]T, 0, len(unique))
	dict := arena.AllocT[T](len(ctx.Unique))
	for v := range ctx.Unique {
		dict = append(dict, v-c.For)
	}

	// sort dict
	slices.Sort(dict)

	// remap dict codes to original values (we re-use the existing Unique map
	// to avoid more allocations)
	for i, v := range dict {
		ctx.Unique[v+c.For] = uint16(i)
	}

	// construct codes
	// codes := make([]uint16, len(vals))
	codes := arena.Alloc(arena.AllocUint16, len(vals)).([]uint16)[:len(vals)]
	for i, v := range vals {
		codes[i] = ctx.Unique[v]
	}

	// encode child containers
	// fmt.Println("Dict Values ..")
	vctx := AnalyzeInt(dict, false)
	c.Values = EncodeInt(vctx, dict, lvl-1)
	vctx.Close()
	if c.Values.Type() != TIntegerRaw {
		arena.FreeT(dict)
	}
	// fmt.Println("Dict Codes ..")
	cctx := AnalyzeInt(codes, false)
	c.Codes = EncodeInt(cctx, codes, lvl-1)
	cctx.Close()
	if c.Codes.Type() != TIntegerRaw {
		arena.Free(arena.AllocUint16, codes)
	}
	// fmt.Println("Dict done.")
	return c
}

func (c *DictionaryContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *DictionaryContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

type DictionaryFactory struct {
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
}

func newDictionaryContainer[T types.Integer]() IntegerContainer[T] {
	switch (any(T(0))).(type) {
	case int64:
		return dictionaryFactory.i64Pool.Get().(IntegerContainer[T])
	case int32:
		return dictionaryFactory.i32Pool.Get().(IntegerContainer[T])
	case int16:
		return dictionaryFactory.i16Pool.Get().(IntegerContainer[T])
	case int8:
		return dictionaryFactory.i8Pool.Get().(IntegerContainer[T])
	case uint64:
		return dictionaryFactory.u64Pool.Get().(IntegerContainer[T])
	case uint32:
		return dictionaryFactory.u32Pool.Get().(IntegerContainer[T])
	case uint16:
		return dictionaryFactory.u16Pool.Get().(IntegerContainer[T])
	case uint8:
		return dictionaryFactory.u8Pool.Get().(IntegerContainer[T])
	default:
		return nil
	}
}

func putDictionaryContainer[T types.Integer](c IntegerContainer[T]) {
	switch (any(T(0))).(type) {
	case int64:
		dictionaryFactory.i64Pool.Put(c)
	case int32:
		dictionaryFactory.i32Pool.Put(c)
	case int16:
		dictionaryFactory.i16Pool.Put(c)
	case int8:
		dictionaryFactory.i8Pool.Put(c)
	case uint64:
		dictionaryFactory.u64Pool.Put(c)
	case uint32:
		dictionaryFactory.u32Pool.Put(c)
	case uint16:
		dictionaryFactory.u16Pool.Put(c)
	case uint8:
		dictionaryFactory.u8Pool.Put(c)
	}
}

var dictionaryFactory = DictionaryFactory{
	i64Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[int64]) },
	},
	i32Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[int32]) },
	},
	i16Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[int16]) },
	},
	i8Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[int8]) },
	},
	u64Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[uint64]) },
	},
	u32Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[uint32]) },
	},
	u16Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[uint16]) },
	},
	u8Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[uint8]) },
	},
}
