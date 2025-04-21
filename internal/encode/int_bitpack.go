// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
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
	dec    *bitpack.Decoder[T]
}

func (c *BitpackContainer[T]) Info() string {
	return fmt.Sprintf("BP(%s)_[w=%d,n=%d]", TypeName[T](), c.Log2, c.N)
}

func (c *BitpackContainer[T]) Close() {
	if c.free {
		arena.Free(c.Packed)
	}
	c.Packed = nil
	c.free = false
	c.dec = nil
	putBitpackContainer[T](c)
}

func (c *BitpackContainer[T]) Type() IntegerContainerType {
	return TIntegerBitpacked
}

func (c *BitpackContainer[T]) Len() int {
	return c.N
}

func (c *BitpackContainer[T]) Size() int {
	// Typ (1) + FOR (varint) + log2 (1) + n (varint) + bits (variable)
	return 2 + num.UvarintLen(c.For) + num.UvarintLen(c.N) + len(c.Packed)
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

	// reference next sz bytes as bitpacked data
	sz := bitpack.EstimateSize(util.SizeOf[T]()*8, c.Log2, c.N)
	c.Packed = buf[:sz]

	// init decoder
	c.dec = bitpack.NewDecoder[T](c.Packed, c.Log2, c.For)

	return buf[sz:], nil
}

func (c *BitpackContainer[T]) Get(n int) T {
	return c.dec.DecodeValue(n)
}

func (c *BitpackContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	if sel == nil {
		for i := range c.Len() {
			dst = append(dst, c.dec.DecodeValue(i))
		}
	} else {
		for _, v := range sel {
			dst = append(dst, c.dec.DecodeValue(int(v)))
		}
	}
	return dst
}

func (c *BitpackContainer[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	c.N = len(vals)
	c.For = ctx.Min

	sz := bitpack.EstimateSize(ctx.PhyBits, ctx.UseBits, len(vals))
	// fmt.Printf("BP size for %d vals at %d bits and %d-bit padding is %d bytes (%d bits), minv=%d maxv=%d typ=%T\n",
	// 	len(vals), ctx.UseBits, ctx.PhyBits, sz, sz*8, ctx.Min, ctx.Max, T(0))
	buf := arena.AllocBytes(sz)[:sz]
	// clear(c.Packed) // arena does not allocate zeroed memory

	c.Packed, c.Log2 = bitpack.Encode(buf, vals, ctx.Min, ctx.Max)
	c.free = true
	c.dec = bitpack.NewDecoder(c.Packed, c.Log2, c.For)

	return c
}

func (c *BitpackContainer[T]) MatchEqual(val T, bits, _ *Bitset) {
	// convert val to MinFOR reference, prevent wrapping
	if val < c.For {
		return
	}
	val -= c.For

	// call bitpack cmp function for width
	bitpack.Equal(c.Packed, c.Log2, uint64(val), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchNotEqual(val T, bits, _ *Bitset) {
	// convert val to MinFOR reference, prevent wrapping
	if val < c.For {
		bits.One()
		return
	}
	val -= c.For

	// call bitpack cmp function for width
	bitpack.NotEqual(c.Packed, c.Log2, uint64(val), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchLess(val T, bits, _ *Bitset) {
	// convert val to MinFOR reference, prevent wrapping
	if val < c.For {
		return
	}
	val -= c.For

	// call bitpack cmp function for width
	bitpack.Less(c.Packed, c.Log2, uint64(val), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchLessEqual(val T, bits, _ *Bitset) {
	// convert val to MinFOR reference, prevent wrapping
	if val < c.For {
		return
	}
	val -= c.For

	// call bitpack cmp function for width
	bitpack.LessEqual(c.Packed, c.Log2, uint64(val), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchGreater(val T, bits, _ *Bitset) {
	// convert val to MinFOR reference, prevent wrapping
	if val < c.For {
		bits.One()
		return
	}
	val -= c.For

	// call bitpack cmp function for width
	bitpack.Greater(c.Packed, c.Log2, uint64(val), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchGreaterEqual(val T, bits, _ *Bitset) {
	// convert val to MinFOR reference, prevent wrapping
	if val < c.For {
		bits.One()
		return
	}
	val -= c.For

	// call bitpack cmp function for width
	bitpack.GreaterEqual(c.Packed, c.Log2, uint64(val), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchBetween(a, b T, bits, _ *Bitset) {
	// convert val to MinFOR reference, prevent wrapping
	if b < c.For {
		return
	}
	if a < c.For {
		a = c.For
	}

	// ensure overflow free calculations
	a = T(uint64(a - c.For))
	b = T(uint64(b - c.For))

	// call bitpack cmp function for width
	bitpack.Between(c.Packed, c.Log2, uint64(a), uint64(b), c.Len(), bits)
}

func (c *BitpackContainer[T]) MatchInSet(s any, bits, mask *Bitset) {
	// TODO: performance: iterator or decode all?
	set := s.(*xroar.Bitmap)
	if mask != nil {
		// only process values from mask
		u32 := arena.Alloc[uint32](mask.Count())
		for _, k := range mask.Indexes(u32) {
			i := int(k)
			if set.Contains(uint64(c.dec.DecodeValue(i))) {
				bits.Set(i)
			}
		}
		arena.Free(u32)
	} else {
		for i := range c.Len() {
			if set.Contains(uint64(c.dec.DecodeValue(i))) {
				bits.Set(i)
			}
		}
	}
}

func (c *BitpackContainer[T]) MatchNotInSet(s any, bits, mask *Bitset) {
	// TODO: performance: iterator or decode all?
	set := s.(*xroar.Bitmap)
	if mask != nil {
		// only process values from mask
		u32 := arena.Alloc[uint32](mask.Count())
		for _, k := range mask.Indexes(u32) {
			i := int(k)
			if !set.Contains(uint64(c.dec.DecodeValue(i))) {
				bits.Set(i)
			}
		}
		arena.Free(u32)
	} else {
		for i := range c.Len() {
			if !set.Contains(uint64(c.dec.DecodeValue(i))) {
				bits.Set(i)
			}
		}
	}
}

// ---------------------------------------
// Factory
//

type BitpackFactory struct {
	i64Pool   sync.Pool // container pools
	i32Pool   sync.Pool
	i16Pool   sync.Pool
	i8Pool    sync.Pool
	u64Pool   sync.Pool
	u32Pool   sync.Pool
	u16Pool   sync.Pool
	u8Pool    sync.Pool
	i64ItPool sync.Pool // iterator pools
	i32ItPool sync.Pool
	i16ItPool sync.Pool
	i8ItPool  sync.Pool
	u64ItPool sync.Pool
	u32ItPool sync.Pool
	u16ItPool sync.Pool
	u8ItPool  sync.Pool
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

func newBitpackIterator[T types.Integer]() *BitpackIterator[T] {
	switch any(T(0)).(type) {
	case int64:
		return bitpackFactory.i64ItPool.Get().(*BitpackIterator[T])
	case int32:
		return bitpackFactory.i32ItPool.Get().(*BitpackIterator[T])
	case int16:
		return bitpackFactory.i16ItPool.Get().(*BitpackIterator[T])
	case int8:
		return bitpackFactory.i8ItPool.Get().(*BitpackIterator[T])
	case uint64:
		return bitpackFactory.u64ItPool.Get().(*BitpackIterator[T])
	case uint32:
		return bitpackFactory.u32ItPool.Get().(*BitpackIterator[T])
	case uint16:
		return bitpackFactory.u16ItPool.Get().(*BitpackIterator[T])
	case uint8:
		return bitpackFactory.u8ItPool.Get().(*BitpackIterator[T])
	default:
		return nil
	}
}

func putBitpackIterator[T types.Integer](c *BitpackIterator[T]) {
	switch any(T(0)).(type) {
	case int64:
		bitpackFactory.i64ItPool.Put(c)
	case int32:
		bitpackFactory.i32ItPool.Put(c)
	case int16:
		bitpackFactory.i16ItPool.Put(c)
	case int8:
		bitpackFactory.i8ItPool.Put(c)
	case uint64:
		bitpackFactory.u64ItPool.Put(c)
	case uint32:
		bitpackFactory.u32ItPool.Put(c)
	case uint16:
		bitpackFactory.u16ItPool.Put(c)
	case uint8:
		bitpackFactory.u8ItPool.Put(c)
	}
}

var bitpackFactory = BitpackFactory{
	i64Pool:   sync.Pool{New: func() any { return new(BitpackContainer[int64]) }},
	i32Pool:   sync.Pool{New: func() any { return new(BitpackContainer[int32]) }},
	i16Pool:   sync.Pool{New: func() any { return new(BitpackContainer[int16]) }},
	i8Pool:    sync.Pool{New: func() any { return new(BitpackContainer[int8]) }},
	u64Pool:   sync.Pool{New: func() any { return new(BitpackContainer[uint64]) }},
	u32Pool:   sync.Pool{New: func() any { return new(BitpackContainer[uint32]) }},
	u16Pool:   sync.Pool{New: func() any { return new(BitpackContainer[uint16]) }},
	u8Pool:    sync.Pool{New: func() any { return new(BitpackContainer[uint8]) }},
	i64ItPool: sync.Pool{New: func() any { return new(BitpackIterator[int64]) }},
	i32ItPool: sync.Pool{New: func() any { return new(BitpackIterator[int32]) }},
	i16ItPool: sync.Pool{New: func() any { return new(BitpackIterator[int16]) }},
	i8ItPool:  sync.Pool{New: func() any { return new(BitpackIterator[int8]) }},
	u64ItPool: sync.Pool{New: func() any { return new(BitpackIterator[uint64]) }},
	u32ItPool: sync.Pool{New: func() any { return new(BitpackIterator[uint32]) }},
	u16ItPool: sync.Pool{New: func() any { return new(BitpackIterator[uint16]) }},
	u8ItPool:  sync.Pool{New: func() any { return new(BitpackIterator[uint8]) }},
}

// ---------------------------------------
// Iterator
//

func (c *BitpackContainer[T]) Iterator() Iterator[T] {
	it := newBitpackIterator[T]()
	it.dec = c.dec
	it.len = c.Len()
	return it
}

type BitpackIterator[T types.Integer] struct {
	vals [CHUNK_SIZE]T
	dec  *bitpack.Decoder[T]
	len  int
	ofs  int
}

func (it *BitpackIterator[T]) Close() {
	it.len = 0
	it.ofs = 0
	putBitpackIterator(it)
}

func (it *BitpackIterator[T]) Reset() {
	it.ofs = 0
}

func (it *BitpackIterator[T]) Len() int {
	return it.len
}

func (it *BitpackIterator[T]) Next() (T, bool) {
	if it.ofs >= it.len {
		// EOF
		return 0, false
	}

	// ofs % CHUNK_SIZE
	i := it.ofs & CHUNK_MASK

	// on first call or start of new chunk
	if i == 0 {
		// load next source chunks
		n := it.dec.DecodeChunk(&it.vals, it.ofs)

		// sanity check, should not happen
		if n == 0 {
			it.ofs = it.len
			return 0, false
		}
	}

	// advance ofs for next call
	it.ofs++

	// return value
	return it.vals[i], true
}

func (it *BitpackIterator[T]) NextChunk() (*[CHUNK_SIZE]T, int) {
	// EOF
	if it.ofs >= it.len {
		return nil, 0
	}
	n := it.dec.DecodeChunk(&it.vals, it.ofs)
	if n > 0 {
		it.ofs += n
	}
	return &it.vals, n
}

func (it *BitpackIterator[T]) SkipChunk() int {
	it.ofs = chunkStart(it.ofs + CHUNK_SIZE)
	return CHUNK_SIZE
}

func (it *BitpackIterator[T]) Seek(n int) bool {
	if n < 0 || n >= it.len {
		return false
	}

	// load when n is in another chunk and not at first position
	if n&CHUNK_MASK != 0 && chunkStart(n) != chunkStart(it.ofs) {
		it.ofs = chunkStart(n)
		it.NextChunk()
	}

	// reset ofs to n, so call to Next() delivers value
	it.ofs = n
	return true
}
