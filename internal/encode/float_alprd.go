// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/alp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// TFloatAlpRd
type FloatAlpRdContainer[T types.Float] struct {
	Left  IntegerContainer[uint16]
	Right IntegerContainer[uint64]
	Shift int
	typ   types.BlockType
}

func (c *FloatAlpRdContainer[T]) Info() string {
	return fmt.Sprintf("ALP-RD(%s)_[>>%d]_[%s]_[%s]",
		TypeName[T](), c.Shift, c.Left.Info(), c.Right.Info())
}

func (c *FloatAlpRdContainer[T]) Close() {
	c.Left.Close()
	c.Right.Close()
	c.Left = nil
	c.Right = nil
	putFloatAlpRdContainer(c)
}

func (c *FloatAlpRdContainer[T]) Type() FloatContainerType {
	return TFloatAlpRd
}

func (c *FloatAlpRdContainer[T]) Len() int {
	return c.Left.Len()
}

func (c *FloatAlpRdContainer[T]) Size() int {
	v := 2 + c.Left.Size() + c.Right.Size()
	return v
}

func (c *FloatAlpRdContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatAlpRd))
	dst = c.Left.Store(dst)
	dst = c.Right.Store(dst)
	dst = num.AppendUvarint(dst, uint64(c.Shift))
	return dst
}

func (c *FloatAlpRdContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatAlpRd) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	c.Left = NewInt[uint16](IntegerContainerType(buf[0]))
	buf, err := c.Left.Load(buf)
	if err != nil {
		return buf, err
	}
	c.Right = NewInt[uint64](IntegerContainerType(buf[0]))
	buf, err = c.Right.Load(buf)
	if err != nil {
		return buf, err
	}
	v, n := num.Uvarint(buf)
	c.Shift = int(v)
	c.typ = BlockType[T]()
	return buf[n:], nil
}

func (c *FloatAlpRdContainer[T]) Get(n int) T {
	left := c.Left.Get(n)
	right := c.Right.Get(n)

	// float64
	if c.typ == types.BlockFloat64 {
		v := uint64(left)<<c.Shift | right
		return *(*T)(unsafe.Pointer(&v))
	}

	// float32
	v := uint32(left)<<c.Shift | uint32(right)
	return *(*T)(unsafe.Pointer(&v))
}

func (c *FloatAlpRdContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	if sel == nil {
		for i := range c.Len() {
			dst = append(dst, c.Get(i))
		}
	} else {
		for _, v := range sel {
			dst = append(dst, c.Get(int(v)))
		}
	}
	return dst
}

func (c *FloatAlpRdContainer[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	cnt := len(vals)
	left := arena.Alloc[uint16](cnt)[:cnt]
	right := arena.Alloc[uint64](cnt)[:cnt]
	c.typ = BlockType[T]()

	// ensure we have an ALP analysis result (mostly relevant for testcases)
	if !ctx.AlpEncoder.IsInit() || ctx.AlpEncoder.State().Scheme != alp.AlpRdScheme {
		// produce a small sample
		sample := arena.Alloc[T](alp.MaxSampleLen(cnt))
		alp.FirstLevelSample(sample, vals)

		// estimate best shift based on sample
		unique := arena.Alloc[uint16](1 << 16)[:1<<16]
		c.Shift = alp.EstimateRD(sample, unique).Shift
		arena.Free(unique)
		arena.Free(sample)
	} else {
		c.Shift = ctx.AlpEncoder.State().RD.Shift
	}

	// split input float vector into left and right integer parts
	alp.SplitRD(vals, left, right, c.Shift)

	// analyze parts
	lctx := AnalyzeInt(left, true)
	rctx := AnalyzeInt(right, false)

	// prefer left side dict compression when more efficient than bit-packing
	leftScheme := TIntegerBitpacked
	if lctx.preferDict() {
		leftScheme = TIntegerDictionary
	}

	// encode parts
	c.Left = NewInt[uint16](leftScheme).Encode(lctx, left, lvl-1)
	c.Right = NewInt[uint64](TIntegerBitpacked).Encode(rctx, right, lvl-1)

	// free temp allocations
	lctx.Close()
	rctx.Close()
	arena.Free(left)
	arena.Free(right)

	return c
}

func (c *FloatAlpRdContainer[T]) MatchEqual(val T, bits, mask *Bitset) {
}

func (c *FloatAlpRdContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) {
}

func (c *FloatAlpRdContainer[T]) MatchLess(val T, bits, mask *Bitset) {
}

func (c *FloatAlpRdContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) {
}

func (c *FloatAlpRdContainer[T]) MatchGreater(val T, bits, mask *Bitset) {
}

func (c *FloatAlpRdContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
}

func (c *FloatAlpRdContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) {
}

// N.A.
func (c *FloatAlpRdContainer[T]) MatchInSet(_ any, _, _ *Bitset)    {}
func (c *FloatAlpRdContainer[T]) MatchNotInSet(_ any, _, _ *Bitset) {}

type FloatAlpRdFactory struct {
	f64Pool   sync.Pool
	f32Pool   sync.Pool
	f64ItPool sync.Pool
	f32ItPool sync.Pool
}

func newFloatAlpRdContainer[T types.Float]() FloatContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpRdFactory.f64Pool.Get().(FloatContainer[T])
	case float32:
		return floatAlpRdFactory.f32Pool.Get().(FloatContainer[T])
	default:
		return nil
	}
}

func putFloatAlpRdContainer[T types.Float](c FloatContainer[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpRdFactory.f64Pool.Put(c)
	case float32:
		floatAlpRdFactory.f32Pool.Put(c)
	}
}

func newFloatAlpRdIterator[T types.Float]() *FloatAlpRdIterator[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpFactory.f64ItPool.Get().(*FloatAlpRdIterator[T])
	case float32:
		return floatAlpFactory.f32ItPool.Get().(*FloatAlpRdIterator[T])
	default:
		return nil
	}
}

func putFloatAlpRdIterator[T types.Float](c *FloatAlpRdIterator[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpFactory.f64ItPool.Put(c)
	case float32:
		floatAlpFactory.f32ItPool.Put(c)
	}
}

var floatAlpRdFactory = FloatAlpRdFactory{
	f64Pool:   sync.Pool{New: func() any { return new(FloatAlpRdContainer[float64]) }},
	f32Pool:   sync.Pool{New: func() any { return new(FloatAlpRdContainer[float32]) }},
	f64ItPool: sync.Pool{New: func() any { return new(FloatAlpRdIterator[float64]) }},
	f32ItPool: sync.Pool{New: func() any { return new(FloatAlpRdIterator[float32]) }},
}

func (c *FloatAlpRdContainer[T]) Iterator() Iterator[T] {
	it := newFloatAlpRdIterator[T]()
	it.leftIt = c.Left.Iterator()
	it.rightIt = c.Right.Iterator()
	it.shift = c.Shift
	it.len = c.Len()
	it.ofs = 0
	return it
}

type FloatAlpRdIterator[T types.Float] struct {
	vals    [CHUNK_SIZE]T
	leftIt  Iterator[uint16]
	rightIt Iterator[uint64]
	shift   int
	len     int
	ofs     int
}

func (it *FloatAlpRdIterator[T]) Close() {
	it.leftIt.Close()
	it.rightIt.Close()
	it.leftIt = nil
	it.rightIt = nil
	it.shift = 0
	it.len = 0
	it.ofs = 0
	putFloatAlpRdIterator(it)
}

func (it *FloatAlpRdIterator[T]) Reset() {
	it.ofs = 0
	it.leftIt.Reset()
	it.rightIt.Reset()
}

func (it *FloatAlpRdIterator[T]) Len() int {
	return it.len
}

func (it *FloatAlpRdIterator[T]) Next() (T, bool) {
	if it.ofs >= it.len {
		// EOF
		return 0, false
	}

	// ofs % CHUNK_SIZE
	i := it.ofs & CHUNK_MASK

	// on first call or start of new chunk
	if i == 0 {
		// load next source chunks
		left, ln := it.leftIt.NextChunk()
		right, rn := it.rightIt.NextChunk()

		// sanity check, should not happen
		if ln == 0 || rn == 0 {
			it.ofs = it.len
			return 0, false
		}

		// decode
		alp.MergeRD(it.vals[:], left[:], right[:], it.shift)
	}

	// advance ofs for next call
	it.ofs++

	// return value
	return it.vals[i], true
}

func (it *FloatAlpRdIterator[T]) NextChunk() (*[CHUNK_SIZE]T, int) {
	// EOF
	if it.ofs >= it.len {
		return nil, 0
	}
	left, ln := it.leftIt.NextChunk()
	right, rn := it.rightIt.NextChunk()
	if ln > 0 && rn > 0 {
		alp.MergeRD(it.vals[:], left[:], right[:], it.shift)
		it.ofs += ln
	}
	return &it.vals, ln
}

func (it *FloatAlpRdIterator[T]) SkipChunk() {
	it.leftIt.SkipChunk()
	it.rightIt.SkipChunk()
	it.ofs = chunkStart(it.ofs + CHUNK_SIZE)
}

func (it *FloatAlpRdIterator[T]) Seek(n int) bool {
	if n < 0 || n >= it.len {
		return false
	}
	ls, rs := it.leftIt.Seek(n), it.rightIt.Seek(n)
	if !ls || !rs {
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
