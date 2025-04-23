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
	c.Shift = 0
	c.typ = 0
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
	it := c.Iterator()
	if sel == nil {
		for {
			src, n := it.NextChunk()
			if n == 0 {
				break
			}
			dst = append(dst, src[:n]...)
		}
	} else {
		for _, v := range sel {
			dst = append(dst, it.Get(int(v)))
		}
	}
	it.Close()
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

	// TODO
	// - left: always use dict when <=8 unique, analyze unique during split (build array)
	// - right: always BP, aggregate min/max during split
	// - skip int analyze and encode direct
	// - fusion: right always bitpack to max width during split

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
	matchIt(c.Iterator(), matchFn[T](types.FilterModeEqual), val, bits, mask)
}

func (c *FloatAlpRdContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeNotEqual), val, bits, mask)
}

func (c *FloatAlpRdContainer[T]) MatchLess(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeLt), val, bits, mask)
}

func (c *FloatAlpRdContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeLe), val, bits, mask)
}

func (c *FloatAlpRdContainer[T]) MatchGreater(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeGt), val, bits, mask)
}

func (c *FloatAlpRdContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeGe), val, bits, mask)
}

func (c *FloatAlpRdContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) {
	matchRangeIt(c.Iterator(), matchFn[T](types.FilterModeRange), a, b, bits, mask)
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
		return floatAlpRdFactory.f64ItPool.Get().(*FloatAlpRdIterator[T])
	case float32:
		return floatAlpRdFactory.f32ItPool.Get().(*FloatAlpRdIterator[T])
	default:
		return nil
	}
}

func putFloatAlpRdIterator[T types.Float](c *FloatAlpRdIterator[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpRdFactory.f64ItPool.Put(c)
	case float32:
		floatAlpRdFactory.f32ItPool.Put(c)
	}
}

var floatAlpRdFactory = FloatAlpRdFactory{
	f64Pool:   sync.Pool{New: func() any { return new(FloatAlpRdContainer[float64]) }},
	f32Pool:   sync.Pool{New: func() any { return new(FloatAlpRdContainer[float32]) }},
	f64ItPool: sync.Pool{New: func() any { return new(FloatAlpRdIterator[float64]) }},
	f32ItPool: sync.Pool{New: func() any { return new(FloatAlpRdIterator[float32]) }},
}

// ---------------------------------------
// Iterator
//

func (c *FloatAlpRdContainer[T]) Iterator() Iterator[T] {
	return NewFloatAlpRdIterator(c)
}

type FloatAlpRdIterator[T types.Float] struct {
	BaseIterator[T]
	left  Iterator[uint16]
	right Iterator[uint64]
	shift int
}

func NewFloatAlpRdIterator[T types.Float](c *FloatAlpRdContainer[T]) *FloatAlpRdIterator[T] {
	it := newFloatAlpRdIterator[T]()
	it.left = c.Left.Iterator()
	it.right = c.Right.Iterator()
	it.shift = c.Shift
	it.base = -1
	it.len = c.Len()
	it.BaseIterator.fill = it.fill
	return it
}

func (it *FloatAlpRdIterator[T]) Close() {
	it.left.Close()
	it.right.Close()
	it.left = nil
	it.right = nil
	it.shift = 0
	it.BaseIterator.Close()
	putFloatAlpRdIterator(it)
}

func (it *FloatAlpRdIterator[T]) fill(base int) int {
	// load next source chunk at base and translate
	it.left.Seek(base)
	it.right.Seek(base)

	left, _ := it.left.NextChunk()
	right, n := it.right.NextChunk()
	if n == 0 {
		it.ofs = it.len
		it.base = -1
		return 0
	}

	// merge ALP pieces
	alp.MergeRD(it.chunk[:n], left[:n], right[:n], it.shift)

	it.base = base
	return n
}
