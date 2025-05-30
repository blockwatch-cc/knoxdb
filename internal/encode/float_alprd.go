// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"iter"
	"sync"

	"blockwatch.cc/knoxdb/internal/encode/alp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// ensure we implement required interfaces
var (
	_ types.NumberAccessor[float64] = (*FloatAlpRdContainer[float64, uint64])(nil)
	_ NumberContainer[float64]      = (*FloatAlpRdContainer[float64, uint64])(nil)
)

// TFloatAlpRd
type FloatAlpRdContainer[T alp.Float, E alp.Uint] struct {
	readOnlyContainer[T]
	Left  NumberContainer[uint16]
	Right NumberContainer[E]
	Shift int
	dec   *alp.DecoderRD[T, E]
}

func (c *FloatAlpRdContainer[T, E]) Info() string {
	return fmt.Sprintf("ALP-RD(%s)_[>>%d]_[%s]_[%s]",
		TypeName[T](), c.Shift, c.Left.Info(), c.Right.Info())
}

func (c *FloatAlpRdContainer[T, E]) Close() {
	c.Left.Close()
	c.Right.Close()
	c.Left = nil
	c.Right = nil
	c.Shift = 0
	c.dec = nil
	putFloatAlpRdContainer[T, E](c)
}

func (c *FloatAlpRdContainer[T, E]) Type() ContainerType {
	return TFloatAlpRd
}

func (c *FloatAlpRdContainer[T, E]) Len() int {
	return c.Left.Len()
}

func (c *FloatAlpRdContainer[T, E]) Size() int {
	v := 2 + c.Left.Size() + c.Right.Size()
	return v
}

func (c *FloatAlpRdContainer[T, E]) Matcher() types.NumberMatcher[T] {
	return c
}

func (c *FloatAlpRdContainer[T, E]) Chunks() types.NumberIterator[T] {
	return NewFloatAlpRdIterator(c)
}

func (c *FloatAlpRdContainer[T, E]) Iterator() iter.Seq2[int, T] {
	return func(fn func(int, T) bool) {
		it := c.Chunks()
		for i := range it.Len() {
			if !fn(i, it.Get(i)) {
				break
			}
		}
		it.Close()
	}
}

func (c *FloatAlpRdContainer[T, E]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatAlpRd))
	dst = c.Left.Store(dst)
	dst = c.Right.Store(dst)
	dst = num.AppendUvarint(dst, uint64(c.Shift))
	return dst
}

func (c *FloatAlpRdContainer[T, E]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatAlpRd) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	c.Left = NewInt[uint16](ContainerType(buf[0]))
	buf, err := c.Left.Load(buf)
	if err != nil {
		return buf, err
	}
	c.Right = NewInt[E](ContainerType(buf[0]))
	buf, err = c.Right.Load(buf)
	if err != nil {
		return buf, err
	}
	v, n := num.Uvarint(buf)
	c.Shift = int(v)
	c.dec = alp.NewDecoderRD[T, E](c.Shift)
	return buf[n:], nil
}

func (c *FloatAlpRdContainer[T, E]) Get(n int) T {
	return c.dec.DecodeValue(c.Left.Get(n), c.Right.Get(n))
}

func (c *FloatAlpRdContainer[T, E]) AppendTo(dst []T, sel []uint32) []T {
	it := c.Chunks()
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

func (c *FloatAlpRdContainer[T, E]) Encode(ctx *Context[T], vals []T) NumberContainer[T] {
	// ensure we have an ALP analysis result (mostly relevant for testcases)
	if ctx.Alp.Scheme != alp.ALP_RD_SCHEME {
		a := alp.AnalyzeRD[T, E](vals)
		ctx.Alp.Split = a.Split
		ctx.Alp.Dict = a.Dict
	}

	enc := alp.NewEncoderRD[T, E]()
	res := enc.Encode(vals, ctx.Alp.Split)

	// Improvement ideas
	// - left: always use dict when <=8 unique, analyze unique during split (build array)
	// - right: always BP, aggregate min/max during split
	// - skip int analyze and encode direct
	// - fusion: right always bitpack to max width during split

	// prefer left side dict when small
	lctx := AnalyzeInt(res.Left, ctx.Alp.Dict).WithLevel(ctx.Lvl - 1)
	leftScheme := TIntBitpacked
	if lctx.NumUnique <= alp.RD_MAX_DICT_SIZE {
		leftScheme = TIntDictionary
	}
	c.Left = NewInt[uint16](leftScheme).Encode(lctx, res.Left)
	lctx.Close()

	// right is always bitpacked, estimate width
	rctx := AnalyzeInt(res.Right, false).WithLevel(ctx.Lvl - 1)
	c.Right = NewInt[E](TIntBitpacked).Encode(rctx, res.Right)
	rctx.Close()

	// close alp result to free resources
	res.Close()

	// store setup and init decoder
	c.Shift = ctx.Alp.Split
	c.dec = alp.NewDecoderRD[T, E](c.Shift)

	return c
}

func (c *FloatAlpRdContainer[T, E]) Cmp(i, j int) int {
	return util.Cmp(c.Get(i), c.Get(j))
}

func (c *FloatAlpRdContainer[T, E]) MatchEqual(val T, bits, mask *Bitset) {
	matchIt(c.Chunks(), matchFn[T](types.FilterModeEqual), val, bits, mask)
}

func (c *FloatAlpRdContainer[T, E]) MatchNotEqual(val T, bits, mask *Bitset) {
	matchIt(c.Chunks(), matchFn[T](types.FilterModeNotEqual), val, bits, mask)
}

func (c *FloatAlpRdContainer[T, E]) MatchLess(val T, bits, mask *Bitset) {
	matchIt(c.Chunks(), matchFn[T](types.FilterModeLt), val, bits, mask)
}

func (c *FloatAlpRdContainer[T, E]) MatchLessEqual(val T, bits, mask *Bitset) {
	matchIt(c.Chunks(), matchFn[T](types.FilterModeLe), val, bits, mask)
}

func (c *FloatAlpRdContainer[T, E]) MatchGreater(val T, bits, mask *Bitset) {
	matchIt(c.Chunks(), matchFn[T](types.FilterModeGt), val, bits, mask)
}

func (c *FloatAlpRdContainer[T, E]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	matchIt(c.Chunks(), matchFn[T](types.FilterModeGe), val, bits, mask)
}

func (c *FloatAlpRdContainer[T, E]) MatchBetween(a, b T, bits, mask *Bitset) {
	matchRangeIt(c.Chunks(), matchFn[T](types.FilterModeRange), a, b, bits, mask)
}

// N.A.
func (c *FloatAlpRdContainer[T, E]) MatchInSet(_ any, _, _ *Bitset)    {}
func (c *FloatAlpRdContainer[T, E]) MatchNotInSet(_ any, _, _ *Bitset) {}

type FloatAlpRdFactory struct {
	f64Pool   sync.Pool
	f32Pool   sync.Pool
	f64ItPool sync.Pool
	f32ItPool sync.Pool
}

func newFloatAlpRdContainer[T alp.Float]() NumberContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpRdFactory.f64Pool.Get().(NumberContainer[T])
	case float32:
		return floatAlpRdFactory.f32Pool.Get().(NumberContainer[T])
	default:
		return nil
	}
}

func putFloatAlpRdContainer[T alp.Float, E alp.Uint](c NumberContainer[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpRdFactory.f64Pool.Put(c)
	case float32:
		floatAlpRdFactory.f32Pool.Put(c)
	}
}

func newFloatAlpRdIterator[T alp.Float, E alp.Uint]() *FloatAlpRdIterator[T, E] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpRdFactory.f64ItPool.Get().(*FloatAlpRdIterator[T, E])
	case float32:
		return floatAlpRdFactory.f32ItPool.Get().(*FloatAlpRdIterator[T, E])
	default:
		return nil
	}
}

func putFloatAlpRdIterator[T alp.Float, E alp.Uint](c *FloatAlpRdIterator[T, E]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpRdFactory.f64ItPool.Put(c)
	case float32:
		floatAlpRdFactory.f32ItPool.Put(c)
	}
}

var floatAlpRdFactory = FloatAlpRdFactory{
	f64Pool:   sync.Pool{New: func() any { return new(FloatAlpRdContainer[float64, uint64]) }},
	f32Pool:   sync.Pool{New: func() any { return new(FloatAlpRdContainer[float32, uint32]) }},
	f64ItPool: sync.Pool{New: func() any { return new(FloatAlpRdIterator[float64, uint64]) }},
	f32ItPool: sync.Pool{New: func() any { return new(FloatAlpRdIterator[float32, uint32]) }},
}

// ---------------------------------------
// Iterator
//

type FloatAlpRdIterator[T alp.Float, E alp.Uint] struct {
	BaseIterator[T]
	left  types.NumberIterator[uint16]
	right types.NumberIterator[E]
	dec   *alp.DecoderRD[T, E]
}

func NewFloatAlpRdIterator[T alp.Float, E alp.Uint](c *FloatAlpRdContainer[T, E]) *FloatAlpRdIterator[T, E] {
	it := newFloatAlpRdIterator[T, E]()
	it.left = c.Left.Chunks()
	it.right = c.Right.Chunks()
	it.dec = c.dec
	it.base = -1
	it.len = c.Len()
	it.BaseIterator.fill = it.fill
	return it
}

func (it *FloatAlpRdIterator[T, E]) Close() {
	it.left.Close()
	it.right.Close()
	it.left = nil
	it.right = nil
	it.dec = nil
	it.BaseIterator.Close()
	putFloatAlpRdIterator(it)
}

func (it *FloatAlpRdIterator[T, E]) fill(base int) int {
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
	it.dec.Decode(it.chunk[:n], left[:n], right[:n])

	it.base = base
	return n
}
