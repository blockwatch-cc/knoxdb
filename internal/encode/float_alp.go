// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"slices"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/alp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

type ExceptionStatus byte

const (
	NoException ExceptionStatus = iota
	HasException
)

// TFloatAlp
type FloatAlpContainer[T types.Float, E int64 | int32] struct {
	Values       IntegerContainer[E]
	Exception    FloatContainer[T]
	Positions    IntegerContainer[uint32]
	Exponent     uint8
	Factor       uint8
	hasException bool
	dec          *alp.Decoder[T, E]
}

func (c *FloatAlpContainer[T, E]) Info() string {
	if c.hasException {
		return fmt.Sprintf("ALP(%s)_[%d,%d]_[v=%s]_[ex=%s]_[pos=%s]",
			TypeName[T](), c.Exponent, c.Factor,
			c.Values.Info(), c.Exception.Info(), c.Positions.Info())
	}
	return fmt.Sprintf("ALP(%s)_[%d,%d]_[v=%s]_[noex]", TypeName[T](),
		c.Exponent, c.Factor, c.Values.Info())
}

func (c *FloatAlpContainer[T, E]) Close() {
	if c.dec != nil {
		c.dec.Close()
		c.dec = nil
	}
	c.Values.Close()
	c.Values = nil
	if c.hasException {
		c.Exception.Close()
		c.Positions.Close()
		c.Exception = nil
		c.Positions = nil
		c.hasException = false
	}
	putFloatAlpContainer[T](c)
}

func (c *FloatAlpContainer[T, E]) Type() FloatContainerType {
	return TFloatAlp
}

func (c *FloatAlpContainer[T, E]) Len() int {
	return c.Values.Len()
}

func (c *FloatAlpContainer[T, E]) Size() int {
	v := 1 + 2 + c.Values.Size()
	if c.hasException {
		v += c.Exception.Size() + c.Positions.Size()
	}
	return v
}

func (c *FloatAlpContainer[T, E]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatAlp))
	dst = num.AppendUvarint(dst, uint64(c.Exponent))
	dst = num.AppendUvarint(dst, uint64(c.Factor))
	dst = c.Values.Store(dst)
	dst = append(dst, util.Bool2byte(c.hasException))
	if c.hasException {
		dst = c.Exception.Store(dst)
		dst = c.Positions.Store(dst)
	}
	return dst
}

func (c *FloatAlpContainer[T, E]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatAlp) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	v, n := num.Uvarint(buf)
	c.Exponent = uint8(v)
	buf = buf[n:]

	v, n = num.Uvarint(buf)
	c.Factor = uint8(v)
	buf = buf[n:]

	// alloc and decode values child container
	c.Values = NewInt[E](IntegerContainerType(buf[0]))
	var err error
	buf, err = c.Values.Load(buf)
	if err != nil {
		return buf, err
	}

	// load exceptions
	c.hasException = buf[0] == byte(HasException)
	buf = buf[1:]
	if c.hasException {
		// exception values
		c.Exception = NewFloat[T](FloatContainerType(buf[0]))
		buf, err = c.Exception.Load(buf)
		if err != nil {
			return buf, err
		}

		// exception positions
		c.Positions = NewInt[uint32](IntegerContainerType(buf[0]))
		buf, err = c.Positions.Load(buf)
		if err != nil {
			return buf, err
		}
	}

	return buf, nil
}

func (c *FloatAlpContainer[T, E]) Get(n int) T {
	c.initDecoder()
	return c.dec.DecodeValue(c.Values.Get(n), n)
}

func (c *FloatAlpContainer[T, E]) AppendTo(sel []uint32, dst []T) []T {
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

func (c *FloatAlpContainer[T, E]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	// encode using parames from analysis
	enc := alp.NewEncoder[T, E]()
	res := enc.Encode(vals, ctx.Alp.Exp)
	c.Exponent = ctx.Alp.Exp.E
	c.Factor = ctx.Alp.Exp.F

	// encode child containers, skip analysis and use known values
	vctx := NewIntegerContext(res.Min, res.Max, len(vals))
	c.Values = EncodeInt(vctx, res.Encoded, lvl-1)
	vctx.Close()
	if n := len(res.PatchValues); n > 0 {
		c.hasException = true
		c.Exception = NewFloat[T](TFloatRaw).Encode(nil, slices.Clone(res.PatchValues), 1)
		ectx := NewIntegerContext(0, res.PatchIndices[n-1], n)
		c.Positions = EncodeInt(ectx, res.PatchIndices, lvl-1)
		ectx.Close()
	}
	res.Close()

	return c
}

func (c *FloatAlpContainer[T, E]) initDecoder() {
	if c.dec != nil {
		return
	}
	c.dec = alp.NewDecoder[T, E](c.Factor, c.Exponent)
	if c.hasException {
		cnt := c.Exception.Len()
		c.dec.WithExceptions(
			c.Exception.AppendTo(nil, arena.Alloc[T](cnt)),
			c.Positions.AppendTo(nil, arena.Alloc[uint32](cnt)),
		)
	}
}

func (c *FloatAlpContainer[T, E]) MatchEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeEqual), val, bits, mask)
}

func (c *FloatAlpContainer[T, E]) MatchNotEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeNotEqual), val, bits, mask)
}

func (c *FloatAlpContainer[T, E]) MatchLess(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeLt), val, bits, mask)
}

func (c *FloatAlpContainer[T, E]) MatchLessEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeLe), val, bits, mask)
}

func (c *FloatAlpContainer[T, E]) MatchGreater(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeGt), val, bits, mask)
}

func (c *FloatAlpContainer[T, E]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeGe), val, bits, mask)
}

func (c *FloatAlpContainer[T, E]) MatchBetween(a, b T, bits, mask *Bitset) {
	matchRangeIt(c.Iterator(), matchFn[T](types.FilterModeRange), a, b, bits, mask)
}

// N.A.
func (c *FloatAlpContainer[T, E]) MatchInSet(_ any, _, _ *Bitset)    {}
func (c *FloatAlpContainer[T, E]) MatchNotInSet(_ any, _, _ *Bitset) {}

type FloatAlpFactory struct {
	f64Pool   sync.Pool
	f32Pool   sync.Pool
	f64ItPool sync.Pool
	f32ItPool sync.Pool
}

func newFloatAlpContainer[T types.Float]() FloatContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpFactory.f64Pool.Get().(FloatContainer[T])
	case float32:
		return floatAlpFactory.f32Pool.Get().(FloatContainer[T])
	default:
		return nil
	}
}

func putFloatAlpContainer[T types.Float](c FloatContainer[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpFactory.f64Pool.Put(c)
	case float32:
		floatAlpFactory.f32Pool.Put(c)
	}
}

func newFloatAlpIterator[T types.Float, E int64 | int32]() *FloatAlpIterator[T, E] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpFactory.f64ItPool.Get().(*FloatAlpIterator[T, E])
	case float32:
		return floatAlpFactory.f32ItPool.Get().(*FloatAlpIterator[T, E])
	default:
		return nil
	}
}

func putFloatAlpIterator[T types.Float, E int64 | int32](c *FloatAlpIterator[T, E]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpFactory.f64ItPool.Put(c)
	case float32:
		floatAlpFactory.f32ItPool.Put(c)
	}
}

var floatAlpFactory = FloatAlpFactory{
	f64Pool:   sync.Pool{New: func() any { return new(FloatAlpContainer[float64, int64]) }},
	f32Pool:   sync.Pool{New: func() any { return new(FloatAlpContainer[float32, int32]) }},
	f64ItPool: sync.Pool{New: func() any { return new(FloatAlpIterator[float64, int64]) }},
	f32ItPool: sync.Pool{New: func() any { return new(FloatAlpIterator[float32, int32]) }},
}

// ---------------------------------------
// Iterator
//

func (c *FloatAlpContainer[T, E]) Iterator() Iterator[T] {
	c.initDecoder()
	return NewFloatAlpIterator(c)
}

type FloatAlpIterator[T types.Float, E int64 | int32] struct {
	BaseIterator[T]
	dec *alp.Decoder[T, E]
	src Iterator[E]
}

func NewFloatAlpIterator[T types.Float, E int64 | int32](c *FloatAlpContainer[T, E]) *FloatAlpIterator[T, E] {
	it := newFloatAlpIterator[T, E]()
	it.dec = c.dec
	it.src = c.Values.Iterator()
	it.base = -1
	it.len = c.Len()
	it.BaseIterator.fill = it.fill
	return it
}

func (it *FloatAlpIterator[T, E]) Close() {
	it.dec = nil
	it.src.Close()
	it.src = nil
	it.BaseIterator.Close()
	putFloatAlpIterator(it)
}

func (it *FloatAlpIterator[T, E]) fill(base int) int {
	// load next source chunk at base and translate
	it.src.Seek(base)
	src, n := it.src.NextChunk()
	if n == 0 {
		it.ofs = it.len
		it.base = -1
		return 0
	}
	it.dec.DecodeChunk(&it.chunk, src, n, base)
	it.base = base
	return n
}
