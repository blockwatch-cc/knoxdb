// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
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
type FloatAlpContainer[T types.Float] struct {
	Values       IntegerContainer[int64]
	Exception    FloatContainer[T]
	Positions    IntegerContainer[uint32]
	Exponent     uint8
	Factor       uint8
	hasException bool
	dec          *alp.Decoder[T]
}

func (c *FloatAlpContainer[T]) Info() string {
	if c.hasException {
		return fmt.Sprintf("ALP(%s)_[%d,%d]_[v=%s]_[ex=%s]_[pos=%s]",
			TypeName[T](), c.Exponent, c.Factor,
			c.Values.Info(), c.Exception.Info(), c.Positions.Info())
	}
	return fmt.Sprintf("ALP(%s)_[%d,%d]_[v=%s]_[noex]", TypeName[T](),
		c.Exponent, c.Factor, c.Values.Info())
}

func (c *FloatAlpContainer[T]) Close() {
	c.Values.Close()
	c.Values = nil
	if c.hasException {
		c.Exception.Close()
		c.Positions.Close()
		c.Exception = nil
		c.Positions = nil
		c.hasException = false
	}
	if c.dec != nil {
		c.dec.Close()
		c.dec = nil
	}
	putFloatAlpContainer(c)
}

func (c *FloatAlpContainer[T]) Type() FloatContainerType {
	return TFloatAlp
}

func (c *FloatAlpContainer[T]) Len() int {
	return c.Values.Len()
}

func (c *FloatAlpContainer[T]) Size() int {
	v := 1 + 2 + c.Values.Size()
	if c.hasException {
		v += c.Exception.Size() + c.Positions.Size()
	}
	return v
}

func (c *FloatAlpContainer[T]) Store(dst []byte) []byte {
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

func (c *FloatAlpContainer[T]) Load(buf []byte) ([]byte, error) {
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
	c.Values = NewInt[int64](IntegerContainerType(buf[0]))
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

func (c *FloatAlpContainer[T]) Get(n int) T {
	// ok := c.it.Seek(n)
	// v, ok := c.it.Next()
	c.initDecoder()
	return c.dec.DecodeValue(c.Values.Get(n), n)
}

func (c *FloatAlpContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	if sel == nil {
		it := c.Iterator()
		for {
			src, n := it.NextChunk()
			if n == 0 {
				break
			}
			dst = append(dst, src[:n]...)
		}
		it.Close()
	} else {
		// TODO: measure iterator vs get performance
		for _, v := range sel {
			dst = append(dst, c.Get(int(v)))
		}
	}
	return dst
}

func (c *FloatAlpContainer[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	enc := ctx.AlpEncoder
	if enc == nil {
		enc = alp.NewEncoder[T]()
		defer enc.Close()
	}
	enc.Encode(vals)
	s := enc.State()

	c.Exponent = s.Encoding.E
	c.Factor = s.Encoding.F

	// fmt.Printf("ALP [%d,%d] vals=%d ex=%d\n", c.Exponent, c.Factor, len(s.Integers), len(s.Exceptions))

	// TODO
	// - aggregate min/max on the fly, set in context -> bitpack width
	// - encode integers as BP (no analysis needed)
	// - encode exceptions as raw (no analysis needed)
	// - encode ex positions as BP (no analysis needed: ex list is ordered: min/max = first/last)
	// - kernel fusion? maybe on decode only

	// encode child containers
	c.Values = EncodeInt(nil, s.Integers, lvl-1)
	if len(s.Exceptions) > 0 {
		c.hasException = true
		c.Exception = EncodeFloat(nil, s.Exceptions, lvl-1)
		ectx := AnalyzeInt(s.Positions, false)
		c.Positions = EncodeInt(ectx, s.Positions, lvl-1)
		ectx.Close()
	}

	return c
}

func (c *FloatAlpContainer[T]) initDecoder() {
	if c.dec != nil {
		return
	}
	c.dec = alp.NewDecoder[T](c.Factor, c.Exponent)
	if c.hasException {
		cnt := c.Exception.Len()
		c.dec.WithExceptions(
			c.Exception.AppendTo(nil, arena.Alloc[T](cnt)),
			c.Positions.AppendTo(nil, arena.Alloc[uint32](cnt)),
		)
	}
}

func (c *FloatAlpContainer[T]) MatchEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeEqual), val, bits, mask)
}

func (c *FloatAlpContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeNotEqual), val, bits, mask)
}

func (c *FloatAlpContainer[T]) MatchLess(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeLt), val, bits, mask)
}

func (c *FloatAlpContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeLe), val, bits, mask)
}

func (c *FloatAlpContainer[T]) MatchGreater(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeGt), val, bits, mask)
}

func (c *FloatAlpContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeGe), val, bits, mask)
}

func (c *FloatAlpContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) {
	matchRangeIt(c.Iterator(), matchFn[T](types.FilterModeRange), a, b, bits, mask)
}

// N.A.
func (c *FloatAlpContainer[T]) MatchInSet(_ any, _, _ *Bitset)    {}
func (c *FloatAlpContainer[T]) MatchNotInSet(_ any, _, _ *Bitset) {}

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

func newFloatAlpIterator[T types.Float]() *FloatAlpIterator[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpFactory.f64ItPool.Get().(*FloatAlpIterator[T])
	case float32:
		return floatAlpFactory.f32ItPool.Get().(*FloatAlpIterator[T])
	default:
		return nil
	}
}

func putFloatAlpIterator[T types.Float](c *FloatAlpIterator[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpFactory.f64ItPool.Put(c)
	case float32:
		floatAlpFactory.f32ItPool.Put(c)
	}
}

var floatAlpFactory = FloatAlpFactory{
	f64Pool: sync.Pool{
		New: func() any { return new(FloatAlpContainer[float64]) },
	},
	f32Pool: sync.Pool{
		New: func() any { return new(FloatAlpContainer[float32]) },
	},
	f64ItPool: sync.Pool{
		New: func() any { return new(FloatAlpIterator[float64]) },
	},
	f32ItPool: sync.Pool{
		New: func() any { return new(FloatAlpIterator[float32]) },
	},
}

func (c *FloatAlpContainer[T]) Iterator() Iterator[T] {
	c.initDecoder()
	it := newFloatAlpIterator[T]()
	it.len = c.Len()
	it.dec = c.dec
	it.valIt = c.Values.Iterator()
	it.ofs = 0
	return it
}

type FloatAlpIterator[T types.Float] struct {
	vals  [CHUNK_SIZE]T
	dec   *alp.Decoder[T]
	valIt Iterator[int64]
	len   int
	ofs   int
}

func (it *FloatAlpIterator[T]) Close() {
	it.dec = nil
	it.valIt.Close()
	it.valIt = nil
	it.len = 0
	it.ofs = 0
	putFloatAlpIterator(it)
}

func (it *FloatAlpIterator[T]) Reset() {
	it.ofs = 0
	it.valIt.Reset()
}

func (it *FloatAlpIterator[T]) Len() int {
	return it.len
}

func (it *FloatAlpIterator[T]) Get(n int) T {
	if it.Seek(n) {
		val, _ := it.Next()
		return val
	}
	return 0
}

func (it *FloatAlpIterator[T]) Next() (T, bool) {
	if it.ofs >= it.len {
		// EOF
		return 0, false
	}

	// ofs % CHUNK_SIZE
	i := it.ofs & CHUNK_MASK

	// on first call or start of new chunk
	if i == 0 {
		// load next source chunk
		src, n := it.valIt.NextChunk()

		// sanity check, should not happen
		if n == 0 {
			it.ofs = it.len
			return 0, false
		}

		// decode
		it.dec.DecodeChunk(&it.vals, src, n, it.ofs)
	}

	// advance ofs for next call
	it.ofs++

	// return value
	return it.vals[i], true
}

func (it *FloatAlpIterator[T]) NextChunk() (*[CHUNK_SIZE]T, int) {
	// EOF
	if it.ofs >= it.len {
		return nil, 0
	}
	src, n := it.valIt.NextChunk()
	if n > 0 {
		it.dec.DecodeChunk(&it.vals, src, n, it.ofs)
		it.ofs += n
	}
	return &it.vals, n
}

func (it *FloatAlpIterator[T]) SkipChunk() int {
	n := it.valIt.SkipChunk()
	it.ofs += n
	return n
}

func (it *FloatAlpIterator[T]) Seek(n int) bool {
	// bounds check
	if n < 0 || n >= it.len {
		it.ofs = it.len
		return false
	}

	// calculate chunk start offsets for n and current offset
	nc := chunkStart(n)
	oc := chunkStart(it.ofs)

	// fmt.Printf("ALP seek n=%d ofs=%d nc=%d oc=%d\n", n, it.ofs, nc, oc)

	// load when n is in another chunk or seek on init
	if nc != oc || it.ofs == 0 {
		// seek base-it to new chunk start
		if !it.valIt.Seek(nc) {
			it.ofs = it.len
			return false
		}

		// load next chunk when not seeking to start (re-use NextChunk method)
		if n&CHUNK_MASK != 0 {
			it.ofs = nc
			it.NextChunk()
		}
	} else if n == 0 {
		// edge case: reset base-it for n=0 because Next() loads again
		it.valIt.Reset()
	}

	// reset ofs to n, so call to Next() delivers value
	it.ofs = n

	return true
}
