// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"math"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/alp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

type AlpFlags byte

const (
	FlagPatched AlpFlags = 1 << iota
	FlagSafeInt          // [-2^51, 2^51]
)

// TFloatAlp
type FloatAlpContainer[T types.Float, E int64 | int32] struct {
	Values    IntegerContainer[E]
	Patches   FloatContainer[T]
	Positions IntegerContainer[uint32]
	Exponent  uint8
	Factor    uint8
	flags     AlpFlags
	dec       *alp.Decoder[T, E]
}

func (c *FloatAlpContainer[T, E]) Info() string {
	if c.flags&FlagPatched > 0 {
		return fmt.Sprintf("ALP(%s)_[%d,%d]_[v=%s]_[ex=%s]_[pos=%s]",
			TypeName[T](), c.Exponent, c.Factor,
			c.Values.Info(), c.Patches.Info(), c.Positions.Info())
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
	if c.flags&FlagPatched > 0 {
		c.Patches.Close()
		c.Positions.Close()
		c.Patches = nil
		c.Positions = nil
	}
	c.flags = 0
	putFloatAlpContainer[T](c)
}

func (c *FloatAlpContainer[T, E]) Type() FloatContainerType {
	return TFloatAlp
}

func (c *FloatAlpContainer[T, E]) Len() int {
	return c.Values.Len()
}

func (c *FloatAlpContainer[T, E]) Size() int {
	v := 4 + c.Values.Size()
	if c.flags&FlagPatched > 0 {
		v += c.Patches.Size() + c.Positions.Size()
	}
	return v
}

func (c *FloatAlpContainer[T, E]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatAlp))
	dst = num.AppendUvarint(dst, uint64(c.Exponent))
	dst = num.AppendUvarint(dst, uint64(c.Factor))
	dst = append(dst, byte(c.flags))
	dst = c.Values.Store(dst)
	if c.flags&FlagPatched > 0 {
		dst = c.Patches.Store(dst)
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

	// load flags
	c.flags = AlpFlags(buf[0])
	buf = buf[1:]

	// alloc and decode values child container
	c.Values = NewInt[E](IntegerContainerType(buf[0]))
	var err error
	buf, err = c.Values.Load(buf)
	if err != nil {
		return buf, err
	}

	if c.flags&FlagPatched > 0 {
		// patch values
		c.Patches = NewFloat[T](FloatContainerType(buf[0]))
		buf, err = c.Patches.Load(buf)
		if err != nil {
			return buf, err
		}

		// patch positions
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
	if sel == nil {
		// faster to do serial unpack & decode
		sz := c.Len()
		tmp := c.Values.AppendTo(nil, arena.Alloc[E](sz))
		c.initDecoder()
		dst = dst[:sz]
		c.dec.Decode(dst, tmp)
		arena.Free(tmp)
	} else {
		it := c.Iterator()
		for _, v := range sel {
			dst = append(dst, it.Get(int(v)))
		}
		it.Close()
	}
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
		c.flags |= FlagPatched
		c.Patches = NewFloat[T](TFloatRaw).Encode(nil, res.PatchValues, 1)
		ectx := NewIntegerContext(0, res.PatchIndices[n-1], n)
		c.Positions = EncodeInt(ectx, res.PatchIndices, lvl-1)
		ectx.Close()
	}
	if res.IsSafeInt {
		c.flags |= FlagSafeInt
	}
	res.Close()

	return c
}

func (c *FloatAlpContainer[T, E]) initDecoder() {
	if c.dec != nil {
		return
	}
	c.dec = alp.NewDecoder[T, E](c.Factor, c.Exponent).WithSafeInt(c.flags&FlagSafeInt > 0)
	if c.flags&FlagPatched > 0 {
		cnt := c.Patches.Len()
		c.dec.WithPatches(
			c.Patches.AppendTo(nil, arena.Alloc[T](cnt)),
			c.Positions.AppendTo(nil, arena.Alloc[uint32](cnt)),
		)
	}
}

func (c *FloatAlpContainer[T, E]) MatchEqual(val T, bits, mask *Bitset) {
	// Golang has no totalOrder semantics for Nan values. Any Nan comparison
	// returns false, so we must explicitly check for NaN when requested.
	// NaN is never encoded as regular float and may only be part of patches.
	isNaN := val != val // math.IsNaN(float64(val))
	if !isNaN {
		// try translate val into ALP domain
		enc := alp.NewEncoder[T, E]()
		av, ok := enc.EncodeSingle(val, alp.Exponents{E: c.Exponent, F: c.Factor})

		// on success, match against encoded int values
		if ok {
			c.Values.MatchEqual(av, bits, mask)
		}
	}

	// merge _all_ patches by flipping bits, note values contain
	// vector min as replacement and value match above may have
	// matched it but the true patched value is not equal
	if c.flags&FlagPatched > 0 {
		// load decoded patch data
		c.initDecoder()
		vals, pos := c.dec.Patches()

		// if av == min we must revert bits on all patch positions
		// we know by checking of any patch position is already set
		if bits.IsSet(int(pos[0])) {
			for _, p := range pos {
				bits.Clear(int(p))
			}
			return
		}

		// otherwise, find all potential matches in patch list
		if isNaN {
			for i, p := range pos {
				if math.IsNaN(float64(vals[i])) {
					bits.Set(int(p))
				}
			}
		} else {
			for i, p := range pos {
				if vals[i] == val {
					bits.Set(int(p))
				}
			}
		}
	}
}

func (c *FloatAlpContainer[T, E]) MatchNotEqual(val T, bits, mask *Bitset) {
	c.MatchEqual(val, bits, mask)
	bits.Neg()
}

func (c *FloatAlpContainer[T, E]) MatchLess(val T, bits, mask *Bitset) {
	// Golang has no totalOrder semantics for Nan values. Any Nan comparison
	// returns false, so no value can be less than NaN.
	f64 := float64(val)
	if math.IsNaN(f64) || math.IsInf(f64, -1) {
		return
	}

	// match integers first. likely includes min val which is used as
	// replacement for all patched values. we need to undo these
	// matches below when creating the union(values, patches)
	enc := alp.NewEncoder[T, E]()
	exp := alp.Exponents{E: c.Exponent, F: c.Factor}
	av, ok := enc.EncodeSingle(val, exp)
	if ok {
		// match successful encoded value
		c.Values.MatchLess(av, bits, mask)
	} else {
		// match using the next smaller integer, use LE
		c.Values.MatchLessEqual(enc.EncodeBelow(val, exp), bits, mask)
	}

	// merge _all_ patches by flipping bits, note values contain
	// vector min as replacement and value match above may have
	// matched it but the true patched value is not less
	if c.flags&FlagPatched > 0 {
		c.initDecoder()
		vals, pos := c.dec.Patches()
		for i, p := range pos {
			if vals[i] < val {
				bits.Set(int(p))
			} else {
				bits.Clear(int(p))
			}
		}
	}
}

func (c *FloatAlpContainer[T, E]) MatchLessEqual(val T, bits, mask *Bitset) {
	// Golang has no totalOrder semantics for Nan values. Any Nan comparison
	// returns false, so no value can be less or equal to NaN.
	f64 := float64(val)
	if math.IsNaN(f64) {
		return
	}
	if math.IsInf(f64, 1) {
		bits.One()
		return
	}

	// match integers first. likely includes min val which is used as
	// replacement for all patched values. we need to undo these
	// matches below when creating the union(values, patches)
	enc := alp.NewEncoder[T, E]()
	exp := alp.Exponents{E: c.Exponent, F: c.Factor}
	av, ok := enc.EncodeSingle(val, exp)
	if ok {
		// match successful encoded value
		c.Values.MatchLessEqual(av, bits, mask)
	} else {
		// match using the next smaller integer
		c.Values.MatchLessEqual(enc.EncodeBelow(val, exp), bits, mask)
	}

	// merge _all_ patches by flipping bits, note values contain
	// vector min as replacement and value match above may have
	// matched it but the true patched value is not less
	if c.flags&FlagPatched > 0 {
		c.initDecoder()
		vals, pos := c.dec.Patches()
		// NaN cannot match here
		for i, p := range pos {
			if vals[i] <= val {
				bits.Set(int(p))
			} else {
				bits.Clear(int(p))
			}
		}
	}
}

func (c *FloatAlpContainer[T, E]) MatchGreater(val T, bits, mask *Bitset) {
	// Golang has no totalOrder semantics for Nan values. Any Nan comparison
	// returns false, so no value can be greater than NaN.
	f64 := float64(val)
	if math.IsNaN(f64) || math.IsInf(f64, 1) {
		return
	}

	// match integers first. likely includes min val which is used as
	// replacement for all patched values. we need to undo these
	// matches below when creating the union(values, patches)
	enc := alp.NewEncoder[T, E]()
	exp := alp.Exponents{E: c.Exponent, F: c.Factor}
	av, ok := enc.EncodeSingle(val, exp)
	if ok {
		// match successful encoded value
		c.Values.MatchGreater(av, bits, mask)
	} else {
		// match using the next larger integer, use GE
		c.Values.MatchGreaterEqual(enc.EncodeAbove(val, exp), bits, mask)
	}

	// merge _all_ patches by flipping bits, note values contain
	// vector min as replacement and value match above may have
	// matched it but the true patched value is not greater
	if c.flags&FlagPatched > 0 {
		c.initDecoder()
		vals, pos := c.dec.Patches()
		for i, p := range pos {
			if vals[i] > val {
				bits.Set(int(p))
			} else {
				bits.Clear(int(p))
			}
		}
	}
}

func (c *FloatAlpContainer[T, E]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	// Golang has no totalOrder semantics for Nan values. Any Nan comparison
	// returns false, so no value can be greater or equal to NaN.
	f64 := float64(val)
	if math.IsNaN(f64) {
		return
	}
	if math.IsInf(f64, -1) {
		bits.One()
		return
	}

	// match integers first. likely includes min val which is used as
	// replacement for all patched values. we need to undo these
	// matches below when creating the union(values, patches)
	enc := alp.NewEncoder[T, E]()
	exp := alp.Exponents{E: c.Exponent, F: c.Factor}
	av, ok := enc.EncodeSingle(val, exp)
	if ok {
		// match successful encoded value
		c.Values.MatchGreaterEqual(av, bits, mask)
	} else {
		// match using the next larger integer
		c.Values.MatchGreaterEqual(enc.EncodeAbove(val, exp), bits, mask)
	}

	// merge _all_ patches by flipping bits, note values contain
	// vector min as replacement and value match above may have
	// matched it but the true patched value is not less
	if c.flags&FlagPatched > 0 {
		c.initDecoder()
		vals, pos := c.dec.Patches()
		// NaN cannot match here
		for i, p := range pos {
			if vals[i] >= val {
				bits.Set(int(p))
			} else {
				bits.Clear(int(p))
			}
		}
	}
}

func (c *FloatAlpContainer[T, E]) MatchBetween(a, b T, bits, mask *Bitset) {
	// Golang has no totalOrder semantics for Nan values. Any Nan comparison
	// returns false, so no value can be inside a range with NaN as border.
	if math.IsNaN(float64(a)) || math.IsNaN(float64(b)) {
		return
	}

	// match integers first. likely includes min val which is used as
	// replacement for all patched values. we need to undo these
	// matches below when creating the union(values, patches)
	enc := alp.NewEncoder[T, E]()
	exp := alp.Exponents{E: c.Exponent, F: c.Factor}
	av, ok := enc.EncodeSingle(a, exp)
	if !ok {
		av = enc.EncodeAbove(a, exp)
	}
	bv, ok := enc.EncodeSingle(b, exp)
	if !ok {
		bv = enc.EncodeBelow(b, exp)
	}

	// match integer range
	c.Values.MatchBetween(av, bv, bits, mask)

	// merge _all_ patches by flipping bits, note values contain
	// vector min as replacement and value match above may have
	// matched it but the true patched value is not less
	if c.flags&FlagPatched > 0 {
		c.initDecoder()
		vals, pos := c.dec.Patches()
		// NaN cannot match here
		for i, p := range pos {
			if vals[i] >= a && vals[i] <= b {
				bits.Set(int(p))
			} else {
				bits.Clear(int(p))
			}
		}
	}
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
