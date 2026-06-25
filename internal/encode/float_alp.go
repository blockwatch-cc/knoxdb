// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"iter"
	"math"
	"sync"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/alp"
	"blockwatch.cc/knoxdb/internal/types"
)

type AlpFlags byte

const (
	FlagPatched AlpFlags = 1 << iota
	FlagSafeInt          // [-2^51, 2^51]
)

// ensure we implement required interfaces
var (
	_ types.NumberAccessor[float64] = (*FloatAlpContainer[float64, int64])(nil)
	_ NumberContainer[float64]      = (*FloatAlpContainer[float64, int64])(nil)
)

// TFloatAlp
type FloatAlpContainer[T types.Float, E int64 | int32] struct {
	readOnlyContainer[T]
	values    NumberContainer[E]
	patches   NumberContainer[T]
	positions NumberContainer[uint32]
	exponent  uint8
	factor    uint8
	flags     AlpFlags
	dec       atomic.Pointer[alp.Decoder[T, E]]
}

func (c *FloatAlpContainer[T, E]) Info() string {
	if c.flags&FlagPatched > 0 {
		return fmt.Sprintf("ALP(%s)_[%d,%d]_[v=%s]_[ex=%s]_[pos=%s]",
			TypeName[T](), c.exponent, c.factor,
			c.values.Info(), c.patches.Info(), c.positions.Info())
	}
	return fmt.Sprintf("ALP(%s)_[%d,%d]_[v=%s]_[noex]", TypeName[T](),
		c.exponent, c.factor, c.values.Info())
}

func (c *FloatAlpContainer[T, E]) Close() {
	p := c.dec.Load()
	if ok := c.dec.CompareAndSwap(p, nil); ok && p != nil {
		p.Close()
	}
	c.values.Close()
	c.values = nil
	if c.flags&FlagPatched > 0 {
		c.patches.Close()
		c.positions.Close()
		c.patches = nil
		c.positions = nil
	}
	c.flags = 0
	putFloatAlpContainer(c)
}

func (c *FloatAlpContainer[T, E]) Type() ContainerType {
	return TFloatAlp
}

func (c *FloatAlpContainer[T, E]) Len() int {
	return c.values.Len()
}

func (c *FloatAlpContainer[T, E]) Size() int {
	v := 4 + c.values.Size()
	if c.flags&FlagPatched > 0 {
		v += c.patches.Size() + c.positions.Size()
	}
	return v
}

func (c *FloatAlpContainer[T, E]) Matcher() types.NumberMatcher[T] {
	return c
}

func (c *FloatAlpContainer[T, E]) Chunks() types.NumberIterator[T] {
	c.initDecoder()
	return NewFloatAlpIterator(c)
}

func (c *FloatAlpContainer[T, E]) All() iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		it := c.Chunks()
		it.All()(yield)
		it.Close()
	}
}

func (c *FloatAlpContainer[T, E]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		it := c.Chunks()
		it.Values()(yield)
		it.Close()
	}
}

func (c *FloatAlpContainer[T, E]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatAlp))
	dst = binary.AppendUvarint(dst, uint64(c.exponent))
	dst = binary.AppendUvarint(dst, uint64(c.factor))
	dst = append(dst, byte(c.flags))
	dst = c.values.Store(dst)
	if c.flags&FlagPatched > 0 {
		dst = c.patches.Store(dst)
		dst = c.positions.Store(dst)
	}
	return dst
}

func (c *FloatAlpContainer[T, E]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatAlp) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	v, n := binary.Uvarint(buf)
	c.exponent = uint8(v)
	buf = buf[n:]

	v, n = binary.Uvarint(buf)
	c.factor = uint8(v)
	buf = buf[n:]

	// load flags
	c.flags = AlpFlags(buf[0])
	buf = buf[1:]

	// alloc and decode values child container
	c.values = NewInt[E](ContainerType(buf[0]))
	var err error
	buf, err = c.values.Load(buf)
	if err != nil {
		return buf, err
	}

	if c.flags&FlagPatched > 0 {
		// patch values
		c.patches = NewFloat[T](ContainerType(buf[0]))
		buf, err = c.patches.Load(buf)
		if err != nil {
			return buf, err
		}

		// patch positions
		c.positions = NewInt[uint32](ContainerType(buf[0]))
		buf, err = c.positions.Load(buf)
		if err != nil {
			return buf, err
		}
	}

	return buf, nil
}

func (c *FloatAlpContainer[T, E]) Get(n int) T {
	c.initDecoder()
	return c.dec.Load().DecodeValue(c.values.Get(n), n)
}

func (c *FloatAlpContainer[T, E]) AppendTo(dst []T, sel []uint32) []T {
	if sel == nil {
		// faster to do serial unpack & decode
		sz := c.Len()
		tmp := c.values.AppendTo(arena.Alloc[E](sz), nil)
		c.initDecoder()
		dst = dst[:sz]
		c.dec.Load().Decode(dst, tmp)
		arena.Free(tmp)
	} else {
		it := c.Chunks()
		for _, v := range sel {
			dst = append(dst, it.Value(int(v)))
		}
		it.Close()
	}
	return dst
}

func (c *FloatAlpContainer[T, E]) Encode(ctx *Context[T], vals []T) NumberContainer[T] {
	// encode using parames from analysis
	enc := alp.NewEncoder[T, E]()
	res := enc.Encode(vals, ctx.Alp.Exp)
	c.exponent = ctx.Alp.Exp.E
	c.factor = ctx.Alp.Exp.F

	// encode child containers, skip analysis and use known values
	vctx := NewIntContext(res.Min, res.Max, len(vals)).WithLevel(ctx.Lvl - 1)
	c.values = EncodeInt(vctx, res.Encoded)
	vctx.Close()
	if n := len(res.PatchValues); n > 0 {
		c.flags |= FlagPatched
		pctx := AnalyzeFloat(res.PatchValues, false, false).WithLevel(1)
		c.patches = NewFloat[T](TFloatRaw).Encode(pctx, res.PatchValues)
		pctx.Close()
		ectx := NewIntContext(0, res.PatchIndices[n-1], n).WithLevel(ctx.Lvl - 1)
		c.positions = EncodeInt(ectx, res.PatchIndices)
		ectx.Close()
	}
	if res.IsSafeInt {
		c.flags |= FlagSafeInt
	}
	res.Close()

	return c
}

func (c *FloatAlpContainer[T, E]) initDecoder() {
	if c.dec.Load() != nil {
		return
	}
	p := alp.NewDecoder[T, E](c.factor, c.exponent).WithSafeInt(c.flags&FlagSafeInt > 0)
	if c.flags&FlagPatched > 0 {
		cnt := c.patches.Len()
		p.WithPatches(
			c.patches.AppendTo(arena.Alloc[T](cnt), nil),
			c.positions.AppendTo(arena.Alloc[uint32](cnt), nil),
		)
	}
	c.dec.Store(p)
}

func (c *FloatAlpContainer[T, E]) Cmp(i, j int) int {
	return cmp.Compare(c.Get(i), c.Get(j))
}

func (c *FloatAlpContainer[T, E]) MatchEqual(val T, bits, mask *Bitset) {
	// Golang has no totalOrder semantics for Nan values. Any Nan comparison
	// returns false, so we must explicitly check for NaN when requested.
	// NaN is never encoded as regular float and may only be part of patches.
	isNaN := val != val // math.IsNaN(float64(val))
	if !isNaN {
		// try translate val into ALP domain
		enc := alp.NewEncoder[T, E]()
		av, ok := enc.EncodeSingle(val, alp.Exponents{E: c.exponent, F: c.factor})

		// on success, match against encoded int values
		if ok {
			c.values.MatchEqual(av, bits, mask)
		}
	}

	// merge _all_ patches by flipping bits, note values contain
	// vector min as replacement and value match above may have
	// matched it but the true patched value would not match
	if c.flags&FlagPatched > 0 {
		// load decoded patch data
		c.initDecoder()
		vals, pos := c.dec.Load().Patches()

		// if av == min we must revert bits on all patch positions
		// we know by checking if any patch position is already set
		if bits.Contains(int(pos[0])) {
			for _, p := range pos {
				bits.Unset(int(p))
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
	exp := alp.Exponents{E: c.exponent, F: c.factor}
	av, ok := enc.EncodeSingle(val, exp)
	if !ok {
		// slow-path: decode and match
		matchIt(c.Chunks(), matchFn[T](types.FilterModeLt), val, bits, mask)
		return
	}

	// match successful encoded value
	c.values.MatchLess(av, bits, mask)

	// merge _all_ patches by flipping bits, note values contain
	// vector min as replacement and value match above may have
	// matched it but the true patched value would not match
	if c.flags&FlagPatched > 0 {
		c.initDecoder()
		vals, pos := c.dec.Load().Patches()
		for i, p := range pos {
			if vals[i] < val {
				bits.Set(int(p))
			} else {
				bits.Unset(int(p))
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
	exp := alp.Exponents{E: c.exponent, F: c.factor}
	av, ok := enc.EncodeSingle(val, exp)
	if !ok {
		// slow-path: decode and match
		matchIt(c.Chunks(), matchFn[T](types.FilterModeLe), val, bits, mask)
		return
	}

	// match successful encoded value
	c.values.MatchLessEqual(av, bits, mask)

	// merge _all_ patches by flipping bits, note values contain
	// vector min as replacement and value match above may have
	// matched it but the true patched value would not match
	if c.flags&FlagPatched > 0 {
		c.initDecoder()
		vals, pos := c.dec.Load().Patches()
		// NaN cannot match here
		for i, p := range pos {
			if vals[i] <= val {
				bits.Set(int(p))
			} else {
				bits.Unset(int(p))
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
	exp := alp.Exponents{E: c.exponent, F: c.factor}
	av, ok := enc.EncodeSingle(val, exp)
	if !ok {
		// slow-path: decode and match
		matchIt(c.Chunks(), matchFn[T](types.FilterModeGt), val, bits, mask)
		return
	}

	// match successful encoded value
	c.values.MatchGreater(av, bits, mask)

	// merge _all_ patches by flipping bits, note values contain
	// vector min as replacement and value match above may have
	// matched it but the true patched value would not match
	if c.flags&FlagPatched > 0 {
		c.initDecoder()
		vals, pos := c.dec.Load().Patches()
		for i, p := range pos {
			if vals[i] > val {
				bits.Set(int(p))
			} else {
				bits.Unset(int(p))
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
	exp := alp.Exponents{E: c.exponent, F: c.factor}
	av, ok := enc.EncodeSingle(val, exp)
	if !ok {
		// slow-path: decode and match
		matchIt(c.Chunks(), matchFn[T](types.FilterModeGe), val, bits, mask)
		return
	}

	// match successful encoded value
	c.values.MatchGreaterEqual(av, bits, mask)

	// merge _all_ patches by flipping bits, note values contain
	// vector min as replacement and value match above may have
	// matched it but the true patched value would not match
	if c.flags&FlagPatched > 0 {
		c.initDecoder()
		vals, pos := c.dec.Load().Patches()
		// NaN cannot match here
		for i, p := range pos {
			if vals[i] >= val {
				bits.Set(int(p))
			} else {
				bits.Unset(int(p))
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

	// try match integers first. likely includes min val which is used as
	// replacement for all patched values. we need to undo these
	// matches below when creating the union(values, patches)
	enc := alp.NewEncoder[T, E]()
	exp := alp.Exponents{E: c.exponent, F: c.factor}
	av, ok1 := enc.EncodeSingle(a, exp)
	bv, ok2 := enc.EncodeSingle(b, exp)
	if !ok1 || !ok2 {
		// slow-path: decode and match because boundary values
		// don't cleanly translate to ALP domain
		matchRangeIt(c.Chunks(), matchFn[T](types.FilterModeRange), a, b, bits, mask)
		return
	}

	// match integer range
	if av <= bv {
		c.values.MatchBetween(av, bv, bits, mask)
	}

	// merge _all_ patches by flipping bits, note values contain
	// vector min as replacement and value match above may have
	// matched it but the true patched value would not match
	if c.flags&FlagPatched > 0 {
		c.initDecoder()
		vals, pos := c.dec.Load().Patches()
		// NaN cannot match here
		for i, p := range pos {
			if vals[i] >= a && vals[i] <= b {
				bits.Set(int(p))
			} else {
				bits.Unset(int(p))
			}
		}
	}
}

// N.A.
func (c *FloatAlpContainer[T, E]) MatchInSet(_ any, _, _ *Bitset)    {}
func (c *FloatAlpContainer[T, E]) MatchNotInSet(_ any, _, _ *Bitset) {}

type FloatAlpfactory struct {
	f64Pool   sync.Pool
	f32Pool   sync.Pool
	f64ItPool sync.Pool
	f32ItPool sync.Pool
}

func newFloatAlpContainer[T types.Float]() NumberContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpfactory.f64Pool.Get().(NumberContainer[T])
	case float32:
		return floatAlpfactory.f32Pool.Get().(NumberContainer[T])
	default:
		return nil
	}
}

func putFloatAlpContainer[T types.Float](c NumberContainer[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpfactory.f64Pool.Put(c)
	case float32:
		floatAlpfactory.f32Pool.Put(c)
	}
}

func newFloatAlpIterator[T types.Float, E int64 | int32]() *FloatAlpIterator[T, E] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpfactory.f64ItPool.Get().(*FloatAlpIterator[T, E])
	case float32:
		return floatAlpfactory.f32ItPool.Get().(*FloatAlpIterator[T, E])
	default:
		return nil
	}
}

func putFloatAlpIterator[T types.Float, E int64 | int32](c *FloatAlpIterator[T, E]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpfactory.f64ItPool.Put(c)
	case float32:
		floatAlpfactory.f32ItPool.Put(c)
	}
}

var floatAlpfactory = FloatAlpfactory{
	f64Pool:   sync.Pool{New: func() any { return new(FloatAlpContainer[float64, int64]) }},
	f32Pool:   sync.Pool{New: func() any { return new(FloatAlpContainer[float32, int32]) }},
	f64ItPool: sync.Pool{New: func() any { return new(FloatAlpIterator[float64, int64]) }},
	f32ItPool: sync.Pool{New: func() any { return new(FloatAlpIterator[float32, int32]) }},
}

// ---------------------------------------
// Iterator
//

type FloatAlpIterator[T types.Float, E int64 | int32] struct {
	BaseIterator[T]
	dec *alp.Decoder[T, E]
	src types.NumberIterator[E]
}

func NewFloatAlpIterator[T types.Float, E int64 | int32](c *FloatAlpContainer[T, E]) *FloatAlpIterator[T, E] {
	it := newFloatAlpIterator[T, E]()
	it.dec = c.dec.Load()
	it.src = c.values.Chunks()
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
	src, n := it.src.Next()
	if n == 0 {
		it.ofs = it.len
		it.base = -1
		return 0
	}
	it.dec.DecodeChunk(&it.chunk, src, n, base)
	it.base = base
	return n
}
