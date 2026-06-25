// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
	"sort"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
)

// ensure we implement required interfaces
var (
	_ types.NumberAccessor[float64] = (*FloatRunEndContainer[float64])(nil)
	_ NumberContainer[float64]      = (*FloatRunEndContainer[float64])(nil)
)

// TFloatRunEnd
type FloatRunEndContainer[T types.Float] struct {
	readOnlyContainer[T]
	values NumberContainer[T]      // []T
	ends   NumberContainer[uint32] // []uint32
}

func (c *FloatRunEndContainer[T]) Info() string {
	return fmt.Sprintf("REE(%s)_[%s]_[%s]", TypeName[T](), c.values.Info(), c.ends.Info())
}

func (c *FloatRunEndContainer[T]) Close() {
	c.values.Close()
	c.ends.Close()
	c.values = nil
	c.ends = nil
	putFloatRunEndContainer(c)
}

func (c *FloatRunEndContainer[T]) Type() ContainerType {
	return TFloatRunEnd
}

func (c *FloatRunEndContainer[T]) Len() int {
	l := c.ends.Len()
	if l == 0 {
		return 0
	}
	return int(c.ends.Get(l-1)) + 1
}

func (c *FloatRunEndContainer[T]) Size() int {
	return 1 + c.values.Size() + c.ends.Size()
}

func (c *FloatRunEndContainer[T]) Matcher() types.NumberMatcher[T] {
	return c
}

func (c *FloatRunEndContainer[T]) Chunks() types.NumberIterator[T] {
	return NewFloatRunEndIterator(c)
}

func (c *FloatRunEndContainer[T]) All() iter.Seq2[int, T] {
	return func(fn func(int, T) bool) {
		it := c.Chunks()
		for i := range it.Len() {
			if !fn(i, it.Value(i)) {
				break
			}
		}
		it.Close()
	}
}

func (c *FloatRunEndContainer[T]) Values() iter.Seq[T] {
	return func(fn func(T) bool) {
		it := c.Chunks()
		for i := range it.Len() {
			if !fn(it.Value(i)) {
				break
			}
		}
		it.Close()
	}
}

func (c *FloatRunEndContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatRunEnd))
	dst = c.values.Store(dst)
	return c.ends.Store(dst)
}

func (c *FloatRunEndContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatRunEnd) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	// alloc and decode values child container
	c.values = NewFloat[T](ContainerType(buf[0]))
	var err error
	buf, err = c.values.Load(buf)
	if err != nil {
		return buf, err
	}

	// alloc and decode ends child container
	c.ends = NewInt[uint32](ContainerType(buf[0]))
	return c.ends.Load(buf)
}

func (c *FloatRunEndContainer[T]) Get(n int) T {
	idx := sort.Search(c.ends.Len(), func(i int) bool {
		return c.ends.Get(i) >= uint32(n)
	})
	return c.values.Get(idx)
}

func (c *FloatRunEndContainer[T]) AppendTo(dst []T, sel []uint32) []T {
	if sel == nil {
		var (
			sz   = c.ends.Len()
			vals = c.values.AppendTo(arena.Alloc[T](sz), nil)
			ends = c.ends.AppendTo(arena.Alloc[uint32](sz), nil)
			i    uint32
		)
		dst = dst[:ends[sz-1]+1]

		for k, end := range ends {
			val := vals[k]
			for range (end - i) / 16 {
				_ = dst[i+15]
				dst[i] = val
				dst[i+1] = val
				dst[i+2] = val
				dst[i+3] = val
				dst[i+4] = val
				dst[i+5] = val
				dst[i+6] = val
				dst[i+7] = val
				dst[i+8] = val
				dst[i+9] = val
				dst[i+10] = val
				dst[i+11] = val
				dst[i+12] = val
				dst[i+13] = val
				dst[i+14] = val
				dst[i+15] = val
				i += 16
			}
			for range (end - i) / 4 {
				_ = dst[i+3]
				dst[i] = val
				dst[i+1] = val
				dst[i+2] = val
				dst[i+3] = val
				i += 4
			}
			for i <= end {
				dst[i] = val
				i++
			}
		}
		arena.Free(ends)
		arena.Free(vals)
	} else {
		if slices.IsSorted(sel) {
			vit := c.values.Chunks()
			eit := c.ends.Chunks()
			idx, end, val := 0, eit.Value(0), vit.Value(0)
			for len(sel) > 0 {
				// use current run while valid
				if sel[0] <= end {
					dst = append(dst, val)
					sel = sel[1:]
					continue
				}
				// find next run
				for end < sel[0] {
					idx++
					end = eit.Value(idx)
				}
				val = vit.Value(idx)
			}
			vit.Close()
			eit.Close()
		} else {
			// fallback to slower get for unsorted selection lists
			it := c.Chunks()
			for _, v := range sel {
				dst = append(dst, it.Value(int(v)))
			}
			it.Close()
		}
	}
	return dst
}

func (c *FloatRunEndContainer[T]) Encode(ctx *Context[T], vals []T) NumberContainer[T] {
	// generate run-end encoding from originals
	values := arena.Alloc[T](ctx.NumRuns)[:ctx.NumRuns]
	ends := arena.Alloc[uint32](ctx.NumRuns)[:ctx.NumRuns]
	values[0] = vals[0]
	var (
		n uint32
		p int
	)
	for i, v := range vals[1:] {
		if vals[i] == v {
			n++
			continue
		}
		ends[p] = n
		n++
		p++
		values[p] = v
	}
	ends[p] = n

	// encode child containers, reuse analysis context
	ctx.NumValues = ctx.NumRuns
	c.values = EncodeFloat(ctx.WithLevel(ctx.Lvl-1), values)
	if c.values.Type() != TFloatRaw {
		arena.Free(values)
	}
	ctx.NumValues = len(vals)

	// create analysis context for known sequential data (min=first, max=last)
	ectx := NewIntContext[uint32](ends[0], ends[len(ends)-1], len(ends)).WithLevel(ctx.Lvl - 1)
	c.ends = EncodeInt(ectx, ends)
	ectx.Close()
	if c.ends.Type() != TIntRaw {
		arena.Free(ends)
	}

	return c
}

func (c *FloatRunEndContainer[T]) Cmp(i, j int) int {
	return cmp.Compare(c.Get(i), c.Get(j))
}

func (c *FloatRunEndContainer[T]) MatchEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.values.Len())
	c.values.MatchEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *FloatRunEndContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.values.Len())
	c.values.MatchNotEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *FloatRunEndContainer[T]) MatchLess(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.values.Len())
	c.values.MatchLess(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *FloatRunEndContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.values.Len())
	c.values.MatchLessEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *FloatRunEndContainer[T]) MatchGreater(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.values.Len())
	c.values.MatchGreater(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *FloatRunEndContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.values.Len())
	c.values.MatchGreaterEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *FloatRunEndContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.values.Len())
	c.values.MatchBetween(a, b, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

// N.A.
func (c *FloatRunEndContainer[T]) MatchInSet(_ any, bits, _ *Bitset)    {}
func (c *FloatRunEndContainer[T]) MatchNotInSet(_ any, bits, _ *Bitset) {}

func (c *FloatRunEndContainer[T]) applyMatch(bits, vbits *Bitset) {
	// catch easy corner cases
	switch {
	case vbits.None():
		return
	case vbits.All():
		bits.One()
		return
	}

	u32 := arena.Alloc[uint32](vbits.Count())
	for _, k := range vbits.AllIndexes(u32) {
		var start uint32
		if k > 0 {
			start = c.ends.Get(int(k-1)) + 1
		}
		end := c.ends.Get(int(k))
		bits.SetRange(int(start), int(end))
	}
	arena.Free(u32)
}

type FloatRunEndFactory struct {
	f64Pool   sync.Pool
	f32Pool   sync.Pool
	f64ItPool sync.Pool
	f32ItPool sync.Pool
}

func newFloatRunEndContainer[T types.Float]() NumberContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatRunEndFactory.f64Pool.Get().(NumberContainer[T])
	case float32:
		return floatRunEndFactory.f32Pool.Get().(NumberContainer[T])
	default:
		return nil
	}
}

func putFloatRunEndContainer[T types.Float](c NumberContainer[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatRunEndFactory.f64Pool.Put(c)
	case float32:
		floatRunEndFactory.f32Pool.Put(c)
	}
}

func newFloatRunEndIterator[T types.Float]() *FloatRunEndIterator[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatRunEndFactory.f64ItPool.Get().(*FloatRunEndIterator[T])
	case float32:
		return floatRunEndFactory.f32ItPool.Get().(*FloatRunEndIterator[T])
	default:
		return nil
	}
}

func putFloatRunEndIterator[T types.Float](c *FloatRunEndIterator[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatRunEndFactory.f64ItPool.Put(c)
	case float32:
		floatRunEndFactory.f32ItPool.Put(c)
	}
}

var floatRunEndFactory = FloatRunEndFactory{
	f64Pool:   sync.Pool{New: func() any { return new(FloatRunEndContainer[float64]) }},
	f32Pool:   sync.Pool{New: func() any { return new(FloatRunEndContainer[float32]) }},
	f64ItPool: sync.Pool{New: func() any { return new(FloatRunEndIterator[float64]) }},
	f32ItPool: sync.Pool{New: func() any { return new(FloatRunEndIterator[float32]) }},
}

// ---------------------------------------
// Iterator
//

type FloatRunEndIterator[T types.Float] struct {
	BaseIterator[T]
	valIt types.NumberIterator[T]
	endIt types.NumberIterator[uint32]
}

func NewFloatRunEndIterator[T types.Float](c *FloatRunEndContainer[T]) *FloatRunEndIterator[T] {
	it := newFloatRunEndIterator[T]()
	it.valIt = c.values.Chunks()
	it.endIt = c.ends.Chunks()
	it.base = -1
	it.len = c.Len()
	it.BaseIterator.fill = it.fill
	return it
}

func (it *FloatRunEndIterator[T]) Close() {
	it.valIt.Close()
	it.endIt.Close()
	it.valIt = nil
	it.endIt = nil
	it.BaseIterator.Close()
	putFloatRunEndIterator(it)
}

func (it *FloatRunEndIterator[T]) fill(base int) int {
	// find which run contains base
	nRuns := it.valIt.Len()
	var k int
	if base > 0 {
		k = sort.Search(nRuns, func(i int) bool {
			return it.endIt.Value(i) >= uint32(base)
		})
		if k == nRuns {
			// not found, should not happen
			return 0
		}
	}

	// process REE pairs up until EOF or chunk is full
	var n int
	for n < CHUNK_SIZE && k < nRuns {
		end, val := it.endIt.Value(k), it.valIt.Value(k)
		for range min(CHUNK_SIZE, int(end+1)-base) - n {
			// fmt.Printf("REE chunk[%d] = ree(%d) = %d\n", n, k, val)
			it.chunk[n] = val
			n++
		}
		k++
	}
	it.base = base

	return n
}
