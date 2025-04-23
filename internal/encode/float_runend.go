// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"slices"
	"sort"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
)

// TFloatRunEnd
type FloatRunEndContainer[T types.Float] struct {
	Values FloatContainer[T]        // []T
	Ends   IntegerContainer[uint32] // []uint32
}

func (c *FloatRunEndContainer[T]) Info() string {
	return fmt.Sprintf("REE(%s)_[%s]_[%s]", TypeName[T](), c.Values.Info(), c.Ends.Info())
}

func (c *FloatRunEndContainer[T]) Close() {
	c.Values.Close()
	c.Ends.Close()
	c.Values = nil
	c.Ends = nil
	putFloatRunEndContainer(c)
}

func (c *FloatRunEndContainer[T]) Type() FloatContainerType {
	return TFloatRunEnd
}

func (c *FloatRunEndContainer[T]) Len() int {
	l := c.Ends.Len()
	if l == 0 {
		return 0
	}
	return int(c.Ends.Get(l-1)) + 1
}

func (c *FloatRunEndContainer[T]) Size() int {
	return 1 + c.Values.Size() + c.Ends.Size()
}

func (c *FloatRunEndContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatRunEnd))
	dst = c.Values.Store(dst)
	return c.Ends.Store(dst)
}

func (c *FloatRunEndContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatRunEnd) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	// alloc and decode values child container
	c.Values = NewFloat[T](FloatContainerType(buf[0]))
	var err error
	buf, err = c.Values.Load(buf)
	if err != nil {
		return buf, err
	}

	// alloc and decode ends child container
	c.Ends = NewInt[uint32](IntegerContainerType(buf[0]))
	return c.Ends.Load(buf)
}

func (c *FloatRunEndContainer[T]) Get(n int) T {
	idx := sort.Search(c.Ends.Len(), func(i int) bool {
		return c.Ends.Get(i) >= uint32(n)
	})
	return c.Values.Get(idx)
}

func (c *FloatRunEndContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	if sel == nil {
		l := uint32(c.Len())
		var i uint32
		var k int
		dst = dst[:l]
		for i < l {
			end, val := c.Ends.Get(k), c.Values.Get(k)
			for range (end - i) / 4 {
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
			k++
		}
	} else {
		if slices.IsSorted(sel) {
			idx, end, val := 0, c.Ends.Get(0), c.Values.Get(0)
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
					end = c.Ends.Get(idx)
				}
				val = c.Values.Get(idx)
			}
		} else {
			// fallback to slower get for unsorted selection lists
			for _, v := range sel {
				dst = append(dst, c.Get(int(v)))
			}
		}
	}
	return dst
}

func (c *FloatRunEndContainer[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
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
	c.Values = EncodeFloat(ctx, values, lvl-1)
	if c.Values.Type() != TFloatRaw {
		arena.Free(values)
	}
	ctx.NumValues = len(vals)

	// create analysis context for known sequential data (min=first, max=last)
	ectx := NewIntegerContext[uint32](ends[0], ends[len(ends)-1], len(ends))
	c.Ends = EncodeInt(ectx, ends, lvl-1)
	ectx.Close()
	if c.Ends.Type() != TIntegerRaw {
		arena.Free(ends)
	}

	return c
}

func (c *FloatRunEndContainer[T]) MatchEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *FloatRunEndContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchNotEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *FloatRunEndContainer[T]) MatchLess(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchLess(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *FloatRunEndContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchLessEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *FloatRunEndContainer[T]) MatchGreater(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchGreater(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *FloatRunEndContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchGreaterEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *FloatRunEndContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchBetween(a, b, vbits, mask)
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
	for _, k := range vbits.Indexes(u32) {
		var start uint32
		if k > 0 {
			start = c.Ends.Get(int(k-1)) + 1
		}
		end := c.Ends.Get(int(k))
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

func newFloatRunEndContainer[T types.Float]() FloatContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatRunEndFactory.f64Pool.Get().(FloatContainer[T])
	case float32:
		return floatRunEndFactory.f32Pool.Get().(FloatContainer[T])
	default:
		return nil
	}
}

func putFloatRunEndContainer[T types.Float](c FloatContainer[T]) {
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

func (c *FloatRunEndContainer[T]) Iterator() Iterator[T] {
	return NewFloatRunEndIterator(c)
}

type FloatRunEndIterator[T types.Float] struct {
	BaseIterator[T]
	valIt Iterator[T]
	endIt Iterator[uint32]
}

func NewFloatRunEndIterator[T types.Float](c *FloatRunEndContainer[T]) *FloatRunEndIterator[T] {
	it := newFloatRunEndIterator[T]()
	it.valIt = c.Values.Iterator()
	it.endIt = c.Ends.Iterator()
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
			return it.endIt.Get(i) >= uint32(base)
		})
		if k == nRuns {
			// not found, should not happen
			return 0
		}
	}

	// process REE pairs up until EOF or chunk is full
	var n int
	for n < CHUNK_SIZE && k < nRuns {
		end, val := it.endIt.Get(k), it.valIt.Get(k)
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
