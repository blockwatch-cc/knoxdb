// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sort"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
)

const RUN_END_THRESHOLD = 4

// TIntegerRunEnd
type RunEndContainer[T types.Integer] struct {
	Values IntegerContainer[T]      // []T
	Ends   IntegerContainer[uint32] // []uint32
	it     Iterator[T]
	n      int
}

func (c *RunEndContainer[T]) Info() string {
	return fmt.Sprintf("REE(%s)_[%s]_[%s]", TypeName[T](), c.Values.Info(), c.Ends.Info())
}

func (c *RunEndContainer[T]) Close() {
	if c.it != nil {
		c.it.Close()
		c.it = nil
	}
	c.Values.Close()
	c.Ends.Close()
	c.Values = nil
	c.Ends = nil
	c.n = 0
	putRunEndContainer[T](c)
}

func (c *RunEndContainer[T]) Type() IntegerContainerType {
	return TIntegerRunEnd
}

func (c *RunEndContainer[T]) Len() int {
	return c.n
}

func (c *RunEndContainer[T]) Size() int {
	return 1 + c.Values.Size() + c.Ends.Size()
}

func (c *RunEndContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntegerRunEnd))
	dst = c.Values.Store(dst)
	return c.Ends.Store(dst)
}

func (c *RunEndContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntegerRunEnd) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	// alloc and decode values child container
	c.Values = NewInt[T](IntegerContainerType(buf[0]))
	var err error
	buf, err = c.Values.Load(buf)
	if err != nil {
		return buf, err
	}

	// alloc and decode ends child container
	c.Ends = NewInt[uint32](IntegerContainerType(buf[0]))
	buf, err = c.Ends.Load(buf)
	if err != nil {
		return buf, err
	}
	c.n = int(c.Ends.Get(c.Ends.Len()-1)) + 1
	return buf, nil
}

func (c *RunEndContainer[T]) Get(n int) T {
	// iterator may be more efficient
	if c.it == nil {
		c.it = c.Iterator()
	}
	return c.it.Get(n)
}

func (c *RunEndContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	it := c.Iterator()
	if sel == nil {
		// TODO: fast algo for REE
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
	// if sel == nil {
	// 	l := uint32(c.Len())
	// 	var i uint32
	// 	var k int
	// 	dst = dst[:l]

	// 	// TODO: use iterators and get chunks of ends and values in an outer loop
	// 	// instead of Get

	// 	for i < l {
	// 		end, val := c.Ends.Get(k), c.Values.Get(k)
	// 		for range (end - i) / 4 {
	// 			dst[i] = val
	// 			dst[i+1] = val
	// 			dst[i+2] = val
	// 			dst[i+3] = val
	// 			i += 4
	// 		}
	// 		for i <= end {
	// 			dst[i] = val
	// 			i++
	// 		}
	// 		k++
	// 	}
	// } else {
	// 	if slices.IsSorted(sel) {
	// 		idx, end, val := 0, c.Ends.Get(0), c.Values.Get(0)
	// 		for len(sel) > 0 {
	// 			// use current run while valid
	// 			if sel[0] <= end {
	// 				dst = append(dst, val)
	// 				sel = sel[1:]
	// 				continue
	// 			}
	// 			// find next run
	// 			for end < sel[0] {
	// 				idx++
	// 				end = c.Ends.Get(idx)
	// 			}
	// 			val = c.Values.Get(idx)
	// 		}
	// 	} else {
	// 		// use iterator for unsorted selection lists
	// 		it := c.Iterator()
	// 		for _, v := range sel {
	// 			dst = append(dst, it.Get(int(v)))
	// 		}
	// 		it.Close()
	// 	}
	// }
	// return dst
}

func (c *RunEndContainer[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	// generate run-end encoding from originals, Min-FOR is done by values child
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

	// fmt.Printf("REE new len=%d\n> vals=%v\n> ends=%v\n", len(vals), values, ends)

	// encode child containers, reuse analysis context
	ctx.NumValues = ctx.NumRuns
	c.Values = EncodeInt(ctx, values, lvl-1)
	if c.Values.Type() != TIntegerRaw {
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
	c.n = len(vals)

	return c
}

func (c *RunEndContainer[T]) MatchEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchNotEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchLess(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchLess(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchLessEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchGreater(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchGreater(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchGreaterEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchBetween(a, b, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchInSet(s any, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchInSet(s, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchNotInSet(s any, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.NewBitset(c.Values.Len())
	c.Values.MatchNotInSet(s, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) applyMatch(bits, vbits *Bitset) {
	// catch easy corner cases
	switch {
	case vbits.None():
		return
	case vbits.All():
		bits.One()
		return
	}

	// handle value matches by unpacking range boundaries
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

type RunEndFactory struct {
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

func newRunEndContainer[T types.Integer]() IntegerContainer[T] {
	switch any(T(0)).(type) {
	case int64:
		return runEndFactory.i64Pool.Get().(IntegerContainer[T])
	case int32:
		return runEndFactory.i32Pool.Get().(IntegerContainer[T])
	case int16:
		return runEndFactory.i16Pool.Get().(IntegerContainer[T])
	case int8:
		return runEndFactory.i8Pool.Get().(IntegerContainer[T])
	case uint64:
		return runEndFactory.u64Pool.Get().(IntegerContainer[T])
	case uint32:
		return runEndFactory.u32Pool.Get().(IntegerContainer[T])
	case uint16:
		return runEndFactory.u16Pool.Get().(IntegerContainer[T])
	case uint8:
		return runEndFactory.u8Pool.Get().(IntegerContainer[T])
	default:
		return nil
	}
}

func putRunEndContainer[T types.Integer](c IntegerContainer[T]) {
	switch any(T(0)).(type) {
	case int64:
		runEndFactory.i64Pool.Put(c)
	case int32:
		runEndFactory.i32Pool.Put(c)
	case int16:
		runEndFactory.i16Pool.Put(c)
	case int8:
		runEndFactory.i8Pool.Put(c)
	case uint64:
		runEndFactory.u64Pool.Put(c)
	case uint32:
		runEndFactory.u32Pool.Put(c)
	case uint16:
		runEndFactory.u16Pool.Put(c)
	case uint8:
		runEndFactory.u8Pool.Put(c)
	}
}

func newRunEndIterator[T types.Integer]() *RunEndIterator[T] {
	switch any(T(0)).(type) {
	case int64:
		return runEndFactory.i64ItPool.Get().(*RunEndIterator[T])
	case int32:
		return runEndFactory.i32ItPool.Get().(*RunEndIterator[T])
	case int16:
		return runEndFactory.i16ItPool.Get().(*RunEndIterator[T])
	case int8:
		return runEndFactory.i8ItPool.Get().(*RunEndIterator[T])
	case uint64:
		return runEndFactory.u64ItPool.Get().(*RunEndIterator[T])
	case uint32:
		return runEndFactory.u32ItPool.Get().(*RunEndIterator[T])
	case uint16:
		return runEndFactory.u16ItPool.Get().(*RunEndIterator[T])
	case uint8:
		return runEndFactory.u8ItPool.Get().(*RunEndIterator[T])
	default:
		return nil
	}
}

func putRunEndIterator[T types.Integer](c *RunEndIterator[T]) {
	switch any(T(0)).(type) {
	case int64:
		runEndFactory.i64ItPool.Put(c)
	case int32:
		runEndFactory.i32ItPool.Put(c)
	case int16:
		runEndFactory.i16ItPool.Put(c)
	case int8:
		runEndFactory.i8ItPool.Put(c)
	case uint64:
		runEndFactory.u64ItPool.Put(c)
	case uint32:
		runEndFactory.u32ItPool.Put(c)
	case uint16:
		runEndFactory.u16ItPool.Put(c)
	case uint8:
		runEndFactory.u8ItPool.Put(c)
	}
}

var runEndFactory = RunEndFactory{
	i64Pool:   sync.Pool{New: func() any { return new(RunEndContainer[int64]) }},
	i32Pool:   sync.Pool{New: func() any { return new(RunEndContainer[int32]) }},
	i16Pool:   sync.Pool{New: func() any { return new(RunEndContainer[int16]) }},
	i8Pool:    sync.Pool{New: func() any { return new(RunEndContainer[int8]) }},
	u64Pool:   sync.Pool{New: func() any { return new(RunEndContainer[uint64]) }},
	u32Pool:   sync.Pool{New: func() any { return new(RunEndContainer[uint32]) }},
	u16Pool:   sync.Pool{New: func() any { return new(RunEndContainer[uint16]) }},
	u8Pool:    sync.Pool{New: func() any { return new(RunEndContainer[uint8]) }},
	i64ItPool: sync.Pool{New: func() any { return new(RunEndIterator[int64]) }},
	i32ItPool: sync.Pool{New: func() any { return new(RunEndIterator[int32]) }},
	i16ItPool: sync.Pool{New: func() any { return new(RunEndIterator[int16]) }},
	i8ItPool:  sync.Pool{New: func() any { return new(RunEndIterator[int8]) }},
	u64ItPool: sync.Pool{New: func() any { return new(RunEndIterator[uint64]) }},
	u32ItPool: sync.Pool{New: func() any { return new(RunEndIterator[uint32]) }},
	u16ItPool: sync.Pool{New: func() any { return new(RunEndIterator[uint16]) }},
	u8ItPool:  sync.Pool{New: func() any { return new(RunEndIterator[uint8]) }},
}

func (c *RunEndContainer[T]) Iterator() Iterator[T] {
	return NewRunEndIterator(c)
}

type RunEndIterator[T types.Integer] struct {
	BaseIterator[T]
	valIt Iterator[T]
	endIt Iterator[uint32]
}

func NewRunEndIterator[T types.Integer](c *RunEndContainer[T]) *RunEndIterator[T] {
	it := newRunEndIterator[T]()
	it.valIt = c.Values.Iterator()
	it.endIt = c.Ends.Iterator()
	it.base = -1
	it.len = c.Len()
	it.BaseIterator.fill = it.fill
	return it
}

func (it *RunEndIterator[T]) Close() {
	it.valIt.Close()
	it.endIt.Close()
	it.valIt = nil
	it.endIt = nil
	it.BaseIterator.Close()
	putRunEndIterator(it)
}

func (it *RunEndIterator[T]) fill(base int) int {
	// find which run contains base
	nRuns := it.valIt.Len()
	var k int
	if base > 0 {
		// FIXME: improve linear walk, remember current pos
		// binary search jumps which leads to unnecessary end chunk decoding
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
			it.chunk[n] = val
			n++
		}
		k++
	}
	it.base = base

	return n
}
