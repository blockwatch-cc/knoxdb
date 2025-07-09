// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"iter"
	"sort"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

const RUN_END_THRESHOLD = 4

// ensure we implement required interfaces
var (
	_ types.NumberAccessor[int64] = (*RunEndContainer[int64])(nil)
	_ NumberContainer[int64]      = (*RunEndContainer[int64])(nil)
)

// TIntRunEnd
type RunEndContainer[T types.Integer] struct {
	readOnlyContainer[T]
	Values NumberContainer[T]      // []T
	Ends   NumberContainer[uint32] // []uint32
	it     types.NumberIterator[T]
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

func (c *RunEndContainer[T]) Type() ContainerType {
	return TIntRunEnd
}

func (c *RunEndContainer[T]) Len() int {
	return c.n
}

func (c *RunEndContainer[T]) Size() int {
	return 1 + c.Values.Size() + c.Ends.Size()
}

func (c *RunEndContainer[T]) Matcher() types.NumberMatcher[T] {
	return c
}

func (c *RunEndContainer[T]) Chunks() types.NumberIterator[T] {
	return NewRunEndIterator(c)
}

func (c *RunEndContainer[T]) Iterator() iter.Seq2[int, T] {
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

func (c *RunEndContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntRunEnd))
	dst = c.Values.Store(dst)
	return c.Ends.Store(dst)
}

func (c *RunEndContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntRunEnd) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	// alloc and decode values child container
	c.Values = NewInt[T](ContainerType(buf[0]))
	var err error
	buf, err = c.Values.Load(buf)
	if err != nil {
		return buf, err
	}

	// alloc and decode ends child container
	c.Ends = NewInt[uint32](ContainerType(buf[0]))
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
		c.it = c.Chunks()
	}
	return c.it.Get(n)
}

func (c *RunEndContainer[T]) AppendTo(dst []T, sel []uint32) []T {
	if sel == nil {
		var (
			sz   = c.Ends.Len()
			vals = c.Values.AppendTo(arena.Alloc[T](sz), nil)
			ends = c.Ends.AppendTo(arena.Alloc[uint32](sz), nil)
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
		it := c.Chunks()
		for _, v := range sel {
			dst = append(dst, it.Get(int(v)))
		}
		it.Close()
	}
	return dst
}

func (c *RunEndContainer[T]) Encode(ctx *Context[T], vals []T) NumberContainer[T] {
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
	c.Values = EncodeInt(ctx.WithLevel(ctx.Lvl-1), values)
	arena.Free(values)
	ctx.NumValues = len(vals)

	// create analysis context for known sequential data (min=first, max=last)
	ectx := NewIntContext[uint32](ends[0], ends[len(ends)-1], len(ends)).WithLevel(ctx.Lvl - 1)
	c.Ends = EncodeInt(ectx, ends)
	ectx.Close()
	if c.Ends.Type() != TIntRaw {
		arena.Free(ends)
	}
	c.n = len(vals)

	return c
}

func (c *RunEndContainer[T]) Cmp(i, j int) int {
	return util.Cmp(c.Get(i), c.Get(j))
}

func (c *RunEndContainer[T]) MatchEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.Values.Len())
	c.Values.MatchEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.Values.Len())
	c.Values.MatchNotEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchLess(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.Values.Len())
	c.Values.MatchLess(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.Values.Len())
	c.Values.MatchLessEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchGreater(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.Values.Len())
	c.Values.MatchGreater(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.Values.Len())
	c.Values.MatchGreaterEqual(val, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.Values.Len())
	c.Values.MatchBetween(a, b, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchInSet(s any, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.Values.Len())
	c.Values.MatchInSet(s, vbits, mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
}

func (c *RunEndContainer[T]) MatchNotInSet(s any, bits, mask *Bitset) {
	// match values container and translate matches
	vbits := bitset.New(c.Values.Len())
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

func newRunEndContainer[T types.Integer]() NumberContainer[T] {
	switch any(T(0)).(type) {
	case int64:
		return runEndFactory.i64Pool.Get().(NumberContainer[T])
	case int32:
		return runEndFactory.i32Pool.Get().(NumberContainer[T])
	case int16:
		return runEndFactory.i16Pool.Get().(NumberContainer[T])
	case int8:
		return runEndFactory.i8Pool.Get().(NumberContainer[T])
	case uint64:
		return runEndFactory.u64Pool.Get().(NumberContainer[T])
	case uint32:
		return runEndFactory.u32Pool.Get().(NumberContainer[T])
	case uint16:
		return runEndFactory.u16Pool.Get().(NumberContainer[T])
	case uint8:
		return runEndFactory.u8Pool.Get().(NumberContainer[T])
	default:
		return nil
	}
}

func putRunEndContainer[T types.Integer](c NumberContainer[T]) {
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

type RunEndIterator[T types.Integer] struct {
	BaseIterator[T]
	valIt types.NumberIterator[T]
	endIt types.NumberIterator[uint32]
}

func NewRunEndIterator[T types.Integer](c *RunEndContainer[T]) *RunEndIterator[T] {
	it := newRunEndIterator[T]()
	it.valIt = c.Values.Chunks()
	it.endIt = c.Ends.Chunks()
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
