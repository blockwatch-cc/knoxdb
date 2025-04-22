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
	// idx := sort.Search(c.Ends.Len(), func(i int) bool {
	// 	return c.Ends.Get(i) >= uint32(n)
	// })
	// return c.Values.Get(idx)
	return c.it.Get(n)
}

func (c *RunEndContainer[T]) AppendTo(sel []uint32, dst []T) []T {
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
			// use iterator for unsorted selection lists
			it := c.Iterator()
			for _, v := range sel {
				dst = append(dst, it.Get(int(v)))
			}
			it.Close()
		}
	}
	return dst
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
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
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

var runEndFactory = RunEndFactory{
	i64Pool: sync.Pool{
		New: func() any { return new(RunEndContainer[int64]) },
	},
	i32Pool: sync.Pool{
		New: func() any { return new(RunEndContainer[int32]) },
	},
	i16Pool: sync.Pool{
		New: func() any { return new(RunEndContainer[int16]) },
	},
	i8Pool: sync.Pool{
		New: func() any { return new(RunEndContainer[int8]) },
	},
	u64Pool: sync.Pool{
		New: func() any { return new(RunEndContainer[uint64]) },
	},
	u32Pool: sync.Pool{
		New: func() any { return new(RunEndContainer[uint32]) },
	},
	u16Pool: sync.Pool{
		New: func() any { return new(RunEndContainer[uint16]) },
	},
	u8Pool: sync.Pool{
		New: func() any { return new(RunEndContainer[uint8]) },
	},
}

func (c *RunEndContainer[T]) Iterator() Iterator[T] {
	return &RunEndIterator[T]{
		vals: c.Values,
		ends: c.Ends,
		len:  c.Len(),
	}
}

type RunEndIterator[T types.Integer] struct {
	chunk [CHUNK_SIZE]T
	vals  IntegerContainer[T]
	ends  IntegerContainer[uint32]
	ofs   int
	len   int
}

func (it *RunEndIterator[T]) Close() {
	it.vals = nil
	it.ends = nil
	it.ofs = 0
	it.len = 0
}

func (it *RunEndIterator[T]) Reset() {
	it.ofs = 0
}

func (it *RunEndIterator[T]) Len() int {
	return it.len
}

func (it *RunEndIterator[T]) Get(n int) T {
	if it.Seek(n) {
		val, _ := it.Next()
		return val
	}
	return 0
}

func (it *RunEndIterator[T]) Next() (T, bool) {
	if it.ofs >= it.len {
		// EOF
		return 0, false
	}

	// ofs % CHUNK_SIZE
	i := it.ofs & CHUNK_MASK

	// on first call or start of new chunk
	if i == 0 {
		// fmt.Printf("REE next load ofs=%d\n", it.ofs)
		// load next values
		n := it.reload()

		// EOF
		if n == 0 {
			it.ofs = it.len
			return 0, false
		}
	}

	// advance ofs for next call
	it.ofs++

	return it.chunk[i], true
}

func (it *RunEndIterator[T]) reload() int {
	// find the REE pair at current offset
	var k int
	l := it.ends.Len()
	if it.ofs > 0 {
		k = sort.Search(l, func(i int) bool {
			return it.ends.Get(i) >= uint32(it.ofs)
		})
		if k == l {
			return 0
		}
	}

	// process REE pairs until EOF or chunk is full
	var n int
	for n < CHUNK_SIZE && k < l {
		end, val := it.ends.Get(k), it.vals.Get(k)
		for range min(CHUNK_SIZE, int(end+1)-it.ofs) - n {
			// fmt.Printf("REE chunk[%d] = ree(%d) = %d\n", n, k, val)
			it.chunk[n] = val
			n++
		}
		k++
	}

	return n
}

func (it *RunEndIterator[T]) NextChunk() (*[CHUNK_SIZE]T, int) {
	// EOF
	if it.ofs >= it.len {
		return nil, 0
	}
	// fmt.Printf("REE next-chunk load ofs=%d\n", it.ofs)
	n := it.reload()
	it.ofs += n
	return &it.chunk, n
}

func (it *RunEndIterator[T]) SkipChunk() int {
	n := min(CHUNK_SIZE, it.len-it.ofs)
	it.ofs += n
	return n
}

func (it *RunEndIterator[T]) Seek(n int) bool {
	if n < 0 || n >= it.len {
		it.ofs = it.len
		return false
	}

	// calculate chunk start offsets for n and current offset
	nc := chunkStart(n)
	oc := chunkStart(it.ofs)

	// fmt.Printf("REE seek n=%d ofs=%d nc=%d oc=%d\n", n, it.ofs, nc, oc)

	// load when n is in another chunk or seek is first call
	if nc != oc || it.ofs&CHUNK_MASK == 0 {
		// load next chunk when not seeking to start (re-use NextChunk method)
		if n&CHUNK_MASK != 0 {
			// fmt.Printf("> REE: reload chunk from nc=%d ...\n", nc)
			it.ofs = nc
			it.NextChunk()
		}
	}

	// reset ofs to n, so call to Next() delivers value
	it.ofs = n
	return true
}
