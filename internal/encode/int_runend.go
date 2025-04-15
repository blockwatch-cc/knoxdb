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
}

func (c *RunEndContainer[T]) Info() string {
	return fmt.Sprintf("REE(%s)_[%s]_[%s]", TypeName[T](), c.Values.Info(), c.Ends.Info())
}

func (c *RunEndContainer[T]) Close() {
	c.Values.Close()
	c.Ends.Close()
	c.Values = nil
	c.Ends = nil
	putRunEndContainer[T](c)
}

func (c *RunEndContainer[T]) Type() IntegerContainerType {
	return TIntegerRunEnd
}

func (c *RunEndContainer[T]) Len() int {
	l := c.Ends.Len()
	if l == 0 {
		return 0
	}
	return int(c.Ends.Get(l-1)) + 1
}

func (c *RunEndContainer[T]) MaxSize() int {
	return 1 + c.Values.MaxSize() + c.Ends.MaxSize()
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
	return c.Ends.Load(buf)
}

func (c *RunEndContainer[T]) Get(n int) T {
	idx := sort.Search(c.Ends.Len(), func(i int) bool {
		return c.Ends.Get(i) >= uint32(n)
	})
	return c.Values.Get(idx)
}

func (c *RunEndContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	if sel == nil {
		l := uint32(c.Len())
		var i uint32
		var k int
		for i < l {
			end, val := c.Ends.Get(k), c.Values.Get(k)
			for range end - i {
				dst = append(dst, val)
			}
			i = end
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

	// encode child containers
	vctx := AnalyzeInt(values, true)
	c.Values = EncodeInt(vctx, values, lvl-1)
	vctx.Close()
	if c.Values.Type() != TIntegerRaw {
		arena.Free(values)
	}

	ectx := AnalyzeInt(ends, false)
	c.Ends = EncodeInt(ectx, ends, lvl-1)
	ectx.Close()
	if c.Ends.Type() != TIntegerRaw {
		arena.Free(ends)
	}

	return c
}

func (c *RunEndContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	// match values container and translate matches
	vbits := c.Values.MatchEqual(val, bitset.NewBitset(c.Values.Len()), mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
	return bits
}

func (c *RunEndContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	// match values container and translate matches
	vbits := c.Values.MatchNotEqual(val, bitset.NewBitset(c.Values.Len()), mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
	return bits
}

func (c *RunEndContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	// match values container and translate matches
	vbits := c.Values.MatchLess(val, bitset.NewBitset(c.Values.Len()), mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
	return bits
}

func (c *RunEndContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	// match values container and translate matches
	vbits := c.Values.MatchLessEqual(val, bitset.NewBitset(c.Values.Len()), mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
	return bits
}

func (c *RunEndContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	// match values container and translate matches
	vbits := c.Values.MatchGreater(val, bitset.NewBitset(c.Values.Len()), mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
	return bits
}

func (c *RunEndContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	// match values container and translate matches
	vbits := c.Values.MatchGreaterEqual(val, bitset.NewBitset(c.Values.Len()), mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
	return bits
}

func (c *RunEndContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	// match values container and translate matches
	vbits := c.Values.MatchBetween(a, b, bitset.NewBitset(c.Values.Len()), mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
	return bits
}

func (c *RunEndContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// match values container and translate matches
	vbits := c.Values.MatchSet(s, bitset.NewBitset(c.Values.Len()), mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
	return bits
}

func (c *RunEndContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// match values container and translate matches
	vbits := c.Values.MatchNotSet(s, bitset.NewBitset(c.Values.Len()), mask)
	c.applyMatch(bits, vbits)
	vbits.Close()
	return bits
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
