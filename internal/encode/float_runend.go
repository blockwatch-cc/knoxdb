// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"slices"
	"sort"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
)

// TFloatRunEnd
type FloatRunEndContainer[T types.Float] struct {
	Values FloatContainer[T]        // []T
	Ends   IntegerContainer[uint32] // []uint32
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

func (c *FloatRunEndContainer[T]) MaxSize() int {
	return 1 + c.Values.MaxSize() + c.Ends.MaxSize()
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
	return dst
}

func (c *FloatRunEndContainer[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	// generate run-end encoding from originals
	// values := make([]T, ctx.NumRuns)
	values := arena.AllocT[T](ctx.NumRuns)[:ctx.NumRuns]
	// ends := make([]uint32, ctx.NumRuns)
	ends := arena.Alloc(arena.AllocUint32, ctx.NumRuns).([]uint32)[:ctx.NumRuns]
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
	// fmt.Println("Run Values ..")
	vctx := AnalyzeFloat(values, false)
	c.Values = EncodeFloat(vctx, values, lvl-1)
	vctx.Close()
	if c.Values.Type() != TFloatRaw {
		arena.FreeT(values)
	}
	// fmt.Println("Run Ends ..")
	ectx := AnalyzeInt(ends, false)
	c.Ends = EncodeInt(ectx, ends, lvl-1)
	ectx.Close()
	if c.Ends.Type() != TIntegerRaw {
		arena.Free(arena.AllocUint32, ends)
	}
	// fmt.Println("Run done.")
	return c
}

func (c *FloatRunEndContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatRunEndContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatRunEndContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatRunEndContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatRunEndContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatRunEndContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatRunEndContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatRunEndContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *FloatRunEndContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

type FloatRunEndFactory struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
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

var floatRunEndFactory = FloatRunEndFactory{
	f64Pool: sync.Pool{
		New: func() any { return new(FloatRunEndContainer[float64]) },
	},
	f32Pool: sync.Pool{
		New: func() any { return new(FloatRunEndContainer[float32]) },
	},
}
