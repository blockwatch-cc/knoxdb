// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"iter"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// ensure we implement required interfaces
var (
	_ types.NumberAccessor[int64] = (*DeltaContainer[int64])(nil)
	_ NumberContainer[int64]      = (*DeltaContainer[int64])(nil)
)

// TIntDelta
type DeltaContainer[T types.Integer] struct {
	readOnlyContainer[T]
	Delta T
	For   T
	N     int
}

func (c *DeltaContainer[T]) Info() string {
	return fmt.Sprintf("Delta(%s)_[n=%d]", TypeName[T](), c.N)
}

func (c *DeltaContainer[T]) Close() {
	putDeltaContainer[T](c)
}

func (c *DeltaContainer[T]) Type() ContainerType {
	return TIntDelta
}

func (c *DeltaContainer[T]) Len() int {
	return c.N
}

func (c *DeltaContainer[T]) Size() int {
	return 1 + num.UvarintLen(c.For) + num.UvarintLen(c.Delta) + num.UvarintLen(c.N)
}

func (c *DeltaContainer[T]) Matcher() types.NumberMatcher[T] {
	return c
}

func (c *DeltaContainer[T]) Chunks() types.NumberIterator[T] {
	return NewDeltaIterator[T](c.Delta, c.For, c.N)
}

func (c *DeltaContainer[T]) Iterator() iter.Seq2[int, T] {
	return func(fn func(int, T) bool) {
		for i := range c.N {
			if !fn(i, c.Get(i)) {
				return
			}
		}
	}
}

func (c *DeltaContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntDelta))
	dst = num.AppendUvarint(dst, uint64(c.For))
	dst = num.AppendUvarint(dst, uint64(c.Delta))
	return num.AppendUvarint(dst, uint64(c.N))
}

func (c *DeltaContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntDelta) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	c.For = T(v)
	buf = buf[n:]
	v, n = num.Uvarint(buf)
	c.Delta = T(v)
	buf = buf[n:]
	v, n = num.Uvarint(buf)
	c.N = int(v)
	return buf[n:], nil
}

func (c *DeltaContainer[T]) Get(n int) T {
	return T(n)*c.Delta + c.For
}

func (c *DeltaContainer[T]) AppendTo(dst []T, sel []uint32) []T {
	if sel == nil {
		dst = dst[:c.N]
		var i int
		for range c.N / 8 {
			dst[i] = T(i)*c.Delta + c.For
			dst[i+1] = T(i+1)*c.Delta + c.For
			dst[i+2] = T(i+2)*c.Delta + c.For
			dst[i+3] = T(i+3)*c.Delta + c.For
			dst[i+4] = T(i+4)*c.Delta + c.For
			dst[i+5] = T(i+5)*c.Delta + c.For
			dst[i+6] = T(i+6)*c.Delta + c.For
			dst[i+7] = T(i+7)*c.Delta + c.For
			i += 8
		}
		for i < c.N {
			dst[i] = T(i)*c.Delta + c.For
			i++
		}
	} else {
		dst = dst[:len(sel)]
		var i int
		for range len(sel) / 8 {
			dst[i] = T(sel[i])*c.Delta + c.For
			dst[i+1] = T(sel[i+1])*c.Delta + c.For
			dst[i+2] = T(sel[i+2])*c.Delta + c.For
			dst[i+3] = T(sel[i+3])*c.Delta + c.For
			dst[i+4] = T(sel[i+4])*c.Delta + c.For
			dst[i+5] = T(sel[i+5])*c.Delta + c.For
			dst[i+6] = T(sel[i+6])*c.Delta + c.For
			dst[i+7] = T(sel[i+7])*c.Delta + c.For
			i += 8
		}
		for i < len(sel) {
			dst[i] = T(sel[i])*c.Delta + c.For
			i++
		}
	}
	return dst
}

func (c *DeltaContainer[T]) Encode(ctx *Context[T], vals []T) NumberContainer[T] {
	c.For = vals[0]
	c.Delta = ctx.Delta
	c.N = len(vals)
	return c
}

func (c *DeltaContainer[T]) Cmp(i, j int) int {
	return util.Cmp(c.Get(i), c.Get(j))
}

func (c *DeltaContainer[T]) MatchEqual(val T, bits, _ *Bitset) {
	// Note: delta = 0 is forbidden
	if c.Delta > 0 {
		if val < c.For {
			return
		}
	} else {
		if val > c.For {
			return
		}
	}

	fmt.Printf("Delta-EQ for=%d delta=%d val=%d v64=%d div=%d mod=%d\n",
		c.For, c.Delta, val, val-c.For, (val-c.For)/c.Delta, (val-c.For)%c.Delta)

	val -= c.For // may wrap

	if val%c.Delta == 0 {
		if n := int(val / c.Delta); n < c.N {
			bits.Set(n)
		}
	}
}

func (c *DeltaContainer[T]) MatchNotEqual(val T, bits, _ *Bitset) {
	// Note: delta = 0 is forbidden
	if c.Delta > 0 {
		if val < c.For {
			bits.One()
			return
		}
	} else {
		if val > c.For {
			bits.One()
			return
		}
	}

	val -= c.For // may wrap

	bits.One()
	if c.Delta == 1 || val%c.Delta == 0 {
		if n := int(val / c.Delta); n < c.N {
			bits.Unset(n)
		}
	}
}

func (c *DeltaContainer[T]) MatchLess(val T, bits, _ *Bitset) {
	// work in int64 space to avoid sign and wrap issues
	v64 := int64(val) - int64(c.For)
	d64 := int64(c.Delta)

	if c.Delta > 0 {
		// positive delta: [for ... for+d*(n-1)]

		// is val smaller than container?
		if val < c.For {
			return
		}

		// is val larger than container?
		if d64*int64(c.N-1) < v64 {
			bits.One()
			return
		}

		// calculate val position
		n := int(v64 / d64)

		// strict less, sub 1 when modulo is zero (val is exact match)
		if v64%d64 == 0 {
			n--
		}

		bits.SetRange(0, n)

	} else {
		// negative delta: [for-d*(n-1) ... for]

		// is val larger than container?
		if val > c.For {
			bits.One()
			return
		}

		// is val smaller than container?
		if d64*int64(c.N-1) >= v64 {
			return
		}

		// calculate val position
		n := int(v64 / d64)

		// strict less: always add 1
		n++

		bits.SetRange(n, c.N-1)
	}
}

func (c *DeltaContainer[T]) MatchLessEqual(val T, bits, _ *Bitset) {
	// work in int64 space to avoid sign and wrap issues
	v64 := int64(val) - int64(c.For)
	d64 := int64(c.Delta)

	if c.Delta > 0 {
		// positive delta: [for ... for+d*(n-1)]

		// is val smaller than container?
		if val < c.For {
			return
		}

		// is val larger than container?
		if d64*int64(c.N-1) < v64 {
			bits.One()
			return
		}

		// calculate val position
		bits.SetRange(0, int(v64/d64))

	} else {
		// negative delta: [for-d*(n-1) ... for]

		// is val larger than container?
		if val >= c.For {
			bits.One()
			return
		}

		// is val smaller than container?
		if d64*int64(c.N-1) > v64 {
			return
		}

		// calculate val position
		n := int(v64 / d64)

		// add 1 when modulo is non-zero
		if v64%d64 != 0 {
			n++
		}

		bits.SetRange(n, c.N-1)
	}
}

func (c *DeltaContainer[T]) MatchGreater(val T, bits, _ *Bitset) {
	// work in int64 space to avoid sign and wrap issues
	v64 := int64(val) - int64(c.For)
	d64 := int64(c.Delta)

	if c.Delta > 0 {
		// positive delta: [for ... for+d*(n-1)]

		// is val smaller than container?
		if val < c.For {
			bits.One()
			return
		}

		// is val larger than container?
		if d64*int64(c.N-1) < v64 {
			return
		}

		// calculate val position
		n := int(v64 / d64)

		// strict greater, always add 1
		n++

		bits.SetRange(n, c.N-1)

	} else {
		// negative delta: [for-d*(n-1) ... for]

		// is val larger than container?
		if val > c.For {
			return
		}

		// is val smaller than container?
		if d64*int64(c.N-1) > v64 {
			bits.One()
			return
		}

		// calculate val position
		n := int(v64 / d64)

		// strict greater, sub 1 when modulo is zero (exact match)
		if v64%d64 == 0 {
			n--
		}

		bits.SetRange(0, n)
	}
}

func (c *DeltaContainer[T]) MatchGreaterEqual(val T, bits, _ *Bitset) {
	// work in int64 space to avoid sign and wrap issues
	v64 := int64(val) - int64(c.For)
	d64 := int64(c.Delta)

	if c.Delta > 0 {
		// positive delta: [for ... for+d*(n-1)]

		// is val smaller than container?
		if val <= c.For {
			bits.One()
			return
		}

		// is val larger than container?
		if d64*int64(c.N-1) < v64 {
			return
		}

		// calculate val position
		n := int(v64 / d64)

		// add 1 when modulo is non-zero
		if v64%d64 > 0 {
			n++
		}

		bits.SetRange(n, c.N-1)

	} else {
		// negative delta: [for-d*(n-1) ... for]

		// is val larger than container?
		if val > c.For {
			return
		}

		// is val smaller than container?
		if d64*int64(c.N-1) > v64 {
			bits.One()
			return
		}

		// calculate val position
		n := int(v64 / d64)

		bits.SetRange(0, n)
	}
}

func (c *DeltaContainer[T]) MatchBetween(a, b T, bits, _ *Bitset) {
	// work in int64 space to avoid sign and wrap issues
	a64 := int64(a) - int64(c.For)
	b64 := int64(b) - int64(c.For)
	d64 := int64(c.Delta)

	// quick checks for outlier cases (no or all matches)
	if c.Delta > 0 {
		// positive delta: [for ... for+d*(n-1)]

		// vals don't overlap container?
		if b < c.For || a64 > d64*int64(c.N-1) {
			return
		}

		// calculate boundary positions
		na := int(a64 / d64)
		nb := int(b64 / d64)

		// adjust a for non-direct match
		if a64%d64 != 0 {
			na++
		}

		// adjust for out of bounds b
		nb = min(nb, c.N-1)

		bits.SetRange(na, nb)

	} else {
		// negative delta: [for-d*(n-1) ... for]

		// vals don't overlap container?
		if a > c.For || b64 < d64*int64(c.N-1) {
			return
		}

		// calculate boundary positions
		na := int(a64 / d64)
		nb := int(b64 / d64)

		// adjust a for non-direct match
		if b64%d64 != 0 {
			nb++
		}

		// adjust for out of bounds b
		nb = min(nb, c.N-1)

		bits.SetRange(nb, na)
	}
}

func (c *DeltaContainer[T]) MatchInSet(s any, bits, mask *Bitset) {
	set := s.(*xroar.Bitmap)

	if mask != nil {
		// only process values from mask
		u32 := arena.Alloc[uint32](mask.Count())
		for _, k := range mask.Indexes(u32) {
			i := int(k)
			if set.Contains(uint64(c.Delta*T(i) + c.For)) {
				bits.Set(i)
			}
		}
		arena.Free(u32)
	} else {
		val := c.For
		for i := range c.N {
			if set.Contains(uint64(val)) {
				bits.Set(i)
			}
			val += c.Delta
		}
	}
}

func (c *DeltaContainer[T]) MatchNotInSet(s any, bits, mask *Bitset) {
	set := s.(*xroar.Bitmap)

	if mask != nil {
		// only process values from mask
		u32 := arena.Alloc[uint32](mask.Count())
		for _, k := range mask.Indexes(u32) {
			i := int(k)
			if !set.Contains(uint64(c.Delta*T(i) + c.For)) {
				bits.Set(i)
			}
		}
		arena.Free(u32)
	} else {
		val := c.For
		for i := range c.N {
			if !set.Contains(uint64(val)) {
				bits.Set(i)
			}
			val += c.Delta
		}
	}
}

type DeltaFactory struct {
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
}

func newDeltaContainer[T types.Integer]() NumberContainer[T] {
	switch any(T(0)).(type) {
	case int64:
		return deltaFactory.i64Pool.Get().(NumberContainer[T])
	case int32:
		return deltaFactory.i32Pool.Get().(NumberContainer[T])
	case int16:
		return deltaFactory.i16Pool.Get().(NumberContainer[T])
	case int8:
		return deltaFactory.i8Pool.Get().(NumberContainer[T])
	case uint64:
		return deltaFactory.u64Pool.Get().(NumberContainer[T])
	case uint32:
		return deltaFactory.u32Pool.Get().(NumberContainer[T])
	case uint16:
		return deltaFactory.u16Pool.Get().(NumberContainer[T])
	case uint8:
		return deltaFactory.u8Pool.Get().(NumberContainer[T])
	default:
		return nil
	}
}

func putDeltaContainer[T types.Integer](c NumberContainer[T]) {
	switch any(T(0)).(type) {
	case int64:
		deltaFactory.i64Pool.Put(c)
	case int32:
		deltaFactory.i32Pool.Put(c)
	case int16:
		deltaFactory.i16Pool.Put(c)
	case int8:
		deltaFactory.i8Pool.Put(c)
	case uint64:
		deltaFactory.u64Pool.Put(c)
	case uint32:
		deltaFactory.u32Pool.Put(c)
	case uint16:
		deltaFactory.u16Pool.Put(c)
	case uint8:
		deltaFactory.u8Pool.Put(c)
	}
}

var deltaFactory = DeltaFactory{
	i64Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[int64]) },
	},
	i32Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[int32]) },
	},
	i16Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[int16]) },
	},
	i8Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[int8]) },
	},
	u64Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[uint64]) },
	},
	u32Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[uint32]) },
	},
	u16Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[uint16]) },
	},
	u8Pool: sync.Pool{
		New: func() any { return new(DeltaContainer[uint8]) },
	},
}
