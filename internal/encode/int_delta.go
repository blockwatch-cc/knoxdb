// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
)

// TIntegerDelta
type DeltaContainer[T types.Integer] struct {
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

func (c *DeltaContainer[T]) Type() IntegerContainerType {
	return TIntegerDelta
}

func (c *DeltaContainer[T]) Len() int {
	return c.N
}

func (c *DeltaContainer[T]) MaxSize() int {
	return 1 + num.UvarintLen(c.For) + num.UvarintLen(c.Delta) + num.UvarintLen(c.N)
}

func (c *DeltaContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntegerDelta))
	dst = num.AppendUvarint(dst, uint64(c.For))
	dst = num.AppendUvarint(dst, uint64(c.Delta))
	return num.AppendUvarint(dst, uint64(c.N))
}

func (c *DeltaContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntegerDelta) {
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
	return c.Delta*T(n) + c.For
}

func (c *DeltaContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	if sel == nil {
		val := c.For
		for range c.Len() {
			dst = append(dst, val)
			val += c.Delta
		}
	} else {
		for _, v := range sel {
			dst = append(dst, c.Delta*T(v)+c.For)
		}
	}
	return dst
}

func (c *DeltaContainer[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	c.For = vals[0]
	c.Delta = ctx.Delta
	c.N = len(vals)
	return c
}

func (c *DeltaContainer[T]) MatchEqual(val T, bits, _ *Bitset) *Bitset {
	if val < c.For {
		return bits
	}
	if c.Delta == 0 {
		if val == c.For {
			return bits.One()
		}
		return bits
	}

	val -= c.For

	if val%c.Delta == 0 {
		if n := int(val / c.Delta); n < c.N {
			bits.Set(n)
		}
	}

	return bits
}

func (c *DeltaContainer[T]) MatchNotEqual(val T, bits, _ *Bitset) *Bitset {
	if val < c.For {
		return bits.One()
	}
	if c.Delta == 0 {
		if val == c.For {
			return bits
		}
		return bits.One()
	}
	val -= c.For

	bits.One()
	if c.Delta == 1 || val%c.Delta == 0 {
		if n := int(val / c.Delta); n < c.N {
			bits.Clear(n)
		}
	}

	return bits
}

func (c *DeltaContainer[T]) MatchLess(val T, bits, _ *Bitset) *Bitset {
	if val < c.For {
		return bits
	}
	if c.Delta == 0 {
		if val > c.For {
			return bits.One()
		}
		return bits
	}
	val -= c.For

	// is val larger than container?
	if c.Delta*T(c.N-1) < val {
		return bits.One()
	}

	// calculate val position
	n := int(val / c.Delta)

	// strict less, sub 1 when val is in match set
	if c.Delta == 1 || val%c.Delta == 0 {
		n--
	}

	if n == 0 {
		bits.Set(n)
	} else {
		bits.SetRange(0, n)
	}

	return bits
}

func (c *DeltaContainer[T]) MatchLessEqual(val T, bits, _ *Bitset) *Bitset {
	if val < c.For {
		return bits
	}
	if c.Delta == 0 {
		if val >= c.For {
			return bits.One()
		}
		return bits
	}
	val -= c.For

	// is val larger than container?
	if c.Delta*T(c.N-1) < val {
		return bits.One()
	}

	// set all bits below or equal to val's position
	n := int(val / c.Delta)
	if n == 0 {
		bits.Set(n)
	} else {
		bits.SetRange(0, n)
	}

	return bits
}

func (c *DeltaContainer[T]) MatchGreater(val T, bits, _ *Bitset) *Bitset {
	if val < c.For {
		return bits.One()
	}
	if c.Delta == 0 {
		if val < c.For {
			return bits.One()
		}
		return bits
	}
	val -= c.For

	// is val larger than container?
	if c.Delta*T(c.N-1) < val {
		return bits
	}

	// calculate val position
	n := int(val / c.Delta)

	// strict greater, add 1 when val is in match set
	if c.Delta == 1 || val%c.Delta == 0 {
		n++
	}

	// set bits range
	if n == c.N-1 {
		bits.Set(n)
	} else {
		bits.SetRange(n, c.N-1)
	}

	return bits
}

func (c *DeltaContainer[T]) MatchGreaterEqual(val T, bits, _ *Bitset) *Bitset {
	if val < c.For {
		return bits.One()
	}
	if c.Delta == 0 {
		if val <= c.For {
			return bits.One()
		}
		return bits
	}
	val -= c.For

	// is val larger than container?
	if c.Delta*T(c.N-1) < val {
		return bits
	}

	// calculate val position
	n := int(val / c.Delta)

	// set bits range
	switch {
	case n == 0:
		bits.One()
	case n == c.N-1:
		bits.Set(n)
	default:
		bits.SetRange(n, c.N-1)
	}

	return bits
}

func (c *DeltaContainer[T]) MatchBetween(a, b T, bits, _ *Bitset) *Bitset {
	// quick checks for outlier cases (no or all matches)
	if b < c.For {
		return bits
	}
	if a <= c.For && b >= c.Delta*T(c.N-1)+c.For {
		return bits.One()
	}
	if c.Delta == 0 {
		return bits
	}

	// adjust for out of bounds a
	// ensure overflow free calculations
	if a <= c.For {
		a = 0
	} else {
		a = T(uint64(a - c.For))
	}
	b = T(uint64(b - c.For))

	// calculate boundary positions
	na := int(a / c.Delta)
	nb := int(b / c.Delta)

	// adjust a for non-direct match
	if a%c.Delta != 0 {
		na++
	}

	// adjust for out of bounds b
	nb = min(nb, c.N-1)

	if na == nb {
		bits.Set(na)
	} else {
		bits.SetRange(na, nb)
	}

	return bits
}

func (c *DeltaContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
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

	return bits
}

func (c *DeltaContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
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

	return bits
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

func newDeltaContainer[T types.Integer]() IntegerContainer[T] {
	switch any(T(0)).(type) {
	case int64:
		return deltaFactory.i64Pool.Get().(IntegerContainer[T])
	case int32:
		return deltaFactory.i32Pool.Get().(IntegerContainer[T])
	case int16:
		return deltaFactory.i16Pool.Get().(IntegerContainer[T])
	case int8:
		return deltaFactory.i8Pool.Get().(IntegerContainer[T])
	case uint64:
		return deltaFactory.u64Pool.Get().(IntegerContainer[T])
	case uint32:
		return deltaFactory.u32Pool.Get().(IntegerContainer[T])
	case uint16:
		return deltaFactory.u16Pool.Get().(IntegerContainer[T])
	case uint8:
		return deltaFactory.u8Pool.Get().(IntegerContainer[T])
	default:
		return nil
	}
}

func putDeltaContainer[T types.Integer](c IntegerContainer[T]) {
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
