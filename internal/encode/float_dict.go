// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"sort"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/hashprobe"
	"blockwatch.cc/knoxdb/internal/types"
)

// TFloatDictionary
type FloatDictionaryContainer[T types.Float] struct {
	Dict  FloatContainer[T]
	Codes IntegerContainer[uint16]
}

func (c *FloatDictionaryContainer[T]) Close() {
	c.Dict.Close()
	c.Codes.Close()
	c.Dict = nil
	c.Codes = nil
	putFloatDictionaryContainer(c)
}

func (c *FloatDictionaryContainer[T]) Type() FloatContainerType {
	return TFloatDictionary
}

func (c *FloatDictionaryContainer[T]) Len() int {
	return c.Codes.Len()
}

func (c *FloatDictionaryContainer[T]) MaxSize() int {
	return 1 + c.Dict.MaxSize() + c.Codes.MaxSize()
}

func (c *FloatDictionaryContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatDictionary))
	dst = c.Dict.Store(dst)
	return c.Codes.Store(dst)
}

func (c *FloatDictionaryContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatDictionary) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	// alloc and decode values child container
	c.Dict = NewFloat[T](FloatContainerType(buf[0]))
	var err error
	buf, err = c.Dict.Load(buf)
	if err != nil {
		return buf, err
	}

	// alloc and decode ends child container
	c.Codes = NewInt[uint16](IntegerContainerType(buf[0]))
	return c.Codes.Load(buf)
}

func (c *FloatDictionaryContainer[T]) Get(n int) T {
	return c.Dict.Get(int(c.Codes.Get(n)))
}

func (c *FloatDictionaryContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	if sel == nil {
		for i := range c.Len() {
			dst = append(dst, c.Get(i))
		}
	} else {
		for _, v := range sel {
			dst = append(dst, c.Get(int(v)))
		}
	}
	return dst
}

func (c *FloatDictionaryContainer[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	// construct dictionary and encode vals
	dict, codes := hashprobe.BuildFloatDict(vals, ctx.NumUnique)

	// encode child containers
	vctx := AnalyzeFloat(dict, false, lvl == MAX_CASCADE)
	c.Dict = EncodeFloat(vctx, dict, lvl-1)
	vctx.Close()
	if c.Dict.Type() != TFloatRaw {
		arena.FreeT(dict)
	}

	cctx := AnalyzeInt(codes, false)
	c.Codes = EncodeInt(cctx, codes, lvl-1)
	cctx.Close()
	if c.Codes.Type() != TIntegerRaw {
		arena.Free(arena.AllocUint16, codes)
	}

	return c
}

func (c *FloatDictionaryContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	// early skip if val is smaller than first or larger than last dict entry
	l := c.Dict.Len()
	if val < c.Dict.Get(0) || val > c.Dict.Get(l-1) {
		return bits
	}

	// find position of val using binary search (dict is sorted and values are unique)
	// TODO: add a `Find(T) int` function to all containers and let them choose the
	// most efficient search strategy
	idx := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= val
	})

	// if not found, equal match does not exist
	if idx == l || c.Dict.Get(idx) != val {
		return bits
	}

	// lookup code at index and run equal search on codes
	return c.Codes.MatchEqual(uint16(idx), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	// early skip if val is smaller than first or larger than last dict entry
	l := c.Dict.Len()
	if val < c.Dict.Get(0) || val > c.Dict.Get(l-1) {
		return bits.One()
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= val
	})

	// if not found, equal match does not exist and we can set all bits one
	if idx == l || c.Dict.Get(idx) != val {
		return bits.One()
	}

	// if found, we run a not equal scan on codes
	return c.Codes.MatchNotEqual(uint16(idx), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	// early skip if val is smaller than first or larger last
	if val < c.Dict.Get(0) {
		return bits
	}
	l := c.Dict.Len()
	if val > c.Dict.Get(l-1) {
		return bits.One()
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= val
	})

	// adjust index for search values > last dict entry
	if idx == l {
		idx--
	}

	// If found we are good. If not found, we have at least found the index of
	// the first value larger than val which is ok too. At this point
	// we know idx is between 0 and l-1, so we can directly translate to a
	// less(code) search.
	return c.Codes.MatchLess(uint16(idx), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	// early skip if val is smaller than first or larger than last
	if val < c.Dict.Get(0) {
		return bits
	}
	l := c.Dict.Len()
	if val >= c.Dict.Get(l-1) {
		return bits.One()
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= val
	})

	// adjust index for search values > last dict entry
	if idx == l {
		idx--
	}

	// If found we are good. If not found, we have at least found the index of
	// the first value larger than val which is ok too. At this point
	// we know idx is between 0 and l-1, so we can directly translate to a
	// less(code) search.
	return c.Codes.MatchLessEqual(uint16(idx), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	// early skip if val is smaller than first or larger or equal to last
	if val < c.Dict.Get(0) {
		return bits.One()
	}
	l := c.Dict.Len()
	if val >= c.Dict.Get(l-1) {
		return bits
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= val
	})

	// If found we are good. If not found, we have at least found the index of
	// the first value larger than val which is ok too. At this point
	// we know idx is between 0 and l-1, so we can directly translate to a
	// less(code) search.
	return c.Codes.MatchGreater(uint16(idx), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	// early skip if val is smaller than first or larger to last
	if val < c.Dict.Get(0) {
		return bits.One()
	}
	l := c.Dict.Len()
	if val > c.Dict.Get(l-1) {
		return bits
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= val
	})

	// If found we are good. If not found, we have at least found the index of
	// the first value larger than val which is ok too. At this point
	// we know idx is between 0 and l-1, so we can directly translate to a
	// less(code) search.
	return c.Codes.MatchGreaterEqual(uint16(idx), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	// skip when range does not intersect with dict or does fully contain dict
	l := c.Dict.Len()
	first, last := c.Dict.Get(0), c.Dict.Get(l-1)
	if b < first || a > last {
		return bits
	}
	if a <= first && b >= last {
		return bits.One()
	}

	// translate range [a,b] into code range [ca, cb]
	ai := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= a
	})
	bi := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= b
	})

	// range is within a dict value gap
	if ai == bi && c.Dict.Get(ai) != a {
		return bits
	}

	// adjust bi when b > last
	if bi == l || c.Dict.Get(bi) != b {
		bi--
	}

	// forward between match on the code vector
	return c.Codes.MatchBetween(uint16(ai), uint16(bi), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchSet(_ any, bits, _ *Bitset) *Bitset {
	// N.A.
	return bits
}

func (c *FloatDictionaryContainer[T]) MatchNotSet(_ any, bits, _ *Bitset) *Bitset {
	// N.A.
	return bits
}

type FloatDictionaryFactory struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
}

func newFloatDictionaryContainer[T types.Float]() FloatContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatDictionaryFactory.f64Pool.Get().(FloatContainer[T])
	case float32:
		return floatDictionaryFactory.f32Pool.Get().(FloatContainer[T])
	default:
		return nil
	}
}

func putFloatDictionaryContainer[T types.Float](c FloatContainer[T]) {
	switch (any(T(0))).(type) {
	case float64:
		floatDictionaryFactory.f64Pool.Put(c)
	case float32:
		floatDictionaryFactory.f32Pool.Put(c)
	}
}

var floatDictionaryFactory = FloatDictionaryFactory{
	f64Pool: sync.Pool{
		New: func() any { return new(FloatDictionaryContainer[float64]) },
	},
	f32Pool: sync.Pool{
		New: func() any { return new(FloatDictionaryContainer[float32]) },
	},
}
