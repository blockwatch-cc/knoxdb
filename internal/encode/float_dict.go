// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"iter"
	"sort"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/hashprobe"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

// ensure we implement required interfaces
var (
	_ types.NumberAccessor[float64] = (*FloatDictionaryContainer[float64])(nil)
	_ NumberContainer[float64]      = (*FloatDictionaryContainer[float64])(nil)
)

// TFloatDictionary
type FloatDictionaryContainer[T types.Float] struct {
	readOnlyContainer[T]
	Dict  NumberContainer[T]
	Codes NumberContainer[uint16]
}

func (c *FloatDictionaryContainer[T]) Info() string {
	return fmt.Sprintf("Dict(%s)_[%s]_[%s]", TypeName[T](), c.Dict.Info(), c.Codes.Info())
}

func (c *FloatDictionaryContainer[T]) Close() {
	c.Dict.Close()
	c.Codes.Close()
	c.Dict = nil
	c.Codes = nil
	putFloatDictionaryContainer(c)
}

func (c *FloatDictionaryContainer[T]) Type() ContainerType {
	return TFloatDictionary
}

func (c *FloatDictionaryContainer[T]) Len() int {
	return c.Codes.Len()
}

func (c *FloatDictionaryContainer[T]) Size() int {
	return 1 + c.Dict.Size() + c.Codes.Size()
}

func (c *FloatDictionaryContainer[T]) Matcher() types.NumberMatcher[T] {
	return c
}

func (c *FloatDictionaryContainer[T]) Chunks() types.NumberIterator[T] {
	return NewFloatDictionaryIterator(c.Dict, c.Codes)
}

func (c *FloatDictionaryContainer[T]) Iterator() iter.Seq2[int, T] {
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
	c.Dict = NewFloat[T](ContainerType(buf[0]))
	var err error
	buf, err = c.Dict.Load(buf)
	if err != nil {
		return buf, err
	}

	// alloc and decode ends child container
	c.Codes = NewInt[uint16](ContainerType(buf[0]))
	return c.Codes.Load(buf)
}

func (c *FloatDictionaryContainer[T]) Get(n int) T {
	return c.Dict.Get(int(c.Codes.Get(n)))
}

func (c *FloatDictionaryContainer[T]) AppendTo(dst []T, sel []uint32) []T {
	it := c.Chunks()
	if sel == nil {
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
}

func (c *FloatDictionaryContainer[T]) Encode(ctx *Context[T], vals []T) NumberContainer[T] {
	// construct dictionary and encode vals
	dict, codes := hashprobe.BuildFloatDict(vals, ctx.NumUnique)

	// encode child containers
	vctx := AnalyzeFloat(dict, false, ctx.Lvl == MAX_LEVEL).WithLevel(ctx.Lvl - 1)
	c.Dict = EncodeFloat(vctx, dict)
	vctx.Close()
	arena.Free(dict)

	cctx := AnalyzeInt(codes, false).WithLevel(ctx.Lvl - 1)
	c.Codes = EncodeInt(cctx, codes)
	cctx.Close()
	arena.Free(codes)

	return c
}

func (c *FloatDictionaryContainer[T]) Cmp(i, j int) int {
	return util.Cmp(c.Get(i), c.Get(j))
}

func (c *FloatDictionaryContainer[T]) MatchEqual(val T, bits, mask *Bitset) {
	// early skip if val is smaller than first or larger than last dict entry
	l := c.Dict.Len()
	if val < c.Dict.Get(0) || val > c.Dict.Get(l-1) {
		return
	}

	// find position of val using binary search (dict is sorted and values are unique)
	// TODO: add a `Find(T) int` function to all containers and let them choose the
	// most efficient search strategy
	idx := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= val
	})

	// if not found, equal match does not exist
	if idx == l || c.Dict.Get(idx) != val {
		return
	}

	// lookup code at index and run equal search on codes
	c.Codes.MatchEqual(uint16(idx), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) {
	// early skip if val is smaller than first or larger than last dict entry
	l := c.Dict.Len()
	if val < c.Dict.Get(0) || val > c.Dict.Get(l-1) {
		bits.One()
		return
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= val
	})

	// if not found, equal match does not exist and we can set all bits one
	if idx == l || c.Dict.Get(idx) != val {
		bits.One()
		return
	}

	// if found, we run a not equal scan on codes
	c.Codes.MatchNotEqual(uint16(idx), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchLess(val T, bits, mask *Bitset) {
	// early skip if val is smaller than first or larger last
	if val < c.Dict.Get(0) {
		return
	}
	l := c.Dict.Len()
	if val > c.Dict.Get(l-1) {
		bits.One()
		return
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
	c.Codes.MatchLess(uint16(idx), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) {
	// early skip if val is smaller than first or larger than last
	if val < c.Dict.Get(0) {
		return
	}
	l := c.Dict.Len()
	if val >= c.Dict.Get(l-1) {
		bits.One()
		return
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
	c.Codes.MatchLessEqual(uint16(idx), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchGreater(val T, bits, mask *Bitset) {
	// early skip if val is smaller than first or larger or equal to last
	if val < c.Dict.Get(0) {
		bits.One()
		return
	}
	l := c.Dict.Len()
	if val >= c.Dict.Get(l-1) {
		return
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= val
	})

	// If found we are good. If not found, we have at least found the index of
	// the first value larger than val which is ok too. At this point
	// we know idx is between 0 and l-1, so we can directly translate to a
	// less(code) search.
	c.Codes.MatchGreater(uint16(idx), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	// early skip if val is smaller than first or larger to last
	if val < c.Dict.Get(0) {
		bits.One()
		return
	}
	l := c.Dict.Len()
	if val > c.Dict.Get(l-1) {
		return
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= val
	})

	// If found we are good. If not found, we have at least found the index of
	// the first value larger than val which is ok too. At this point
	// we know idx is between 0 and l-1, so we can directly translate to a
	// less(code) search.
	c.Codes.MatchGreaterEqual(uint16(idx), bits, mask)
}

func (c *FloatDictionaryContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) {
	// skip when range does not intersect with dict or does fully contain dict
	l := c.Dict.Len()
	first, last := c.Dict.Get(0), c.Dict.Get(l-1)
	if b < first || a > last {
		return
	}
	if a <= first && b >= last {
		bits.One()
		return
	}

	// translate range [a,b] into code range [ca, cb]
	ai := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= a
	})
	bi := sort.Search(l, func(i int) bool {
		return c.Dict.Get(i) >= b
	})

	// range is within a dict value gap
	if v := c.Dict.Get(ai); ai == bi && v != a && v != b {
		return
	}

	// adjust bi when b > last
	if bi == l || c.Dict.Get(bi) != b {
		bi--
	}

	// forward between match on the code vector
	c.Codes.MatchBetween(uint16(ai), uint16(bi), bits, mask)
}

// N.A.
func (c *FloatDictionaryContainer[T]) MatchInSet(_ any, _, _ *Bitset)    {}
func (c *FloatDictionaryContainer[T]) MatchNotInSet(_ any, _, _ *Bitset) {}

type FloatDictionaryFactory struct {
	f64Pool   sync.Pool
	f32Pool   sync.Pool
	f64ItPool sync.Pool
	f32ItPool sync.Pool
}

func newFloatDictionaryContainer[T types.Float]() NumberContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatDictionaryFactory.f64Pool.Get().(NumberContainer[T])
	case float32:
		return floatDictionaryFactory.f32Pool.Get().(NumberContainer[T])
	default:
		return nil
	}
}

func putFloatDictionaryContainer[T types.Float](c NumberContainer[T]) {
	switch (any(T(0))).(type) {
	case float64:
		floatDictionaryFactory.f64Pool.Put(c)
	case float32:
		floatDictionaryFactory.f32Pool.Put(c)
	}
}

func newFloatDictionaryIterator[T types.Float]() *FloatDictionaryIterator[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatDictionaryFactory.f64ItPool.Get().(*FloatDictionaryIterator[T])
	case float32:
		return floatDictionaryFactory.f32ItPool.Get().(*FloatDictionaryIterator[T])
	default:
		return nil
	}
}

func putFloatDictionaryIterator[T types.Float](c *FloatDictionaryIterator[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatDictionaryFactory.f64ItPool.Put(c)
	case float32:
		floatDictionaryFactory.f32ItPool.Put(c)
	}
}

var floatDictionaryFactory = FloatDictionaryFactory{
	f64Pool:   sync.Pool{New: func() any { return new(FloatDictionaryContainer[float64]) }},
	f32Pool:   sync.Pool{New: func() any { return new(FloatDictionaryContainer[float32]) }},
	f64ItPool: sync.Pool{New: func() any { return new(FloatDictionaryIterator[float64]) }},
	f32ItPool: sync.Pool{New: func() any { return new(FloatDictionaryIterator[float32]) }},
}

type FloatDictionaryIterator[T types.Float] struct {
	BaseIterator[T]
	dict []T
	code types.NumberIterator[uint16]
}

func NewFloatDictionaryIterator[T types.Float](dict NumberContainer[T], code NumberContainer[uint16]) *FloatDictionaryIterator[T] {
	it := newFloatDictionaryIterator[T]()
	it.dict = dict.AppendTo(arena.Alloc[T](dict.Len()), nil)
	it.code = code.Chunks()
	it.base = -1
	it.len = it.code.Len()
	it.BaseIterator.fill = it.fill
	return it
}

func (it *FloatDictionaryIterator[T]) Close() {
	arena.Free(it.dict)
	it.dict = nil
	it.code.Close()
	it.code = nil
	it.base = 0
	it.ofs = 0
	it.len = 0
	putFloatDictionaryIterator(it)
}

func (it *FloatDictionaryIterator[T]) fill(base int) int {
	// load code chunk at base and translate
	it.code.Seek(base)
	codes, n := it.code.NextChunk()
	if n == 0 {
		it.ofs = it.len
		it.base = -1
		return 0
	}

	// translate codes
	var i int
	for range n / 16 {
		it.chunk[i] = it.dict[codes[i]]
		it.chunk[i+1] = it.dict[codes[i+1]]
		it.chunk[i+2] = it.dict[codes[i+2]]
		it.chunk[i+3] = it.dict[codes[i+3]]
		it.chunk[i+4] = it.dict[codes[i+4]]
		it.chunk[i+5] = it.dict[codes[i+5]]
		it.chunk[i+6] = it.dict[codes[i+6]]
		it.chunk[i+7] = it.dict[codes[i+7]]
		it.chunk[i+8] = it.dict[codes[i+8]]
		it.chunk[i+9] = it.dict[codes[i+9]]
		it.chunk[i+10] = it.dict[codes[i+10]]
		it.chunk[i+11] = it.dict[codes[i+11]]
		it.chunk[i+12] = it.dict[codes[i+12]]
		it.chunk[i+13] = it.dict[codes[i+13]]
		it.chunk[i+14] = it.dict[codes[i+14]]
		it.chunk[i+15] = it.dict[codes[i+15]]
		i += 16
	}
	for i < n {
		it.chunk[i] = it.dict[codes[i]]
		i++
	}

	it.base = base
	return n
}
