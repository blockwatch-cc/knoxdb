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
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/util"
)

// ensure we implement required interfaces
var (
	_ types.NumberAccessor[int64] = (*DictionaryContainer[int64])(nil)
	_ NumberContainer[int64]      = (*DictionaryContainer[int64])(nil)
)

// TIntDictionary
type DictionaryContainer[T types.Integer] struct {
	readOnlyContainer[T]
	Dict  NumberContainer[T]
	Codes NumberContainer[uint16]
}

func (c *DictionaryContainer[T]) Info() string {
	return fmt.Sprintf("Dict(%s)_[%s]_[%s]", TypeName[T](), c.Dict.Info(), c.Codes.Info())
}

func (c *DictionaryContainer[T]) Close() {
	c.Dict.Close()
	c.Codes.Close()
	c.Dict = nil
	c.Codes = nil
	putDictionaryContainer[T](c)
}

func (c *DictionaryContainer[T]) Type() ContainerType {
	return TIntDictionary
}

func (c *DictionaryContainer[T]) Len() int {
	return c.Codes.Len()
}

func (c *DictionaryContainer[T]) Size() int {
	return 1 + c.Dict.Size() + c.Codes.Size()
}

func (c *DictionaryContainer[T]) Matcher() types.NumberMatcher[T] {
	return c
}

func (c *DictionaryContainer[T]) Chunks() types.NumberIterator[T] {
	return NewDictionaryIterator(c.Dict, c.Codes)
}

func (c *DictionaryContainer[T]) Iterator() iter.Seq2[int, T] {
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

func (c *DictionaryContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntDictionary))
	dst = c.Dict.Store(dst)
	return c.Codes.Store(dst)
}

func (c *DictionaryContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntDictionary) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	// alloc and decode dict child container
	c.Dict = NewInt[T](ContainerType(buf[0]))
	var err error
	buf, err = c.Dict.Load(buf)
	if err != nil {
		return buf, err
	}

	// alloc and decode codes child container
	c.Codes = NewInt[uint16](ContainerType(buf[0]))
	return c.Codes.Load(buf)
}

func (c *DictionaryContainer[T]) Get(n int) T {
	return c.Dict.Get(int(c.Codes.Get(n)))
}

func (c *DictionaryContainer[T]) AppendTo(dst []T, sel []uint32) []T {
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

func (c *DictionaryContainer[T]) Encode(ctx *Context[T], vals []T) NumberContainer[T] {
	// construct dictionary and encode vals
	var (
		dict  []T
		codes []uint16
	)
	if len(ctx.UniqueArray) > 0 || util.SizeOf[T]() <= 2 {
		dict, codes = dictEncodeArray(ctx, vals)
	} else {
		dict, codes = hashprobe.BuildDict(vals, ctx.NumUnique)
	}

	// encode child containers
	vctx := AnalyzeInt(dict, false).WithLevel(ctx.Lvl - 1)
	c.Dict = EncodeInt(vctx, dict)
	vctx.Close()
	arena.Free(dict)

	cctx := AnalyzeInt(codes, false).WithLevel(ctx.Lvl - 1)
	c.Codes = EncodeInt(cctx, codes)
	cctx.Close()
	arena.Free(codes)

	return c
}

func dictEncodeArray[T types.Integer](ctx *Context[T], vals []T) ([]T, []uint16) {
	// cross-check we have the unique array initialized
	if len(ctx.UniqueArray) == 0 {
		ctx.NumUnique = ctx.buildUniqueArray(vals)
	}

	// construct sorted dict from code+1 array
	dict := arena.Alloc[T](ctx.NumUnique)[:0]
	for i, v := range ctx.UniqueArray {
		if v > 0 {
			// reverse min-FOR applied by buildUniqueArray()
			// dict itself does not use min-For, but dict child
			// container may be
			dict = append(dict, T(i)+ctx.Min)
		}
	}

	codes := arena.AllocUint16(len(vals))[:len(vals)]
	for i, v := range vals {
		// apply min-FOR to value for compatibility with buildUniqueArray()
		// subtract -1 from code (buildUniqueArray had added +1)
		codes[i] = uint16(ctx.UniqueArray[int64(v)-int64(ctx.Min)] - 1)
	}

	return dict, codes
}

func (c *DictionaryContainer[T]) Cmp(i, j int) int {
	return util.Cmp(c.Get(i), c.Get(j))
}

func (c *DictionaryContainer[T]) MatchEqual(val T, bits, mask *Bitset) {
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

func (c *DictionaryContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) {
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

func (c *DictionaryContainer[T]) MatchLess(val T, bits, mask *Bitset) {
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

func (c *DictionaryContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) {
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

	// adjust when we reached the end or no exact match was found
	if idx == l || val < c.Dict.Get(idx) {
		idx--
	}

	// If found we are good. If not found, we have at least found the index of
	// the first value larger than val which is ok too. At this point
	// we know idx is between 0 and l-1, so we can directly translate to a
	// less(code) search.
	c.Codes.MatchLessEqual(uint16(idx), bits, mask)
}

func (c *DictionaryContainer[T]) MatchGreater(val T, bits, mask *Bitset) {
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
		return c.Dict.Get(i) > val
	})

	// Since we are searching for strictly greater dict entries we found the
	// next higher code (or end of dict). Use GE for code match.
	c.Codes.MatchGreaterEqual(uint16(idx), bits, mask)
}

func (c *DictionaryContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
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

func (c *DictionaryContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) {
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

func (c *DictionaryContainer[T]) MatchInSet(s any, bits, mask *Bitset) {
	// Translate set members to codes, ignore when not in dict. If none
	// or only a single set value matches we can optimize.
	cset, code, hasCode := c.translateSet(s)

	// no match at all, return empty match
	if !hasCode {
		return
	}

	// single match, translate to equal match
	if cset == nil {
		c.Codes.MatchEqual(code, bits, mask)
	} else {
		// code set match
		c.Codes.MatchInSet(cset, bits, mask)
	}
}

func (c *DictionaryContainer[T]) MatchNotInSet(s any, bits, mask *Bitset) {
	// Translate set members to codes, ignore when not in dict. If none
	// or only a single set value matches we can optimize.
	cset, code, hasCode := c.translateSet(s)

	// no set value matches, set all bits one
	if !hasCode {
		bits.One()
		return
	}

	// single match, translate to not equal match
	if cset == nil {
		c.Codes.MatchNotEqual(code, bits, mask)
	} else {
		// code set match
		c.Codes.MatchNotInSet(cset, bits, mask)
	}
}

func (c *DictionaryContainer[T]) translateSet(s any) (any, uint16, bool) {
	// Translate values from set to dictionary codes. This strategy only
	// works because the dict is sorted, hence we can efficiently use
	// binary search on each set value and quickly exclude out of bounds
	// values. The result can be no, one or multiple dict matches.
	// In an attempt to be efficient we only allocate a new bitset if
	// we have found more than one set value in the dict.
	var (
		l            = c.Dict.Len()
		first, last  = c.Dict.Get(0), c.Dict.Get(l - 1)
		firstCode    uint16
		hasFirstCode bool
		cset         *xroar.Bitmap
		it           = s.(*xroar.Bitmap).NewIterator()
	)

next:
	for {
		// process the next value from the set
		v, ok := it.Next()
		if !ok {
			break
		}
		val := T(v)

		// skip values when not in dict
		if val < first || val > last {
			continue
		}

		// find position of val using binary search (dict is sorted and values are unique)
		var idx int
		switch val {
		case first:
			idx = 0
		case last:
			idx = l - 1
		default:
			idx = sort.Search(l, func(i int) bool {
				return c.Dict.Get(i) >= val
			})
			// not found
			if c.Dict.Get(idx) != val {
				continue next
			}
		}

		// handle first code match
		if !hasFirstCode {
			hasFirstCode = true
			firstCode = uint16(idx)
			continue
		}

		// handle following matches (if any), on second match, create bitset and
		// insert first code match
		if cset == nil {
			cset = xroar.New()
			cset.Set(uint64(firstCode))
		}

		// insert codes into bitset
		cset.Set(uint64(idx))
	}

	// must return nil interface for nil checks to work
	if cset == nil {
		return nil, firstCode, hasFirstCode
	}

	return cset, firstCode, hasFirstCode
}

type DictionaryFactory struct {
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

func newDictionaryContainer[T types.Integer]() NumberContainer[T] {
	switch any(T(0)).(type) {
	case int64:
		return dictionaryFactory.i64Pool.Get().(NumberContainer[T])
	case int32:
		return dictionaryFactory.i32Pool.Get().(NumberContainer[T])
	case int16:
		return dictionaryFactory.i16Pool.Get().(NumberContainer[T])
	case int8:
		return dictionaryFactory.i8Pool.Get().(NumberContainer[T])
	case uint64:
		return dictionaryFactory.u64Pool.Get().(NumberContainer[T])
	case uint32:
		return dictionaryFactory.u32Pool.Get().(NumberContainer[T])
	case uint16:
		return dictionaryFactory.u16Pool.Get().(NumberContainer[T])
	case uint8:
		return dictionaryFactory.u8Pool.Get().(NumberContainer[T])
	default:
		return nil
	}
}

func putDictionaryContainer[T types.Integer](c NumberContainer[T]) {
	switch (any(T(0))).(type) {
	case int64:
		dictionaryFactory.i64Pool.Put(c)
	case int32:
		dictionaryFactory.i32Pool.Put(c)
	case int16:
		dictionaryFactory.i16Pool.Put(c)
	case int8:
		dictionaryFactory.i8Pool.Put(c)
	case uint64:
		dictionaryFactory.u64Pool.Put(c)
	case uint32:
		dictionaryFactory.u32Pool.Put(c)
	case uint16:
		dictionaryFactory.u16Pool.Put(c)
	case uint8:
		dictionaryFactory.u8Pool.Put(c)
	}
}

func newDictionaryIterator[T types.Integer]() *DictionaryIterator[T] {
	switch any(T(0)).(type) {
	case int64:
		return dictionaryFactory.i64ItPool.Get().(*DictionaryIterator[T])
	case int32:
		return dictionaryFactory.i32ItPool.Get().(*DictionaryIterator[T])
	case int16:
		return dictionaryFactory.i16ItPool.Get().(*DictionaryIterator[T])
	case int8:
		return dictionaryFactory.i8ItPool.Get().(*DictionaryIterator[T])
	case uint64:
		return dictionaryFactory.u64ItPool.Get().(*DictionaryIterator[T])
	case uint32:
		return dictionaryFactory.u32ItPool.Get().(*DictionaryIterator[T])
	case uint16:
		return dictionaryFactory.u16ItPool.Get().(*DictionaryIterator[T])
	case uint8:
		return dictionaryFactory.u8ItPool.Get().(*DictionaryIterator[T])
	default:
		return nil
	}
}

func putDictionaryIterator[T types.Integer](c *DictionaryIterator[T]) {
	switch any(T(0)).(type) {
	case int64:
		dictionaryFactory.i64ItPool.Put(c)
	case int32:
		dictionaryFactory.i32ItPool.Put(c)
	case int16:
		dictionaryFactory.i16ItPool.Put(c)
	case int8:
		dictionaryFactory.i8ItPool.Put(c)
	case uint64:
		dictionaryFactory.u64ItPool.Put(c)
	case uint32:
		dictionaryFactory.u32ItPool.Put(c)
	case uint16:
		dictionaryFactory.u16ItPool.Put(c)
	case uint8:
		dictionaryFactory.u8ItPool.Put(c)
	}
}

var dictionaryFactory = DictionaryFactory{
	i64Pool:   sync.Pool{New: func() any { return new(DictionaryContainer[int64]) }},
	i32Pool:   sync.Pool{New: func() any { return new(DictionaryContainer[int32]) }},
	i16Pool:   sync.Pool{New: func() any { return new(DictionaryContainer[int16]) }},
	i8Pool:    sync.Pool{New: func() any { return new(DictionaryContainer[int8]) }},
	u64Pool:   sync.Pool{New: func() any { return new(DictionaryContainer[uint64]) }},
	u32Pool:   sync.Pool{New: func() any { return new(DictionaryContainer[uint32]) }},
	u16Pool:   sync.Pool{New: func() any { return new(DictionaryContainer[uint16]) }},
	u8Pool:    sync.Pool{New: func() any { return new(DictionaryContainer[uint8]) }},
	i64ItPool: sync.Pool{New: func() any { return new(DictionaryIterator[int64]) }},
	i32ItPool: sync.Pool{New: func() any { return new(DictionaryIterator[int32]) }},
	i16ItPool: sync.Pool{New: func() any { return new(DictionaryIterator[int16]) }},
	i8ItPool:  sync.Pool{New: func() any { return new(DictionaryIterator[int8]) }},
	u64ItPool: sync.Pool{New: func() any { return new(DictionaryIterator[uint64]) }},
	u32ItPool: sync.Pool{New: func() any { return new(DictionaryIterator[uint32]) }},
	u16ItPool: sync.Pool{New: func() any { return new(DictionaryIterator[uint16]) }},
	u8ItPool:  sync.Pool{New: func() any { return new(DictionaryIterator[uint8]) }},
}

type DictionaryIterator[T types.Integer] struct {
	BaseIterator[T]
	dict []T
	code types.NumberIterator[uint16]
}

func NewDictionaryIterator[T types.Integer](dict NumberContainer[T], code NumberContainer[uint16]) *DictionaryIterator[T] {
	it := newDictionaryIterator[T]()
	it.dict = dict.AppendTo(arena.Alloc[T](dict.Len()), nil)
	it.code = code.Chunks()
	it.base = -1
	it.len = it.code.Len()
	it.BaseIterator.fill = it.fill
	return it
}

func (it *DictionaryIterator[T]) Close() {
	arena.Free(it.dict)
	it.dict = nil
	it.code.Close()
	it.code = nil
	it.BaseIterator.Close()
	putDictionaryIterator(it)
}

func (it *DictionaryIterator[T]) fill(base int) int {
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
