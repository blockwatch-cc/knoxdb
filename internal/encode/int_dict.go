// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/hashprobe"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
)

// TIntegerDictionary
type DictionaryContainer[T types.Integer] struct {
	Dict  IntegerContainer[T]
	Codes IntegerContainer[uint16]
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

func (c *DictionaryContainer[T]) Type() IntegerContainerType {
	return TIntegerDictionary
}

func (c *DictionaryContainer[T]) Len() int {
	return c.Codes.Len()
}

func (c *DictionaryContainer[T]) MaxSize() int {
	return 1 + c.Dict.MaxSize() + c.Codes.MaxSize()
}

func (c *DictionaryContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntegerDictionary))
	dst = c.Dict.Store(dst)
	return c.Codes.Store(dst)
}

func (c *DictionaryContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntegerDictionary) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	// alloc and decode values child container
	c.Dict = NewInt[T](IntegerContainerType(buf[0]))
	var err error
	buf, err = c.Dict.Load(buf)
	if err != nil {
		return buf, err
	}

	// alloc and decode ends child container
	c.Codes = NewInt[uint16](IntegerContainerType(buf[0]))
	return c.Codes.Load(buf)
}

func (c *DictionaryContainer[T]) Get(n int) T {
	return c.Dict.Get(int(c.Codes.Get(n)))
}

func (c *DictionaryContainer[T]) AppendTo(sel []uint32, dst []T) []T {
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

func (c *DictionaryContainer[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	// construct dictionary and encode vals
	var (
		dict  []T
		codes []uint16
	)
	if len(ctx.UniqueArray) > 0 {
		dict, codes = dictEncodeArray(ctx, vals)
	} else {
		dict, codes = hashprobe.BuildDict(vals, ctx.NumUnique)
	}

	// encode child containers
	vctx := AnalyzeInt(dict, false)
	c.Dict = EncodeInt(vctx, dict, lvl-1)
	vctx.Close()
	if c.Dict.Type() != TIntegerRaw {
		arena.FreeT(dict)
	}

	cctx := AnalyzeInt(codes, false)
	c.Codes = EncodeInt(cctx, codes, lvl-1)
	cctx.Close()
	if c.Codes.Type() != TIntegerRaw {
		arena.FreeT(codes)
	}

	// fmt.Printf("Dict keys(%s)=%d/%d codes(%s)=%d/%d\n",
	// 	c.Dict.Type(), len(dict), c.Dict.Len(),
	// 	c.Codes.Type(), len(codes), c.Codes.Len(),
	// )

	return c
}

func dictEncodeArray[T types.Integer](ctx *IntegerContext[T], vals []T) ([]T, []uint16) {
	// construct sorted dict from code+1 array
	dict := arena.AllocT[T](ctx.NumUnique)[:0]
	for i, v := range ctx.UniqueArray {
		if v > 0 {
			// reverse min-FOR applied by buildUniqueArray()
			// dict itself does not use min-For, but dict child
			// container may be
			dict = append(dict, T(i)+ctx.Min)
		}
	}

	codes := arena.Alloc(arena.AllocUint16, len(vals)).([]uint16)[:len(vals)]
	for i, v := range vals {
		// apply min-FOR to value for compatibility with buildUniqueArray()
		codes[i] = uint16(ctx.UniqueArray[int64(v)-int64(ctx.Min)] - 1)
	}

	return dict, codes
}

func (c *DictionaryContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
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

func (c *DictionaryContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
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

func (c *DictionaryContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
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

func (c *DictionaryContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
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

func (c *DictionaryContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
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

func (c *DictionaryContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
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

func (c *DictionaryContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
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

func (c *DictionaryContainer[T]) String() string {
	var b strings.Builder
	b.WriteString("Dict (")
	b.WriteString(c.Dict.Type().String())
	b.WriteString("/")
	b.WriteString(c.Codes.Type().String())
	b.WriteString(fmt.Sprintf(") [%d, %d] [", c.Dict.Len(), c.Codes.Len()))
	for i := range c.Dict.Len() {
		b.WriteString(strconv.FormatInt(int64(c.Dict.Get(i)), 16))
		if i < c.Dict.Len()-1 {
			b.WriteRune(',')
		}
	}
	b.WriteString("] Codes: ")
	b.WriteString(fmt.Sprintf("%#v", c.Codes))

	return b.String()
}

func (c *DictionaryContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// Translate set members to codes, ignore when not in dict. If none
	// or only a single set value matches we can optimize.
	cset, code, hasCode := c.translateSet(s)

	// no match at all, return empty match
	if !hasCode {
		return bits
	}

	// single match, translate to equal match
	if cset == nil {
		return c.Codes.MatchEqual(code, bits, mask)
	}

	// code set match
	return c.Codes.MatchSet(cset, bits, mask)
}

func (c *DictionaryContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// Translate set members to codes, ignore when not in dict. If none
	// or only a single set value matches we can optimize.
	cset, code, hasCode := c.translateSet(s)

	// no set value matches, set all bits one
	if !hasCode {
		return bits.One()
	}

	// single match, translate to not equal match
	if cset == nil {
		return c.Codes.MatchNotEqual(code, bits, mask)
	}

	// code set match
	return c.Codes.MatchNotSet(cset, bits, mask)
}

func (c *DictionaryContainer[T]) translateSet(s any) (any, uint16, bool) {
	// Translate values from set to dictionary codes. This strategy only
	// works because the dict is sorted, hence we can efficiently use
	// binary search on each set value and quickly exclude out of bounds
	// values. The result can be no, one or multiple dict matches.
	// In an attempt to be efficient we only allocate a new bitset if
	// we have found more than one set value in the dict.
	var (
		l            int = c.Dict.Len()
		first, last  T   = c.Dict.Get(0), c.Dict.Get(l - 1)
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
		switch {
		case val == first:
			idx = 0
		case val == last:
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
			cset = xroar.NewBitmap()
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
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
}

func newDictionaryContainer[T types.Integer]() IntegerContainer[T] {
	switch any(T(0)).(type) {
	case int64:
		return dictionaryFactory.i64Pool.Get().(IntegerContainer[T])
	case int32:
		return dictionaryFactory.i32Pool.Get().(IntegerContainer[T])
	case int16:
		return dictionaryFactory.i16Pool.Get().(IntegerContainer[T])
	case int8:
		return dictionaryFactory.i8Pool.Get().(IntegerContainer[T])
	case uint64:
		return dictionaryFactory.u64Pool.Get().(IntegerContainer[T])
	case uint32:
		return dictionaryFactory.u32Pool.Get().(IntegerContainer[T])
	case uint16:
		return dictionaryFactory.u16Pool.Get().(IntegerContainer[T])
	case uint8:
		return dictionaryFactory.u8Pool.Get().(IntegerContainer[T])
	default:
		return nil
	}
}

func putDictionaryContainer[T types.Integer](c IntegerContainer[T]) {
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

var dictionaryFactory = DictionaryFactory{
	i64Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[int64]) },
	},
	i32Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[int32]) },
	},
	i16Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[int16]) },
	},
	i8Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[int8]) },
	},
	u64Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[uint64]) },
	},
	u32Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[uint32]) },
	},
	u16Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[uint16]) },
	},
	u8Pool: sync.Pool{
		New: func() any { return new(DictionaryContainer[uint8]) },
	},
}
