// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"sort"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
)

// ensure we implement required interfaces
var (
	_ types.StringAccessor = (*DictStringContainer)(nil)
	_ StringContainer      = (*DictStringContainer)(nil)
)

type DictStringContainer struct {
	readOnlyContainer[[]byte]
	dict []byte
	ofs  NumberContainer[uint32]
	len  NumberContainer[uint32]
	code NumberContainer[uint32]
	n    int
	free bool
}

func (c *DictStringContainer) Info() string {
	return fmt.Sprintf("Dict(string)_[ofs=%s]_[len=%s]_[code=%s]",
		c.ofs.Info(), c.len.Info(), c.code.Info())
}

func (c *DictStringContainer) Close() {
	if c.ofs != nil {
		c.ofs.Close()
	}
	if c.len != nil {
		c.len.Close()
	}
	if c.code != nil {
		c.code.Close()
	}
	c.ofs = nil
	c.len = nil
	c.code = nil
	if c.free {
		arena.Free(c.dict)
		c.free = false
	}
	c.dict = nil
	c.n = 0
	putStringContainer(c)
}

func (c *DictStringContainer) Type() ContainerType {
	return TStringDictionary
}

func (c *DictStringContainer) Len() int {
	return c.n
}

func (c *DictStringContainer) Size() int {
	return 1 + c.ofs.Size() + c.len.Size() + c.code.Size() +
		UvarintLen(len(c.dict)) + len(c.dict)
}

func (c *DictStringContainer) Matcher() types.StringMatcher {
	return c
}

func (c *DictStringContainer) Store(dst []byte) []byte {
	dst = append(dst, byte(TStringDictionary))
	dst = c.ofs.Store(dst)
	dst = c.len.Store(dst)
	dst = c.code.Store(dst)
	dst = binary.AppendUvarint(dst, uint64(len(c.dict)))
	return append(dst, c.dict...)
}

func (c *DictStringContainer) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TStringDictionary) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	var err error
	c.ofs = NewInt[uint32](ContainerType(buf[0]))
	buf, err = c.ofs.Load(buf)
	if err != nil {
		return buf, err
	}

	c.len = NewInt[uint32](ContainerType(buf[0]))
	buf, err = c.len.Load(buf)
	if err != nil {
		return buf, err
	}

	c.code = NewInt[uint32](ContainerType(buf[0]))
	buf, err = c.code.Load(buf)
	if err != nil {
		return buf, err
	}
	c.n = c.code.Len()

	v, n := binary.Uvarint(buf)
	buf = buf[n:]
	c.dict = buf[:int(v)]
	return buf[int(v):], nil
}

func (c *DictStringContainer) Get(i int) []byte {
	if i < 0 || i >= c.n {
		return nil
	}
	ptr := c.code.Get(i)
	len := c.len.Get(int(ptr))
	ofs := c.ofs.Get(int(ptr))
	return c.dict[ofs : ofs+len]
}

func (c *DictStringContainer) dictValue(i int) []byte {
	len := c.len.Get(i)
	ofs := c.ofs.Get(i)
	return c.dict[ofs : ofs+len]
}

func (c *DictStringContainer) All() iter.Seq2[int, []byte] {
	return func(yield func(int, []byte) bool) {
		it := c.Chunks()
		it.All()(yield)
		it.Close()
	}
}

func (c *DictStringContainer) Values() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		it := c.Chunks()
		it.Values()(yield)
		it.Close()
	}
}

func (c *DictStringContainer) Chunks() types.StringIterator {
	return NewDictStringIterator(c)
}

func (c *DictStringContainer) AppendTo(dst types.StringWriter, sel []uint32) {
	if sel == nil {
		for i := range c.n {
			ptr := c.code.Get(i)
			ofs := c.ofs.Get(int(ptr))
			len := c.len.Get(int(ptr))
			dst.Append(c.dict[ofs : ofs+len])
		}
	} else {
		for _, v := range sel {
			ptr := c.code.Get(int(v))
			ofs := c.ofs.Get(int(ptr))
			len := c.len.Get(int(ptr))
			dst.Append(c.dict[ofs : ofs+len])
		}
	}
}

func (c *DictStringContainer) Encode(ctx *StringContext, vals types.StringAccessor) StringContainer {
	// alloc target buffers
	dict := arena.Alloc[byte](ctx.UniqueSize)
	offs := arena.Alloc[uint32](ctx.NumUnique)[:ctx.NumUnique]
	size := arena.Alloc[uint32](ctx.NumUnique)[:ctx.NumUnique]
	code := arena.Alloc[uint32](ctx.NumValues)[:ctx.NumValues]

	// alloc temp data
	idx := arena.Alloc[uint32](ctx.NumUnique)[:ctx.NumUnique]
	rank := arena.Alloc[uint32](ctx.NumUnique)[:ctx.NumUnique]

	// sort dict keys
	ctx.SortDictKeys(idx, vals)

	// build dict and rank
	for i, discId := range idx {
		rank[discId] = uint32(i)
		val := vals.Get(int(ctx.FirstPos[discId]))
		offs[i] = uint32(len(dict))
		size[i] = uint32(len(val))
		dict = append(dict, val...)
	}

	// emit codes
	for i := range vals.Len() {
		code[i] = rank[ctx.DiscId[i]]
	}
	arena.Free(idx)
	arena.Free(rank)

	// encode child containers
	c.ofs = EncodeInt(nil, offs)
	arena.Free(offs)
	c.len = EncodeInt(nil, size)
	arena.Free(size)
	c.code = EncodeInt(nil, code)
	arena.Free(code)
	c.dict = dict
	c.free = true
	c.n = ctx.NumValues

	return c
}

func (c *DictStringContainer) Cmp(i, j int) int {
	return bytes.Compare(c.Get(i), c.Get(j))
}

// TODO: optimize matching by comparing dict codes instead,
// see integer dictionary

func (c *DictStringContainer) MatchEqual(val []byte, bits, mask *Bitset) {
	// early skip if val is smaller than first or larger than last dict entry
	l := c.len.Len()
	if bytes.Compare(val, c.dictValue(0)) < 0 || bytes.Compare(val, c.dictValue(l-1)) > 0 {
		return
	}

	// find position of val using binary search (dict is sorted and values are unique)
	// TODO: add a `Find(T) int` function to all containers and let them choose the
	// most efficient search strategy
	idx := sort.Search(l, func(i int) bool {
		return bytes.Compare(c.dictValue(i), val) >= 0
	})

	// if not found, equal match does not exist
	if idx == l || !bytes.Equal(c.dictValue(idx), val) {
		return
	}

	// run equal match on codes
	c.code.Matcher().MatchEqual(uint32(idx), bits, mask)
}

func (c *DictStringContainer) MatchNotEqual(val []byte, bits, mask *Bitset) {
	// early skip if val is smaller than first or larger than last dict entry
	l := c.len.Len()
	if bytes.Compare(val, c.dictValue(0)) < 0 || bytes.Compare(val, c.dictValue(l-1)) > 0 {
		bits.One()
		return
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return bytes.Compare(c.dictValue(i), val) >= 0
	})

	// if not found, equal match does not exist and we can set all bits one
	if idx == l || !bytes.Equal(c.dictValue(idx), val) {
		bits.One()
		return
	}

	// if found, we run a not equal scan on codes
	c.code.Matcher().MatchNotEqual(uint32(idx), bits, mask)
}

func (c *DictStringContainer) MatchLess(val []byte, bits, mask *Bitset) {
	// early skip if val is smaller than first or larger last
	if bytes.Compare(val, c.dictValue(0)) < 0 {
		return
	}
	l := c.len.Len()
	if bytes.Compare(val, c.dictValue(l-1)) > 0 {
		bits.One()
		return
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return bytes.Compare(c.dictValue(i), val) >= 0
	})

	// adjust index for search values > last dict entry
	if idx == l {
		idx--
	}

	// If found we are good. If not found, we have at least found the index of
	// the first value larger than val which is ok too. At this point
	// we know idx is between 0 and l-1, so we can directly translate to a
	// less(code) search.
	c.code.Matcher().MatchLess(uint32(idx), bits, mask)
}

func (c *DictStringContainer) MatchLessEqual(val []byte, bits, mask *Bitset) {
	// early skip if val is smaller than first or larger than last
	if bytes.Compare(val, c.dictValue(0)) < 0 {
		return
	}
	l := c.len.Len()
	if bytes.Compare(val, c.dictValue(l-1)) >= 0 {
		bits.One()
		return
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return bytes.Compare(c.dictValue(i), val) >= 0
	})

	// adjust when we reached the end or no exact match was found
	if idx == l || bytes.Compare(val, c.dictValue(idx)) < 0 {
		idx--
	}

	// If found we are good. If not found, we have at least found the index of
	// the first value larger than val which is ok too. At this point
	// we know idx is between 0 and l-1, so we can directly translate to a
	// less(code) search.
	c.code.Matcher().MatchLessEqual(uint32(idx), bits, mask)
}

func (c *DictStringContainer) MatchGreater(val []byte, bits, mask *Bitset) {
	// early skip if val is smaller than first or larger or equal to last
	if bytes.Compare(val, c.dictValue(0)) < 0 {
		bits.One()
		return
	}
	l := c.len.Len()
	if bytes.Compare(val, c.dictValue(l-1)) >= 0 {
		return
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return bytes.Compare(c.dictValue(i), val) > 0
	})

	// Since we are searching for strictly greater dict entries we found the
	// next higher code (or end of dict). Use GE for code match.
	c.code.Matcher().MatchGreaterEqual(uint32(idx), bits, mask)
}

func (c *DictStringContainer) MatchGreaterEqual(val []byte, bits, mask *Bitset) {
	// early skip if val is smaller than first or larger to last
	if bytes.Compare(val, c.dictValue(0)) < 0 {
		bits.One()
		return
	}
	l := c.len.Len()
	if bytes.Compare(val, c.dictValue(l-1)) > 0 {
		return
	}

	// find position of val using binary search (dict is sorted and values are unique)
	idx := sort.Search(l, func(i int) bool {
		return bytes.Compare(c.dictValue(i), val) >= 0
	})

	// If found we are good. If not found, we have at least found the index of
	// the first value larger than val which is ok too. At this point
	// we know idx is between 0 and l-1, so we can directly translate to a
	// less(code) search.
	c.code.Matcher().MatchGreaterEqual(uint32(idx), bits, mask)
}

func (c *DictStringContainer) MatchBetween(a, b []byte, bits, mask *Bitset) {
	// skip when range does not intersect with dict or does fully contain dict
	l := c.len.Len()
	first, last := c.dictValue(0), c.dictValue(l-1)
	if bytes.Compare(b, first) < 0 || bytes.Compare(a, last) > 0 {
		return
	}
	if bytes.Compare(a, first) <= 0 && bytes.Compare(b, last) >= 0 {
		bits.One()
		return
	}

	// translate range [a,b] into code range [ca, cb]
	ai := sort.Search(l, func(i int) bool {
		return bytes.Compare(c.dictValue(i), a) >= 0
	})
	bi := sort.Search(l, func(i int) bool {
		return bytes.Compare(c.dictValue(i), b) >= 0
	})

	// range is within a dict value gap
	if v := c.dictValue(ai); ai == bi && !bytes.Equal(v, a) && !bytes.Equal(v, b) {
		return
	}

	// adjust bi when b > last
	if bi == l || !bytes.Equal(c.dictValue(bi), b) {
		bi--
	}

	// forward between match on the code vector
	c.code.Matcher().MatchBetween(uint32(ai), uint32(bi), bits, mask)
}

type DictStringIterator struct {
	BaseIterator[[]byte]
	dict  []byte
	start []uint32
	size  []uint32
	code  types.NumberIterator[uint32]
}

func NewDictStringIterator(c *DictStringContainer) *DictStringIterator {
	it := newStringIterator[DictStringIterator](TStringDictionary)
	it.dict = c.dict
	it.start = c.ofs.AppendTo(arena.Alloc[uint32](c.ofs.Len()), nil)
	it.size = c.len.AppendTo(arena.Alloc[uint32](c.len.Len()), nil)
	it.code = c.code.Chunks()
	it.base = -1
	it.len = c.Len()
	it.BaseIterator.fill = it.fill
	return it
}

func (it *DictStringIterator) Close() {
	arena.Free(it.start)
	arena.Free(it.size)
	it.start = nil
	it.size = nil
	it.code.Close()
	it.code = nil
	it.dict = nil
	clear(it.chunk[:])
	it.BaseIterator.Close()
	putStringIterator(it)
}

func (it *DictStringIterator) fill(base int) int {
	// load code chunk at base and translate
	it.code.Seek(base)
	codes, n := it.code.Next()
	if n == 0 {
		it.ofs = it.len
		it.base = -1
		return 0
	}

	// translate codes
	for i := range n {
		code := codes[i]
		len := it.size[code]
		ofs := it.start[code]
		it.chunk[i] = it.dict[ofs : ofs+len : ofs+len]
	}

	it.base = base
	return n
}
