// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"fmt"
	"iter"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
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
	c.ofs.Close()
	c.len.Close()
	c.code.Close()
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
		num.UvarintLen(len(c.dict)) + len(c.dict)
}

func (c *DictStringContainer) Matcher() types.StringMatcher {
	return c
}

func (c *DictStringContainer) Store(dst []byte) []byte {
	dst = append(dst, byte(TStringDictionary))
	dst = c.ofs.Store(dst)
	dst = c.len.Store(dst)
	dst = c.code.Store(dst)
	dst = num.AppendUvarint(dst, uint64(len(c.dict)))
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

	v, n := num.Uvarint(buf)
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

func (c *DictStringContainer) Iterator() iter.Seq2[int, []byte] {
	return func(fn func(int, []byte) bool) {
		for i := range c.n {
			ptr := c.code.Get(i)
			ofs := c.ofs.Get(int(ptr))
			len := c.len.Get(int(ptr))
			if !fn(i, c.dict[ofs:ofs+len:ofs+len]) {
				return
			}
		}
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
	dict := arena.Alloc[byte](ctx.UniqueSize)
	offs := arena.Alloc[uint32](ctx.NumUnique)
	size := arena.Alloc[uint32](ctx.NumUnique)
	code := arena.Alloc[uint32](ctx.NumValues)

	// TODO: sorted dict for dict fusion match

	// compact and reference duplicates
	for i, v := range vals.Iterator() {
		k := ctx.Dups[i]
		if k < 0 {
			// append non duplicate strings to dict, register dict position
			code = append(code, uint32(len(offs)))
			offs = append(offs, uint32(len(dict)))
			size = append(size, uint32(len(v)))
			dict = append(dict, v...)
		} else {
			// reference as duplicate
			code = append(code, uint32(k))
		}
	}

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

func (c *DictStringContainer) MatchEqual(val []byte, bits, mask *Bitset) {
	matchStringEqual(c, val, bits, mask)
}

func (c *DictStringContainer) MatchNotEqual(val []byte, bits, mask *Bitset) {
	matchStringNotEqual(c, val, bits, mask)
}

func (c *DictStringContainer) MatchLess(val []byte, bits, mask *Bitset) {
	matchStringLess(c, val, bits, mask)
}

func (c *DictStringContainer) MatchLessEqual(val []byte, bits, mask *Bitset) {
	matchStringLessEqual(c, val, bits, mask)
}

func (c *DictStringContainer) MatchGreater(val []byte, bits, mask *Bitset) {
	matchStringGreater(c, val, bits, mask)
}

func (c *DictStringContainer) MatchGreaterEqual(val []byte, bits, mask *Bitset) {
	matchStringGreaterEqual(c, val, bits, mask)
}

func (c *DictStringContainer) MatchBetween(a, b []byte, bits, mask *Bitset) {
	matchStringBetween(c, a, b, bits, mask)
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
	it.start = c.ofs.AppendTo(arena.AllocUint32(c.ofs.Len()), nil)
	it.size = c.len.AppendTo(arena.AllocUint32(c.len.Len()), nil)
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
	codes, n := it.code.NextChunk()
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
