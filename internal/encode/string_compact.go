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
	_ types.StringAccessor = (*CompactStringContainer)(nil)
	_ StringContainer      = (*CompactStringContainer)(nil)
)

type CompactStringContainer struct {
	readOnlyContainer[[]byte]
	buf  []byte
	ofs  NumberContainer[uint32]
	len  NumberContainer[uint32]
	n    int
	free bool
}

func (c *CompactStringContainer) Info() string {
	return fmt.Sprintf("Compact(string)_[%s]_[%s]", c.ofs.Info(), c.len.Info())
}

func (c *CompactStringContainer) Close() {
	c.ofs.Close()
	c.len.Close()
	c.ofs = nil
	c.len = nil
	if c.free {
		arena.Free(c.buf)
		c.free = false
	}
	c.buf = nil
	putStringContainer(c)
}

func (c *CompactStringContainer) Type() ContainerType {
	return TStringCompact
}

func (c *CompactStringContainer) Len() int {
	return c.n
}

func (c *CompactStringContainer) Size() int {
	return 1 + c.ofs.Size() + c.len.Size() + num.UvarintLen(len(c.buf)) + len(c.buf)
}

func (c *CompactStringContainer) Matcher() types.StringMatcher {
	return c
}

func (c *CompactStringContainer) Store(dst []byte) []byte {
	dst = append(dst, byte(TStringCompact))
	dst = c.ofs.Store(dst)
	dst = c.len.Store(dst)
	dst = num.AppendUvarint(dst, uint64(len(c.buf)))
	return append(dst, c.buf...)
}

func (c *CompactStringContainer) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TStringCompact) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	var err error
	c.ofs = NewInt[uint32](ContainerType(buf[0]))
	buf, err = c.ofs.Load(buf)
	if err != nil {
		return buf, err
	}
	c.n = c.ofs.Len()

	c.len = NewInt[uint32](ContainerType(buf[0]))
	buf, err = c.len.Load(buf)
	if err != nil {
		return buf, err
	}

	v, n := num.Uvarint(buf)
	buf = buf[n:]
	c.buf = buf[:int(v)]
	return buf[int(v):], nil
}

func (c *CompactStringContainer) Get(i int) []byte {
	if i < 0 || i >= c.n {
		return nil
	}
	len := c.len.Get(i)
	ofs := c.ofs.Get(i)
	return c.buf[ofs : ofs+len]
}

func (c *CompactStringContainer) Iterator() iter.Seq2[int, []byte] {
	return func(fn func(int, []byte) bool) {
		for i := range c.n {
			ofs := c.ofs.Get(i)
			len := c.len.Get(i)
			if !fn(i, c.buf[ofs:ofs+len:ofs+len]) {
				return
			}
		}
	}
}

func (c *CompactStringContainer) Chunks() types.StringIterator {
	return NewCompactStringIterator(c)
}

func (c *CompactStringContainer) AppendTo(dst types.StringWriter, sel []uint32) {
	if sel == nil {
		for i := range c.n {
			ofs := c.ofs.Get(i)
			len := c.len.Get(i)
			dst.Append(c.buf[ofs : ofs+len])
		}
	} else {
		for _, v := range sel {
			ofs := c.ofs.Get(int(v))
			len := c.len.Get(int(v))
			dst.Append(c.buf[ofs : ofs+len])
		}
	}
}

func (c *CompactStringContainer) Encode(ctx *StringContext, vals types.StringAccessor) StringContainer {
	c.n = ctx.NumValues
	buf := arena.Alloc[byte](vals.Size())
	offs := arena.Alloc[uint32](ctx.NumValues)[:ctx.NumValues]
	size := arena.Alloc[uint32](ctx.NumValues)[:ctx.NumValues]
	uniq := arena.Alloc[int32](ctx.NumUnique)

	// compact and reference duplicates
	for i, v := range vals.Iterator() {
		k := ctx.Dups[i]
		if k < 0 {
			// append non duplicate
			offs[i] = uint32(len(buf))
			size[i] = uint32(len(v))
			buf = append(buf, v...)
			uniq = append(uniq, int32(i))
		} else {
			// reference as duplicate
			offs[i] = offs[uniq[k]]
			size[i] = size[uniq[k]]
		}
	}
	arena.Free(uniq)

	// encode child containers
	c.ofs = EncodeInt(nil, offs)
	arena.Free(offs)
	c.len = EncodeInt(nil, size)
	arena.Free(size)
	c.buf = buf
	c.free = true

	return c
}

func (c *CompactStringContainer) Cmp(i, j int) int {
	return bytes.Compare(c.Get(i), c.Get(j))
}

func (c *CompactStringContainer) MatchEqual(val []byte, bits, mask *Bitset) {
	matchStringEqual(c, val, bits, mask)
}

func (c *CompactStringContainer) MatchNotEqual(val []byte, bits, mask *Bitset) {
	matchStringNotEqual(c, val, bits, mask)
}

func (c *CompactStringContainer) MatchLess(val []byte, bits, mask *Bitset) {
	matchStringLess(c, val, bits, mask)
}

func (c *CompactStringContainer) MatchLessEqual(val []byte, bits, mask *Bitset) {
	matchStringLessEqual(c, val, bits, mask)
}

func (c *CompactStringContainer) MatchGreater(val []byte, bits, mask *Bitset) {
	matchStringGreater(c, val, bits, mask)
}

func (c *CompactStringContainer) MatchGreaterEqual(val []byte, bits, mask *Bitset) {
	matchStringGreaterEqual(c, val, bits, mask)
}

func (c *CompactStringContainer) MatchBetween(a, b []byte, bits, mask *Bitset) {
	matchStringBetween(c, a, b, bits, mask)
}

type CompactStringIterator struct {
	BaseIterator[[]byte]
	buf   []byte
	start types.NumberIterator[uint32]
	size  types.NumberIterator[uint32]
}

func NewCompactStringIterator(c *CompactStringContainer) *CompactStringIterator {
	it := newStringIterator[CompactStringIterator](TStringCompact)
	it.buf = c.buf
	it.start = c.ofs.Chunks()
	it.size = c.len.Chunks()
	it.base = -1
	it.len = c.Len()
	it.BaseIterator.fill = it.fill
	return it
}

func (it *CompactStringIterator) Close() {
	it.start.Close()
	it.start = nil
	it.size.Close()
	it.size = nil
	clear(it.chunk[:])
	it.BaseIterator.Close()
	putStringIterator(it)
}

func (it *CompactStringIterator) fill(base int) int {
	// load code chunk at base and translate
	it.start.Seek(base)
	it.size.Seek(base)
	ofs, n := it.start.NextChunk()
	len, m := it.size.NextChunk()
	if n == 0 || n != m {
		it.ofs = it.len
		it.base = -1
		return 0
	}

	// translate
	var i int
	for range n / 16 {
		it.chunk[i] = it.buf[ofs[i] : ofs[i]+len[i] : ofs[i]+len[i]]
		it.chunk[i+1] = it.buf[ofs[i+1] : ofs[i]+len[i+1] : ofs[i]+len[i+1]]
		it.chunk[i+2] = it.buf[ofs[i+2] : ofs[i]+len[i+2] : ofs[i]+len[i+2]]
		it.chunk[i+3] = it.buf[ofs[i+3] : ofs[i]+len[i+3] : ofs[i]+len[i+3]]
		it.chunk[i+4] = it.buf[ofs[i+4] : ofs[i]+len[i+4] : ofs[i]+len[i+4]]
		it.chunk[i+5] = it.buf[ofs[i+5] : ofs[i]+len[i+5] : ofs[i]+len[i+5]]
		it.chunk[i+6] = it.buf[ofs[i+6] : ofs[i]+len[i+6] : ofs[i]+len[i+6]]
		it.chunk[i+7] = it.buf[ofs[i+7] : ofs[i]+len[i+7] : ofs[i]+len[i+7]]
		it.chunk[i+8] = it.buf[ofs[i+8] : ofs[i]+len[i+8] : ofs[i]+len[i+8]]
		it.chunk[i+9] = it.buf[ofs[i+9] : ofs[i]+len[i+9] : ofs[i]+len[i+9]]
		it.chunk[i+10] = it.buf[ofs[i+10] : ofs[i]+len[i+10] : ofs[i]+len[i+10]]
		it.chunk[i+11] = it.buf[ofs[i+11] : ofs[i]+len[i+11] : ofs[i]+len[i+11]]
		it.chunk[i+12] = it.buf[ofs[i+12] : ofs[i]+len[i+12] : ofs[i]+len[i+12]]
		it.chunk[i+13] = it.buf[ofs[i+13] : ofs[i]+len[i+13] : ofs[i]+len[i+13]]
		it.chunk[i+14] = it.buf[ofs[i+14] : ofs[i]+len[i+14] : ofs[i]+len[i+14]]
		it.chunk[i+15] = it.buf[ofs[i+15] : ofs[i]+len[i+15] : ofs[i]+len[i+15]]
		i += 16
	}
	for i < n {
		l := len[i]
		o := ofs[i]
		it.chunk[i] = it.buf[o : o+l : o+l]
		i++
	}

	it.base = base
	return n
}
