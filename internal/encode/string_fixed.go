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
	_ types.StringAccessor = (*FixedStringContainer)(nil)
	_ StringContainer      = (*FixedStringContainer)(nil)
)

type FixedStringContainer struct {
	readOnlyContainer[[]byte]
	buf  []byte
	sz   int
	n    int
	free bool
}

func (c *FixedStringContainer) Info() string {
	return fmt.Sprintf("Fixed(string)_[sz=%d,n=%d]", c.sz, c.n)
}

func (c *FixedStringContainer) Close() {
	if c.free {
		arena.Free(c.buf)
		c.free = false
	}
	c.buf = nil
	putStringContainer(c)
}

func (c *FixedStringContainer) Type() ContainerType {
	return TStringFixed
}

func (c *FixedStringContainer) Len() int {
	return c.n
}

func (c *FixedStringContainer) Size() int {
	return 1 + num.UvarintLen(c.n) + num.UvarintLen(c.sz) + len(c.buf)
}

func (c *FixedStringContainer) Matcher() types.StringMatcher {
	return c
}

func (c *FixedStringContainer) Store(dst []byte) []byte {
	dst = append(dst, byte(TStringFixed))
	dst = num.AppendUvarint(dst, uint64(c.n))
	dst = num.AppendUvarint(dst, uint64(c.sz))
	return append(dst, c.buf...)
}

func (c *FixedStringContainer) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TStringFixed) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	c.n = int(v)
	buf = buf[n:]
	v, n = num.Uvarint(buf)
	buf = buf[n:]
	c.sz = int(v)
	sz := c.sz * c.n
	c.buf = buf[:sz]
	return buf[sz:], nil
}

func (c *FixedStringContainer) Get(i int) []byte {
	if i < 0 || i >= c.n {
		return nil
	}
	return c.buf[i*c.sz : (i+1)*c.sz]
}

func (c *FixedStringContainer) Iterator() iter.Seq2[int, []byte] {
	return func(fn func(int, []byte) bool) {
		var n int
		for i := range c.n {
			if !fn(i, c.buf[n:n+c.sz:n+c.sz]) {
				return
			}
			n += c.sz
		}
	}
}

func (c *FixedStringContainer) Chunks() types.StringIterator {
	return NewFixedStringIterator(c)
}

func (c *FixedStringContainer) AppendTo(dst types.StringWriter, sel []uint32) {
	if sel == nil {
		var i int
		for range c.n {
			dst.Append(c.buf[i : i+c.sz])
			i += c.sz
		}
	} else {
		for _, v := range sel {
			dst.Append(c.buf[int(v)*c.sz : int(v+1)*c.sz])
		}
	}
}

func (c *FixedStringContainer) Encode(ctx *StringContext, vals types.StringAccessor) StringContainer {
	c.sz = ctx.MaxLen
	c.n = ctx.NumValues
	sz := c.sz * c.n
	c.buf = arena.Alloc[byte](sz)
	c.free = true
	for _, v := range vals.Iterator() {
		c.buf = append(c.buf, v...)
	}
	return c
}

func (c *FixedStringContainer) Cmp(i, j int) int {
	return bytes.Compare(c.Get(i), c.Get(j))
}

func (c *FixedStringContainer) MatchEqual(val []byte, bits, mask *Bitset) {
	matchStringEqual(c, val, bits, mask)
}

func (c *FixedStringContainer) MatchNotEqual(val []byte, bits, mask *Bitset) {
	matchStringNotEqual(c, val, bits, mask)
}

func (c *FixedStringContainer) MatchLess(val []byte, bits, mask *Bitset) {
	matchStringLess(c, val, bits, mask)
}

func (c *FixedStringContainer) MatchLessEqual(val []byte, bits, mask *Bitset) {
	matchStringLessEqual(c, val, bits, mask)
}

func (c *FixedStringContainer) MatchGreater(val []byte, bits, mask *Bitset) {
	matchStringGreater(c, val, bits, mask)
}

func (c *FixedStringContainer) MatchGreaterEqual(val []byte, bits, mask *Bitset) {
	matchStringGreaterEqual(c, val, bits, mask)
}

func (c *FixedStringContainer) MatchBetween(a, b []byte, bits, mask *Bitset) {
	matchStringBetween(c, a, b, bits, mask)
}

type FixedStringIterator struct {
	BaseIterator[[]byte]
	buf []byte
	sz  int
}

func NewFixedStringIterator(c *FixedStringContainer) *FixedStringIterator {
	it := newStringIterator[FixedStringIterator](TStringFixed)
	it.buf = c.buf
	it.sz = c.sz
	it.base = -1
	it.len = c.n
	it.BaseIterator.fill = it.fill
	return it
}

func (it *FixedStringIterator) Close() {
	it.buf = nil
	it.sz = 0
	clear(it.chunk[:])
	it.BaseIterator.Close()
	putStringIterator(it)
}

func (it *FixedStringIterator) fill(base int) int {
	n := min(CHUNK_SIZE, it.len-base)

	// translate codes
	var i int
	for range n / 16 {
		it.chunk[i] = it.buf[(base+i)*it.sz : (base+i+1)*it.sz : (base+i+1)*it.sz]
		it.chunk[i+1] = it.buf[(base+i+1)*it.sz : (base+i+2)*it.sz : (base+i+2)*it.sz]
		it.chunk[i+2] = it.buf[(base+i+2)*it.sz : (base+i+3)*it.sz : (base+i+3)*it.sz]
		it.chunk[i+3] = it.buf[(base+i+3)*it.sz : (base+i+4)*it.sz : (base+i+4)*it.sz]
		it.chunk[i+4] = it.buf[(base+i+4)*it.sz : (base+i+5)*it.sz : (base+i+5)*it.sz]
		it.chunk[i+5] = it.buf[(base+i+5)*it.sz : (base+i+6)*it.sz : (base+i+6)*it.sz]
		it.chunk[i+6] = it.buf[(base+i+6)*it.sz : (base+i+7)*it.sz : (base+i+7)*it.sz]
		it.chunk[i+7] = it.buf[(base+i+7)*it.sz : (base+i+8)*it.sz : (base+i+8)*it.sz]
		it.chunk[i+8] = it.buf[(base+i+8)*it.sz : (base+i+9)*it.sz : (base+i+9)*it.sz]
		it.chunk[i+9] = it.buf[(base+i+9)*it.sz : (base+i+10)*it.sz : (base+i+10)*it.sz]
		it.chunk[i+10] = it.buf[(base+i+10)*it.sz : (base+i+11)*it.sz : (base+i+11)*it.sz]
		it.chunk[i+11] = it.buf[(base+i+11)*it.sz : (base+i+12)*it.sz : (base+i+12)*it.sz]
		it.chunk[i+12] = it.buf[(base+i+12)*it.sz : (base+i+13)*it.sz : (base+i+13)*it.sz]
		it.chunk[i+13] = it.buf[(base+i+13)*it.sz : (base+i+14)*it.sz : (base+i+14)*it.sz]
		it.chunk[i+14] = it.buf[(base+i+14)*it.sz : (base+i+15)*it.sz : (base+i+15)*it.sz]
		it.chunk[i+15] = it.buf[(base+i+15)*it.sz : (base+i+16)*it.sz : (base+i+16)*it.sz]
		i += 16
	}
	for i < n {
		it.chunk[i] = it.buf[(base+i)*it.sz : (base+i+1)*it.sz : (base+i+1)*it.sz]
		i++
	}
	clear(it.chunk[n:])

	it.base = base
	return n
}
