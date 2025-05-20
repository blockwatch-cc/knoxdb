// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"iter"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

type FixedStringContainer struct {
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

func (c *FixedStringContainer) Iterator() iter.Seq[[]byte] {
	return func(fn func([]byte) bool) {
		var i int
		for range c.n {
			if !fn(c.buf[i : i+c.sz]) {
				return
			}
			i += c.sz
		}
	}
}

func (c *FixedStringContainer) AppendTo(dst types.StringSetter, sel []uint32) {
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
	for v := range vals.Iterator() {
		c.buf = append(c.buf, v...)
	}
	return c
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
