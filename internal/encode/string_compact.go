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

type CompactStringContainer struct {
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
	ofs := c.ofs.Get(i)
	len := c.len.Get(i)
	return c.buf[ofs : ofs+len]
}

func (c *CompactStringContainer) Iterator() iter.Seq[[]byte] {
	return func(fn func([]byte) bool) {
		for i := range c.n {
			ofs := c.ofs.Get(i)
			len := c.len.Get(i)
			if !fn(c.buf[ofs : ofs+len]) {
				return
			}
		}
	}
}

func (c *CompactStringContainer) AppendTo(dst types.StringSetter, sel []uint32) {
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
	var i int32
	for v := range vals.Iterator() {
		k := ctx.Dups[i]
		if k < 0 {
			// append non duplicate
			offs[i] = uint32(len(buf))
			size[i] = uint32(len(v))
			buf = append(buf, v...)
			uniq = append(uniq, i)
		} else {
			// reference as duplicate
			offs[i] = offs[uniq[k]]
			size[i] = size[uniq[k]]
		}
		i++
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
