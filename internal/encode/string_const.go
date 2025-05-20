// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"fmt"
	"iter"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

type ConstStringContainer struct {
	val []byte
	n   int
}

func (c *ConstStringContainer) Info() string {
	return fmt.Sprintf("Const(string)_[n=%d]", c.n)
}

func (c *ConstStringContainer) Close() {
	putStringContainer(c)
}

func (c *ConstStringContainer) Type() ContainerType {
	return TStringConstant
}

func (c *ConstStringContainer) Len() int {
	return c.n
}

func (c *ConstStringContainer) Size() int {
	return 1 + num.UvarintLen(c.n) + num.UvarintLen(len(c.val)) + len(c.val)
}

func (c *ConstStringContainer) Store(dst []byte) []byte {
	dst = append(dst, byte(TStringConstant))
	dst = num.AppendUvarint(dst, uint64(c.n))
	dst = num.AppendUvarint(dst, uint64(len(c.val)))
	return append(dst, c.val...)
}

func (c *ConstStringContainer) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TStringConstant) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	c.n = int(v)
	buf = buf[n:]
	v, n = num.Uvarint(buf)
	buf = buf[n:]
	c.val = buf[:v]
	return buf[v:], nil
}

func (c *ConstStringContainer) Get(i int) []byte {
	if i < 0 || i >= c.n {
		return nil
	}
	return c.val
}

func (c *ConstStringContainer) Iterator() iter.Seq[[]byte] {
	return func(fn func([]byte) bool) {
		for range c.n {
			if !fn(c.val) {
				return
			}
		}
	}
}

func (c *ConstStringContainer) AppendTo(dst types.StringSetter, sel []uint32) {
	n := c.n
	if sel != nil {
		n = len(sel)
	}
	for range n {
		dst.Append(c.val)
	}
}

func (c *ConstStringContainer) Encode(ctx *StringContext, vals types.StringAccessor) StringContainer {
	c.val = ctx.Min
	c.n = ctx.NumValues
	return c
}

func (c *ConstStringContainer) MatchEqual(val []byte, bits, _ *Bitset) {
	if bytes.Equal(c.val, val) {
		bits.One()
	}
}

func (c *ConstStringContainer) MatchNotEqual(val []byte, bits, _ *Bitset) {
	if !bytes.Equal(c.val, val) {
		bits.One()
	}
}

func (c *ConstStringContainer) MatchLess(val []byte, bits, _ *Bitset) {
	if bytes.Compare(c.val, val) < 0 {
		bits.One()
	}
}

func (c *ConstStringContainer) MatchLessEqual(val []byte, bits, _ *Bitset) {
	if bytes.Compare(c.val, val) <= 0 {
		bits.One()
	}
}

func (c *ConstStringContainer) MatchGreater(val []byte, bits, _ *Bitset) {
	if bytes.Compare(c.val, val) > 0 {
		bits.One()
	}
}

func (c *ConstStringContainer) MatchGreaterEqual(val []byte, bits, _ *Bitset) {
	if bytes.Compare(c.val, val) >= 0 {
		bits.One()
	}
}

func (c *ConstStringContainer) MatchBetween(a, b []byte, bits, _ *Bitset) {
	if bytes.Compare(c.val, a) >= 0 && bytes.Compare(c.val, b) <= 0 {
		bits.One()
	}
}
