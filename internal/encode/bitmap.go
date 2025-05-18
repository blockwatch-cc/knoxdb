// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
)

type BitmapContainerType byte

const (
	TBitmapZero BitmapContainerType = iota
	TBitmapOne
	TBitmapDense
	TBitmapSparse
)

var (
	bTypeNames    = "zero_one_dense_sparse"
	bTypeNamesOfs = []int{0, 5, 9, 15, 22}
)

func (t BitmapContainerType) String() string {
	return bTypeNames[bTypeNamesOfs[t] : bTypeNamesOfs[t+1]-1]
}

// TBitmap
type BitmapContainer struct {
	Buf []byte
	N   int
	Typ BitmapContainerType
}

// NewBitmap creates a new biitmap integer container.
func NewBitmap() *BitmapContainer {
	return newBitmapContainer()
}

// EncodeBitmap encodes an optimized bitmap vector
// selecting the most efficient encoding scheme.
func EncodeBitmap(v *bitset.Bitset) *BitmapContainer {
	return NewBitmap().Encode(v)
}

// LoadBitmap loads a bitmap container from buffer.
func LoadBitmap(buf []byte) (*BitmapContainer, error) {
	c := NewBitmap()
	if _, err := c.Load(buf); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *BitmapContainer) Info() string {
	return fmt.Sprintf("Bitmap(%s)_[n=%d]", c.Typ, c.N)
}

func (c *BitmapContainer) Close() {
	c.Buf = nil
	c.N = 0
	c.Typ = 0
	putBitmapContainer(c)
}

func (c *BitmapContainer) Type() BitmapContainerType {
	return c.Typ
}

func (c *BitmapContainer) Len() int {
	return c.N
}

func (c *BitmapContainer) Size() int {
	// Typ (1) + n (varint) + bits (variable)
	return 1 + num.UvarintLen(c.N) + num.UvarintLen(len(c.Buf)) + len(c.Buf)
}

func (c *BitmapContainer) Store(dst []byte) []byte {
	dst = append(dst, byte(c.Typ))
	dst = num.AppendUvarint(dst, uint64(c.N))
	dst = num.AppendUvarint(dst, uint64(len(c.Buf)))
	return append(dst, c.Buf...)
}

func (c *BitmapContainer) Load(buf []byte) ([]byte, error) {
	if buf[0] > byte(TBitmapSparse) {
		return buf, ErrInvalidType
	}
	c.Typ = BitmapContainerType(buf[0])
	buf = buf[1:]

	// num bits
	v, n := num.Uvarint(buf)
	c.N = int(v)
	buf = buf[n:]

	// buf len
	v, n = num.Uvarint(buf)
	sz := int(v)
	buf = buf[n:]
	if sz > 0 {
		c.Buf = buf[:sz]
	}

	return buf[sz:], nil
}

func (c *BitmapContainer) Get(n int) bool {
	switch c.Typ {
	case TBitmapZero:
		return false
	case TBitmapOne:
		return true
	case TBitmapDense:
		b := bitset.NewFromBuffer(c.Buf, c.N)
		ok := b.Contains(n)
		b.Close()
		return ok
	case TBitmapSparse:
		return xroar.NewFromBuffer(c.Buf).Contains(uint64(n))
	default:
		return false
	}
}

func (c *BitmapContainer) AppendTo(sel []uint32, dst *bitset.Bitset) *bitset.Bitset {
	if sel == nil {
		start := dst.Len()
		switch c.Typ {
		case TBitmapZero:
			dst.Grow(c.N)
		case TBitmapOne:
			dst.Grow(c.N).SetRange(start, start+c.N)
		case TBitmapDense:
			// don't grow, append already extends dst
			b := bitset.NewFromBuffer(c.Buf, c.N)
			dst.AppendFrom(b, 0, c.N)
			b.Close()
		case TBitmapSparse:
			dst.Grow(c.N)
			it := xroar.NewFromBuffer(c.Buf).NewIterator()
			for {
				i, ok := it.Next()
				if !ok {
					break
				}
				dst.Set(start + int(i))
			}
		}
	} else {
		start := dst.Len()
		dst.Grow(len(sel))
		switch c.Typ {
		case TBitmapZero:
			// noop, assuming dst is cleared
		case TBitmapOne:
			dst.SetRange(start, start+len(sel))
		case TBitmapDense:
			b := bitset.NewFromBuffer(c.Buf, c.N)
			for i, v := range sel {
				if b.Contains(int(v)) {
					dst.Set(start + i)
				}
			}
			b.Close()
		case TBitmapSparse:
			b := xroar.NewFromBuffer(c.Buf)
			for i, v := range sel {
				if b.Contains(uint64(v)) {
					dst.Set(start + i)
				}
			}
		}
	}
	return dst
}

func (c *BitmapContainer) Encode(vals *bitset.Bitset) *BitmapContainer {
	c.N = vals.Len()
	n := vals.Count()
	switch {
	case n == 0:
		c.Typ = TBitmapZero
		c.Buf = nil
	case n == c.N:
		c.Typ = TBitmapOne
		c.Buf = nil
	case n*2 < c.N/8:
		c.Typ = TBitmapSparse
		keys := vals.Indexes(arena.Alloc[uint32](n))
		c.Buf = xroar.NewFromSortedList(keys).ToBuffer()
		arena.Free(keys)
	default:
		c.Typ = TBitmapDense
		c.Buf = vals.Bytes()
	}
	return c
}

func (c *BitmapContainer) MatchEqual(val bool, bits, _ *Bitset) {
	switch c.Typ {
	case TBitmapZero:
		if !val {
			bits.One()
		}
	case TBitmapOne:
		if val {
			bits.One()
		}
	case TBitmapDense:
		copy(bits.Bytes(), c.Buf)
		if !val {
			bits.Neg()
		}
	case TBitmapSparse:
		it := xroar.NewFromBuffer(c.Buf).NewIterator()
		if val {
			for {
				i, ok := it.Next()
				if !ok {
					break
				}
				bits.Set(int(i))
			}
		} else {
			var last int
			for {
				i, ok := it.Next()
				if !ok {
					break
				}
				next := int(i)
				if next > last {
					bits.SetRange(last, next-1)
				}
				last = next + 1
			}
			if last < c.N {
				bits.SetRange(last, c.N)
			}
		}
	}
}

func (c *BitmapContainer) MatchNotEqual(val bool, bits, _ *Bitset) {
	c.MatchEqual(!val, bits, nil)
}

func (c *BitmapContainer) MatchLess(val bool, bits, _ *Bitset) {
	if val {
		c.MatchEqual(false, bits, nil)
	}
}

func (c *BitmapContainer) MatchLessEqual(val bool, bits, _ *Bitset) {
	if val {
		bits.One()
	} else {
		c.MatchEqual(false, bits, nil)
	}
}

func (c *BitmapContainer) MatchGreater(val bool, bits, _ *Bitset) {
	if !val {
		c.MatchEqual(true, bits, nil)
	}
}

func (c *BitmapContainer) MatchGreaterEqual(val bool, bits, _ *Bitset) {
	if !val {
		bits.One()
	} else {
		c.MatchEqual(true, bits, nil)
	}
}

func (c *BitmapContainer) MatchBetween(a, b bool, bits, _ *Bitset) {
	switch {
	case a && b:
		c.MatchEqual(true, bits, nil)
	case !a && b:
		bits.One()
	case !a && !b:
		c.MatchEqual(false, bits, nil)
	}
}

func (c *BitmapContainer) MatchInSet(s any, bits, mask *Bitset) {
	// N.A.
}

func (c *BitmapContainer) MatchNotInSet(s any, bits, mask *Bitset) {
	// N.A.
}

// ---------------------------------------
// Factory
//

type BitmapFactory struct {
	cpool sync.Pool // container pool
}

func newBitmapContainer() *BitmapContainer {
	return bitmapFactory.cpool.Get().(*BitmapContainer)
}

func putBitmapContainer(c *BitmapContainer) {
	bitmapFactory.cpool.Put(c)
}

var bitmapFactory = BitmapFactory{
	cpool: sync.Pool{New: func() any { return new(BitmapContainer) }},
}
