// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"iter"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
)

var _ bitset.BitmapAccessor = (*BitmapContainer)(nil)

type BitmapContext struct {
	Min       bool
	Max       bool
	NumValues int
	Count     int
}

// AnalyzeBitmap produces statistics
func AnalyzeBitmap(v *bitset.Bitset) *BitmapContext {
	cnt, len := v.Count(), v.Len()
	return &BitmapContext{
		Min:       cnt < len,
		Max:       cnt > 0,
		NumValues: len,
		Count:     cnt,
	}
}

func (c *BitmapContext) Close() {}

func (c *BitmapContext) MinMax() (any, any) {
	return c.Min, c.Max
}

func (c *BitmapContext) Unique() int {
	if c.Min != c.Max {
		return 2
	}
	return 1
}

// TBitmap
type BitmapContainer struct {
	readOnlyContainer[bool]
	Buf []byte
	N   int
	Typ ContainerType
}

// NewBitmap creates a new biitmap integer container.
func NewBitmap() *BitmapContainer {
	return newBitmapContainer()
}

// EncodeBitmap encodes an optimized bitmap vector
// selecting the most efficient encoding scheme.
func EncodeBitmap(ctx *BitmapContext, v *bitset.Bitset) *BitmapContainer {
	return NewBitmap().Encode(ctx, v)
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

func (c *BitmapContainer) Type() ContainerType {
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
	switch typ := ContainerType(buf[0]); typ {
	case TBitmapZero, TBitmapOne, TBitmapDense, TBitmapSparse:
		c.Typ = ContainerType(buf[0])
		buf = buf[1:]
	default:
		return buf, ErrInvalidType
	}

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
		b := bitset.NewFromBytes(c.Buf, c.N)
		ok := b.Contains(n)
		b.Close()
		return ok
	case TBitmapSparse:
		return xroar.NewFromBytes(c.Buf).Contains(uint64(n))
	default:
		return false
	}
}

func (c *BitmapContainer) All() bool {
	switch c.Typ {
	case TBitmapZero:
		return false
	case TBitmapOne:
		return true
	case TBitmapDense:
		b := bitset.NewFromBytes(c.Buf, c.N)
		ok := b.All()
		b.Close()
		return ok
	case TBitmapSparse:
		return xroar.NewFromBytes(c.Buf).Count() == c.N
	default:
		return false
	}
}

func (c *BitmapContainer) Any() bool {
	switch c.Typ {
	case TBitmapZero:
		return false
	case TBitmapOne:
		return true
	case TBitmapDense:
		b := bitset.NewFromBytes(c.Buf, c.N)
		ok := b.Any()
		b.Close()
		return ok
	case TBitmapSparse:
		return xroar.NewFromBytes(c.Buf).Count() > 0
	default:
		return false
	}
}

func (c *BitmapContainer) None() bool {
	return c.Typ == TBitmapZero
}

func (c *BitmapContainer) AppendTo(dst *bitset.Bitset, sel []uint32) {
	if sel == nil {
		start := dst.Len()
		switch c.Typ {
		case TBitmapZero:
			dst.Grow(c.N)
		case TBitmapOne:
			dst.Grow(c.N).SetRange(start, start+c.N)
		case TBitmapDense:
			// don't grow, append already extends dst
			b := bitset.NewFromBytes(c.Buf, c.N)
			dst.AppendRange(b, 0, c.N)
			b.Close()
		case TBitmapSparse:
			dst.Grow(c.N)
			it := xroar.NewFromBytes(c.Buf).NewIterator()
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
			b := bitset.NewFromBytes(c.Buf, c.N)
			for i, v := range sel {
				if b.Contains(int(v)) {
					dst.Set(start + i)
				}
			}
			b.Close()
		case TBitmapSparse:
			b := xroar.NewFromBytes(c.Buf)
			for i, v := range sel {
				if b.Contains(uint64(v)) {
					dst.Set(start + i)
				}
			}
		}
	}
}

func (c *BitmapContainer) Encode(ctx *BitmapContext, vals *bitset.Bitset) *BitmapContainer {
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
		c.Buf = xroar.NewFromSorted(keys).Bytes()
		arena.Free(keys)
	default:
		c.Typ = TBitmapDense
		c.Buf = vals.Bytes()
	}
	return c
}

func (c *BitmapContainer) Iterator() iter.Seq[int] {
	switch c.Typ {
	case TBitmapOne:
		return func(fn func(int) bool) {
			for i := range c.N {
				if !fn(i) {
					return
				}
			}
		}
	case TBitmapDense:
		b := bitset.NewFromBytes(c.Buf, c.N)
		return b.Iterator()
	case TBitmapSparse:
		return func(fn func(int) bool) {
			it := xroar.NewFromBytes(c.Buf).NewIterator()
			for {
				n, ok := it.Next()
				if !ok {
					break
				}
				if !fn(int(n)) {
					break
				}
			}
		}
	default:
		// TBitmapZero
		return func(fn func(int) bool) {}
	}
}

func (c *BitmapContainer) Chunks() bitset.BitmapIterator {
	switch c.Typ {
	case TBitmapOne:
		return &oneBitmapIterator{size: c.N, last: -1}
	case TBitmapDense:
		b := bitset.NewFromBytes(c.Buf, c.N)
		return b.Chunks()
	case TBitmapSparse:
		return &xroarBitmapIterator{it: xroar.NewFromBytes(c.Buf).NewIterator()}
	default:
		// TBitmapZero
		return &zeroBitmapIterator{}
	}
}

func (c *BitmapContainer) Matcher() bitset.BitmapMatcher {
	return c
}

func (c *BitmapContainer) Cmp(i, j int) int {
	x, y := c.Get(i), c.Get(j)
	switch {
	case x == y:
		return 0
	case !x && y:
		return -1
	default:
		return 1
	}
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
		it := xroar.NewFromBytes(c.Buf).NewIterator()
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

// N.A.
func (_ *BitmapContainer) MatchInSet(_ any, _, _ *Bitset)    {}
func (_ *BitmapContainer) MatchNotInSet(_ any, _, _ *Bitset) {}

// special read-only bitmap function overrides
func (_ *BitmapContainer) Set(_ int)       {}
func (_ *BitmapContainer) Unset(_ int)     {}
func (_ *BitmapContainer) Writer() *Bitset { return nil }

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

// ---------------------------------------
// Iterators
//

type xroarBitmapIterator struct {
	chunk [types.CHUNK_SIZE]int
	it    *xroar.Iterator
}

func (it *xroarBitmapIterator) Close() {
	it.it = nil
}

func (it *xroarBitmapIterator) Next() ([]int, bool) {
	var n int
	for {
		val, ok := it.it.Next()
		if !ok {
			break
		}
		it.chunk[n] = int(val)
		n++
		if n == types.CHUNK_SIZE {
			break
		}
	}
	return it.chunk[:n], n > 0
}

type zeroBitmapIterator struct{}

func (_ *zeroBitmapIterator) Close()              {}
func (_ *zeroBitmapIterator) Next() ([]int, bool) { return nil, false }

type oneBitmapIterator struct {
	chunk [types.CHUNK_SIZE]int
	last  int
	size  int
}

func (it *oneBitmapIterator) Close() {
	it.last = 0
	it.size = 0
}

func (it *oneBitmapIterator) Next() ([]int, bool) {
	if it.last+1 >= it.size {
		return nil, false
	}
	var (
		i int
		v = it.last + 1
	)
	for range 16 {
		it.chunk[i] = v
		it.chunk[i+1] = v + 1
		it.chunk[i+2] = v + 2
		it.chunk[i+3] = v + 3
		it.chunk[i+4] = v + 4
		it.chunk[i+5] = v + 5
		it.chunk[i+6] = v + 6
		it.chunk[i+7] = v + 7
		i += 8
		v += 8
	}
	n := min(128, it.size-it.last-1)
	it.last += n
	return it.chunk[:n], true
}
