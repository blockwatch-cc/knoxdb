// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"encoding/binary"
	"fmt"
	"iter"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
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
		Min:       cnt == len,
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
	return 1 + UvarintLen(c.N) + UvarintLen(len(c.Buf)) + len(c.Buf)
}

func (c *BitmapContainer) Store(dst []byte) []byte {
	dst = append(dst, byte(c.Typ))
	dst = binary.AppendUvarint(dst, uint64(c.N))
	dst = binary.AppendUvarint(dst, uint64(len(c.Buf)))
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
	v, n := binary.Uvarint(buf)
	c.N = int(v)
	buf = buf[n:]

	// buf len
	v, n = binary.Uvarint(buf)
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

func (c *BitmapContainer) Some() bool {
	switch c.Typ {
	case TBitmapZero:
		return false
	case TBitmapOne:
		return true
	case TBitmapDense:
		b := bitset.NewFromBytes(c.Buf, c.N)
		ok := b.Some()
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
		keys := vals.AllIndexes(arena.Alloc[uint32](n))
		c.Buf = xroar.NewFromSorted(keys).Bytes()
		arena.Free(keys)
	default:
		c.Typ = TBitmapDense
		c.Buf = vals.Bytes()
	}
	return c
}

func (c *BitmapContainer) Ones() iter.Seq[int] {
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
		return b.Ones()
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

func (c *BitmapContainer) Values() iter.Seq[bool] {
	switch c.Typ {
	case TBitmapOne:
		return func(fn func(bool) bool) {
			for range c.N {
				if !fn(true) {
					return
				}
			}
		}
	case TBitmapDense:
		return func(fn func(bool) bool) {
			b := bitset.NewFromBytes(c.Buf, c.N)
			b.Values()(fn)
			b.Close()
		}
	case TBitmapSparse:
		return func(fn func(bool) bool) {
			it := xroar.NewFromBytes(c.Buf).NewIterator()
			var i uint64
			for {
				n, ok := it.Next()
				if !ok {
					break
				}
				for i < n {
					if !fn(false) {
						break
					}
					i++
				}
				if !fn(true) {
					break
				}
				i++
			}
		}
	default:
		// TBitmapZero
		return func(fn func(bool) bool) {
			for range c.N {
				if !fn(false) {
					return
				}
			}
		}
	}
}

func (c *BitmapContainer) Chunks() bitset.BitmapIterator {
	switch c.Typ {
	case TBitmapOne:
		return newStaticBitmapIterator(c.N, true)
	case TBitmapDense:
		b := bitset.NewFromBytes(c.Buf, c.N)
		return b.Chunks()
	case TBitmapSparse:
		return newXroarBitmapIterator(xroar.NewFromBytes(c.Buf), c.N)
	default:
		// TBitmapZero
		return newStaticBitmapIterator(c.N, false)
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
func (*BitmapContainer) MatchInSet(_ any, _, _ *Bitset)    {}
func (*BitmapContainer) MatchNotInSet(_ any, _, _ *Bitset) {}

// special read-only bitmap function overrides
func (*BitmapContainer) Set(_ int)       {}
func (*BitmapContainer) Unset(_ int)     {}
func (*BitmapContainer) Writer() *Bitset { return nil }

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
	chunk [types.CHUNK_SIZE]bool
	set   *xroar.Bitmap
	len   int
	last  int
}

func newXroarBitmapIterator(set *xroar.Bitmap, n int) bitset.BitmapIterator {
	return &xroarBitmapIterator{
		set: set,
		len: n,
	}
}

func (it *xroarBitmapIterator) Len() int {
	return it.len
}

func (it *xroarBitmapIterator) Value(i int) bool {
	return it.set.Contains(uint64(i))
}

func (it *xroarBitmapIterator) Skip() int {
	n := min(types.CHUNK_SIZE, it.len-it.last)
	it.last += n
	return n
}

func (it *xroarBitmapIterator) Seek(n int) bool {
	if n < 0 || n >= it.len {
		it.last = it.len
		return false
	}
	it.last = n
	return true
}

func (it *xroarBitmapIterator) Next() (*[types.CHUNK_SIZE]bool, int) {
	if it.last >= it.len {
		return nil, 0
	}
	n := min(it.len-it.last, types.CHUNK_SIZE)
	for i := range n {
		it.chunk[i] = it.set.Contains(uint64(it.last))
		it.last++
	}
	return &it.chunk, n
}

func (it *xroarBitmapIterator) All() iter.Seq2[int, bool] {
	return func(yield func(int, bool) bool) {
		for i := range it.len {
			if !yield(i, it.set.Contains(uint64(i))) {
				return
			}
		}
	}
}

func (it *xroarBitmapIterator) Values() iter.Seq[bool] {
	return func(yield func(bool) bool) {
		for i := range it.len {
			if !yield(it.set.Contains(uint64(i))) {
				return
			}
		}
	}
}

func (it *xroarBitmapIterator) Select(sel []uint32) iter.Seq[bool] {
	return func(yield func(bool) bool) {
		for _, i := range sel {
			if !yield(it.set.Contains(uint64(i))) {
				return
			}
		}
	}
}

func (it *xroarBitmapIterator) Close() {
	it.set = nil
}

type staticBitmapIterator struct {
	chunk [types.CHUNK_SIZE]bool
	last  int
	len   int
}

func newStaticBitmapIterator(n int, val bool) bitset.BitmapIterator {
	it := &staticBitmapIterator{
		len: n,
	}
	for i := range it.chunk {
		it.chunk[i] = val
	}
	return it
}

func (it *staticBitmapIterator) Len() int {
	return it.len
}

func (it *staticBitmapIterator) Value(i int) bool {
	return it.chunk[0]
}

func (it *staticBitmapIterator) Next() (*[types.CHUNK_SIZE]bool, int) {
	if it.last >= it.len {
		return nil, 0
	}
	n := min(it.len-it.last, types.CHUNK_SIZE)
	it.last += n
	return &it.chunk, n
}

func (it *staticBitmapIterator) Seek(n int) bool {
	if n < 0 || n >= it.len {
		it.last = it.len
		return false
	}
	it.last = n
	return true
}

func (it *staticBitmapIterator) Skip() int {
	n := min(types.CHUNK_SIZE, it.len-it.last)
	it.last += n
	return n
}

func (it *staticBitmapIterator) Close() {
	it.len = 0
	it.last = 0
}

func (it *staticBitmapIterator) All() iter.Seq2[int, bool] {
	return func(yield func(int, bool) bool) {
		for i := range it.len {
			if !yield(i, it.chunk[0]) {
				return
			}
		}
	}
}

func (it *staticBitmapIterator) Values() iter.Seq[bool] {
	return func(yield func(bool) bool) {
		for range it.len {
			if !yield(it.chunk[0]) {
				return
			}
		}
	}
}

func (it *staticBitmapIterator) Select(sel []uint32) iter.Seq[bool] {
	return func(yield func(bool) bool) {
		for range sel {
			if !yield(it.chunk[0]) {
				return
			}
		}
	}
}
