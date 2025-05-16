// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sync"

	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/pkg/num"
)

type Int128Container struct {
	X0 IntegerContainer[int64]
	X1 IntegerContainer[uint64]
}

// NewInt128 creates a new 128bit integer container.
func NewInt128() *Int128Container {
	return newInt128Container()
}

// EncodeInt128 encodes a strided 128-bit integer vector
// selecting the most efficient encoding schemes per stride.
func EncodeInt128(v num.Int128Stride) *Int128Container {
	return NewInt128().Encode(v)
}

// LoadInt128 loads a 128bit integer container from buffer.
func LoadInt128(buf []byte) (*Int128Container, error) {
	c := NewInt128()
	if _, err := c.Load(buf); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Int128Container) Info() string {
	return fmt.Sprintf("i128_[%s]_[%s]", c.X0.Info(), c.X1.Info())
}

func (c *Int128Container) Close() {
	c.X0.Close()
	c.X1.Close()
	c.X0 = nil
	c.X1 = nil
	putInt128Container(c)
}

func (c *Int128Container) Type() IntegerContainerType {
	return TInteger128
}

func (c *Int128Container) Len() int {
	return c.X0.Len()
}

func (c *Int128Container) Size() int {
	return 1 + c.X0.Size() + c.X1.Size()
}

func (c *Int128Container) Store(dst []byte) []byte {
	dst = append(dst, byte(TInteger128))
	dst = c.X0.Store(dst)
	return c.X1.Store(dst)
}

func (c *Int128Container) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TInteger128) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	// alloc and decode child containers
	c.X0 = NewInt[int64](IntegerContainerType(buf[0]))
	var err error
	buf, err = c.X0.Load(buf)
	if err != nil {
		return buf, err
	}

	c.X1 = NewInt[uint64](IntegerContainerType(buf[0]))
	return c.X1.Load(buf)
}

func (c *Int128Container) Get(n int) num.Int128 {
	return num.Int128{uint64(c.X0.Get(n)), c.X1.Get(n)}
}

func (c *Int128Container) AppendTo(sel []uint32, dst num.Int128Stride) num.Int128Stride {
	dst.X0 = c.X0.AppendTo(sel, dst.X0[:0])
	dst.X1 = c.X1.AppendTo(sel, dst.X1[:0])
	return dst
}

func (c *Int128Container) Encode(vals num.Int128Stride) *Int128Container {
	c.X0 = EncodeInt(nil, vals.X0, MAX_CASCADE-1)
	c.X1 = EncodeInt(nil, vals.X1, MAX_CASCADE-1)
	return c
}

func (c *Int128Container) MatchEqual(val num.Int128, bits, mask *Bitset) {
	// iterator based
	c.match(cmp.Int128Equal, val, bits, mask)
}

func (c *Int128Container) MatchNotEqual(val num.Int128, bits, mask *Bitset) {
	// iterator based
	c.match(cmp.Int128NotEqual, val, bits, mask)
}

func (c *Int128Container) MatchLess(val num.Int128, bits, mask *Bitset) {
	// iterator based
	c.match(cmp.Int128Less, val, bits, mask)
}

func (c *Int128Container) MatchLessEqual(val num.Int128, bits, mask *Bitset) {
	// iterator based
	c.match(cmp.Int128LessEqual, val, bits, mask)
}

func (c *Int128Container) MatchGreater(val num.Int128, bits, mask *Bitset) {
	// iterator based
	c.match(cmp.Int128Greater, val, bits, mask)
}

func (c *Int128Container) MatchGreaterEqual(val num.Int128, bits, mask *Bitset) {
	// iterator based
	c.match(cmp.Int128GreaterEqual, val, bits, mask)
}

func (c *Int128Container) MatchBetween(a, b num.Int128, bits, mask *Bitset) {
	// iterator based
	c.matchRange(cmp.Int128Between, a, b, bits, mask)
}

func (c *Int128Container) MatchInSet(_ any, _, _ *Bitset) {
	// N.A.
}

func (c *Int128Container) MatchNotInSet(_ any, _, _ *Bitset) {
	// N.A.
}

func (c *Int128Container) match(cmpFn I128MatchFunc, val num.Int128, bits, mask *Bitset) {
	var (
		i   int
		cnt int64
		buf = bits.Bytes()
		it  = c.Iterator()
	)

	for {
		// check mask and skip chunks if not required
		if mask != nil && !mask.ContainsRange(i, i+CHUNK_SIZE-1) {
			n := it.SkipChunk()
			i += n
			if i >= it.Len() {
				break
			}
		}

		// get next chunk, on tail n may be < CHUNK_SZIE
		src, n := it.NextChunk()
		if n == 0 {
			break
		}

		// compare
		if mask != nil {
			cnt += cmpFn(*src, val, buf[i>>3:], mask.Bytes()[i>>3:])
		} else {
			cnt += cmpFn(*src, val, buf[i>>3:], nil)
		}

		i += n
	}
	bits.ResetCount(int(cnt))
	it.Close()
}

func (c *Int128Container) matchRange(cmpFn I128RangeMatchFunc, a, b num.Int128, bits, mask *Bitset) {
	var (
		i   int
		cnt int64
		buf = bits.Bytes()
		it  = c.Iterator()
	)

	for {
		// check mask and skip chunks if not required
		if mask != nil && !mask.ContainsRange(i, i+CHUNK_SIZE-1) {
			n := it.SkipChunk()
			i += n
			if i >= it.Len() {
				break
			}
		}

		// get next chunk, on tail n may be < CHUNK_SZIE
		src, n := it.NextChunk()
		if n == 0 {
			break
		}

		// compare
		if mask != nil {
			cnt += cmpFn(*src, a, b, buf[i>>3:], mask.Bytes()[i>>3:])
		} else {
			cnt += cmpFn(*src, a, b, buf[i>>3:], nil)
		}

		i += n
	}
	bits.ResetCount(int(cnt))
	it.Close()
}

type Int128Factory struct {
	cPool  sync.Pool // container pool
	itPool sync.Pool // iterator pool
}

func newInt128Container() *Int128Container {
	return i128Factory.cPool.Get().(*Int128Container)
}

func putInt128Container(c *Int128Container) {
	i128Factory.cPool.Put(c)
}

func newInt128Iterator() *Int128Iterator {
	return i128Factory.itPool.Get().(*Int128Iterator)
}

func putInt128Iterator(c *Int128Iterator) {
	i128Factory.itPool.Put(c)
}

var i128Factory = Int128Factory{
	cPool:  sync.Pool{New: func() any { return new(Int128Container) }},
	itPool: sync.Pool{New: func() any { return new(Int128Iterator) }},
}

func (c *Int128Container) Iterator() *Int128Iterator {
	return NewInt128Iterator(c.X0.Iterator(), c.X1.Iterator())
}

type Int128Iterator struct {
	chunk num.Int128Stride
	x0    Iterator[int64]
	x1    Iterator[uint64]
	base  int
	len   int
	ofs   int
}

func NewInt128Iterator(x0 Iterator[int64], x1 Iterator[uint64]) *Int128Iterator {
	it := newInt128Iterator()
	it.x0 = x0
	it.x1 = x1
	it.base = -1
	it.len = x0.Len()
	return it
}

func (it *Int128Iterator) Close() {
	it.chunk.X0 = nil
	it.chunk.X1 = nil
	it.x0.Close()
	it.x0 = nil
	it.x1.Close()
	it.x1 = nil
	it.base = 0
	it.len = 0
	it.ofs = 0
	putInt128Iterator(it)
}

func (it *Int128Iterator) Reset() {
	it.ofs = 0
}

func (it *Int128Iterator) Len() int {
	return it.len
}

func (it *Int128Iterator) Get(n int) num.Int128 {
	if n < 0 || n >= it.len {
		return num.ZeroInt128
	}
	if base := chunkBase(n); base != it.base {
		it.fill(base)
	}
	return it.chunk.Elem(chunkPos(n))
}

func (it *Int128Iterator) Next() (num.Int128, bool) {
	if it.ofs >= it.len {
		// EOF
		return num.ZeroInt128, false
	}

	// refill on chunk boundary
	if base := chunkBase(it.ofs); base != it.base {
		it.fill(base)
	}
	i := chunkPos(it.ofs)

	// advance ofs for next call
	it.ofs++

	// return calculated value
	return it.chunk.Elem(i), true
}

func (it *Int128Iterator) NextChunk() (*num.Int128Stride, int) {
	// EOF
	if it.ofs >= it.len {
		return nil, 0
	}

	// refill (considering seek/skip/reset state updates)
	n := min(CHUNK_SIZE, it.len-it.base)
	if base := chunkBase(it.ofs); base != it.base {
		n = it.fill(base)
	}
	it.ofs = it.base + n

	return &it.chunk, n
}

func (it *Int128Iterator) SkipChunk() int {
	n := min(CHUNK_SIZE, it.len-it.ofs)
	it.ofs += n
	return n
}

func (it *Int128Iterator) Seek(n int) bool {
	if n < 0 || n >= it.len {
		it.ofs = it.len
		return false
	}

	// fill on seek to an unloaded chunk
	if base := chunkBase(n); base != it.base {
		it.fill(base)
	}

	// reset ofs to n, so call to Next() delivers value
	it.ofs = n
	return true
}

func (it *Int128Iterator) fill(base int) int {
	// load chunks at base and relink into stride
	it.x0.Seek(base)
	x0, n := it.x0.NextChunk()
	it.x1.Seek(base)
	x1, m := it.x1.NextChunk()

	if n != m {
		panic(fmt.Errorf("i128-it: unexpected base it fill n=%d m=%d", n, m))
	}
	if n == 0 {
		it.ofs = it.len
		it.base = -1
		return 0
	}

	it.chunk.X0 = x0[:n]
	it.chunk.X1 = x1[:n]

	it.base = base
	return n
}
