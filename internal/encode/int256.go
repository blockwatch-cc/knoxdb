// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sync"

	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/pkg/num"
)

type Int256Container struct {
	X0 IntegerContainer[int64]
	X1 IntegerContainer[uint64]
	X2 IntegerContainer[uint64]
	X3 IntegerContainer[uint64]
}

// NewInt256 creates a new 256bit integer container.
func NewInt256() *Int256Container {
	return newInt256Container()
}

// EncodeInt256 encodes a strided 256-bit integer vector
// selecting the most efficient encoding schemes per stride.
func EncodeInt256(v num.Int256Stride) *Int256Container {
	return NewInt256().Encode(v)
}

// LoadInt256 loads a 256bit integer container from buffer.
func LoadInt256(buf []byte) (*Int256Container, error) {
	c := NewInt256()
	if _, err := c.Load(buf); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Int256Container) Info() string {
	return fmt.Sprintf("i256_[%s]_[%s]_[%s]_[%s]", c.X0.Info(), c.X1.Info(), c.X2.Info(), c.X3.Info())
}

func (c *Int256Container) Close() {
	c.X0.Close()
	c.X1.Close()
	c.X2.Close()
	c.X3.Close()
	c.X0 = nil
	c.X1 = nil
	c.X2 = nil
	c.X3 = nil
	putInt256Container(c)
}

func (c *Int256Container) Type() IntegerContainerType {
	return TInteger256
}

func (c *Int256Container) Len() int {
	return c.X0.Len()
}

func (c *Int256Container) Size() int {
	return 1 + c.X0.Size() + c.X1.Size() + c.X2.Size() + c.X3.Size()
}

func (c *Int256Container) Store(dst []byte) []byte {
	dst = append(dst, byte(TInteger256))
	dst = c.X0.Store(dst)
	dst = c.X1.Store(dst)
	dst = c.X2.Store(dst)
	return c.X3.Store(dst)
}

func (c *Int256Container) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TInteger256) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	// alloc and decode child containers
	var err error
	c.X0 = NewInt[int64](IntegerContainerType(buf[0]))
	buf, err = c.X0.Load(buf)
	if err != nil {
		return buf, err
	}

	c.X1 = NewInt[uint64](IntegerContainerType(buf[0]))
	buf, err = c.X1.Load(buf)
	if err != nil {
		return buf, err
	}

	c.X2 = NewInt[uint64](IntegerContainerType(buf[0]))
	buf, err = c.X2.Load(buf)
	if err != nil {
		return buf, err
	}

	c.X3 = NewInt[uint64](IntegerContainerType(buf[0]))
	return c.X3.Load(buf)
}

func (c *Int256Container) Get(n int) num.Int256 {
	return num.Int256{uint64(c.X0.Get(n)), c.X1.Get(n), c.X2.Get(n), c.X3.Get(n)}
}

func (c *Int256Container) AppendTo(sel []uint32, dst num.Int256Stride) num.Int256Stride {
	dst.X0 = c.X0.AppendTo(sel, dst.X0[:0])
	dst.X1 = c.X1.AppendTo(sel, dst.X1[:0])
	dst.X2 = c.X2.AppendTo(sel, dst.X2[:0])
	dst.X3 = c.X3.AppendTo(sel, dst.X3[:0])
	return dst
}

func (c *Int256Container) Encode(vals num.Int256Stride) *Int256Container {
	c.X0 = EncodeInt(nil, vals.X0, MAX_CASCADE-1)
	c.X1 = EncodeInt(nil, vals.X1, MAX_CASCADE-1)
	c.X2 = EncodeInt(nil, vals.X2, MAX_CASCADE-1)
	c.X3 = EncodeInt(nil, vals.X3, MAX_CASCADE-1)
	return c
}

func (c *Int256Container) MatchEqual(val num.Int256, bits, mask *Bitset) {
	// iterator based
	c.match(cmp.Int256Equal, val, bits, mask)
}

func (c *Int256Container) MatchNotEqual(val num.Int256, bits, mask *Bitset) {
	// iterator based
	c.match(cmp.Int256NotEqual, val, bits, mask)
}

func (c *Int256Container) MatchLess(val num.Int256, bits, mask *Bitset) {
	// iterator based
	c.match(cmp.Int256Less, val, bits, mask)
}

func (c *Int256Container) MatchLessEqual(val num.Int256, bits, mask *Bitset) {
	// iterator based
	c.match(cmp.Int256LessEqual, val, bits, mask)
}

func (c *Int256Container) MatchGreater(val num.Int256, bits, mask *Bitset) {
	// iterator based
	c.match(cmp.Int256Greater, val, bits, mask)
}

func (c *Int256Container) MatchGreaterEqual(val num.Int256, bits, mask *Bitset) {
	// iterator based
	c.match(cmp.Int256GreaterEqual, val, bits, mask)
}

func (c *Int256Container) MatchBetween(a, b num.Int256, bits, mask *Bitset) {
	// iterator based
	c.matchRange(cmp.Int256Between, a, b, bits, mask)
}

func (c *Int256Container) MatchInSet(_ any, _, _ *Bitset) {
	// N.A.
}

func (c *Int256Container) MatchNotInSet(_ any, _, _ *Bitset) {
	// N.A.
}

func (c *Int256Container) match(cmpFn I256MatchFunc, val num.Int256, bits, mask *Bitset) {
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

func (c *Int256Container) matchRange(cmpFn I256RangeMatchFunc, a, b num.Int256, bits, mask *Bitset) {
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

type Int256Factory struct {
	cPool  sync.Pool // container pool
	itPool sync.Pool // iterator pool
}

func newInt256Container() *Int256Container {
	return i256Factory.cPool.Get().(*Int256Container)
}

func putInt256Container(c *Int256Container) {
	i256Factory.cPool.Put(c)
}

func newInt256Iterator() *Int256Iterator {
	return i256Factory.itPool.Get().(*Int256Iterator)
}

func putInt256Iterator(c *Int256Iterator) {
	i256Factory.itPool.Put(c)
}

var i256Factory = Int256Factory{
	cPool:  sync.Pool{New: func() any { return new(Int256Container) }},
	itPool: sync.Pool{New: func() any { return new(Int256Iterator) }},
}

func (c *Int256Container) Iterator() *Int256Iterator {
	return NewInt256Iterator(c.X0.Iterator(), c.X1.Iterator(), c.X2.Iterator(), c.X3.Iterator())
}

type Int256Iterator struct {
	chunk num.Int256Stride
	x0    Iterator[int64]
	x1    Iterator[uint64]
	x2    Iterator[uint64]
	x3    Iterator[uint64]
	base  int
	len   int
	ofs   int
}

func NewInt256Iterator(x0 Iterator[int64], x1, x2, x3 Iterator[uint64]) *Int256Iterator {
	it := newInt256Iterator()
	it.x0 = x0
	it.x1 = x1
	it.x2 = x2
	it.x3 = x3
	it.base = -1
	it.len = x0.Len()
	return it
}

func (it *Int256Iterator) Close() {
	it.chunk.X0 = nil
	it.chunk.X1 = nil
	it.x0.Close()
	it.x0 = nil
	it.x1.Close()
	it.x1 = nil
	it.x2.Close()
	it.x2 = nil
	it.x3.Close()
	it.x3 = nil
	it.base = 0
	it.len = 0
	it.ofs = 0
	putInt256Iterator(it)
}

func (it *Int256Iterator) Reset() {
	it.ofs = 0
}

func (it *Int256Iterator) Len() int {
	return it.len
}

func (it *Int256Iterator) Get(n int) num.Int256 {
	if n < 0 || n >= it.len {
		return num.ZeroInt256
	}
	if base := chunkBase(n); base != it.base {
		it.fill(base)
	}
	return it.chunk.Elem(chunkPos(n))
}

func (it *Int256Iterator) Next() (num.Int256, bool) {
	if it.ofs >= it.len {
		// EOF
		return num.ZeroInt256, false
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

func (it *Int256Iterator) NextChunk() (*num.Int256Stride, int) {
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

func (it *Int256Iterator) SkipChunk() int {
	n := min(CHUNK_SIZE, it.len-it.ofs)
	it.ofs += n
	return n
}

func (it *Int256Iterator) Seek(n int) bool {
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

func (it *Int256Iterator) fill(base int) int {
	// load chunks at base and relink into stride
	it.x0.Seek(base)
	x0, n := it.x0.NextChunk()
	it.x1.Seek(base)
	x1, m := it.x1.NextChunk()
	it.x2.Seek(base)
	x2, o := it.x2.NextChunk()
	it.x3.Seek(base)
	x3, p := it.x3.NextChunk()

	if n != m || n != o || n != p {
		panic(fmt.Errorf("i256-it: unexpected base it fill [%d,%d,%d,%d]", n, m, o, p))
	}
	if n == 0 {
		it.ofs = it.len
		it.base = -1
		return 0
	}

	it.chunk.X0 = x0[:n]
	it.chunk.X1 = x1[:n]
	it.chunk.X2 = x2[:n]
	it.chunk.X3 = x3[:n]

	it.base = base
	return n
}
