// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"iter"
	"sync"

	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// ensure we implement required interfaces
var _ num.BigIntAccessor[num.Int256, num.Int256Stride] = (*Int256Container)(nil)

type Int256Context struct {
	Min       num.Int256
	Max       num.Int256
	NumValues int
}

// AnalyzeInt256 produces statistics
func AnalyzeInt256(v *num.Int256Stride) *Int256Context {
	minv, maxv := v.MinMax()
	return &Int256Context{
		Min:       minv,
		Max:       maxv,
		NumValues: v.Len(),
	}
}

func (c *Int256Context) Close() {}

func (c *Int256Context) MinMax() (any, any) {
	return c.Min, c.Max
}

func (c *Int256Context) Unique() int {
	return -1
}

type Int256Container struct {
	readOnlyContainer[num.Int256]
	X0 NumberContainer[int64]
	X1 NumberContainer[uint64]
	X2 NumberContainer[uint64]
	X3 NumberContainer[uint64]
}

// NewInt256 creates a new 256bit integer container.
func NewInt256() *Int256Container {
	return newInt256Container()
}

// EncodeInt256 encodes a strided 256-bit integer vector
// selecting the most efficient encoding schemes per stride.
func EncodeInt256(ctx *Int256Context, v *num.Int256Stride) *Int256Container {
	return NewInt256().Encode(ctx, v)
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

func (c *Int256Container) Type() ContainerType {
	return TInt256
}

func (c *Int256Container) Len() int {
	return c.X0.Len()
}

func (c *Int256Container) Size() int {
	return 1 + c.X0.Size() + c.X1.Size() + c.X2.Size() + c.X3.Size()
}

func (c *Int256Container) All() iter.Seq2[int, num.Int256] {
	return func(yield func(int, num.Int256) bool) {
		it := c.Chunks()
		it.All()(yield)
		it.Close()
	}
}

func (c *Int256Container) Values() iter.Seq[num.Int256] {
	return func(yield func(num.Int256) bool) {
		it := c.Chunks()
		it.Values()(yield)
		it.Close()
	}
}

func (c *Int256Container) Chunks() num.BigIntIterator[num.Int256] {
	return NewInt256Iterator(c)
}

func (c *Int256Container) Slice() *num.Int256Stride {
	return nil
}

func (c *Int256Container) Store(dst []byte) []byte {
	dst = append(dst, byte(TInt256))
	dst = c.X0.Store(dst)
	dst = c.X1.Store(dst)
	dst = c.X2.Store(dst)
	return c.X3.Store(dst)
}

func (c *Int256Container) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TInt256) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	// alloc and decode child containers
	var err error
	c.X0 = NewInt[int64](ContainerType(buf[0]))
	buf, err = c.X0.Load(buf)
	if err != nil {
		return buf, err
	}

	c.X1 = NewInt[uint64](ContainerType(buf[0]))
	buf, err = c.X1.Load(buf)
	if err != nil {
		return buf, err
	}

	c.X2 = NewInt[uint64](ContainerType(buf[0]))
	buf, err = c.X2.Load(buf)
	if err != nil {
		return buf, err
	}

	c.X3 = NewInt[uint64](ContainerType(buf[0]))
	return c.X3.Load(buf)
}

func (c *Int256Container) Get(n int) num.Int256 {
	return num.Int256{uint64(c.X0.Get(n)), c.X1.Get(n), c.X2.Get(n), c.X3.Get(n)}
}

func (c *Int256Container) AppendTo(v num.BigIntWriter[num.Int256], sel []uint32) {
	dst := v.(*num.Int256Stride)
	dst.X0 = c.X0.AppendTo(dst.X0[:0], sel)
	dst.X1 = c.X1.AppendTo(dst.X1[:0], sel)
	dst.X2 = c.X2.AppendTo(dst.X2[:0], sel)
	dst.X3 = c.X3.AppendTo(dst.X3[:0], sel)
}

func (c *Int256Container) Encode(ctx *Int256Context, vals *num.Int256Stride) *Int256Container {
	bl := 256
	if ctx != nil {
		bl = max(ctx.Max.BitLen(), ctx.Min.BitLen())
	}
	if bl <= 192 {
		vctx := NewIntContext[int64](0, 0, vals.Len())
		c.X0 = EncodeInt(vctx, vals.X0)
		vctx.Close()
	} else {
		c.X0 = EncodeInt(nil, vals.X0)
	}
	if bl <= 128 {
		vctx := NewIntContext[uint64](0, 0, vals.Len())
		c.X1 = EncodeInt(vctx, vals.X1)
		vctx.Close()
	} else {
		c.X1 = EncodeInt(nil, vals.X1)
	}
	if bl <= 64 {
		vctx := NewIntContext[uint64](0, 0, vals.Len())
		c.X2 = EncodeInt(vctx, vals.X2)
		vctx.Close()
	} else {
		c.X2 = EncodeInt(nil, vals.X2)
	}
	c.X3 = EncodeInt(nil, vals.X3)
	return c
}

func (c *Int256Container) Matcher() num.BigIntMatcher[num.Int256] {
	return c
}

func (c *Int256Container) Cmp(i, j int) int {
	return num.Compare256(c.Get(i), c.Get(j))
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
		it  = NewInt256StrideIterator(c)
	)

	for {
		// check mask and skip chunks if not required
		if mask != nil && !mask.ContainsRange(i, i+CHUNK_SIZE-1) {
			n := it.Skip()
			i += n
			if i >= it.Len() {
				break
			}
		}

		// get next chunk, on tail n may be < CHUNK_SZIE
		src, n := it.Next()
		if n == 0 {
			break
		}

		// compare
		if mask != nil {
			cnt += cmpFn(src, val, buf[i>>3:], mask.Bytes()[i>>3:])
		} else {
			cnt += cmpFn(src, val, buf[i>>3:], nil)
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
		it  = NewInt256StrideIterator(c)
	)

	for {
		// check mask and skip chunks if not required
		if mask != nil && !mask.ContainsRange(i, i+CHUNK_SIZE-1) {
			n := it.Skip()
			i += n
			if i >= it.Len() {
				break
			}
		}

		// get next chunk, on tail n may be < CHUNK_SZIE
		src, n := it.Next()
		if n == 0 {
			break
		}

		// compare
		if mask != nil {
			cnt += cmpFn(src, a, b, buf[i>>3:], mask.Bytes()[i>>3:])
		} else {
			cnt += cmpFn(src, a, b, buf[i>>3:], nil)
		}

		i += n
	}
	bits.ResetCount(int(cnt))
	it.Close()
}

type Int256Factory struct {
	cPool   sync.Pool // container pool
	itPool  sync.Pool // iterator pool
	sitPool sync.Pool // stride iterator pool
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

func newInt256StrideIterator() *Int256StrideIterator {
	return i256Factory.sitPool.Get().(*Int256StrideIterator)
}

func putInt256StrideIterator(c *Int256StrideIterator) {
	i256Factory.sitPool.Put(c)
}

var i256Factory = Int256Factory{
	cPool:   sync.Pool{New: func() any { return new(Int256Container) }},
	itPool:  sync.Pool{New: func() any { return new(Int256Iterator) }},
	sitPool: sync.Pool{New: func() any { return new(Int256StrideIterator) }},
}

// Stride iterator (use for matching)
type Int256StrideIterator struct {
	chunk num.Int256Stride
	x0    types.NumberIterator[int64]
	x1    types.NumberIterator[uint64]
	x2    types.NumberIterator[uint64]
	x3    types.NumberIterator[uint64]
	base  int
	len   int
	ofs   int
}

func NewInt256StrideIterator(c *Int256Container) *Int256StrideIterator {
	it := newInt256StrideIterator()
	it.x0 = c.X0.Chunks()
	it.x1 = c.X1.Chunks()
	it.x2 = c.X2.Chunks()
	it.x3 = c.X3.Chunks()
	it.base = -1
	it.len = c.Len()
	return it
}

func (it *Int256StrideIterator) Close() {
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
	putInt256StrideIterator(it)
}

func (it *Int256StrideIterator) Len() int {
	return it.len
}

func (it *Int256StrideIterator) Value(n int) num.Int256 {
	if n < 0 || n >= it.len {
		return num.ZeroInt256
	}
	if base := chunkBase(n); base != it.base {
		it.fill(base)
	}
	return it.chunk.Get(chunkPos(n))
}

func (it *Int256StrideIterator) Next() (*num.Int256Stride, int) {
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

func (it *Int256StrideIterator) Skip() int {
	n := min(CHUNK_SIZE, it.len-it.ofs)
	it.ofs += n
	return n
}

func (it *Int256StrideIterator) Seek(n int) bool {
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

func (it *Int256StrideIterator) fill(base int) int {
	// load chunks at base and relink into stride
	it.x0.Seek(base)
	x0, n := it.x0.Next()
	it.x1.Seek(base)
	x1, m := it.x1.Next()
	it.x2.Seek(base)
	x2, o := it.x2.Next()
	it.x3.Seek(base)
	x3, p := it.x3.Next()

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

// Int256 iterator
type Int256Iterator struct {
	BaseIterator[num.Int256]
	x0 types.NumberIterator[int64]
	x1 types.NumberIterator[uint64]
	x2 types.NumberIterator[uint64]
	x3 types.NumberIterator[uint64]
}

func NewInt256Iterator(c *Int256Container) *Int256Iterator {
	it := newInt256Iterator()
	it.x0 = c.X0.Chunks()
	it.x1 = c.X1.Chunks()
	it.x2 = c.X2.Chunks()
	it.x3 = c.X3.Chunks()
	it.base = -1
	it.len = c.Len()
	it.BaseIterator.fill = it.fill
	return it
}

func (it *Int256Iterator) Close() {
	it.x0.Close()
	it.x0 = nil
	it.x1.Close()
	it.x1 = nil
	it.x2.Close()
	it.x2 = nil
	it.x3.Close()
	it.x3 = nil
	it.BaseIterator.Close()
	putInt256Iterator(it)
}

func (it *Int256Iterator) fill(base int) int {
	// load chunks at base and relink into stride
	it.x0.Seek(base)
	x0, n := it.x0.Next()
	it.x1.Seek(base)
	x1, m := it.x1.Next()
	it.x2.Seek(base)
	x2, o := it.x2.Next()
	it.x3.Seek(base)
	x3, p := it.x3.Next()

	if n != m || n != o || n != p {
		panic(fmt.Errorf("i256-it: unexpected base it fill [%d,%d,%d,%d]", n, m, o, p))
	}
	if n == 0 {
		it.ofs = it.len
		it.base = -1
		return 0
	}

	stride := num.Int256Stride{
		X0: x0[:n],
		X1: x1[:n],
		X2: x2[:n],
		X3: x3[:n],
	}

	var i int
	for range n / 16 {
		it.chunk[i] = stride.Get(i)
		it.chunk[i+1] = stride.Get(i + 1)
		it.chunk[i+2] = stride.Get(i + 2)
		it.chunk[i+3] = stride.Get(i + 3)
		it.chunk[i+4] = stride.Get(i + 4)
		it.chunk[i+5] = stride.Get(i + 5)
		it.chunk[i+6] = stride.Get(i + 6)
		it.chunk[i+7] = stride.Get(i + 7)
		it.chunk[i+8] = stride.Get(i + 8)
		it.chunk[i+9] = stride.Get(i + 9)
		it.chunk[i+10] = stride.Get(i + 10)
		it.chunk[i+11] = stride.Get(i + 11)
		it.chunk[i+12] = stride.Get(i + 12)
		it.chunk[i+13] = stride.Get(i + 13)
		it.chunk[i+14] = stride.Get(i + 14)
		it.chunk[i+15] = stride.Get(i + 15)
		i += 16
	}
	for i < n {
		it.chunk[i] = stride.Get(i)
		i++
	}

	it.base = base
	return n
}
