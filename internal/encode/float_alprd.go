// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/alp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// TFloatAlpRd
type FloatAlpRdContainer[T types.Float] struct {
	Left  IntegerContainer[uint16]
	Right IntegerContainer[uint64]
	Shift int
	typ   types.BlockType
}

func (c *FloatAlpRdContainer[T]) Info() string {
	return fmt.Sprintf("ALP-RD(%s)_[>>%d]_[%s]_[%s]",
		TypeName[T](), c.Shift, c.Left.Info(), c.Right.Info())
}

func (c *FloatAlpRdContainer[T]) Close() {
	c.Left.Close()
	c.Right.Close()
	c.Left = nil
	c.Right = nil
	putFloatAlpRdContainer(c)
}

func (c *FloatAlpRdContainer[T]) Type() FloatContainerType {
	return TFloatAlpRd
}

func (c *FloatAlpRdContainer[T]) Len() int {
	return c.Left.Len()
}

func (c *FloatAlpRdContainer[T]) Size() int {
	v := 2 + c.Left.Size() + c.Right.Size()
	return v
}

func (c *FloatAlpRdContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatAlpRd))
	dst = c.Left.Store(dst)
	dst = c.Right.Store(dst)
	dst = num.AppendUvarint(dst, uint64(c.Shift))
	return dst
}

func (c *FloatAlpRdContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatAlpRd) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	c.Left = NewInt[uint16](IntegerContainerType(buf[0]))
	buf, err := c.Left.Load(buf)
	if err != nil {
		return buf, err
	}
	c.Right = NewInt[uint64](IntegerContainerType(buf[0]))
	buf, err = c.Right.Load(buf)
	if err != nil {
		return buf, err
	}
	v, n := num.Uvarint(buf)
	c.Shift = int(v)
	c.typ = BlockType[T]()
	return buf[n:], nil
}

func (c *FloatAlpRdContainer[T]) Get(n int) T {
	left := c.Left.Get(n)
	right := c.Right.Get(n)

	// float64
	if c.typ == types.BlockFloat64 {
		v := uint64(left)<<c.Shift | right
		return *(*T)(unsafe.Pointer(&v))
	}

	// float32
	v := uint32(left)<<c.Shift | uint32(right)
	return *(*T)(unsafe.Pointer(&v))
}

func (c *FloatAlpRdContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	if sel == nil {
		for i := range c.Len() {
			dst = append(dst, c.Get(i))
		}
	} else {
		for _, v := range sel {
			dst = append(dst, c.Get(int(v)))
		}
	}
	return dst
}

func (c *FloatAlpRdContainer[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	cnt := len(vals)
	left := arena.Alloc[uint16](cnt)[:cnt]
	right := arena.Alloc[uint64](cnt)[:cnt]
	c.typ = BlockType[T]()

	// ensure we have an ALP analysis result (mostly relevant for testcases)
	if !ctx.AlpEncoder.IsInit() || ctx.AlpEncoder.State().Scheme != alp.AlpRdScheme {
		// produce a small sample
		sample := arena.Alloc[T](alp.MaxSampleLen(cnt))
		alp.FirstLevelSample(sample, vals)

		// estimate best shift based on sample
		unique := arena.Alloc[uint16](1 << 16)[:1<<16]
		c.Shift = alp.EstimateRD(sample, unique).Shift
		arena.Free(unique)
		arena.Free(sample)
	} else {
		c.Shift = ctx.AlpEncoder.State().RD.Shift
	}

	// split input float vector into left and right integer parts
	alp.SplitRD(vals, left, right, c.Shift)

	// TODO
	// - left: always use dict, analyze unique during split (build array)
	// - right: always BP, aggregate min/max during split
	// - skip int analyze and encode direct
	// - fusion: right always bitpack to max width during split

	// analyze parts
	lctx := AnalyzeInt(left, true)
	rctx := AnalyzeInt(right, false)

	// prefer left side dict compression when more efficient than bit-packing
	leftScheme := TIntegerBitpacked
	if lctx.preferDict() {
		leftScheme = TIntegerDictionary
	}

	// encode parts
	c.Left = NewInt[uint16](leftScheme).Encode(lctx, left, lvl-1)
	c.Right = NewInt[uint64](TIntegerBitpacked).Encode(rctx, right, lvl-1)

	// free temp allocations
	lctx.Close()
	rctx.Close()
	arena.Free(left)
	arena.Free(right)

	return c
}

func (c *FloatAlpRdContainer[T]) MatchEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeEqual), val, bits, mask)
}

func (c *FloatAlpRdContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeNotEqual), val, bits, mask)
}

func (c *FloatAlpRdContainer[T]) MatchLess(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeLt), val, bits, mask)
}

func (c *FloatAlpRdContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeLe), val, bits, mask)
}

func (c *FloatAlpRdContainer[T]) MatchGreater(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeGt), val, bits, mask)
}

func (c *FloatAlpRdContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	matchIt(c.Iterator(), matchFn[T](types.FilterModeGe), val, bits, mask)
}

func (c *FloatAlpRdContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) {
	matchRangeIt(c.Iterator(), matchFn[T](types.FilterModeRange), a, b, bits, mask)
}

// N.A.
func (c *FloatAlpRdContainer[T]) MatchInSet(_ any, _, _ *Bitset)    {}
func (c *FloatAlpRdContainer[T]) MatchNotInSet(_ any, _, _ *Bitset) {}

type FloatAlpRdFactory struct {
	f64Pool   sync.Pool
	f32Pool   sync.Pool
	f64ItPool sync.Pool
	f32ItPool sync.Pool
}

func newFloatAlpRdContainer[T types.Float]() FloatContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpRdFactory.f64Pool.Get().(FloatContainer[T])
	case float32:
		return floatAlpRdFactory.f32Pool.Get().(FloatContainer[T])
	default:
		return nil
	}
}

func putFloatAlpRdContainer[T types.Float](c FloatContainer[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpRdFactory.f64Pool.Put(c)
	case float32:
		floatAlpRdFactory.f32Pool.Put(c)
	}
}

func newFloatAlpRdIterator[T types.Float]() *FloatAlpRdIterator[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpRdFactory.f64ItPool.Get().(*FloatAlpRdIterator[T])
	case float32:
		return floatAlpRdFactory.f32ItPool.Get().(*FloatAlpRdIterator[T])
	default:
		return nil
	}
}

func putFloatAlpRdIterator[T types.Float](c *FloatAlpRdIterator[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpRdFactory.f64ItPool.Put(c)
	case float32:
		floatAlpRdFactory.f32ItPool.Put(c)
	}
}

var floatAlpRdFactory = FloatAlpRdFactory{
	f64Pool:   sync.Pool{New: func() any { return new(FloatAlpRdContainer[float64]) }},
	f32Pool:   sync.Pool{New: func() any { return new(FloatAlpRdContainer[float32]) }},
	f64ItPool: sync.Pool{New: func() any { return new(FloatAlpRdIterator[float64]) }},
	f32ItPool: sync.Pool{New: func() any { return new(FloatAlpRdIterator[float32]) }},
}

func (c *FloatAlpRdContainer[T]) Iterator() Iterator[T] {
	it := newFloatAlpRdIterator[T]()
	it.leftIt = c.Left.Iterator()
	it.rightIt = c.Right.Iterator()
	it.shift = c.Shift
	it.len = c.Len()
	it.ofs = 0
	return it
}

type FloatAlpRdIterator[T types.Float] struct {
	vals    [CHUNK_SIZE]T
	leftIt  Iterator[uint16]
	rightIt Iterator[uint64]
	shift   int
	len     int
	ofs     int
}

func (it *FloatAlpRdIterator[T]) Close() {
	it.leftIt.Close()
	it.rightIt.Close()
	it.leftIt = nil
	it.rightIt = nil
	it.shift = 0
	it.len = 0
	it.ofs = 0
	putFloatAlpRdIterator(it)
}

func (it *FloatAlpRdIterator[T]) Reset() {
	it.ofs = 0
	it.leftIt.Reset()
	it.rightIt.Reset()
}

func (it *FloatAlpRdIterator[T]) Len() int {
	return it.len
}

func (it *FloatAlpRdIterator[T]) Get(n int) T {
	if it.Seek(n) {
		val, _ := it.Next()
		return val
	}
	return 0
}

func (it *FloatAlpRdIterator[T]) Next() (T, bool) {
	if it.ofs >= it.len {
		// EOF
		return 0, false
	}

	// ofs % CHUNK_SIZE
	i := it.ofs & CHUNK_MASK

	// on first call or start of new chunk
	if i == 0 {
		// fmt.Printf("ALP-RD next load ofs=%d\n", it.ofs)

		// load next source chunks
		left, ln := it.leftIt.NextChunk()
		right, rn := it.rightIt.NextChunk()

		// sanity check, should not happen
		if ln == 0 || rn == 0 {
			// fmt.Printf("ALP-RD failed base seek left=%d right=%d\n", ln, rn)
			it.ofs = it.len
			return 0, false
		}

		// decode
		alp.MergeRD(it.vals[:], left[:], right[:], it.shift)
	}

	// advance ofs for next call
	it.ofs++

	// fmt.Printf("ALP-RD next ofs=%d i=%d\n", it.ofs, i)

	// return value
	return it.vals[i], true
}

func (it *FloatAlpRdIterator[T]) NextChunk() (*[CHUNK_SIZE]T, int) {
	// EOF
	if it.ofs >= it.len {
		return nil, 0
	}
	// fmt.Printf("ALP-RD next-chunk load ofs=%d\n", it.ofs)
	left, ln := it.leftIt.NextChunk()
	right, rn := it.rightIt.NextChunk()
	if ln > 0 && rn > 0 {
		alp.MergeRD(it.vals[:], left[:], right[:], it.shift)
		it.ofs += rn
	}
	return &it.vals, rn
}

func (it *FloatAlpRdIterator[T]) SkipChunk() int {
	it.leftIt.SkipChunk()
	n := it.rightIt.SkipChunk()
	it.ofs += n
	return n
}

func (it *FloatAlpRdIterator[T]) Seek(n int) bool {
	// bounds check
	if n < 0 || n >= it.len {
		it.ofs = it.len
		return false
	}

	// calculate chunk start offsets for n and current offset
	nc := chunkStart(n)
	oc := chunkStart(it.ofs)

	// fmt.Printf("ALP-RD seek n=%d ofs=%d nc=%d oc=%d\n", n, it.ofs, nc, oc)

	// TODO: simplify load logic (keep oc as state and replace it.ofs & CHUNK_MASK == 0
	// as load criteria in Next), this should make seek easier

	// load when n is in another chunk
	if nc != oc || it.ofs&CHUNK_MASK == 0 {
		// seek base-it to new chunk start
		// fmt.Printf("> ALP-RD: seeking bases nc=%d...\n", nc)
		ls, rs := it.leftIt.Seek(nc), it.rightIt.Seek(nc)
		if !ls || !rs {
			// fmt.Printf("> ALP-RD: seek bases failed; left=%t righ=%t\n", ls, rs)
			it.ofs = it.len
			return false
		}

		// load next chunk when not seeking to start (re-use NextChunk method)
		if n&CHUNK_MASK != 0 {
			// fmt.Printf("> ALP-RD: try loading chunk from nc=%d\n", nc)
			it.ofs = nc
			it.NextChunk()
		}
	} else if n == 0 {
		// edge case: reset base-it for n=0 because Next() loads again
		// fmt.Printf("> ALP-RD: resetting bases\n")
		it.leftIt.Reset()
		it.rightIt.Reset()
	}

	// reset ofs to n, so call to Next() delivers value
	it.ofs = n
	return true
}
