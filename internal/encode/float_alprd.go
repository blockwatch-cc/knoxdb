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

func (c *FloatAlpRdContainer[T]) MaxSize() int {
	v := 1 + num.MaxVarintLen64 + c.Left.MaxSize() + c.Right.MaxSize()
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
	left := arena.AllocT[uint16](max(cnt, 1<<16))[:1<<16] // alloc more to use as scratch array
	right := arena.AllocT[uint64](cnt)[:cnt]
	c.typ = BlockType[T]()

	// produce a small sample
	sample := arena.AllocT[T](alp.MaxSampleLen(cnt))
	alp.FirstLevelSample(sample, vals)

	// estimate best shift based on sample (use left array as scratch space for unique count)
	c.Shift = alp.EstimateShift(sample, left)

	// free sample and reset left to input vector length
	arena.FreeT(sample)
	left = left[:cnt]

	// split input float vector into left and right integer parts
	alp.Split(vals, left, right, c.Shift)

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
	// fmt.Printf("ALP-RD: left encoded as %s in %d bytes\n", c.Left.Type(), c.Left.MaxSize())
	c.Right = NewInt[uint64](TIntegerBitpacked).Encode(rctx, right, lvl-1)
	// fmt.Printf("ALP-RD: right encoded as %s in %d bytes\n", c.Right.Type(), c.Right.MaxSize())

	// free temp allocations
	lctx.Close()
	rctx.Close()
	arena.FreeT(left)
	arena.FreeT(right)

	return c
}

func (c *FloatAlpRdContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdContainer[T]) MatchSet(_ any, bits, _ *Bitset) *Bitset {
	// N.A.
	return bits
}

func (c *FloatAlpRdContainer[T]) MatchNotSet(_ any, bits, _ *Bitset) *Bitset {
	// N.A.
	return bits
}

type FloatAlpRdFactory struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
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

var floatAlpRdFactory = FloatAlpRdFactory{
	f64Pool: sync.Pool{
		New: func() any { return new(FloatAlpRdContainer[float64]) },
	},
	f32Pool: sync.Pool{
		New: func() any { return new(FloatAlpRdContainer[float32]) },
	},
}
