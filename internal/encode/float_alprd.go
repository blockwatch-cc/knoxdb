package encode

import (
	"math"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TFloatAlpRdV2
type FloatAlpRdV2Container[T types.Float] struct {
	Left  IntegerContainer[uint16]
	Right IntegerContainer[uint64]
	Shift int
	typ   types.BlockType
}

func (c *FloatAlpRdV2Container[T]) Close() {
	c.Left.Close()
	c.Right.Close()
	c.Left = nil
	c.Right = nil
	putFloatAlpRdV2Container(c)
}

func (c *FloatAlpRdV2Container[T]) Type() FloatContainerType {
	return TFloatAlpRd
}

func (c *FloatAlpRdV2Container[T]) Len() int {
	return c.Left.Len()
}

func (c *FloatAlpRdV2Container[T]) MaxSize() int {
	v := 1 + num.MaxVarintLen64 + c.Left.MaxSize() + c.Right.MaxSize()
	return v
}

func (c *FloatAlpRdV2Container[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatAlpRd))
	dst = c.Left.Store(dst)
	dst = c.Right.Store(dst)
	dst = num.AppendUvarint(dst, uint64(c.Shift))
	return dst
}

func (c *FloatAlpRdV2Container[T]) Load(buf []byte) ([]byte, error) {
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

func (c *FloatAlpRdV2Container[T]) Get(n int) T {
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

func (c *FloatAlpRdV2Container[T]) AppendTo(sel []uint32, dst []T) []T {
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

func (c *FloatAlpRdV2Container[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	cnt := len(vals)
	left := arena.AllocT[uint16](cnt)[:cnt]
	right := arena.AllocT[uint64](cnt)[:cnt]
	c.typ = BlockType[T]()

	sample, ok := SampleFloat(vals)
	bestShift := estimateShift(sample, left, right, lvl)
	if ok {
		arena.FreeT(sample)
	}

	mask := uint64(1<<bestShift) - 1
	switch c.typ {
	case types.BlockFloat64:
		for k, v := range util.ReinterpretSlice[T, uint64](vals) {
			right[k] = v & mask
			left[k] = uint16(v >> bestShift)
		}
	case types.BlockFloat32:
		for k, v := range util.ReinterpretSlice[T, uint32](vals) {
			right[k] = uint64(v) & mask
			left[k] = uint16(v >> bestShift)
		}
	}

	lctx := AnalyzeInt(left, true)
	rctx := AnalyzeInt(right, false)
	c.Shift = bestShift
	c.Left = NewInt[uint16](TIntegerDictionary).Encode(lctx, left, lvl-1)
	c.Right = NewInt[uint64](TIntegerBitpacked).Encode(rctx, right, lvl-1)
	lctx.Close()
	rctx.Close()
	arena.FreeT(left)
	arena.FreeT(right)

	return c
}

func estimateShift[T types.Float](sample []T, leftInts []uint16, rightInts []uint64, lvl int) int {
	var (
		bestShift int
		bestSize  int = math.MaxInt32
		sz            = len(sample)
		w         int = SizeOf[T]()
	)

	for i := 1; i <= 16; i++ {
		rightBitWidth := 64 - i
		mask := uint64(1<<rightBitWidth) - 1

		// split vals into left and right
		switch w {
		case 8:
			for k, v := range util.ReinterpretSlice[T, uint64](sample) {
				rightInts[k] = v & mask
				leftInts[k] = uint16(v >> rightBitWidth)
			}
		case 4:
			for k, v := range util.ReinterpretSlice[T, uint32](sample) {
				rightInts[k] = uint64(v) & mask
				leftInts[k] = uint16(v >> rightBitWidth)
			}
		}

		// try estimate integer sizes
		lctx := AnalyzeInt(leftInts[:sz], true)
		rctx := AnalyzeInt(rightInts[:sz], false)
		leftC := NewInt[uint16](TIntegerDictionary).Encode(lctx, leftInts[:sz], lvl-1)
		// rightC := NewInt[uint64](TIntegerBitpacked).Encode(rctx, rightInts[:sz], lvl-1)

		// get total size
		maxSz := leftC.MaxSize() + 2 + 2*num.MaxVarintLen64 + (rctx.UseBits*rctx.NumValues+7)/8

		lctx.Close()
		rctx.Close()

		// compare against previous know best ratio and keep best containers
		if maxSz <= bestSize {
			bestSize = maxSz
			bestShift = rightBitWidth
		}
		leftC.Close()
		// rightC.Close()
	}

	return bestShift
}

func (c *FloatAlpRdV2Container[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdV2Container[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdV2Container[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdV2Container[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdV2Container[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdV2Container[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdV2Container[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpRdV2Container[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *FloatAlpRdV2Container[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

type FloatAlpRdV2Factory struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
}

func newFloatAlpRdContainer[T types.Float]() FloatContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpRdV2Factory.f64Pool.Get().(FloatContainer[T])
	case float32:
		return floatAlpRdV2Factory.f32Pool.Get().(FloatContainer[T])
	default:
		return nil
	}
}

func putFloatAlpRdV2Container[T types.Float](c FloatContainer[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpRdV2Factory.f64Pool.Put(c)
	case float32:
		floatAlpRdV2Factory.f32Pool.Put(c)
	}
}

var floatAlpRdV2Factory = FloatAlpRdV2Factory{
	f64Pool: sync.Pool{
		New: func() any { return new(FloatAlpRdV2Container[float64]) },
	},
	f32Pool: sync.Pool{
		New: func() any { return new(FloatAlpRdV2Container[float32]) },
	},
}
