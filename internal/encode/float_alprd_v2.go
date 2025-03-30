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
	Left       IntegerContainer[uint16]
	Right      IntegerContainer[uint64]
	RightShift int
}

func (c *FloatAlpRdV2Container[T]) Close() {
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
	dst = num.AppendUvarint(dst, uint64(c.RightShift))
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
	c.RightShift = int(v)
	return buf[n:], nil
}

func (c *FloatAlpRdV2Container[T]) Get(n int) T {
	left := c.Left.Get(n)
	right := c.Right.Get(n)
	var val T
	switch any(T(0)).(type) {
	case float64:
		v := uint64(left)<<c.RightShift | uint64(right)
		val = *(*T)(unsafe.Pointer(&v))
	case float32:
		v := uint32(left)<<c.RightShift | uint32(right)
		val = *(*T)(unsafe.Pointer(&v))
	}
	return val
}

func (c *FloatAlpRdV2Container[T]) AppendTo(sel []uint32, dst []T) []T {
	for _, v := range sel {
		dst = append(dst, c.Get(int(v)))
	}
	return dst

}

func (c *FloatAlpRdV2Container[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	cnt := len(vals)
	left := arena.Alloc(arena.AllocUint16, cnt).([]uint16)[:cnt]
	right := arena.Alloc(arena.AllocUint64, cnt).([]uint64)[:cnt]

	c.split(vals, left, right, lvl)

	return c
}

func (c *FloatAlpRdV2Container[T]) split(vals []T, left []uint16, right []uint64, lvl int) {
	switch any(T(0)).(type) {
	case float64:
		u64 := util.ReinterpretSlice[T, uint64](vals)
		c.split64(u64, left, right, lvl)
	case float32:
		u32 := util.ReinterpretSlice[T, uint32](vals)
		c.split32(u32, left, right, lvl)
	}
}

func (c *FloatAlpRdV2Container[T]) split64(u64 []uint64, leftInts []uint16, rightInts []uint64, lvl int) {
	// try different right bit widths to find the optimal encoding for left&right containers
	sampleU64, ok := SampleInt(u64)
	bestShift := shift64(sampleU64, leftInts, rightInts, lvl)
	if ok {
		arena.FreeT(sampleU64)
	}

	mask := uint64(1<<bestShift) - 1
	for k := range u64 {
		rightInts[k] = u64[k] & mask
		leftInts[k] = uint16(u64[k] >> bestShift)
	}

	lctx := AnalyzeInt(leftInts, false)
	rctx := AnalyzeInt(rightInts, false)
	defer lctx.Close()
	defer rctx.Close()

	c.RightShift = bestShift
	c.Left = NewInt[uint16](TIntegerRaw).Encode(lctx, leftInts, lvl-1)
	c.Right = NewInt[uint64](TIntegerRaw).Encode(rctx, rightInts, lvl-1)
}

func shift64(sampleU64 []uint64, leftInts []uint16, rightInts []uint64, lvl int) int {
	var (
		bestShift int
		bestSize  int = math.MaxInt32
		sz            = len(sampleU64)
		lctx          = AnalyzeInt(leftInts, false)
		rctx          = AnalyzeInt(rightInts, false)
	)

	defer lctx.Close()
	defer rctx.Close()

	for i := 1; i <= 16; i++ {
		// split vals into left and right
		rightBitWidth := 64 - i
		for k := range sampleU64 {
			mask := uint64(1<<rightBitWidth) - 1
			rightInts[k] = sampleU64[k] & mask
			leftInts[k] = uint16(sampleU64[k] >> rightBitWidth)
		}

		// try estimate integer sizes
		leftC := NewInt[uint16](TIntegerRaw).Encode(lctx, leftInts[:sz], lvl-1)
		rightC := NewInt[uint64](TIntegerRaw).Encode(rctx, rightInts[:sz], lvl-1)

		// get total size
		maxSz := leftC.MaxSize() + rightC.MaxSize()

		// compare against previous know best ratio and keep best containers
		if maxSz <= bestSize {
			bestSize = maxSz
			bestShift = rightBitWidth
		}
		leftC.Close()
		rightC.Close()
	}

	return bestShift
}

func (c *FloatAlpRdV2Container[T]) split32(u32 []uint32, leftInts []uint16, rightInts []uint64, lvl int) {
	// try different right bit widths to find the optimal encoding for left&right containers
	sampleU32, ok := SampleInt(u32)
	bestShift := shift32(sampleU32, leftInts, rightInts, lvl)
	if ok {
		arena.FreeT(sampleU32)
	}

	mask := uint32(1<<bestShift) - 1
	for k := range u32 {
		rightInts[k] = uint64(u32[k] & mask)
		leftInts[k] = uint16(u32[k] >> bestShift)
	}

	lctx := AnalyzeInt(leftInts, false)
	rctx := AnalyzeInt(rightInts, false)

	defer lctx.Close()
	defer rctx.Close()

	c.Left = NewInt[uint16](TIntegerRaw).Encode(lctx, leftInts, lvl-1)
	c.Right = NewInt[uint64](TIntegerRaw).Encode(rctx, rightInts, lvl-1)
	c.RightShift = bestShift
}

func shift32(sampleU32 []uint32, leftInts []uint16, rightInts []uint64, lvl int) int {
	var (
		bestShift int
		bestSize  int = math.MaxInt32
		sz            = len(sampleU32)
		lctx          = AnalyzeInt(leftInts, false)
		rctx          = AnalyzeInt(rightInts, false)
	)

	defer lctx.Close()
	defer rctx.Close()

	for i := 1; i <= 16; i++ {
		// split vals into left and right
		rightBitWidth := 32 - i
		for k := range sampleU32 {
			mask := uint32(1<<rightBitWidth) - 1
			rightInts[k] = uint64(sampleU32[k] & mask)
			leftInts[k] = uint16(sampleU32[k] >> rightBitWidth)
		}

		// try estimate integer sizes
		leftC := NewInt[uint16](TIntegerRaw).Encode(lctx, leftInts[:sz], lvl-1)
		rightC := NewInt[uint64](TIntegerRaw).Encode(rctx, rightInts[:sz], lvl-1)

		// get total size
		maxSz := leftC.MaxSize() + rightC.MaxSize()

		// compare against previous know best ratio and keep best containers
		if maxSz <= bestSize {
			bestSize = maxSz
			bestShift = rightBitWidth
		}
		leftC.Close()
		rightC.Close()
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

func newFloatAlpRdV2Container[T types.Float]() FloatContainer[T] {
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
