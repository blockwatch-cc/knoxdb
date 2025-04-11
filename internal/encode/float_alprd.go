package encode

import (
	"math"
	"math/bits"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/hashprobe"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TFloatAlpRd
type FloatAlpRdContainer[T types.Float] struct {
	Left  IntegerContainer[uint16]
	Right IntegerContainer[uint64]
	Shift int
	typ   types.BlockType
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
	left := arena.AllocT[uint16](1 << 16)[:1<<16]
	right := arena.AllocT[uint64](cnt)[:cnt]
	c.typ = BlockType[T]()

	sample, ok := SampleFloat(vals)
	bestShift := estimateShift(sample, left)
	if ok {
		arena.FreeT(sample)
	}
	left = left[:cnt]

	// TODO: SIMD
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

	// left side dict compression only makes sense when more efficient than bit-packing
	leftScheme := TIntegerBitpacked
	if lctx.preferDict() {
		leftScheme = TIntegerDictionary
	}

	c.Left = NewInt[uint16](leftScheme).Encode(lctx, left, lvl-1)
	c.Right = NewInt[uint64](TIntegerBitpacked).Encode(rctx, right, lvl-1)
	lctx.Close()
	rctx.Close()
	arena.FreeT(left)
	arena.FreeT(right)

	return c
}

func estimateShift[T types.Float](sample []T, unique []uint16) int {
	var (
		bestShift int
		bestSize  int = math.MaxInt32
		sz            = len(sample)
		w         int = SizeOf[T]()
	)
	for i := 1; i <= 16; i++ {
		shift := 64 - i
		mask := uint64(1<<shift - 1)

		var (
			lmin, lmax uint16 = 0, math.MaxUint16
			rmin, rmax uint64 = 0, math.MaxUint64
		)

		switch w {
		case 8:
			// min/max
			for _, v := range util.ReinterpretSlice[T, uint64](sample) {
				l, r := uint16(v>>shift), v&mask
				if l < lmin {
					lmin = l
				} else if l > lmax {
					lmax = l
				}
				if r < rmin {
					rmin = r
				} else if r > rmax {
					rmax = r
				}
			}
			// mark uniques
			for _, v := range util.ReinterpretSlice[T, uint64](sample) {
				unique[uint16(v>>shift)-lmin] = 1
			}

		case 4:
			// min/max
			for _, v := range util.ReinterpretSlice[T, uint32](sample) {
				l, r := uint16(v>>shift), uint64(v)&mask
				if l < lmin {
					lmin = l
				} else if l > lmax {
					lmax = l
				}
				if r < rmin {
					rmin = r
				} else if r > rmax {
					rmax = r
				}
				unique[l] = 1
			}
			// mark uniques
			for _, v := range util.ReinterpretSlice[T, uint32](sample) {
				unique[uint16(v>>shift)-lmin] = 1
			}
		}

		// count uniques
		var lunique int
		for _, v := range unique[:lmax-lmin+1] {
			lunique = util.Bool2int(v > 0)
		}
		lbits, rbits := bits.Len16(lmax-lmin), bits.Len64(rmax-rmin)

		// estimate encoded size
		// - left side may be dict compressed
		// - right side will be bitpacked
		ldcost := dictCosts(sz, lbits, lunique)
		lbcost := bitPackCosts(sz, lbits)

		var maxSz int
		if lunique <= hashprobe.MAX_DICT_LIMIT && ldcost < lbcost {
			maxSz += ldcost
		} else {
			maxSz += lbcost
		}
		maxSz += bitPackCosts(sz, rbits) // bitpack only

		// compare against previous know best ratio and keep best containers
		if maxSz <= bestSize {
			bestSize = maxSz
			bestShift = shift
		}

		// cleanup
		clear(unique[:lmax-lmin+1])
	}
	return bestShift
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

func (c *FloatAlpRdContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *FloatAlpRdContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
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
