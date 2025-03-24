package encode

import (
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/dedup"
	"blockwatch.cc/knoxdb/internal/encode/alp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TFloatAlpRd
type FloatAlpRdContainer[T types.Float] struct {
	LeftBitWidth     uint8
	RightBitWidth    uint8
	LeftPartEncoded  []uint8 // packed
	RightPartEncoded []uint8 // packed
	ValCount         uint64
	leftPart         []uint16
	rightPart        []uint64
	LeftPartsDict    IntegerContainer[uint16]
	Exception        IntegerContainer[uint16]
	Ends             IntegerContainer[uint32]
	hasException     bool
	exceptions       map[uint32]T
}

func (c *FloatAlpRdContainer[T]) Close() {
	clear(c.leftPart)
	clear(c.rightPart)
	clear(c.exceptions)

	putFloatAlpRdContainer(c)
}

func (c *FloatAlpRdContainer[T]) Type() FloatContainerType {
	return TFloatAlpRd
}

func (c *FloatAlpRdContainer[T]) Len() int {
	return len(c.leftPart)
}

func (c *FloatAlpRdContainer[T]) MaxSize() int {
	v := 1 + 2 + 8 + num.MaxVarintLen64 + len(c.LeftPartEncoded) + 8 + len(c.RightPartEncoded) + c.LeftPartsDict.MaxSize()
	if c.hasException {
		v += c.Exception.MaxSize() + c.Ends.MaxSize()
	}
	return v
}

func (c *FloatAlpRdContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatAlpRd))
	dst = num.AppendUvarint(dst, uint64(c.LeftBitWidth))
	dst = num.AppendUvarint(dst, uint64(c.RightBitWidth))
	dst = num.AppendUvarint(dst, uint64(len(c.LeftPartEncoded)))
	dst = append(dst, c.LeftPartEncoded...)
	dst = num.AppendUvarint(dst, uint64(len(c.RightPartEncoded)))
	dst = append(dst, c.RightPartEncoded...)
	dst = num.AppendUvarint(dst, uint64(c.ValCount))
	dst = c.LeftPartsDict.Store(dst)
	hasException := c.Exception != nil && c.Exception.Len() > 0
	dst = append(dst, byte(util.Bool2byte(hasException)))
	if hasException {
		dst = c.Exception.Store(dst)
		dst = c.Ends.Store(dst)
	}
	return dst
}

func (c *FloatAlpRdContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatAlpRd) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	c.LeftBitWidth = uint8(v)
	buf = buf[n:]

	v, n = num.Uvarint(buf)
	c.RightBitWidth = uint8(v)
	buf = buf[n:]

	v, n = num.Uvarint(buf)
	buf = buf[n:]
	c.LeftPartEncoded = buf[:v]
	buf = buf[v:]

	v, n = num.Uvarint(buf)
	buf = buf[n:]
	c.RightPartEncoded = buf[:v]
	buf = buf[v:]

	v, n = num.Uvarint(buf)
	buf = buf[n:]
	c.leftPart = arena.Alloc(arena.AllocUint16, int(v)).([]uint16)[:v]
	c.rightPart = arena.Alloc(arena.AllocUint64, int(v)).([]uint64)[:v]

	dedup.UnpackBits(c.LeftPartEncoded, c.leftPart, int(c.LeftBitWidth))
	dedup.UnpackBits(c.RightPartEncoded, c.rightPart, int(c.RightBitWidth))

	c.LeftPartsDict = NewInt[uint16](IntegerContainerType(buf[0]))
	var err error
	buf, err = c.LeftPartsDict.Load(buf)
	if err != nil {
		return buf, err
	}

	// alloc and decode values child container
	c.hasException = buf[0] == byte(HasException)
	buf = buf[1:]
	if c.hasException {
		// exception
		c.Exception = NewInt[uint16](IntegerContainerType(buf[0]))
		buf, err = c.Exception.Load(buf)
		if err != nil {
			return buf, err
		}

		// ExceptionPosition
		c.Ends = NewInt[uint32](IntegerContainerType(buf[0]))
		buf, err = c.Ends.Load(buf)
		if err != nil {
			return buf, err
		}
	}

	return buf, nil
}

func (c *FloatAlpRdContainer[T]) Get(n int) T {
	if c.hasException {
		if c.exceptions == nil {
			c.exceptions = make(map[uint32]T, c.Ends.Len())
		}
		if len(c.exceptions) == 0 {
			for i := range c.Ends.Len() {
				right := c.RightPartEncoded[c.Ends.Get(i)]
				left := c.Exception.Get(i)

				switch any(T(0)).(type) {
				case float64:
					d := uint64(left<<uint16(c.RightBitWidth)) | uint64(right)
					c.exceptions[uint32(left)] = *(*T)(unsafe.Pointer(&d))
				case float32:
					d := uint32(left<<uint16(c.RightBitWidth)) | uint32(right)
					c.exceptions[uint32(left)] = *(*T)(unsafe.Pointer(&d))
				}
			}
		}
		if v, ok := c.exceptions[uint32(n)]; ok {
			return v
		}
	}
	var d T
	switch any(T(0)).(type) {
	case float64:
		d = alp.RDDecompressValue[T](c.LeftPartsDict.Get(int(c.leftPart[n])), uint64(c.rightPart[n]), c.RightBitWidth)
	case float32:
		d = alp.RDDecompressValue[T](c.LeftPartsDict.Get(int(c.leftPart[n])), uint32(c.rightPart[n]), c.RightBitWidth)
	}
	return d
}

func (c *FloatAlpRdContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	for _, v := range sel {
		dst = append(dst, c.Get(int(v)))
	}
	return dst

}

func (c *FloatAlpRdContainer[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	cnt := len(vals)
	c.leftPart = arena.Alloc(arena.AllocUint16, cnt).([]uint16)[:cnt]
	c.rightPart = arena.Alloc(arena.AllocUint64, cnt).([]uint64)[:cnt]

	var s *alp.RdState[T]
	switch any(T(0)).(type) {
	case float64:
		s = alp.RDCompress[T, uint64](vals)
	case float32:
		s = alp.RDCompress[T, uint32](vals)
	}

	c.LeftBitWidth = s.LeftBitWidth
	c.RightBitWidth = s.RightBitWidth

	c.LeftPartEncoded = arena.Alloc(arena.AllocUint8, len(s.LeftPartEncoded)).([]uint8)[:len(s.LeftPartEncoded)]
	copy(c.LeftPartEncoded, s.LeftPartEncoded)

	c.RightPartEncoded = arena.Alloc(arena.AllocUint8, len(s.RightPartEncoded)).([]uint8)[:len(s.RightPartEncoded)]
	copy(c.RightPartEncoded, s.RightPartEncoded)

	dedup.UnpackBits(c.LeftPartEncoded, c.leftPart, int(c.LeftBitWidth))
	dedup.UnpackBits(c.RightPartEncoded, c.rightPart, int(c.RightBitWidth))

	c.ValCount = uint64(len(vals))

	c.LeftPartsDict = EncodeInt(nil, s.LeftPartsDict[:], lvl-1)

	// encode child containers
	if s.ExceptionsCount > 0 {
		c.hasException = true
		c.Exception = EncodeInt(nil, s.Exceptions, lvl-1)
		c.Ends = EncodeInt(nil, s.ExceptionsPositions, lvl-1)
	}

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
