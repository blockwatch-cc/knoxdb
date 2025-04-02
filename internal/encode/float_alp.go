package encode

import (
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/alp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

type ExceptionStatus byte

const (
	NoException ExceptionStatus = iota
	HasException
)

// TFloatAlp
type FloatAlpContainer[T types.Float] struct {
	For          int64
	Exponent     uint8
	Factor       uint8
	Values       IntegerContainer[int64]
	Exception    FloatContainer[T]
	Ends         IntegerContainer[uint32]
	DecodedVals  []T
	hasException bool
	exceptions   map[uint32]T
}

func (c *FloatAlpContainer[T]) Close() {
	if c.DecodedVals != nil {
		arena.FreeT(c.DecodedVals)
		c.DecodedVals = nil
	}
	clear(c.exceptions)
	putFloatAlpContainer(c)
}

func (c *FloatAlpContainer[T]) Type() FloatContainerType {
	return TFloatAlp
}

func (c *FloatAlpContainer[T]) Len() int {
	return c.Values.Len()
}

func (c *FloatAlpContainer[T]) MaxSize() int {
	v := 1 + 2 + num.MaxVarintLen64 + c.Values.MaxSize()
	if c.hasException {
		v += c.Exception.MaxSize() + c.Ends.MaxSize()
	}
	return v
}

func (c *FloatAlpContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatAlp))
	dst = num.AppendUvarint(dst, uint64(c.For))
	dst = num.AppendUvarint(dst, uint64(c.Exponent))
	dst = num.AppendUvarint(dst, uint64(c.Factor))
	dst = c.Values.Store(dst)
	hasException := c.Exception != nil && c.Exception.Len() > 0
	dst = append(dst, byte(util.Bool2byte(hasException)))
	if hasException {
		dst = c.Exception.Store(dst)
		dst = c.Ends.Store(dst)
	}
	return dst
}

func (c *FloatAlpContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatAlp) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	c.For = int64(v)
	buf = buf[n:]

	v, n = num.Uvarint(buf)
	c.Exponent = uint8(v)
	buf = buf[n:]

	v, n = num.Uvarint(buf)
	c.Factor = uint8(v)
	buf = buf[n:]

	// alloc and decode values child container
	c.Values = NewInt[int64](IntegerContainerType(buf[0]))
	var err error
	buf, err = c.Values.Load(buf)
	if err != nil {
		return buf, err
	}

	c.hasException = buf[0] == byte(HasException)
	buf = buf[1:]
	if c.hasException {
		// exception
		c.Exception = NewFloat[T](FloatContainerType(buf[0]))
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

		c.exceptions = make(map[uint32]T, c.Ends.Len())
		for i := range c.Ends.Len() {
			c.exceptions[c.Ends.Get(i)] = c.Exception.Get(i)
		}

		c.Exception.Close()
		c.Ends.Close()
		c.Exception = nil
		c.Ends = nil
	}

	return buf, nil
}

func (c *FloatAlpContainer[T]) Get(n int) T {
	if c.hasException {
		if c.exceptions == nil {
			c.exceptions = make(map[uint32]T, c.Ends.Len())
		}
		if len(c.exceptions) == 0 {
			for i := range c.Ends.Len() {
				c.exceptions[c.Ends.Get(i)] = c.Exception.Get(i)
			}
		}
		if v, ok := c.exceptions[uint32(n)]; ok {
			return v
		}
	}
	return alp.DecompressValue[T](c.Values.Get(n), c.Factor, c.Exponent, c.For)
}

func (c *FloatAlpContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	for _, v := range sel {
		dst = append(dst, c.Get(int(v)))
	}
	return dst

}

func (c *FloatAlpContainer[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	s := alp.Compress(vals)

	c.For = s.FOR
	c.Exponent = s.EncodingIndice.Exponent
	c.Factor = s.EncodingIndice.Factor

	// encode child containers
	c.Values = EncodeInt(nil, s.EncodedIntegers, lvl-1)
	if s.ExceptionsCount > 0 {
		c.hasException = true
		c.Exception = EncodeFloat(nil, s.Exceptions, lvl-1)
		c.Ends = EncodeInt(nil, s.ExceptionPositions, lvl-1)
	}

	return c
}

func (c *FloatAlpContainer[T]) decodeAll() error {
	var cnt, valsLen int
	if c.hasException {
		cnt = c.Exception.Len()
	}
	exceptions := arena.AllocT[T](cnt)[:cnt]
	ends := arena.Alloc(arena.AllocUint32, cnt).([]uint32)[:cnt]

	valsLen = c.Values.Len()
	values := arena.Alloc(arena.AllocInt64, valsLen).([]int64)[:valsLen]

	for i := range values {
		values[i] = c.Values.Get(i)
	}
	c.DecodedVals = arena.AllocT[T](valsLen)[:valsLen]

	alp.Decompress(c.DecodedVals, c.Factor, c.Exponent, c.For, exceptions, ends, values)

	arena.FreeT(exceptions)
	arena.Free(arena.AllocUint32, ends)
	arena.Free(arena.AllocInt64, values)

	return nil
}

func (c *FloatAlpContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *FloatAlpContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *FloatAlpContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

type FloatAlpFactory struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
}

func newFloatAlpContainer[T types.Float]() FloatContainer[T] {
	switch any(T(0)).(type) {
	case float64:
		return floatAlpFactory.f64Pool.Get().(FloatContainer[T])
	case float32:
		return floatAlpFactory.f32Pool.Get().(FloatContainer[T])
	default:
		return nil
	}
}

func putFloatAlpContainer[T types.Float](c FloatContainer[T]) {
	switch any(T(0)).(type) {
	case float64:
		floatAlpFactory.f64Pool.Put(c)
	case float32:
		floatAlpFactory.f32Pool.Put(c)
	}
}

var floatAlpFactory = FloatAlpFactory{
	f64Pool: sync.Pool{
		New: func() any { return new(FloatAlpContainer[float64]) },
	},
	f32Pool: sync.Pool{
		New: func() any { return new(FloatAlpContainer[float32]) },
	},
}
