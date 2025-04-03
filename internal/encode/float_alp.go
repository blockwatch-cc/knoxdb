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
	Exponent     uint8
	Factor       uint8
	Values       IntegerContainer[int64]
	Exception    FloatContainer[T]
	Positions    IntegerContainer[uint32]
	decoded      []T
	hasException bool
	exceptions   map[uint32]T
	dec          *alp.Decoder[T]
}

func (c *FloatAlpContainer[T]) Close() {
	if c.decoded != nil {
		arena.FreeT(c.decoded)
		c.decoded = nil
	}
	c.Values.Close()
	if c.hasException {
		c.Exception.Close()
		c.Positions.Close()
		c.Exception = nil
		c.Positions = nil
		c.hasException = false
	}
	clear(c.exceptions)
	c.dec = nil
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
		v += c.Exception.MaxSize() + c.Positions.MaxSize()
	}
	return v
}

func (c *FloatAlpContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TFloatAlp))
	// dst = num.AppendUvarint(dst, uint64(c.For))
	dst = num.AppendUvarint(dst, uint64(c.Exponent))
	dst = num.AppendUvarint(dst, uint64(c.Factor))
	dst = c.Values.Store(dst)
	dst = append(dst, util.Bool2byte(c.hasException))
	if c.hasException {
		dst = c.Exception.Store(dst)
		dst = c.Positions.Store(dst)
	}
	return dst
}

func (c *FloatAlpContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TFloatAlp) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]

	v, n := num.Uvarint(buf)
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
		// exception values
		c.Exception = NewFloat[T](FloatContainerType(buf[0]))
		buf, err = c.Exception.Load(buf)
		if err != nil {
			return buf, err
		}

		// exception positions
		c.Positions = NewInt[uint32](IntegerContainerType(buf[0]))
		buf, err = c.Positions.Load(buf)
		if err != nil {
			return buf, err
		}

		// construct exception map
		c.exceptions = make(map[uint32]T, c.Positions.Len())
		for i := range c.Positions.Len() {
			c.exceptions[c.Positions.Get(i)] = c.Exception.Get(i)
		}
	}
	c.dec = alp.NewDecoder[T](c.Factor, c.Exponent)

	return buf, nil
}

func (c *FloatAlpContainer[T]) Get(n int) T {
	if c.hasException {
		if c.exceptions == nil {
			c.exceptions = make(map[uint32]T, c.Positions.Len())
		}
		if len(c.exceptions) == 0 {
			for i := range c.Positions.Len() {
				c.exceptions[c.Positions.Get(i)] = c.Exception.Get(i)
			}
		}
		if v, ok := c.exceptions[uint32(n)]; ok {
			return v
		}
	}
	return c.dec.DecompressValue(c.Values.Get(n))
}

func (c *FloatAlpContainer[T]) AppendTo(sel []uint32, dst []T) []T {
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

func (c *FloatAlpContainer[T]) Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T] {
	enc := alp.NewEncoder[T]().Compress(vals)
	s := enc.State()

	c.Exponent = s.EncodingIndice.Exponent
	c.Factor = s.EncodingIndice.Factor
	c.dec = alp.NewDecoder[T](c.Factor, c.Exponent)

	// encode child containers
	c.Values = EncodeInt(nil, s.EncodedIntegers, lvl-1)
	// fmt.Printf("ALP: int encoded as %s in %d bytes\n", c.Values.Type(), c.Values.MaxSize())
	if len(s.Exceptions) > 0 {
		c.hasException = true
		c.Exception = EncodeFloat(nil, s.Exceptions, lvl-1)
		// fmt.Printf("ALP: ex encoded as %s in %d bytes raw=%v\n", c.Exception.Type(), c.Exception.MaxSize(), s.Exceptions)
		c.Positions = EncodeInt(nil, s.ExceptionPositions, lvl-1)
		// fmt.Printf("ALP: pos encoded as %s in %d bytes raw=%d\n", c.Positions.Type(), c.Positions.MaxSize(), s.ExceptionPositions)
	}
	enc.Close()

	return c
}

// func (c *FloatAlpContainer[T]) decodeAll() error {
// 	var cnt, valsLen int
// 	if c.hasException {
// 		cnt = c.Exception.Len()
// 	}
// 	exceptions := arena.AllocT[T](cnt)[:cnt]
// 	positions := arena.AllocT[uint32](cnt)[:cnt]

// 	valsLen = c.Values.Len()
// 	values := arena.AllocT[int64](valsLen)
// 	values = c.Values.AppendTo(nil, values)

// 	c.decoded = arena.AllocT[T](valsLen)[:valsLen]
// 	c.dec.Decompress(c.decoded, c.Factor, c.Exponent, c.For, exceptions, positions, values)

// 	arena.FreeT(exceptions)
// 	arena.FreeT(positions)
// 	arena.FreeT(values)

// 	return nil
// }

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
