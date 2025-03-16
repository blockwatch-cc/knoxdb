// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"errors"
	"math/bits"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/filter/loglogbeta"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	ErrInvalidType = errors.New("invalid container type")
)

type Bitset = bitset.Bitset

type IntegerContainerType byte

const (
	TIntegerConstant IntegerContainerType = iota
	TIntegerDelta
	TIntegerRunEnd
	TIntegerBitpacked
	TIntegerDictionary
	TIntegerSimple8
	TIntegerRaw
)

var (
	iTypeNames    = "const_delta_run_bp_dict_s8_raw"
	iTypeNamesOfs = []int{0, 6, 12, 16, 19, 24, 27, 31}
)

func (t IntegerContainerType) String() string {
	return iTypeNames[iTypeNamesOfs[t] : iTypeNamesOfs[t+1]-1]
}

type IntegerContainer[T types.Integer] interface {
	// introspect
	Type() IntegerContainerType
	Len() int

	// data access
	Get(int) T
	AppendTo([]uint32, []T) []T

	// encode
	Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T]

	// IO
	MaxSize() int                // helps dimension buffer before write
	Store([]byte) []byte         // simple, composable, pre-alloc via MaxSize
	Load([]byte) ([]byte, error) // simple, composable
	Close()                      // free resources

	// matchers
	types.NumberMatcher[T]
}

// AnalyzeInt produces statistics about slice vals which are used to
// find the most efficient encoding scheme.
func AnalyzeInt[T types.Integer](vals []T) *IntegerContext[T] {
	c := newIntegerContext[T]()
	c.Min = vals[0]
	c.Max = vals[0]
	c.Delta = vals[util.Bool2int(len(vals) > 1)] - vals[0]
	c.PhyBits = int(unsafe.Sizeof(T(0))) * 8
	c.NumRuns = 1
	c.NumValues = len(vals)
	for i, v := range vals[1:] {
		if v < c.Min {
			c.Min = v
		} else if v > c.Max {
			c.Max = v
		}
		c.NumRuns += util.Bool2int(vals[i] != v)
		delta := v - vals[i]
		c.Delta = delta * T(util.Bool2int(c.Delta == delta))
	}

	// count unique only if necessary
	doCountUnique := c.Min != c.Max && c.Delta == 0
	c.NumUnique = min(c.NumRuns, int(c.Max)-int(c.Min))

	switch c.PhyBits {
	case 64:
		c.UseBits = bits.Len64(uint64(c.Max - c.Min))
		if doCountUnique {
			unique := loglogbeta.NewFilter()
			unique.AddManyUint64(util.ReinterpretSlice[T, uint64](vals))
			c.NumUnique = int(unique.Cardinality())
		}
	case 32:
		c.UseBits = bits.Len32(uint32(c.Max - c.Min))
		if doCountUnique {
			unique := loglogbeta.NewFilter()
			unique.AddManyUint32(util.ReinterpretSlice[T, uint32](vals))
			c.NumUnique = int(unique.Cardinality())
		}
	case 16:
		c.UseBits = bits.Len16(uint16(c.Max - c.Min))
		if doCountUnique {
			if c.Unique == nil {
				c.Unique = make(map[T]uint16, int(c.Max-c.Min))
			}
			for _, v := range vals {
				c.Unique[v] = 0
			}
			c.NumUnique = len(c.Unique)
		}
	case 8:
		c.UseBits = bits.Len8(uint8(c.Max - c.Min))
		if doCountUnique {
			if c.Unique == nil {
				c.Unique = make(map[T]uint16, int(c.Max-c.Min))
			}
			for _, v := range vals {
				c.Unique[v] = 0
			}
			c.NumUnique = len(c.Unique)
		}
	}
	// fmt.Printf("Analyze: %#v\n", c)
	return c
}

// NewInt creates a new integer container from scheme type.
func NewInt[T types.Integer](scheme IntegerContainerType) IntegerContainer[T] {
	switch scheme {
	case TIntegerConstant:
		return newConstContainer[T]()
	case TIntegerDelta:
		return newDeltaContainer[T]()
	case TIntegerRunEnd:
		return newRunEndContainer[T]()
	case TIntegerBitpacked:
		return newBitpackContainer[T]()
	case TIntegerDictionary:
		return newDictionaryContainer[T]()
	case TIntegerSimple8:
		return newSimple8Container[T]()
	case TIntegerRaw:
		return newRawContainer[T]()
	default:
		return nil
	}
}

const (
	MAX_CASCADE  = 3
	SAMPLE_SIZE  = 64
	SAMPLE_COUNT = 10
)

// SampleInt extracts a random sample from integer slice v. It is used
// when estimating the effectiveness of different encoders.
func SampleInt[T types.Integer](v []T) ([]T, bool) {
	if len(v) <= SAMPLE_COUNT*SAMPLE_SIZE {
		return v, false
	}
	// s := make([]T, SAMPLE_COUNT*SAMPLE_SIZE)
	sz := SAMPLE_COUNT * SAMPLE_SIZE
	s := arena.AllocT[T](sz)[:sz]
	chunk := len(v) / SAMPLE_COUNT
	for i := 0; i < SAMPLE_COUNT; i++ {
		start := chunk*i + util.RandIntn(chunk-SAMPLE_SIZE)
		end := start + SAMPLE_SIZE
		copy(s[SAMPLE_SIZE*i:], v[start:end])
	}
	return s, true
}

// EncodeInt encodes an integer type slice into an integer container
// selecting the most efficient encoding scheme.
func EncodeInt[T types.Integer](ctx *IntegerContext[T], v []T, lvl int) IntegerContainer[T] {
	// analyze full data if missing
	if ctx == nil {
		ctx = AnalyzeInt(v)
		defer ctx.Close()
	}
	// fmt.Printf("Enc %d vals @ lvl %d %#v\n", len(v), lvl, ctx)

	// try all eligible encoding schemes
	var (
		bestScheme IntegerContainerType = TIntegerRaw
		bestRatio  float64              = 1.0
	)
	if lvl > 0 {
		for _, scheme := range ctx.EligibleSchemes() {
			if rd := EstimateInt(scheme, ctx, v, lvl); rd < bestRatio {
				// fmt.Printf("> %s costs %f !!\n", scheme, rd)
				bestRatio = rd
				bestScheme = scheme
				// } else {
				// 	fmt.Printf("> %s costs %f\n", scheme, rd)
			}
		}
	}

	// alloc best container and encode
	// fmt.Printf("= SELECT enc %s %f\n", bestScheme, bestRatio)
	return NewInt[T](bestScheme).Encode(ctx, v, lvl)
}

// EstimateInt provides encoded size estimation without running the full encoder
// in some cases. In others, particularly nested cases, we need a full encode but
// on a small sample only.
func EstimateInt[T types.Integer](scheme IntegerContainerType, ctx *IntegerContext[T], v []T, lvl int) float64 {
	raw := NewInt[T](TIntegerRaw).Encode(ctx, v, lvl)
	rawSize := raw.MaxSize()
	raw.Close()
	var (
		estSize int
		ok      bool
	)
	switch scheme {
	case TIntegerConstant:
		// varint (max len)
		enc := NewInt[T](scheme).Encode(ctx, v, lvl)
		estSize, ok = enc.MaxSize(), true
		enc.Close()
	case TIntegerDelta:
		// 2x varint (max len)
		enc := NewInt[T](scheme).Encode(ctx, v, lvl)
		estSize, ok = enc.MaxSize(), true
		enc.Close()
	case TIntegerBitpacked:
		// bit packed with max depth and no patching
		estSize, ok = 2+2*num.MaxVarintLen64+(ctx.UseBits*ctx.NumValues+7)/8, true
	case TIntegerRaw:
		estSize, ok = rawSize, true
	}
	if ok {
		return float64(estSize) / float64(rawSize)
	}

	// the remaining schemes TIntegerSimple8, TIntegerRunEnd, TIntegerDictionary
	// use child containers which we cannot easily estimate
	// without running the encoder itself, to save time we use a sample

	// sample
	sample, freeSample := SampleInt(v)

	// analyze sample
	sctx := AnalyzeInt(sample)
	raw = NewInt[T](TIntegerRaw).Encode(sctx, sample, lvl)
	rawSize = raw.MaxSize()
	raw.Close()

	// fmt.Printf("> est sample with %s %#v\n", scheme, sctx)
	enc := NewInt[T](scheme).Encode(sctx, sample, lvl)
	estSize = enc.MaxSize()
	enc.Close()
	sctx.Close()
	if freeSample {
		arena.FreeT(sample)
	}

	return float64(estSize) / float64(rawSize)
}

// LoadInt loads an integer container from buffer.
func LoadInt[T types.Integer](buf []byte) (IntegerContainer[T], error) {
	c := NewInt[T](IntegerContainerType(buf[0]))
	if _, err := c.Load(buf); err != nil {
		return nil, err
	}
	return c, nil
}
