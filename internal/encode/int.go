// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"errors"
	"math/bits"
	"unsafe"

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

	// matchers
	types.NumberMatcher[T]
}

type IntegerContext[T types.Integer] struct {
	Min       T            // vector minimum
	Max       T            // vector maximum
	Delta     T            // common delta between vector values
	PhyBits   int          // phy type bit width 8, 16, 32, 64
	UseBits   int          // used bits for bit-packing
	NumUnique int          // vector cardinality (hint, may not be precise)
	NumRuns   int          // vector runs
	NumValues int          // vector length
	Unique    map[T]uint16 // unique values (optional)
}

// AnalyzeInt produces statistics about slice vals which are used to
// find the most efficient encoding scheme.
func AnalyzeInt[T types.Integer](vals []T) *IntegerContext[T] {
	c := &IntegerContext[T]{
		Min:       vals[0],
		Max:       vals[0],
		Delta:     vals[util.Bool2int(len(vals) > 1)] - vals[0],
		PhyBits:   int(unsafe.Sizeof(T(0))) * 8,
		NumRuns:   1,
		NumValues: len(vals),
	}
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
			c.Unique = make(map[T]uint16, int(c.Max-c.Min))
			for _, v := range vals {
				c.Unique[v] = 0
			}
			c.NumUnique = len(c.Unique)
		}
	case 8:
		c.UseBits = bits.Len8(uint8(c.Max - c.Min))
		if doCountUnique {
			c.Unique = make(map[T]uint16, int(c.Max-c.Min))
			for _, v := range vals {
				c.Unique[v] = 0
			}
			c.NumUnique = len(c.Unique)
		}
	}
	// fmt.Printf("Analyze: %#v\n", c)
	return c
}

func (c *IntegerContext[T]) EligibleSchemes() []IntegerContainerType {
	// constant only
	if c.Min == c.Max {
		return []IntegerContainerType{TIntegerConstant}
	}
	// delta only with at least 3 values
	if c.Delta > 0 && c.NumValues > 2 {
		return []IntegerContainerType{TIntegerDelta}
	}
	// raw always works
	schemes := []IntegerContainerType{
		TIntegerRaw,
	}
	// bit packed must decrease bit width by at least 8
	if c.UseBits+8 < c.PhyBits {
		schemes = append(schemes, TIntegerBitpacked)
	}
	// run-end requires avg run lengths >= 2
	if c.NumRuns*2 <= c.NumValues {
		schemes = append(schemes, TIntegerRunEnd)
	}
	// dict makes only sense if <64k entries, value range is reduced and card < 3/4
	if c.NumUnique < 1<<16-1 && c.Max-c.Min > T(c.NumUnique) && c.NumUnique*4/3 < c.NumValues {
		schemes = append(schemes, TIntegerDictionary)
	}
	// simple 8 requires max 60bit values
	if c.UseBits <= 60 {
		schemes = append(schemes, TIntegerSimple8)
	}
	return schemes
}

// NewInt creates a new integer container from scheme type.
func NewInt[T types.Integer](scheme IntegerContainerType) IntegerContainer[T] {
	switch scheme {
	case TIntegerConstant:
		return new(ConstContainer[T])
	case TIntegerDelta:
		return new(DeltaContainer[T])
	case TIntegerRunEnd:
		return new(RunEndContainer[T])
	case TIntegerBitpacked:
		return new(BitpackContainer[T])
	case TIntegerDictionary:
		return new(DictionaryContainer[T])
	case TIntegerSimple8:
		return new(Simple8Container[T])
	case TIntegerRaw:
		return new(RawContainer[T])
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
func SampleInt[T types.Integer](v []T) []T {
	if len(v) <= SAMPLE_COUNT*SAMPLE_SIZE {
		return v
	}
	s := make([]T, SAMPLE_COUNT*SAMPLE_SIZE)
	chunk := len(v) / SAMPLE_COUNT
	for i := 0; i < SAMPLE_COUNT; i++ {
		start := chunk*i + util.RandIntn(chunk-SAMPLE_SIZE)
		end := start + SAMPLE_SIZE
		copy(s[SAMPLE_SIZE*i:], v[start:end])
	}
	return s
}

// EncodeInt encodes an integer type slice into an integer container
// selecting the most efficient encoding scheme.
func EncodeInt[T types.Integer](ctx *IntegerContext[T], v []T, lvl int) IntegerContainer[T] {
	// analyze full data if missing
	if ctx == nil {
		ctx = AnalyzeInt(v)
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
	rawSize := NewInt[T](TIntegerRaw).Encode(ctx, v, lvl).MaxSize()
	var (
		estSize int
		ok      bool
	)
	switch scheme {
	case TIntegerConstant:
		// varint (max len)
		estSize, ok = NewInt[T](scheme).Encode(ctx, v, lvl).MaxSize(), true
	case TIntegerDelta:
		// 2x varint (max len)
		estSize, ok = NewInt[T](scheme).Encode(ctx, v, lvl).MaxSize(), true
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
	sample := SampleInt(v)

	// analyze sample
	sctx := AnalyzeInt(sample)
	rawSize = NewInt[T](TIntegerRaw).Encode(sctx, sample, lvl).MaxSize()

	// fmt.Printf("> est sample with %s %#v\n", scheme, sctx)
	estSize = NewInt[T](scheme).Encode(sctx, sample, lvl).MaxSize()

	// switch scheme {
	// case TIntegerSimple8:
	// 	// run real s8b encode
	// case TIntegerRunEnd:
	// 	fmt.Printf("> est sample with %s\n", scheme)
	// 	// recurse encode to decide who runs are coded (dict, bitpack, etc)
	// 	estSize = NewInt[T](scheme).Encode(sctx, sample, lvl).MaxSize()
	// case TIntegerDictionary:
	// 	fmt.Printf("> est sample with %s\n", scheme)
	// 	// recurse encode to decide how dict & codes are coded (ree, bitpack)
	// 	estSize = NewInt[T](scheme).Encode(sctx, sample, lvl).MaxSize()
	// }

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
