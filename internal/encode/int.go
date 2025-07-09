// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// NewInt creates a new integer container from scheme type.
func NewInt[T types.Integer](scheme ContainerType) NumberContainer[T] {
	switch scheme {
	case TIntConstant:
		return newConstContainer[T]()
	case TIntDelta:
		return newDeltaContainer[T]()
	case TIntRunEnd:
		return newRunEndContainer[T]()
	case TIntBitpacked:
		return newBitpackContainer[T]()
	case TIntDictionary:
		return newDictionaryContainer[T]()
	case TIntSimple8:
		return newSimple8Container[T]()
	case TIntRaw:
		return newRawContainer[T]()
	default:
		panic(fmt.Errorf("invalid integer scheme %d (%s)", scheme, scheme))
	}
}

// EncodeInt encodes an integer type slice into an integer container
// selecting the most efficient encoding scheme.
func EncodeInt[T types.Integer](ctx *Context[T], v []T) NumberContainer[T] {
	// analyze full data if missing
	if ctx == nil {
		ctx = AnalyzeInt(v, true)
		defer ctx.Close()
	}

	// try all eligible encoding schemes
	var (
		bestScheme = TIntRaw
		bestRatio  = 1.0
	)
	if ctx.Lvl > 0 {
		for _, scheme := range ctx.EligibleIntSchemes() {
			if rd := EstimateInt(ctx, scheme, v); rd < bestRatio {
				bestRatio = rd
				bestScheme = scheme
			}
		}
	}

	// alloc best container and encode
	return NewInt[T](bestScheme).Encode(ctx, v)
}

// EstimateInt provides encoded size estimation without running the full encoder
// in some cases. In others, particularly nested cases, we need a full encode but
// on a small sample only.
func EstimateInt[T types.Integer](ctx *Context[T], scheme ContainerType, v []T) float64 {
	// estimate cheap encodings
	var (
		rawSize = ctx.rawCosts()
		estSize int
		ok      bool
	)
	switch scheme {
	case TIntConstant:
		estSize, ok = 1+2*num.MaxVarintLen32, true
	case TIntDelta:
		estSize, ok = 1+3*num.MaxVarintLen64, true
	case TIntBitpacked:
		estSize, ok = ctx.bitPackCosts(), true
	case TIntRaw:
		estSize, ok = rawSize, true
	case TIntDictionary:
		// upper bound for dict encoding using bit-packing as child base
		// penalize dict at lower levels
		estSize, ok = ctx.dictCosts()+100*(MAX_LEVEL-ctx.Lvl), true
	case TIntRunEnd:
		// upper bound for run end encoding using bit-packing as child base
		estSize, ok = ctx.runEndCosts(), true
	}
	if ok {
		return float64(estSize) / float64(rawSize)
	}

	// use sampling for TIntegerSimple8
	if ctx.Sample == nil {
		ctx.Sample, ctx.FreeSample = Sample(v)
		ctx.SampleCtx = AnalyzeInt(ctx.Sample, false)
		ctx.SampleCtx.Lvl = ctx.Lvl
	}

	// trail encode the sample as simple8
	enc := NewInt[T](scheme).Encode(ctx.SampleCtx, ctx.Sample)
	estSize = enc.Size()
	enc.Close()

	return float64(estSize) / float64(ctx.SampleCtx.rawCosts())
}

// LoadInt loads an integer container from buffer.
func LoadInt[T types.Integer](buf []byte) (NumberContainer[T], error) {
	c := NewInt[T](ContainerType(buf[0]))
	if _, err := c.Load(buf); err != nil {
		return nil, err
	}
	return c, nil
}
