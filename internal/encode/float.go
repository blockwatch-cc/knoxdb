// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc, alex@blockwatch.cc

package encode

import (
	"encoding/binary"
	"fmt"
	"math"

	"blockwatch.cc/knoxdb/internal/encode/alp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// NewFloat creates a new integer container from scheme type.
func NewFloat[T types.Float](scheme ContainerType) NumberContainer[T] {
	switch scheme {
	case TFloatConstant:
		return newFloatConstContainer[T]()
	case TFloatRunEnd:
		return newFloatRunEndContainer[T]()
	case TFloatDictionary:
		return newFloatDictionaryContainer[T]()
	case TFloatAlp:
		return newFloatAlpContainer[T]()
	case TFloatAlpRd:
		return newFloatAlpRdContainer[T]()
	case TFloatRaw:
		return newFloatRawContainer[T]()
	default:
		panic(fmt.Errorf("invalid float scheme %d (%s)", scheme, scheme))
	}
}

// EncodeFloat encodes a float type slice into a float container
// selecting the most efficient encoding scheme
func EncodeFloat[T types.Float](ctx *Context[T], v []T) NumberContainer[T] {
	// analyze full data if missing
	if ctx == nil {
		ctx = AnalyzeFloat(v, true, true)
		defer ctx.Close()
	}

	// try all eligible encoding schemes
	var (
		bestScheme ContainerType = TFloatRaw
		bestRatio  float64       = 1.0
	)
	if ctx.Lvl > 0 {
		for _, scheme := range ctx.EligibleFloatSchemes() {
			if rd := EstimateFloat(ctx, scheme, v); rd < bestRatio {
				bestRatio = rd
				bestScheme = scheme
			}
		}
	}

	// alloc best container and encode
	return NewFloat[T](bestScheme).Encode(ctx, v)
}

// EstimateFloat provides encoded size estimation without running the full encoder
// in some cases. In others, particularly nested cases, we need a full encode but
// on a small sample only.
func EstimateFloat[T types.Float](ctx *Context[T], scheme ContainerType, vals []T) float64 {
	// estimate cheap encodings
	var (
		w       int = util.SizeOf[T]()
		rawSize int = ctx.rawCosts()
		estSize int
	)
	switch scheme {
	case TFloatConstant:
		estSize = 1 + w + num.MaxVarintLen32

	case TFloatRaw:
		estSize = rawSize

	case TFloatAlp, TFloatAlpRd:
		// at this point we have an ALP analysis available
		// compare suggested ALP scheme with requested scheme
		switch ctx.Alp.Scheme {
		case alp.ALP_SCHEME:
			// predict encoding size from best sample encoding rate
			if scheme != TFloatAlp {
				return 1.0
			}
			estSize = 6 + int(ctx.Alp.Rate*float64(ctx.rawCosts()-5))

		case alp.ALP_RD_SCHEME:
			// predict encoding size from best sample encoding rate
			if scheme != TFloatAlpRd {
				return 1.0
			}
			estSize = 6 + int(ctx.Alp.Rate*float64(ctx.rawCosts()-5))
		}

	case TFloatDictionary:
		// upper bound for dict encoding using raw as child base
		estSize = ctx.dictCosts()

	case TFloatRunEnd:
		// upper bound for run end encoding using raw as child base
		estSize = ctx.runEndCosts()
	}

	// return final rate
	return float64(estSize) / float64(rawSize)
}

// LoadFloat loads a float container from buffer
func LoadFloat[T types.Float](buf []byte) (NumberContainer[T], error) {
	c := NewFloat[T](ContainerType(buf[0]))
	if _, err := c.Load(buf); err != nil {
		return nil, err
	}
	return c, nil
}

// storeFloat stores a float to a buffer
func storeFloat[T types.Float](buf []byte, val T) []byte {
	switch any(T(0)).(type) {
	case float64:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], math.Float64bits(float64(val)))
		buf = append(buf, b[:]...)
	case float32:
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], math.Float32bits(float32(val)))
		buf = append(buf, b[:]...)
	}
	return buf
}

// loadFloat stores a float to a buffer
func loadFloat[T types.Float](buf []byte) (T, []byte) {
	var v T
	switch any(T(0)).(type) {
	case float64:
		v = T(math.Float64frombits(binary.LittleEndian.Uint64(buf)))
		buf = buf[8:]
	case float32:
		v = T(math.Float32frombits(binary.LittleEndian.Uint32(buf)))
		buf = buf[4:]
	}
	return v, buf
}
