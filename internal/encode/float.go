// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package encode

import (
	"encoding/binary"
	"fmt"
	"math"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

type FloatContainerType byte

const (
	TFloatConstant FloatContainerType = iota
	TFloatRunEnd
	TFloatDictionary
	TFloatAlp
	TFloatAlpRd
	TFloatRaw
)

var (
	fTypeNames    = "const_run_dict_alp_alprd_raw"
	fTypeNamesOfs = []int{0, 6, 10, 15, 19, 25, 29}
)

func (f FloatContainerType) String() string {
	return fTypeNames[fTypeNamesOfs[f] : fTypeNamesOfs[f+1]-1]
}

type FloatContainer[T types.Float] interface {
	// introspect
	Type() FloatContainerType
	Len() int
	Info() string

	// data access
	Get(int) T
	AppendTo([]uint32, []T) []T

	// encode
	Encode(ctx *FloatContext[T], vals []T, lvl int) FloatContainer[T]

	// IO
	MaxSize() int                // helps dimension buffer before write
	Store([]byte) []byte         // simple, composable, pre-alloc via MaxSize
	Load([]byte) ([]byte, error) // simple, composable
	Close()                      // free resources

	// matchers
	types.NumberMatcher[T]
}

// NewFloat creates a new integer container from scheme type.
func NewFloat[T types.Float](scheme FloatContainerType) FloatContainer[T] {
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
		panic(fmt.Errorf("invalid scheme %d", scheme))
	}
}

// SampleFloat extracts a random sample from float slice v. It is used
// when estimating the effectiveness of different encoders.
func SampleFloat[T types.Float](v []T) ([]T, bool) {
	if len(v) <= SAMPLE_COUNT*SAMPLE_SIZE {
		return v, false
	}
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

// EncodeFloat encodes a float type slice into a float container
// selecting the most efficient encoding scheme
func EncodeFloat[T types.Float](ctx *FloatContext[T], v []T, lvl int) FloatContainer[T] {
	// analyze full data if missing
	if ctx == nil {
		ctx = AnalyzeFloat(v, true, lvl == MAX_CASCADE)
		defer ctx.Close()
	}

	// try all eligible encoding schemes
	var (
		bestScheme FloatContainerType = TFloatRaw
		bestRatio  float64            = 1.0
	)
	if lvl > 0 {
		for _, scheme := range ctx.EligibleSchemes(lvl) {
			if rd := EstimateFloat(scheme, ctx, v, lvl); rd < bestRatio {
				bestRatio = rd
				bestScheme = scheme

				// TODO: consider a cut-off when already good enough
				// if bestRatio < 0.05 {
				// 	break
				// }
			}
		}
	}

	// alloc best container and encode
	return NewFloat[T](bestScheme).Encode(ctx, v, lvl)
}

// EstimateFloat provides encoded size estimation without running the full encoder
// in some cases. In others, particularly nested cases, we need a full encode but
// on a small sample only.
func EstimateFloat[T types.Float](scheme FloatContainerType, ctx *FloatContext[T], v []T, lvl int) float64 {
	// estimate cheap encodings
	var (
		rawSize int = 1 + num.MaxVarintLen32 + SizeOf[T]()*len(v)
		estSize int
		ok      bool
	)
	switch scheme {
	case TFloatConstant:
		estSize, ok = 1+SizeOf[T]()+num.MaxVarintLen32, true
	case TFloatRaw:
		estSize, ok = rawSize, true
	}
	if ok {
		return float64(estSize) / float64(rawSize)
	}

	// the remaining schemes TFloatRunEnd, TFloatDictionary, TFloatAlp, TFloatAlpRd
	// use child containers which we cannot easily estimate without running
	// the encoder itself, to save time we use a sample

	// sample
	if ctx.Sample == nil {
		ctx.Sample, ctx.FreeSample = SampleFloat(v)
		ctx.SampleCtx = AnalyzeFloat(ctx.Sample, true, lvl == MAX_CASCADE)
	}

	// trail encode the sample as target scheme
	rawSize = 1 + num.MaxVarintLen32 + SizeOf[T]()*len(ctx.Sample)
	enc := NewFloat[T](scheme).Encode(ctx.SampleCtx, ctx.Sample, lvl)
	estSize = enc.MaxSize()
	enc.Close()

	return float64(estSize) / float64(rawSize)
}

// LoadFloat loads a float container from buffer
func LoadFloat[T types.Float](buf []byte) (FloatContainer[T], error) {
	c := NewFloat[T](FloatContainerType(buf[0]))
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
