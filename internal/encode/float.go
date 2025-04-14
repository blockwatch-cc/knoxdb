// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package encode

import (
	"encoding/binary"
	"fmt"
	"math"

	"blockwatch.cc/knoxdb/internal/encode/alp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
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

// EncodeFloat encodes a float type slice into a float container
// selecting the most efficient encoding scheme
func EncodeFloat[T types.Float](ctx *FloatContext[T], v []T, lvl int) FloatContainer[T] {
	// analyze full data if missing
	if ctx == nil {
		ctx = AnalyzeFloat(v, true, lvl == MAX_CASCADE)
		defer ctx.Close()
	}

	// fmt.Printf("Enc %d vals lvl=%d unique=%d schemes=%v----\n", len(v), lvl, ctx.NumUnique, ctx.EligibleSchemes(lvl))

	// try all eligible encoding schemes
	var (
		bestScheme FloatContainerType = TFloatRaw
		bestRatio  float64            = 1.0
	)
	if lvl > 0 {
		for _, scheme := range ctx.EligibleSchemes(lvl) {
			// fmt.Printf("%s Try %s\n", strings.Repeat(">", 3-lvl), scheme)
			if rd := EstimateFloat(scheme, ctx, v, lvl); rd < bestRatio {
				bestRatio = rd
				bestScheme = scheme

				// TODO: consider a cut-off when already good enough
				// if bestRatio < 0.05 {
				// 	break
				// }

				// 	fmt.Printf("%s => %f !!\n", scheme, rd)
				// } else {
				// 	fmt.Printf("%s => %f\n", scheme, rd)
			}
		}
	}
	// fmt.Printf("%s Use %s => %f ----\n", strings.Repeat(">", 3-lvl), bestScheme, bestRatio)

	// alloc best container and encode
	return NewFloat[T](bestScheme).Encode(ctx, v, lvl)
}

// EstimateFloat provides encoded size estimation without running the full encoder
// in some cases. In others, particularly nested cases, we need a full encode but
// on a small sample only.
func EstimateFloat[T types.Float](scheme FloatContainerType, ctx *FloatContext[T], vals []T, lvl int) float64 {
	// estimate cheap encodings
	var (
		w       int = SizeOf[T]()
		rawSize int = ctx.rawCosts()
		estSize int
		ok      bool
	)
	switch scheme {
	case TFloatConstant:
		estSize, ok = 1+w+num.MaxVarintLen32, true
	case TFloatRaw:
		estSize, ok = rawSize, true
	case TFloatAlp, TFloatAlpRd:
		// at this point we have an ALP analysis available
		as := ctx.AlpEncoder.State()
		// fmt.Printf("Analyzed ALP: escheme=%s scheme=%d alp=%#v rd=%#v\n", scheme, as.Scheme, as.Top(), as.RD)

		// compare suggested ALP scheme with requested scheme
		switch as.Scheme {
		case alp.AlpScheme:
			// predict encoding size from best sample encoding rate
			if scheme == TFloatAlp {
				estSize, ok = 6+int(as.Top().Rate()*float64(ctx.rawCosts()-5)), true
			}
		case alp.AlpRdScheme:
			// predict encoding size from best sample encoding rate
			if scheme == TFloatAlpRd {
				estSize, ok = 6+int(as.RD.Rate*float64(ctx.rawCosts()-5)), true
			}
		}

		// don't use this scheme on mismatch
		if !ok {
			return 10.0
		}

	case TFloatDictionary:
		// upper bound for dict encoding using raw as child base
		estSize, ok = dictCosts(ctx.NumValues, ctx.PhyBits, ctx.NumUnique), true

	case TFloatRunEnd:
		// upper bound for run end encoding using raw as child base
		estSize, ok = runEndCosts(ctx.NumValues, ctx.NumRuns, ctx.PhyBits), true

	}
	if ok {
		return float64(estSize) / float64(rawSize)
	}

	// // the remaining schemes TFloatRunEnd, TFloatDictionary, TFloatAlp, TFloatAlpRd
	// // use child containers which we cannot easily estimate without running
	// // the encoder itself, to save time we use a sample

	// // sample
	// if ctx.Sample == nil {
	// 	ctx.Sample, ctx.FreeSample = Sample(vals)
	// 	ctx.SampleCtx = AnalyzeFloat(ctx.Sample, true, lvl == MAX_CASCADE)
	// }

	// // trail encode the sample as target scheme
	// rawSize = 1 + num.MaxVarintLen32 + w*len(ctx.Sample)
	// enc := NewFloat[T](scheme).Encode(ctx.SampleCtx, ctx.Sample, lvl)
	// estSize = enc.MaxSize()
	// enc.Close()

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
