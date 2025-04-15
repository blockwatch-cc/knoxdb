// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc,alex@blockwatch.cc

package alp

import (
	"fmt"
	"math"
	"math/bits"
	"sort"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	InvalidScheme = iota
	AlpScheme
	AlpRdScheme
)

type Encoding struct {
	E uint8
	F uint8
}

type Combination struct {
	enc   Encoding
	count int
	rate  float64
}

func (c Combination) Key() uint16 {
	return uint16(c.enc.E)<<8 | uint16(c.enc.F)
}

func (c Combination) Rate() float64 {
	return c.rate
}

type Combinations []Combination

func (l Combinations) Len() int      { return len(l) }
func (l Combinations) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l Combinations) Less(i, j int) bool {
	c1 := l[i]
	c2 := l[j]
	return c1.count > c2.count ||
		(c1.count == c2.count && c1.enc.E > c2.enc.E) ||
		(c1.count == c2.count && c1.enc.E == c2.enc.E && c1.enc.F > c2.enc.F)
}

type State[T types.Float] struct {
	Scheme     int
	Integers   []int64
	Exceptions []T
	Positions  []uint32
	Encoding   Encoding
	RD         EncodingRD
	topk       Combinations
	allk       map[uint16]int
	sample     []T
	unique     []uint16
}

type StateFy struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
}

func allocState[T types.Float]() *State[T] {
	switch any(T(0)).(type) {
	case float64:
		return stateFy.f64Pool.Get().(*State[T])
	case float32:
		return stateFy.f32Pool.Get().(*State[T])
	default:
		return nil
	}
}

func putState[T types.Float](s *State[T]) {
	s.Reset()
	switch any(T(0)).(type) {
	case float64:
		stateFy.f64Pool.Put(s)
	case float32:
		stateFy.f32Pool.Put(s)
	}
}

const StateBufferSize = 1 << 16

var stateFy = StateFy{
	f64Pool: sync.Pool{
		New: func() any { return newState[float64](1 << 16) },
	},
	f32Pool: sync.Pool{
		New: func() any { return newState[float32](1 << 16) },
	},
}

func newState[T types.Float](sz int) *State[T] {
	return &State[T]{
		Scheme:     InvalidScheme,
		Integers:   make([]int64, 0, sz),
		Exceptions: make([]T, 0, sz),
		Positions:  make([]uint32, 0, sz),
		sample:     make([]T, 0, VECTOR_SIZE),
		topk:       make(Combinations, 0, MAX_K_COMBINATIONS),
		allk:       make(map[uint16]int, MAX_K_COMBINATIONS),
		unique:     make([]uint16, 1<<16),
	}
}

func NewState[T types.Float](sz int) *State[T] {
	if sz <= StateBufferSize {
		return allocState[T]()
	}
	return newState[T](sz)
}

func (s *State[T]) Reset() {
	s.Scheme = InvalidScheme
	s.Positions = s.Positions[:0]
	s.sample = s.sample[:0]
	s.topk = s.topk[:0]
	clear(s.allk)
	s.Integers = s.Integers[:0]
	s.Exceptions = s.Exceptions[:0]
}

func (s *State[T]) Top() Combination {
	if len(s.topk) > 0 {
		return s.topk[0]
	}
	return Combination{rate: 1.0}
}

type Encoder[T types.Float] struct {
	bits     int
	state    *State[T]
	constant *constant
}

func (e *Encoder[T]) State() *State[T] {
	return e.state
}

func (e *Encoder[T]) Close() {
	if e.state != nil {
		putState(e.state)
	}
	e.state = nil
	e.constant = nil
}

func NewEncoder[T types.Float]() *Encoder[T] {
	return &Encoder[T]{
		bits:     int(unsafe.Sizeof(T(0)) * 8),
		constant: newConstant[T](),
	}
}

func (e *Encoder[T]) IsInit() bool {
	return e != nil && e.state != nil && e.state.Scheme != InvalidScheme
}

func (e *Encoder[T]) Analyze(values []T) *Encoder[T] {
	if e.state == nil {
		e.state = NewState[T](len(values))
	}
	if e.state.Scheme == InvalidScheme {
		// fmt.Printf("Analyze: %d values\n", len(values))
		e.state.sample = FirstLevelSample(e.state.sample, values)
		e.analyzeSample(e.state.sample)
	}
	return e
}

// Find the best combinations of Factor-Exponent from a sampled vector
// This operates over ALP first level samples
func (e *Encoder[T]) analyzeSample(sample []T) {
	var voffs, szSample int

	// a sample may contain data from up to 100 probes, each 32 items long
	nSamples := min(len(sample), SAMPLES_PER_VECTOR)

	// probes are called vectors here and we visit each of them individually
	nVectors := (len(sample) + SAMPLES_PER_VECTOR - 1) / SAMPLES_PER_VECTOR

	// fmt.Printf("ALP: %d samples\n", nSamples)
	// fmt.Printf("ALP: %d vectors\n", nVectors)

	// init best size found so far to a worst-case max
	bestSize := math.MaxInt
	rawSize := len(sample) * int(unsafe.Sizeof(T(0)))

	// For each vector in the rg sample
	for range nVectors {
		// the best factor / exponent pair we have found for this vector
		var useFac, useExp int

		// We start our optimization with the worst possible total bits obtained from compression
		bestProbeSize := math.MaxInt

		// We try all combinations in search for the one which minimize the compression size
		for ei := int(e.constant.MAX_EXPONENT); ei >= 0; ei-- {
			for fi := ei; fi >= 0; fi-- {
				var (
					nEx, nNonEx int
					minv, maxv  int64 = 1<<63 - 1, 0
				)

				// lookup current factors
				encFactor := T(e.constant.FRAC_ARR[fi])
				encExponent := T(e.constant.EXP_ARR[ei])
				decFactor := FACT_ARR[fi]
				decExponent := T(e.constant.FRAC_ARR[ei])

				// analyze probe (32 values)
				for _, val := range sample[voffs : voffs+nSamples] {
					enc := e.encodeValue(val, encExponent, encFactor)
					dec := decodeValue(enc, decFactor, decExponent)
					if dec == val {
						nNonEx++
						maxv = max(enc, maxv)
						minv = min(enc, minv)
					} else {
						nEx++
					}
				}

				// Ignore combinations with many exceptions
				if nNonEx < 2 {
					// fmt.Printf("> E=%d F=%d => too many exceptions %d/%d\n", ei, fi, nEx, nSamples)
					continue
				}

				// evaluate performance
				nBits := bits.Len64(uint64(maxv - minv))
				szBits := (nSamples*nBits + 7) + nEx*(e.constant.EXCEPTION_SIZE+EXCEPTION_POSITION_SIZE)

				// keep better compressing versions
				if szBits < bestProbeSize || // We prefer better size
					(szBits == bestProbeSize && useExp < ei) || // or bigger exponents
					(szBits == bestProbeSize && useExp == ei && useFac < fi) { // or bigger factors

					// fmt.Printf("> E=%d F=%d => ex=%d size=%d\n", ei, fi, nEx, szBits/8)

					bestProbeSize = szBits
					useExp = ei
					useFac = fi
					if bestProbeSize < bestSize {
						bestSize = bestProbeSize
					}
				}
			}
		}

		// fmt.Printf("ALP: keep E=%d F=%d sz=%d\n", useExp, useFac, bestSize/8)

		// remember encoding and count how often it appeared
		key := uint16(useExp)<<8 | uint16(useFac)
		e.state.allk[key]++
		szSample += bestProbeSize

		voffs += nSamples
	}

	// We switch scheme if we were not able to achieve compression in any of the probes
	if bestSize >= e.constant.RD_SIZE_THRESHOLD_LIMIT {
		// fmt.Printf("ALP: should use ALP-RD (sz: %d > %d) on %v ...\n",
		// 	bestSize/8, e.constant.RD_SIZE_THRESHOLD_LIMIT/8, sample[:min(5, len(sample))])
		e.state.Scheme = AlpRdScheme
		e.state.RD = EstimateRD(sample, e.state.unique)
		return
	}

	// Convert encoding combination map to vector for sorting
	e.state.topk = e.state.topk[:0]
	for k, c := range e.state.allk {
		e.state.topk = append(e.state.topk, Combination{
			enc:   Encoding{E: uint8(k >> 8), F: uint8(k)},
			count: c,
			rate:  float64(szSample/8) / float64(rawSize),
		})
	}

	// We sort combinations based on times they appeared
	sort.Sort(e.state.topk)

	// limit
	e.state.topk = e.state.topk[:min(len(e.state.topk), MAX_K_COMBINATIONS)]

	// if len(e.state.topk) > 1 {
	// 	fmt.Printf("ALP: %#v\n", e.state.topk)
	// }
	e.state.Scheme = AlpScheme
}

// Select the best combination of Factor-Exponent for a vector from
// within the best k combinations. This is ALP second level sampling.
func (e *Encoder[T]) selectBestEncoding(src []T) Encoding {
	// We sample equidistant values within the src vector;
	// to do this we skip a fixed number of values
	nValues := len(src)
	step := max(1, (nValues+SAMPLES_PER_VECTOR-1)/SAMPLES_PER_VECTOR)

	var (
		best       Encoding
		bestSize   int
		worseCount int
	)

	// We try each K combination on a small sample in search for
	// the one which minimizes the compression size across the vector.
	for k, c := range e.state.topk {
		var (
			nEx        int
			minv, maxv int64 = 1<<63 - 1, 0
		)

		// lookup current factors
		encFactor := T(e.constant.FRAC_ARR[c.enc.F])
		encExponent := T(e.constant.EXP_ARR[c.enc.E])
		decFactor := FACT_ARR[c.enc.F]
		decExponent := T(e.constant.FRAC_ARR[c.enc.E])

		// analyze probe (32 values)
		for i := 0; i < len(src); i += step {
			val := src[i]
			enc := e.encodeValue(val, encExponent, encFactor)
			dec := decodeValue(enc, decFactor, decExponent)
			if dec == val {
				maxv = max(enc, maxv)
				minv = min(enc, minv)
			} else {
				nEx++
			}
		}

		// Evaluate factor/exponent performance
		nBits := bits.Len64(uint64(maxv - minv))
		size := SAMPLES_PER_VECTOR*nBits + nEx*(e.constant.EXCEPTION_SIZE+EXCEPTION_POSITION_SIZE)

		// init from first encoding of K
		if k == 0 {
			best.F = c.enc.F
			best.E = c.enc.E
			bestSize = size
			continue
		}

		if size >= bestSize {
			worseCount++
			if worseCount == SAMPLING_EARLY_EXIT_THRESHOLD {
				break // We stop only if two are worse
			}
			continue
		}

		// Otherwise we replace the best and continue trying with the next combination
		best.F = c.enc.F
		best.E = c.enc.E
		bestSize = size
		worseCount = 0
	}

	// use the best encoding
	return best
}

func (e *Encoder[T]) Encode(src []T) *Encoder[T] {
	// alloc state and analyze vector (first pass)
	if !e.IsInit() {
		e.Analyze(src)
	}

	// must have ALP detected and set up
	if e.state.Scheme != AlpScheme {
		panic(fmt.Errorf("must encode source with ALP/RD"))
	}

	// select best encoding process vector (second pass)
	if len(e.state.topk) > 1 {
		e.state.Encoding = e.selectBestEncoding(src)
	} else {
		e.state.Encoding = e.state.topk[0].enc
	}

	// Encoding Floating-Point to Int64
	// We encode all the values regardless of their correctness to
	// recover the original floating-point
	encFactor := T(e.constant.FRAC_ARR[e.state.Encoding.F])
	encExponent := T(e.constant.EXP_ARR[e.state.Encoding.E])
	decFactor := FACT_ARR[e.state.Encoding.F]
	decExponent := T(e.constant.FRAC_ARR[e.state.Encoding.E])

	exceptionsIdx := 0
	positions := e.state.Positions[:cap(e.state.Positions)]
	e.state.Integers = e.state.Integers[:len(src)]
	for i, val := range src {
		enc := e.encodeValue(val, encExponent, encFactor)
		dec := decodeValue(enc, decFactor, decExponent)
		e.state.Integers[i] = enc
		positions[exceptionsIdx] = uint32(i)
		exceptionsIdx += util.Bool2int(dec != val)
	}

	// Find first non exception value
	nonExceptionValue := int64(0)
	for i := range src {
		if i != int(positions[i]) {
			nonExceptionValue = e.state.Integers[i]
			break
		}
	}

	// Replace that first non exception value on the vector exceptions
	for i := range exceptionsIdx {
		exceptionPos := positions[i]
		actualValue := src[exceptionPos]
		e.state.Integers[exceptionPos] = nonExceptionValue
		e.state.Exceptions = append(e.state.Exceptions, actualValue)
	}
	e.state.Positions = positions[:exceptionsIdx]

	return e
}

// Scalar encoding a single value with ALP
func (e *Encoder[T]) encodeValue(value, exp, fact T) int64 {
	n := value * exp * fact
	if isImpossibleToEncode(float64(n)) {
		return int64(ENCODING_UPPER_LIMIT)
	}
	return int64(n + T(e.constant.MAGIC_NUMER) - T(e.constant.MAGIC_NUMER))
}
