// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"math"
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

type EncodingIndice struct {
	Exponent uint8
	Factor   uint8
}

func (e EncodingIndice) Hash() uint8 {
	return e.Exponent ^ e.Factor
}

type Combination struct {
	encodingIndice           EncodingIndice
	nAppearances             uint64
	estimatedCompressionSize uint64
}

type State[T types.Float] struct {
	EncodedIntegers    []int64
	Exceptions         []T
	ExceptionPositions []uint32
	Scheme             int
	EncodingIndice     EncodingIndice

	bestKCombinations []Combination
	sample            []T
}

type StateFactory struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
}

func allocState[T types.Float]() *State[T] {
	switch any(T(0)).(type) {
	case float64:
		return stateFactory.f64Pool.Get().(*State[T])
	case float32:
		return stateFactory.f32Pool.Get().(*State[T])
	default:
		return nil
	}
}

func putState[T types.Float](s *State[T]) {
	s.Reset()
	switch any(T(0)).(type) {
	case float64:
		stateFactory.f64Pool.Put(s)
	case float32:
		stateFactory.f32Pool.Put(s)
	}
}

const StateFactoryBufferSize = 1 << 16

var stateFactory = StateFactory{
	f64Pool: sync.Pool{
		New: func() any { return newState[float64](1 << 16) },
	},
	f32Pool: sync.Pool{
		New: func() any { return newState[float32](1 << 16) },
	},
}

func newState[T types.Float](sz int) *State[T] {
	return &State[T]{
		EncodedIntegers:    make([]int64, 0, sz),
		Exceptions:         make([]T, 0, sz),
		ExceptionPositions: make([]uint32, 0, sz),
		sample:             make([]T, 0, VECTOR_SIZE),
		bestKCombinations:  make([]Combination, 0, MAX_K_COMBINATIONS),
		Scheme:             AlpScheme,
	}
}

func NewState[T types.Float](sz int) *State[T] {
	if sz <= StateFactoryBufferSize {
		return allocState[T]()
	}
	return newState[T](sz)
}

func (s *State[T]) Reset() {
	s.Scheme = AlpScheme
	s.ResetCombinations()
	s.ExceptionPositions = s.ExceptionPositions[:0]
	s.sample = s.sample[:0]
	s.bestKCombinations = s.bestKCombinations[:0]
	s.EncodedIntegers = s.EncodedIntegers[:0]
	s.Exceptions = s.Exceptions[:0]
}

func (s *State[T]) ResetCombinations() {
	s.bestKCombinations = s.bestKCombinations[:0]
}

func Scheme[T types.Float](values []T) int {
	enc := NewEncoder[T]()
	enc.analyze(values)
	s := enc.state.Scheme
	enc.Close()
	return s
}

type Encoder[T types.Float] struct {
	exactTypeBitSize int
	state            *State[T]
	constant         *constant
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
		exactTypeBitSize: int(unsafe.Sizeof(T(0)) * 8),
		constant:         newConstant[T](),
	}
}

func (e *Encoder[T]) analyze(values []T) *Encoder[T] {
	if e.state == nil {
		e.state = NewState[T](len(values))
	}
	e.state.sample = FirstLevelSample(e.state.sample, values)
	e.findTopKCombinations(e.state.sample)
	return e
}

// ! Scalar encoding a single value with ALP
func (e *Encoder[T]) encodeValue(value, exp, fact T) int64 {
	n := value * exp * fact
	if isImpossibleToEncode(float64(n)) {
		return ENCODING_UPPER_LIMIT
	}
	return int64(n + T(e.constant.MAGIC_NUMER) - T(e.constant.MAGIC_NUMER))
}

/*
 * Function to sort the best combinations from each vector sampled from the rowgroup
 * First criteria is number of times it appears
 * Second criteria is bigger Exponent
 * Third criteria is bigger Factor
 */

func (e *Encoder[T]) compareCombinations(c1, c2 Combination) bool {
	return (c1.nAppearances > c2.nAppearances) ||
		(c1.nAppearances == c2.nAppearances &&
			(c1.estimatedCompressionSize < c2.estimatedCompressionSize)) ||
		((c1.nAppearances == c2.nAppearances &&
			c1.estimatedCompressionSize == c2.estimatedCompressionSize) &&
			(c2.encodingIndice.Exponent < c1.encodingIndice.Exponent)) ||
		((c1.nAppearances == c2.nAppearances &&
			c1.estimatedCompressionSize == c2.estimatedCompressionSize &&
			c2.encodingIndice.Exponent == c1.encodingIndice.Exponent) &&
			(c2.encodingIndice.Factor < c1.encodingIndice.Factor))

}

/*
 * Find the best combinations of Factor-Exponent from each vector sampled from a rowgroup
 * This function is called once per rowgroup
 * This operates over ALP first level samples
 */
func (e *Encoder[T]) findTopKCombinations(sample []T) {
	e.state.ResetCombinations()

	nSamples := len(sample)
	sampleSize := min(nSamples, SAMPLES_PER_VECTOR)
	bestEstimatedCompressionSize := uint64((sampleSize * (e.constant.EXCEPTION_SIZE + EXCEPTION_POSITION_SIZE)) + (sampleSize * e.constant.EXCEPTION_SIZE))

	bestEncodingIndice := EncodingIndice{
		Exponent: e.constant.MAX_EXPONENT,
		Factor:   e.constant.MAX_EXPONENT,
	}

	foundFactor := uint8(0)
	foundExponent := uint8(0)

	//! We start our optimization with the worst possible total bits obtained from compression
	bestTotalBits := (nSamples * (e.exactTypeBitSize + int(EXCEPTION_POSITION_SIZE*8))) + (nSamples * e.exactTypeBitSize)
	sampleEstimatedCompressionSize := uint64((sampleSize * (e.constant.EXCEPTION_SIZE + EXCEPTION_POSITION_SIZE)) + (sampleSize * e.constant.EXCEPTION_SIZE)) // worse scenario

	// N of appearances is irrelevant at this phase; we search for the best compression for the vector
	bestCombination := Combination{
		encodingIndice:           bestEncodingIndice,
		nAppearances:             0,
		estimatedCompressionSize: uint64(bestTotalBits),
	}
	//! We try all combinations in search for the one which minimize the compression size
	for expIdx := int(e.constant.MAX_EXPONENT); expIdx >= 0; expIdx-- {
		for factorIdx := expIdx; factorIdx >= 0; factorIdx-- {
			currentEncodingIndice := EncodingIndice{Exponent: uint8(expIdx), Factor: uint8(factorIdx)}
			estimatedCompressionSize := e.compressToEstimateSize(sample, currentEncodingIndice)
			currentCombination := Combination{
				encodingIndice:           currentEncodingIndice,
				nAppearances:             0,
				estimatedCompressionSize: estimatedCompressionSize,
			}
			if e.compareCombinations(currentCombination, bestCombination) {
				bestCombination = currentCombination
			}

			if (estimatedCompressionSize < sampleEstimatedCompressionSize) ||
				(estimatedCompressionSize == sampleEstimatedCompressionSize && foundExponent < uint8(expIdx)) ||
				((estimatedCompressionSize == sampleEstimatedCompressionSize && foundExponent == uint8(expIdx)) && (foundFactor < uint8(factorIdx))) {
				sampleEstimatedCompressionSize = estimatedCompressionSize
				foundExponent = uint8(expIdx)
				foundFactor = uint8(factorIdx)
				if sampleEstimatedCompressionSize < bestEstimatedCompressionSize {
					bestEstimatedCompressionSize = sampleEstimatedCompressionSize
				}
			}
		}
	}

	// We adapt scheme if we were not able to achieve compression in the current rg
	if bestEstimatedCompressionSize >= e.constant.RD_SIZE_THRESHOLD_LIMIT {
		e.state.Scheme = AlpRdScheme
	}

	e.state.bestKCombinations = append(e.state.bestKCombinations, bestCombination)
}

func (e Encoder[T]) compressToEstimateSize(sample []T, encodingIndice EncodingIndice) uint64 {
	nValues := len(sample)
	exceptionsCount := 0
	nonExceptionsCount := 0
	estimatedBitsPerValue := uint32(0)
	estimatedCompressionSize := uint64(0)
	var maxEncodedValue int64 = math.MaxInt64
	var minEncodedValue int64 = math.MinInt64

	fac := FACT_ARR[encodingIndice.Factor]
	exp := T(e.constant.FRAC_ARR[encodingIndice.Exponent])

	frac := T(e.constant.FRAC_ARR[encodingIndice.Factor])
	expr := T(e.constant.EXP_ARR[encodingIndice.Exponent])

	for _, value := range sample {
		encodedValue := e.encodeValue(value, frac, expr)
		decodedValue := decodeValue(encodedValue, fac, exp)
		if decodedValue == value {
			nonExceptionsCount++
			maxEncodedValue = max(encodedValue, maxEncodedValue)
			minEncodedValue = min(encodedValue, minEncodedValue)
			continue
		}
		exceptionsCount++
	}

	// We penalize combinations which yields to almost all exceptions
	if nonExceptionsCount < 2 {
		return math.MaxUint64
	}

	// Evaluate Factor/Exponent compression size (we optimize for FOR)
	delta := uint64(maxEncodedValue) - uint64(minEncodedValue)
	estimatedBitsPerValue = uint32(math.Ceil(math.Log2(float64(delta + 1))))
	estimatedCompressionSize += uint64(nValues) * uint64(estimatedBitsPerValue)
	estimatedCompressionSize += uint64(exceptionsCount) * (uint64(e.exactTypeBitSize) + (EXCEPTION_POSITION_SIZE * 8))
	return estimatedCompressionSize
}

/*
 * Find the best combination of Factor-Exponent for a vector from within the best k combinations
 * This is ALP second level sampling
 */
func (e *Encoder[T]) findBestExponentFactorFromCombinations(src []T) {
	//! We sample equidistant values within a vector; to do this we skip a fixed number of values
	sample := make([]T, 0) // TODO: reuse state sample vector
	nValue := len(src)
	idxIncrements := max(1, (nValue / int(SAMPLES_PER_VECTOR))) //?

	for i := 0; i < nValue; i += idxIncrements {
		sample = append(sample, src[i])
	}

	worseTotalBitsCounter := uint64(0)
	bestTotalBits := uint64(math.MaxUint64)
	bestEncodingIndice := EncodingIndice{}

	//! We try each K combination in search for the one which minimize the compression size in the vector
	for i := 0; i < len(e.state.bestKCombinations); i++ {
		estimateCompressionSize := e.compressToEstimateSize(sample, e.state.bestKCombinations[i].encodingIndice)

		// If current compression size is worse (higher) or equal than the current best combination
		if estimateCompressionSize >= bestTotalBits {
			worseTotalBitsCounter += 1
			// Early exit strategy
			if worseTotalBitsCounter == SAMPLING_EARLY_EXIT_THRESHOLD {
				break
			}
			continue
		}
		// Otherwise we replace the best and continue trying with the next combination
		bestTotalBits = estimateCompressionSize
		bestEncodingIndice = e.state.bestKCombinations[i].encodingIndice
		worseTotalBitsCounter = 0
	}
	e.state.EncodingIndice = bestEncodingIndice
}

func (e *Encoder[T]) Compress(src []T) *Encoder[T] {
	// alloc state and analyze vector
	e.analyze(src)

	// process vector
	if len(e.state.bestKCombinations) > 1 { // Only if more than 1 found top combinations we sample and search
		e.findBestExponentFactorFromCombinations(src) // ?
	} else {
		e.state.EncodingIndice = e.state.bestKCombinations[0].encodingIndice
	}

	// Encoding Floating-Point to Int64
	//! We encode all the values regardless of their correctness to recover the original floating-point
	fac := FACT_ARR[e.state.EncodingIndice.Factor]
	exp := T(e.constant.FRAC_ARR[e.state.EncodingIndice.Exponent])
	frac := T(e.constant.FRAC_ARR[e.state.EncodingIndice.Factor])
	expr := T(e.constant.EXP_ARR[e.state.EncodingIndice.Exponent])

	exceptionsIdx := 0
	exceptionPositions := e.state.ExceptionPositions[:cap(e.state.ExceptionPositions)]
	e.state.EncodedIntegers = e.state.EncodedIntegers[:len(src)]
	for i := range len(src) {
		actualValue := src[i]
		encodeValue := e.encodeValue(actualValue, frac, expr)
		decodeValue := decodeValue(encodeValue, fac, exp)
		e.state.EncodedIntegers[i] = encodeValue
		exceptionPositions[exceptionsIdx] = uint32(i)
		exceptionsIdx += util.Bool2int(decodeValue != actualValue)
	}

	// Finding first non exception value
	nonExceptionValue := int64(0)
	for i := range src {
		if i != int(exceptionPositions[i]) {
			nonExceptionValue = e.state.EncodedIntegers[i]
			break
		}
	}

	// Replacing that first non exception value on the vector exceptions
	for i := range exceptionsIdx {
		exceptionPos := exceptionPositions[i]
		actualValue := src[exceptionPos]
		e.state.EncodedIntegers[exceptionPos] = nonExceptionValue
		e.state.Exceptions = append(e.state.Exceptions, actualValue)
	}
	e.state.ExceptionPositions = exceptionPositions[:exceptionsIdx]

	return e
}
