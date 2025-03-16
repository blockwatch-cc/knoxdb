// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"math"
	"math/bits"

	"golang.org/x/exp/constraints"
)

const (
	InvalidScheme = iota
	AlpScheme
	AlpRdScheme
)

type EncodingIndice struct {
	exponent uint8
	factor   uint8
}

func (e EncodingIndice) Hash() uint8 {
	return e.exponent ^ e.factor
}

type Combination struct {
	encodingIndice           EncodingIndice
	nAppearances             uint64
	estimatedCompressionSize uint64
}

type State[T constraints.Float] struct {
	ExceptionsCount    uint16
	ExceptionPositions []int
	sampledValuesN     []T
	bestKCombinations  []Combination
	BitWidth           uint8
	FOR                int64 // Frame of Reference
	EncodingIndice     EncodingIndice
	EncodedIntegers    []int64
	Exceptions         []T
	Scheme             int
}

func NewState[T constraints.Float](sz int) *State[T] {
	return &State[T]{
		EncodedIntegers:    make([]int64, 0, sz),
		Exceptions:         make([]T, 0, sz),
		ExceptionPositions: make([]int, 0, sz),
		sampledValuesN:     make([]T, 0, VECTOR_SIZE),
		bestKCombinations:  make([]Combination, 0, MAX_K_COMBINATIONS),
		Scheme:             AlpScheme,
	}
}

func (s *State[T]) Reset() {
	s.BitWidth = 0
	s.ExceptionsCount = 0
	s.ResetCombinations()
}

func (s *State[T]) ResetCombinations() {
	s.bestKCombinations = s.bestKCombinations[:0]
}

func Scheme[T constraints.Float](values []T) (int, error) {
	enc := newEncoder(values, 0)
	return enc.state.Scheme, nil
}

func Compress[T constraints.Float](values []T) *State[T] {
	enc := newEncoder(values, 0)
	enc.compress(values, enc.state)
	return enc.state
}

type Encoder[T constraints.Float] struct {
	exactTypeBitSize uint8
	state            *State[T]
	constant         constant
}

func newEncoder[T constraints.Float](dataColumn []T, columnOffset int) *Encoder[T] {
	e := &Encoder[T]{}
	e.constant = newConstant[T]()
	e.state = NewState[T](len(dataColumn))
	e.state.sampledValuesN = FirstLevelSample(e.state.sampledValuesN, dataColumn, columnOffset)
	e.findTopKCombinations(e.state.sampledValuesN, e.state)
	return e
}

/*
 * Check for special values which are impossible for ALP to encode
 * because they cannot be cast to int64 without an undefined behaviour
 */
func (e Encoder[T]) isImpossibleToEncode(n T) bool {
	return math.IsInf(float64(n), 0) || math.IsNaN(float64(n)) || n > ENCODING_UPPER_LIMIT || n < ENCODING_LOWER_LIMIT ||
		(n == 0.0 && math.Signbit(float64(n))) //! Verification for -0.0
}

// ! Scalar encoding a single value with ALP
func (e Encoder[T]) encodeValue(value, frac, exp T) int64 {
	tmpEncodedValue := value * frac * exp
	if e.isImpossibleToEncode(tmpEncodedValue) {
		return ENCODING_UPPER_LIMIT
	}
	tmpVal := tmpEncodedValue + T(e.constant.MAGIC_NUMER-e.constant.MAGIC_NUMER)
	return int64(tmpVal)
}

func (e Encoder[T]) countBits(x int64) uint8 {
	if x == 0 {
		return 0
	}
	var v any = T(0)
	switch v.(type) {
	case float64:
		return uint8(bits.Len64(uint64(x)))
	case float32:
		return uint8(bits.Len32(uint32(x)))
	}
	return 0
}

func (e Encoder[T]) countBitsMinMax(max, min int64) uint8 {
	return e.countBits(max - min)
}

// ! Analyze FFOR to obtain bitwidth and frame-of-reference value
func (e Encoder[T]) analyzeFFOR(inputVector []int64) (uint8, int64) {
	maxVal := inputVector[0]
	minVal := inputVector[0]

	for i := 1; i < len(inputVector); i++ {
		minVal = min(minVal, inputVector[i])
		maxVal = max(maxVal, inputVector[i])
	}

	bitWidth := e.countBitsMinMax(maxVal, minVal)
	return bitWidth, minVal
}

/*
 * Function to sort the best combinations from each vector sampled from the rowgroup
 * First criteria is number of times it appears
 * Second criteria is bigger exponent
 * Third criteria is bigger factor
 */

func (e Encoder[T]) compareCombinations(c1, c2 Combination) bool {
	return (c1.nAppearances > c2.nAppearances) ||
		(c1.nAppearances == c2.nAppearances &&
			(c1.estimatedCompressionSize < c2.estimatedCompressionSize)) ||
		((c1.nAppearances == c2.nAppearances &&
			c1.estimatedCompressionSize == c2.estimatedCompressionSize) &&
			(c2.encodingIndice.exponent < c1.encodingIndice.exponent)) ||
		((c1.nAppearances == c2.nAppearances &&
			c1.estimatedCompressionSize == c2.estimatedCompressionSize &&
			c2.encodingIndice.exponent == c1.encodingIndice.exponent) &&
			(c2.encodingIndice.factor < c1.encodingIndice.factor))

}

/*
 * Find the best combinations of factor-exponent from each vector sampled from a rowgroup
 * This function is called once per rowgroup
 * This operates over ALP first level samples
 */
func (e Encoder[T]) findTopKCombinations(sampledVector []T, state *State[T]) {
	state.ResetCombinations()

	nSamples := len(sampledVector)
	sampleSize := min(nSamples, SAMPLES_PER_VECTOR)
	bestEstimatedCompressionSize := uint64((sampleSize * (e.constant.EXCEPTION_SIZE + EXCEPTION_POSITION_SIZE)) + (sampleSize * e.constant.EXCEPTION_SIZE))

	bestEncodingIndice := EncodingIndice{
		exponent: e.constant.MAX_EXPONENT,
		factor:   e.constant.MAX_EXPONENT,
	}

	foundFactor := uint8(0)
	foundExponent := uint8(0)

	//! We start our optimization with the worst possible total bits obtained from compression
	bestTotalBits := (nSamples * (int(e.exactTypeBitSize) + int(EXCEPTION_POSITION_SIZE*8))) + (nSamples * int(e.exactTypeBitSize))

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
			currentEncodingIndice := EncodingIndice{exponent: uint8(expIdx), factor: uint8(factorIdx)}
			estimatedCompressionSize := e.compressToEstimateSize(sampledVector, currentEncodingIndice)
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
		state.Scheme = AlpRdScheme
	}

	state.bestKCombinations = append(state.bestKCombinations, bestCombination)
}

func (e Encoder[T]) compressToEstimateSize(inputVector []T, encodingIndice EncodingIndice) uint64 {
	nValues := len(inputVector)
	exceptionsCount := 0
	nonExceptionsCount := 0
	estimatedBitsPerValue := uint32(0)
	estimatedCompressionSize := uint64(0)
	var maxEncodedValue int64 = math.MaxInt64
	var minEncodedValue int64 = math.MinInt64
	fac := FACT_ARR[encodingIndice.factor]
	exp := T(e.constant.FRAC_ARR[encodingIndice.exponent])

	frac := T(e.constant.FRAC_ARR[encodingIndice.factor])
	expr := T(e.constant.EXP_ARR[encodingIndice.exponent])
	for _, value := range inputVector {
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

	// Evaluate factor/exponent compression size (we optimize for FOR)
	delta := uint64(maxEncodedValue) - uint64(minEncodedValue)
	estimatedBitsPerValue = uint32(math.Ceil(math.Log2(float64(delta + 1))))
	estimatedCompressionSize += uint64(nValues) * uint64(estimatedBitsPerValue)
	estimatedCompressionSize += uint64(exceptionsCount) * (uint64(e.exactTypeBitSize) + (EXCEPTION_POSITION_SIZE * 8))
	return estimatedCompressionSize
}

/*
 * Find the best combination of factor-exponent for a vector from within the best k combinations
 * This is ALP second level sampling
 */
func (e Encoder[T]) findBestExponentFactorFromCombinations(inputVector []T, state *State[T]) {
	//! We sample equidistant values within a vector; to do this we skip a fixed number of values
	sample := make([]T, 0)
	nValue := len(inputVector)
	idxIncrements := max(1, (nValue / int(SAMPLES_PER_VECTOR))) //?

	for i := 0; i < nValue; i += int(idxIncrements) {
		sample = append(sample, inputVector[i])
	}

	worseTotalBitsCounter := uint64(0)
	bestTotalBits := uint64(math.MaxUint64)
	bestEncodingIndice := EncodingIndice{}

	//! We try each K combination in search for the one which minimize the compression size in the vector
	for i := 0; i < len(state.bestKCombinations); i++ {
		estimateCompressionSize := e.compressToEstimateSize(sample, state.bestKCombinations[i].encodingIndice)

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
		bestEncodingIndice = state.bestKCombinations[i].encodingIndice
		worseTotalBitsCounter = 0
	}
	state.EncodingIndice = bestEncodingIndice
}

func (e Encoder[T]) compress(inputVector []T, state *State[T]) {
	lenInputVector := len(inputVector)
	if len(state.bestKCombinations) > 1 { // Only if more than 1 found top combinations we sample and search
		e.findBestExponentFactorFromCombinations(inputVector, state) // ?
	} else {
		state.EncodingIndice = state.bestKCombinations[0].encodingIndice
	}

	// Encoding Floating-Point to Int64
	//! We encode all the values regardless of their correctness to recover the original floating-point
	exceptionsIdx := 0
	fac := FACT_ARR[state.EncodingIndice.factor]
	exp := T(e.constant.FRAC_ARR[state.EncodingIndice.exponent])

	frac := T(e.constant.FRAC_ARR[state.EncodingIndice.factor])
	expr := T(e.constant.EXP_ARR[state.EncodingIndice.exponent])
	for i := 0; i < lenInputVector; i++ {
		encodeValue := e.encodeValue(inputVector[i], frac, expr)
		decodeValue := decodeValue(encodeValue, fac, exp)
		state.EncodedIntegers = append(state.EncodedIntegers, encodeValue)
		//! We detect exceptions using a predicated comparison
		if isException := decodeValue != inputVector[i]; isException {
			exceptionsIdx++
		}
		state.ExceptionPositions = append(state.ExceptionPositions, i)
	}

	// Finding first non exception value
	nonExceptionValue := int64(0)
	for i := 0; i < lenInputVector; i++ {
		if i != state.ExceptionPositions[i] {
			nonExceptionValue = state.EncodedIntegers[i]
			break
		}
	}

	// Replacing that first non exception value on the vector exceptions
	for i := 0; i < exceptionsIdx; i++ {
		exceptionPos := state.ExceptionPositions[i]
		actualValue := inputVector[exceptionPos]
		state.EncodedIntegers[exceptionPos] = nonExceptionValue
		state.Exceptions = append(state.Exceptions, actualValue)
	}
	state.ExceptionsCount = uint16(exceptionsIdx)

	// Analyze FFOR
	bitWidth, minVal := e.analyzeFFOR(state.EncodedIntegers)
	state.FOR = minVal

	// Subtract FOR
	for i := 0; i < len(state.EncodedIntegers); i++ {
		state.EncodedIntegers[i] -= minVal

	}

	state.BitWidth = bitWidth
}
