// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"fmt"
	"math"
	"math/bits"
	"sort"
	"unsafe"

	"golang.org/x/exp/constraints"
)

type EncodingIndice struct {
	exponent uint8
	factor   uint8
}

func (e EncodingIndice) Hash() uint64 {
	return 0
}

type Combination struct {
	encodingIndice           EncodingIndice
	nAppearances             uint64
	estimatedCompressionSize uint64
	frequency                int64
}

type State[T constraints.Float] struct {
	VectorSize         uint16
	ExceptionsCount    uint16
	ExceptionPositions []int
	SampledValuesN     []T
	BestKCombinations  []Combination
	BitWidth           uint8
	FOR                uint64 // Frame of Reference
	EncodingIndice     EncodingIndice
	EncodedIntegers    []int64
	Exceptions         []T
}

func NewState[T constraints.Float]() *State[T] {
	return &State[T]{
		VectorSize:        VECTOR_SIZE,
		SampledValuesN:    make([]T, 0),
		BestKCombinations: make([]Combination, 0),
	}
}

func (s *State[T]) Reset() {
	s.BitWidth = 0
	s.ExceptionsCount = 0
	s.BestKCombinations = make([]Combination, 0)
}

func (s *State[T]) ResetCombinations() {
	s.BestKCombinations = make([]Combination, 0)
}

func Compress[T constraints.Float](values []T) (*Encoder[T], error) {
	enc, err := newEncoder(values, 0)
	if err != nil {
		return nil, err
	}
	enc.compress(values, enc.State)
	return enc, nil
}

type Encoder[T constraints.Float] struct {
	ExactTypeBitSize uint8
	State            *State[T]
	Constant         Constant[T]
}

func newEncoder[T constraints.Float](dataColumn []T, columnOffset int) (*Encoder[T], error) {
	constant, err := newConstant[T]()
	if err != nil {
		return nil, err
	}
	state := NewState[T]()
	state.SampledValuesN = FirstLevelSample[T](dataColumn, columnOffset)
	state.BestKCombinations = make([]Combination, MAX_K_COMBINATIONS)
	e := &Encoder[T]{
		State:    state,
		Constant: constant,
	}
	e.findTopKCombinations(state.SampledValuesN, state)
	return e, nil
}

/*
 * Check for special values which are impossible for ALP to encode
 * because they cannot be cast to int64 without an undefined behaviour
 */
func (e Encoder[T]) IsImpossibleToEncode(n T) bool {
	return math.IsInf(float64(n), 0) || math.IsNaN(float64(n)) || n > ENCODING_UPPER_LIMIT || n < ENCODING_LOWER_LIMIT ||
		(n == 0.0 && math.Signbit(float64(n))) //! Verification for -0.0
}

// ! Scalar encoding a single value with ALP
func (e Encoder[T]) encodeValue(value T, encodingIndice EncodingIndice) int64 {
	tmpEncodedValue := value * e.Constant.EXP_ARR[encodingIndice.exponent] * e.Constant.FRAC_ARR[encodingIndice.factor]
	if e.IsImpossibleToEncode(tmpEncodedValue) {
		return ENCODING_UPPER_LIMIT
	}
	tmpVal := uint64(tmpEncodedValue) + e.Constant.MAGIC_NUMER - e.Constant.MAGIC_NUMER
	return int64(tmpVal)
}

func (e Encoder[T]) countBits(x uint) uint8 {
	if x == 0 {
		return 0
	}
	if unsafe.Sizeof(x) == 8 {
		return uint8(bits.Len64(uint64(x)))
	}
	return uint8(bits.Len32(uint32(x)))
}

func (e Encoder[T]) countBitsMinMax(max, min int64) uint8 {
	delta := uint(max) - uint(min)
	return e.countBits(delta)
}

// ! Analyze FFOR to obtain bitwidth and frame-of-reference value
func (e Encoder[T]) analyzeFFOR(inputVector []int64) (bitWidth uint8, min int64) {
	max := inputVector[0]
	min = inputVector[0]
	// baseFor := make([]T, 0)

	for _, val := range inputVector {
		if val < min {
			min = val
		}
		if val > max {
			max = val
		}
	}

	// baseFor[0] = min
	bitWidth = e.countBitsMinMax(max, min)
	return
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

	bestCombinationsFreq := map[EncodingIndice]uint64{}

	// For each vector sampled
	// for _, sampledVector := range sampledVectors {
	nSamples := len(sampledVector)
	bestEncodingIndice := EncodingIndice{
		exponent: e.Constant.MAX_EXPONENT,
		factor:   e.Constant.MAX_EXPONENT,
	}

	//! We start our optimization with the worst possible total bits obtained from compression
	bestTotalBits := (nSamples * (int(e.ExactTypeBitSize) + int(e.Constant.EXCEPTION_POSITION_SIZE*8))) + (nSamples * int(e.ExactTypeBitSize))

	// N of appearances is irrelevant at this phase; we search for the best compression for the vector
	bestCombination := Combination{
		encodingIndice:           bestEncodingIndice,
		nAppearances:             0,
		estimatedCompressionSize: uint64(bestTotalBits),
	}
	//! We try all combinations in search for the one which minimize the compression size
	for expIdx := int(e.Constant.MAX_EXPONENT); expIdx >= 0; expIdx-- {
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
		}
	}
	bestCombinationsFreq[bestCombination.encodingIndice]++
	// }

	// Convert our hash to a Combination vector to be able to sort
	// Note that this vector is always small (< 10 combinations)
	topCombinations := make([]Combination, 0, len(bestCombinationsFreq))
	for indice, freq := range bestCombinationsFreq {
		topCombinations = append(topCombinations, Combination{
			encodingIndice: indice,
			frequency:      int64(freq),
		})
	}

	sort.Slice(topCombinations, func(i, j int) bool {
		return e.compareCombinations(topCombinations[i], topCombinations[j])
	})

	// Save k' best combinations
	m := min(len(topCombinations), MAX_K_COMBINATIONS)
	for i := 0; i < int(m); i++ {
		state.BestKCombinations = append(state.BestKCombinations, topCombinations[i])
	}
}

func (e Encoder[T]) compressToEstimateSize(inputVector []T, encodingIndice EncodingIndice) uint64 {
	nValues := len(inputVector)
	exceptionsCount := 0
	nonExceptionsCount := 0
	estimatedBitsPerValue := uint32(0)
	estimatedCompressionSize := uint64(0)
	var maxEncodedValue int64 = math.MaxInt64
	var minEncodedValue int64 = math.MinInt64

	for _, value := range inputVector {
		encodedValue := e.encodeValue(value, encodingIndice)
		decodedValue := decodeValue(encodedValue, encodingIndice, e.Constant)
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
	estimatedCompressionSize += uint64(exceptionsCount) * (uint64(e.ExactTypeBitSize) + (e.Constant.EXCEPTION_POSITION_SIZE * 8))
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
	idxIncrements := max(1, (nValue / int(e.Constant.SAMPLES_PER_VECTOR))) //?

	for i := 0; i < nValue; i += int(idxIncrements) {
		sample = append(sample, inputVector[i])
	}

	worseTotalBitsCounter := uint64(0)
	bestTotalBits := uint64(math.MaxUint64)
	bestEncodingIndice := EncodingIndice{}

	//! We try each K combination in search for the one which minimize the compression size in the vector
	for _, combination := range state.BestKCombinations {
		estimateCompressionSize := e.compressToEstimateSize(sample, combination.encodingIndice)

		// If current compression size is worse (higher) or equal than the current best combination
		if estimateCompressionSize >= bestTotalBits {
			worseTotalBitsCounter += 1
			// Early exit strategy
			if worseTotalBitsCounter == e.Constant.SAMPLING_EARLY_EXIT_THRESHOLD {
				break
			}
			continue
		}
		// Otherwise we replace the best and continue trying with the next combination
		bestTotalBits = estimateCompressionSize
		bestEncodingIndice = combination.encodingIndice
		worseTotalBitsCounter = 0
	}
	state.EncodingIndice = bestEncodingIndice
}

func (e Encoder[T]) compress(inputVector []T, state *State[T]) {
	if len(state.BestKCombinations) > 1 { // Only if more than 1 found top combinations we sample and search
		e.findBestExponentFactorFromCombinations(inputVector, state) // ?
	} else {
		fmt.Println(state.BestKCombinations)
		state.EncodingIndice = state.BestKCombinations[0].encodingIndice
	}

	// Encoding Floating-Point to Int64
	//! We encode all the values regardless of their correctness to recover the original floating-point
	exceptionsIdx := 0
	for i, actualValue := range inputVector {
		encodeValue := e.encodeValue(actualValue, state.EncodingIndice)
		decodeValue := decodeValue(encodeValue, state.EncodingIndice, e.Constant)
		state.EncodedIntegers = append(state.EncodedIntegers, encodeValue)
		//! We detect exceptions using a predicated comparison
		if isException := decodeValue != actualValue; isException {
			state.ExceptionPositions = append(state.ExceptionPositions, i)
			exceptionsIdx++
		}
	}

	// Finding first non exception value
	nonExceptionValue := int64(0)
	for i := range inputVector {
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

	// Subtract FOR
	for i, v := range state.EncodedIntegers {
		state.EncodedIntegers[i] = v - minVal
	}

	state.BitWidth = bitWidth
}
