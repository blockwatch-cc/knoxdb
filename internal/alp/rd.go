// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"math"
	"sort"

	"blockwatch.cc/knoxdb/internal/dedup"
	"blockwatch.cc/knoxdb/pkg/assert"
)

type Uint interface {
	~uint32 | uint64
}

type Float interface {
	~float32 | float64
}

const (
	FLOAT32_EXACT_TYPE = 32
	FLOAT64_EXACT_TYPE = 64
)

type RdLeftPartInfo[T Float] struct {
	count int32
	hash  uint16
	val   T
}

type RdEncoder[T Float, U Uint] struct {
	State              *RdState[T]
	EXACT_TYPE_BITSIZE uint8
}

type RdState[T Float] struct {
	RightBitWidth        uint8
	LeftBitWidth         uint8
	ExceptionsCount      uint16
	RightPartsEncoded    [VECTOR_SIZE * 8]uint8
	LeftPartEncoded      [VECTOR_SIZE * 8]uint8
	LeftPartsDict        [RD_MAX_DICTIONARY_SIZE]uint16
	Exceptions           [VECTOR_SIZE]uint16
	ExceptionsPositions  [VECTOR_SIZE]uint16
	ValueCount           int
	leftBitPackedSize    uint64
	rightBitPackedSize   uint64
	leftPartsDictMap     map[uint16]uint16
	actualDictionarySize uint8
	sampledValuesN       []T
}

func NewRdState[T Float]() *RdState[T] {
	return &RdState[T]{
		leftPartsDictMap: make(map[uint16]uint16),
	}
}

func (r *RdState[T]) Reset() {
	r.LeftBitWidth = 0
	r.ExceptionsCount = 0
	r.rightBitPackedSize = 0
	r.leftPartsDictMap = make(map[uint16]uint16)
}

func newRDEncoder[T Float, U Uint](dataColumn []T, columnOffset int) RdEncoder[T, U] {
	enc := RdEncoder[T, U]{
		State: NewRdState[T](),
	}
	var v any = T(0)
	switch v.(type) {
	case float32:
		enc.EXACT_TYPE_BITSIZE = FLOAT32_EXACT_TYPE
	case float64:
		enc.EXACT_TYPE_BITSIZE = FLOAT64_EXACT_TYPE
	}
	enc.State.ValueCount = len(dataColumn)
	enc.State.sampledValuesN = FirstLevelSample(dataColumn, columnOffset)
	enc.findBestDictionary(enc.State.sampledValuesN, enc.State)
	return enc
}

/*
 * Estimate the bits per value of ALPRD within a sample
 */
func (r RdEncoder[T, U]) estimateCompressionSize(rightBitWidth, leftBitWidth uint8, exceptionsCount uint16, sampleCount uint64) float64 {
	var exceptionsSize float64 = float64(exceptionsCount * ((RD_EXCEPTION_POSITION_SIZE + RD_EXCEPTION_SIZE) * 8))
	var estimatedSize float64 = float64(rightBitWidth+leftBitWidth) + (exceptionsSize / float64(sampleCount))
	return estimatedSize
}

func (r RdEncoder[T, U]) buildLeftPartsDictionary(values []T, rightBitWidth uint8, state *RdState[T], persistDict bool) float64 {
	leftPartsHash := make(map[U]RdLeftPartInfo[T])

	// Building a hash for all the left parts and how many times they appear
	for _, val := range values {
		v := castToUint[T, U](val)
		leftTmp := v >> rightBitWidth
		if v, ok := leftPartsHash[leftTmp]; ok {
			v.count++
			leftPartsHash[leftTmp] = v
		} else {
			leftPartsHash[leftTmp] = RdLeftPartInfo[T]{
				val:   val,
				count: 1,
				hash:  uint16(leftTmp),
			}
		}
	}

	// We build a vector from the hash to be able to sort it by repetition count
	leftPartsSortedRepetitions := make([]RdLeftPartInfo[T], 0)
	for _, c := range leftPartsHash {
		leftPartsSortedRepetitions = append(leftPartsSortedRepetitions, c)
	}

	sort.Slice(leftPartsSortedRepetitions, func(i, j int) bool {
		return leftPartsSortedRepetitions[i].count > leftPartsSortedRepetitions[j].count
	})

	// Exceptions are left parts which do not fit in the fixed dictionary size
	exceptionsCount := 0
	for i := RD_MAX_DICTIONARY_SIZE; i < len(leftPartsSortedRepetitions); i++ {
		exceptionsCount += int(leftPartsSortedRepetitions[i].count)
	}

	// The left parts bit width after compression is determined by how many elements are in the dictionary
	actualDictionarySize := min(RD_MAX_DICTIONARY_SIZE, len(leftPartsSortedRepetitions))
	leftBitWidth := max(1, math.Ceil(math.Log2(float64(actualDictionarySize))))

	if persistDict {
		clear(state.leftPartsDictMap)
		for dictIdx := 0; dictIdx < actualDictionarySize; dictIdx++ {
			//! The dict keys are mapped to the left part themselves
			state.LeftPartsDict[dictIdx] = uint16(leftPartsSortedRepetitions[dictIdx].hash)
			state.leftPartsDictMap[state.LeftPartsDict[dictIdx]] = uint16(dictIdx)
		}
		//! Pararelly we store a map of the dictionary to quickly resolve exceptions during encoding
		for i := actualDictionarySize + 1; i < len(leftPartsSortedRepetitions); i++ {
			state.leftPartsDictMap[leftPartsSortedRepetitions[i].hash] = uint16(i)
		}
		state.LeftBitWidth = uint8(leftBitWidth)
		state.RightBitWidth = rightBitWidth
		state.actualDictionarySize = uint8(actualDictionarySize)

		assert.Always(state.LeftBitWidth > 0 &&
			state.RightBitWidth > 0 &&
			state.LeftBitWidth <= RD_MAX_DICTIONARY_BIT_WIDTH &&
			state.actualDictionarySize <= RD_MAX_DICTIONARY_SIZE, "")
	}

	estimatedSize := r.estimateCompressionSize(rightBitWidth, uint8(leftBitWidth), uint16(exceptionsCount), uint64(len(values)))

	return estimatedSize
}

func (r RdEncoder[T, U]) findBestDictionary(values []T, state *RdState[T]) float64 {
	rightBitWidth := uint8(0)
	var bestDictSize float64 = math.MaxInt32
	//! Finding the best position to CUT the values
	for i := uint8(1); i <= CUTTING_LIMIT; i++ {
		candidateRightBitWidth := r.EXACT_TYPE_BITSIZE - i
		estimatedSize := r.buildLeftPartsDictionary(values, candidateRightBitWidth, state, false)
		if estimatedSize <= bestDictSize {
			rightBitWidth = candidateRightBitWidth
			bestDictSize = estimatedSize
		}
	}
	return r.buildLeftPartsDictionary(values, rightBitWidth, state, true)
}

func RDCompress[T Float, U Uint](values []T) *RdState[T] {
	enc := newRDEncoder[T, U](values, 0)

	nValues := len(values)
	rightParts := [VECTOR_SIZE]U{}
	leftParts := [VECTOR_SIZE]uint16{}

	// Cutting the floating point values
	for i, val := range values {
		v := castToUint[T, U](val)
		rightParts[i] = U(v & ((1 << enc.State.RightBitWidth) - 1))
		leftParts[i] = uint16(v >> enc.State.RightBitWidth)
	}

	// Dictionary encoding for left parts
	for i := range values {
		dictionaryKey := leftParts[i]
		var dictionaryIndex uint16
		if _, ok := enc.State.leftPartsDictMap[dictionaryKey]; !ok {
			//! If not found on the dictionary we store the smallest non-key index as exception (the dict size)
			dictionaryIndex = uint16(enc.State.actualDictionarySize)
		} else {
			dictionaryIndex = enc.State.leftPartsDictMap[dictionaryKey]
		}
		leftParts[i] = dictionaryIndex

		//! Left parts not found in the dictionary are stored as exceptions
		if dictionaryIndex >= uint16(enc.State.actualDictionarySize) {
			enc.State.Exceptions[enc.State.ExceptionsCount] = uint16(dictionaryIndex)
			enc.State.ExceptionsPositions[enc.State.ExceptionsCount] = uint16(i)
			enc.State.ExceptionsCount++
		}
	}

	rightBitPackedSize := getRequiredSize(nValues, int(enc.State.RightBitWidth))
	leftBitPackedSize := getRequiredSize(nValues, int(enc.State.LeftBitWidth))

	dedup.PackBits(enc.State.LeftPartEncoded[:], leftParts[:], int(enc.State.LeftBitWidth))
	dedup.PackBits(enc.State.RightPartsEncoded[:], rightParts[:], int(enc.State.RightBitWidth))

	enc.State.leftBitPackedSize = uint64(leftBitPackedSize)
	enc.State.rightBitPackedSize = uint64(rightBitPackedSize)
	return enc.State
}

func RDDecompress[T Float, U Uint](state *RdState[T]) []T {
	output := make([]T, 0)
	leftParts := make([]uint16, VECTOR_SIZE)
	rightParts := make([]U, VECTOR_SIZE)

	// Bitunpacking left and right parts
	dedup.UnpackBits(state.LeftPartEncoded[:], leftParts, int(state.LeftBitWidth))
	dedup.UnpackBits(state.RightPartsEncoded[:], rightParts, int(state.RightBitWidth))

	// Decoding
	for i := 0; i < state.ValueCount; i++ {
		left := state.LeftPartsDict[leftParts[i]]
		right := rightParts[i]
		v := U(left)<<state.RightBitWidth | right
		output = append(output, castToFloat[T, U](v))
	}

	// Exceptions Patching (exceptions only occur in left parts)
	for i := 0; i < int(state.ExceptionsCount); i++ {
		right := rightParts[state.ExceptionsPositions[i]]
		left := state.Exceptions[i]
		v := U(left<<state.RightBitWidth) | right
		output[state.ExceptionsPositions[i]] = castToFloat[T, U](v)
	}
	return output
}

func getRequiredSize(nValues, bitWidth int) int {
	totalBits := nValues * bitWidth
	return (totalBits + 7) / 8
}

func castToUint[T Float, U Uint](v T) U {
	var val U
	switch any(v).(type) {
	case float64:
		val = U(math.Float64bits(float64(v)))
	case float32:
		val = U(math.Float32bits(float32(v)))
	}
	return val
}

func castToFloat[T Float, U Uint](v U) T {
	var val T
	switch any(v).(type) {
	case uint64:
		val = T(math.Float64frombits(uint64(v)))
	case uint32:
		val = T(math.Float32frombits(uint32(v)))
	}
	return val
}
