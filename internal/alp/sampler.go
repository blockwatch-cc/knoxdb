// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"math"

	"golang.org/x/exp/constraints"
)

func FirstLevelSample[T constraints.Float](data []T, dataOffset int) []T {
	dataSample := make([]T, 0)
	dataSize := len(data)
	leftInData := dataSize - dataOffset
	portionToSample := min(ROWGROUP_SIZE, leftInData)
	availableAlpVectors := int(math.Ceil(float64(portionToSample) / VECTOR_SIZE))
	sampleIdx := 0
	dataIdx := dataOffset

	for vectorIdx := 0; vectorIdx < availableAlpVectors; vectorIdx++ {
		currentVectorNValues := min(dataSize-dataIdx, VECTOR_SIZE)

		//! We sample equidistant vectors; to do this we skip a fixed values of vectors
		//! If we are not in the correct jump, we do not take sample from this vector
		if !((vectorIdx % ROWGROUP_SAMPLES_JUMP) == 0) {
			dataIdx += currentVectorNValues
			continue
		}

		nSampledIncrements := max(1, int(math.Ceil(float64(currentVectorNValues)/SAMPLES_PER_VECTOR)))

		//! We do not take samples of non-complete duckdb vectors (usually the last one)
		//! Except in the case of too little data
		if currentVectorNValues < SAMPLES_PER_VECTOR && sampleIdx != 0 {
			dataIdx += currentVectorNValues
			continue
		}

		// Storing the sample of that vector
		for i := 0; i < currentVectorNValues; i += nSampledIncrements {
			dataSample = append(dataSample, data[dataIdx+i])
			sampleIdx++
		}
		dataIdx += currentVectorNValues
	}
	return dataSample
}
