// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"math"

	"blockwatch.cc/knoxdb/internal/types"
)

func FirstLevelSample[T types.Float](dst, src []T) []T {
	portionToSample := min(ROWGROUP_SIZE, len(src))
	availableAlpVectors := int(math.Ceil(float64(portionToSample) / VECTOR_SIZE))
	sampleIdx := 0
	dataIdx := 0

	for vectorIdx := 0; vectorIdx < availableAlpVectors; vectorIdx++ {
		currentVectorNValues := min(len(src)-dataIdx, VECTOR_SIZE)

		//! We sample equidistant vectors; to do this we skip a fixed values of vectors
		//! If we are not in the correct jump, we do not take sample from this vector
		if !((vectorIdx % ROWGROUP_SAMPLES_JUMP) == 0) {
			dataIdx += currentVectorNValues
			continue
		}

		nSampledIncrements := max(1, int(math.Ceil(float64(currentVectorNValues)/SAMPLES_PER_VECTOR)))

		//! We do not take samples of non-full vectors (usually the last one)
		//! Except in the case of too little data
		if currentVectorNValues < SAMPLES_PER_VECTOR && sampleIdx != 0 {
			dataIdx += currentVectorNValues
			continue
		}

		// Storing the sample of that vector
		for i := 0; i < currentVectorNValues; i += nSampledIncrements {
			dst = append(dst, src[dataIdx+i])
			sampleIdx++
		}
		dataIdx += currentVectorNValues
	}
	return dst
}
