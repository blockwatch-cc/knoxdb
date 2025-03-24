// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"golang.org/x/exp/constraints"
)

// Scalar decoding of an ALP vector
func Decompress[T constraints.Float](out []T, factor, exponent uint8, frameOfReference int64, exceptions []T, exceptionPositions []uint32, encodedIntegers []int64) {
	constant := newConstant[T]()

	fac := FACT_ARR[factor]
	exp := T(constant.FRAC_ARR[exponent])
	_ = out[len(encodedIntegers)-1]
	for i, encInt := range encodedIntegers {
		// unFOR+decoding
		out[i] = T((encInt+frameOfReference)*fac) * exp
	}

	// patching exceptions
	for i, expPos := range exceptionPositions[:len(exceptions)] {
		out[expPos] = exceptions[i]
	}
}

// DecompressValue decompresses value by unFOR+decode. Doesnt take account of exceptions
func DecompressValue[T constraints.Float](v int64, factor, exponent uint8, frameOfReference int64) T {
	constant := newConstant[T]()

	fac := FACT_ARR[factor]
	exp := T(constant.FRAC_ARR[exponent])

	// unFOR+decoding
	return T((v+frameOfReference)*fac) * exp
}

// Scalar decoding a single value with ALP
func decodeValue[T constraints.Float](v, fac int64, exp T) T {
	return T(v*fac) * exp
}
