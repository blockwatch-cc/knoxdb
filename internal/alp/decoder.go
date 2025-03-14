// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"golang.org/x/exp/constraints"
)

// Scalar decoding of an ALP vector
func Decompress[T constraints.Float](out []T, state *State[T]) {
	constant := newConstant[T]()
	e := state.EncodingIndice
	exceptions := state.Exceptions
	encodedIntegers := state.EncodedIntegers
	frameOfReference := state.FOR
	exceptionPositions := state.ExceptionPositions

	fac := FACT_ARR[e.factor]
	exp := T(constant.FRAC_ARR[e.exponent])
	_ = out[len(encodedIntegers)-1]
	for i, encInt := range encodedIntegers {
		// unFOR+decoding
		out[i] = T((encInt+frameOfReference)*fac) * exp
	}

	// patching exceptions
	for i, expPos := range exceptionPositions[:state.ExceptionsCount] {
		out[expPos] = exceptions[i]
	}
}

// Scalar decoding a single value with ALP
func decodeValue[T constraints.Float](v, fac int64, exp T) T {
	return T(v*fac) * exp
}
