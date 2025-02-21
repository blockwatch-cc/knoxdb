// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import "golang.org/x/exp/constraints"

// Scalar decoding of an ALP vector
func Decompress[T constraints.Float](enc *Encoder[T]) ([]T, error) {
	constants, err := newConstant[T]()
	if err != nil {
		return nil, err
	}
	e := enc.State.EncodingIndice
	exceptions := enc.State.Exceptions
	encodedIntegers := enc.State.EncodedIntegers
	frameOfReference := enc.State.FOR
	exceptionPositions := enc.State.ExceptionPositions
	count := len(encodedIntegers)
	out := make([]T, count)

	// unFOR
	for i := 0; i < count; i++ {
		encodedIntegers[i] += int64(frameOfReference)
	}

	// decoding
	for i := 0; i < count; i++ {
		out[i] = decodeValue(int64(encodedIntegers[i]), e, constants)
	}

	// patching exceptions
	for k := range exceptionPositions {
		out[exceptionPositions[k]] = T(exceptions[k])
	}

	return out, nil
}

// Scalar decoding a single value with ALP
func decodeValue[T constraints.Float](v int64, e EncodingIndice, c Constant[T]) T {
	return T(v) * c.FACT_ARR[e.factor] * c.FRAC_ARR[e.exponent]
}
