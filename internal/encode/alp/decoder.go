// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"blockwatch.cc/knoxdb/internal/types"
)

type Decoder[T types.Float] struct {
	factor     T
	exponent   T
	exceptions []T
	positions  []uint32
}

func NewDecoder[T types.Float](factor, exponent uint8) *Decoder[T] {
	c := newConstant[T]()
	return &Decoder[T]{
		factor:   T(FACT_ARR[factor]),
		exponent: T(c.FRAC_ARR[exponent]),
	}
}

func (d *Decoder[T]) WithExceptions(values []T, pos []uint32) *Decoder[T] {
	d.exceptions = values
	d.positions = pos
	return d
}

// Scalar decoding of an ALP vector
func (d *Decoder[T]) Decompress(dst []T, src []int64) {
	_ = dst[len(src)-1]
	for i, v := range src {
		dst[i] = T(v) * d.factor * d.exponent
	}

	// patching exceptions
	for i, expPos := range d.positions {
		dst[expPos] = d.exceptions[i]
	}
}

// DecompressValue decompresses value by unFOR+decode. Doesnt take account of exceptions
func (d *Decoder[T]) DecompressValue(v int64) T {
	return T(v) * d.factor * d.exponent
}

// Scalar decoding a single value with ALP
func decodeValue[T types.Float](v, fac int64, exp T) T {
	return T(v) * T(fac) * exp
}
