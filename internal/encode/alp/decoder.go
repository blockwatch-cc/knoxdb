// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc,alex@blockwatch.cc

package alp

import (
	"slices"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
)

type Decoder[T types.Float] struct {
	factor     T
	exponent   T
	exceptions []T
	positions  []uint32
	exmap      map[uint32]T
}

func NewDecoder[T types.Float](factor, exponent uint8) *Decoder[T] {
	c := newConstant[T]()
	return &Decoder[T]{
		factor:   T(FACT_ARR[factor]),
		exponent: T(c.FRAC_ARR[exponent]),
	}
}

func (d *Decoder[T]) Close() {
	if d.exceptions != nil {
		arena.Free(d.exceptions)
		d.exceptions = nil
		arena.Free(d.positions)
		d.positions = nil
		clear(d.exmap)
	}
}

func (d *Decoder[T]) WithExceptions(values []T, pos []uint32) *Decoder[T] {
	d.exceptions = values
	d.positions = pos
	return d
}

func (d *Decoder[T]) DecodeValue(v int64, i int) T {
	if d.exceptions != nil && d.exmap == nil {
		d.exmap = make(map[uint32]T, len(d.exceptions))
		for i, v := range d.exceptions {
			d.exmap[d.positions[i]] = v
		}
	}
	if e, ok := d.exmap[uint32(i)]; ok {
		return e
	}
	return d.decode(v)
}

func (d *Decoder[T]) decode(v int64) T {
	return T(v) * d.factor * d.exponent
}

// Scalar decoding a single value with ALP
func decodeValue[T types.Float](v, fac int64, exp T) T {
	return T(v) * T(fac) * exp
}

// Scalar decoding of an ALP vector
func (d *Decoder[T]) Decode(dst []T, src []int64) {
	l := len(src)
	if l == 0 {
		return
	}

	_ = dst[l-1]
	var i int
	for range l / 8 {
		dst[i] = T(src[i]) * d.factor * d.exponent
		dst[i+1] = T(src[i+1]) * d.factor * d.exponent
		dst[i+2] = T(src[i+2]) * d.factor * d.exponent
		dst[i+3] = T(src[i+3]) * d.factor * d.exponent
		dst[i+4] = T(src[i+4]) * d.factor * d.exponent
		dst[i+5] = T(src[i+5]) * d.factor * d.exponent
		dst[i+6] = T(src[i+6]) * d.factor * d.exponent
		dst[i+7] = T(src[i+7]) * d.factor * d.exponent
		i += 8
	}

	for i < l {
		dst[i] = T(src[i]) * d.factor * d.exponent
		i++
	}

	// patching exceptions
	for i, expPos := range d.positions {
		dst[expPos] = d.exceptions[i]
	}
}

func (d *Decoder[T]) DecodeChunk(dst *[128]T, src *[128]int64, ofs int) {
	// decode values
	var i int
	for range 16 {
		dst[i] = T(src[i]) * d.factor * d.exponent
		dst[i+1] = T(src[i+1]) * d.factor * d.exponent
		dst[i+2] = T(src[i+2]) * d.factor * d.exponent
		dst[i+3] = T(src[i+3]) * d.factor * d.exponent
		dst[i+4] = T(src[i+4]) * d.factor * d.exponent
		dst[i+5] = T(src[i+5]) * d.factor * d.exponent
		dst[i+6] = T(src[i+6]) * d.factor * d.exponent
		dst[i+7] = T(src[i+7]) * d.factor * d.exponent
		i += 8
	}

	// patch exceptions in range
	if len(d.positions) > 0 {
		i, _ := slices.BinarySearch(d.positions, uint32(ofs))
		for i < len(d.positions) {
			p := int(d.positions[i])
			if p >= ofs+128 {
				break
			}
			dst[p-ofs] = d.exceptions[i]
		}
	}
}
