// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package alp

import (
	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
)

// Encoder implements a variation of the floating point compression algorithm
// from the paper ["ALP: Adaptive Lossless floating-Point Compression"][paper]
// by Afroozeh et al.
//
// The encoder comes in two variants, classic ALP which is well-suited for data
// that does not use the full floating point precision, and RD "real doubles",
// values that do.
//
// Classic ALP will return small integers, and it is meant to be cascaded with
// other integer compression techniques such as bit-packing and frame-of-reference
// encoding. Combined this allows for significant compression on the order of
// what you can get for integer values.
//
// ALP-RD is generally terminal, and in the ideal case it can represent an f64
// in just 49 bits, though generally it is closer to 54 bits per value or
// ~12.5% compression.
//
// Notable differences from the reference implementation are
// - single-level sampling and best exponent search
// - samples only 32 values from each input vector
// - no extra handling for special float values (nan, inf, -0.0)
//
// [paper]: https://ir.cwi.nl/pub/33334/33334.pdf

type Encoder[T Float, E Int] struct {
	constant[T]
}

func NewEncoder[T Float, E Int]() *Encoder[T, E] {
	return &Encoder[T, E]{
		constant: getConstant[T](),
	}
}

type Result[T Float, E Int] struct {
	Min          E
	Max          E
	Encoded      []E
	PatchValues  []T
	PatchIndices []uint32
}

func NewResult[T Float, E Int](sz int) *Result[T, E] {
	return &Result[T, E]{
		Encoded:      arena.Alloc[E](sz),
		PatchValues:  arena.Alloc[T](sz),
		PatchIndices: arena.Alloc[uint32](sz),
	}
}

func (r *Result[T, E]) Close() {
	arena.Free(r.Encoded)
	arena.Free(r.PatchValues)
	arena.Free(r.PatchIndices)
	r.Encoded = nil
	r.PatchValues = nil
	r.PatchIndices = nil
}

func (e *Encoder[T, E]) Encode(src []T, exp Exponents) *Result[T, E] {
	numPatches := 0
	r := NewResult[T, E](len(src))
	r.PatchIndices = r.PatchIndices[:cap(r.PatchIndices)]
	r.Encoded = r.Encoded[:len(src)]
	r.Min = types.MaxVal[E]()
	r.Max = 0

	// load exponents
	encE := e.F10[exp.E]
	encF := e.IF10[exp.F]
	decE := e.IF10[exp.E]
	decF := e.F10[exp.F]
	magic := e.MAGIC_NUMBER

	// encode values
	for i, val := range src {
		enc := E((val*encE*encF + magic) - magic)
		if val == T(enc)*decF*decE {
			r.Encoded[i] = enc
			r.Min = min(r.Min, enc)
			r.Max = max(r.Max, enc)
		} else {
			r.PatchIndices[numPatches] = uint32(i)
			numPatches++
		}
	}

	// replace exceptions with the minimum
	for i := range numPatches {
		pos := r.PatchIndices[i]
		r.Encoded[pos] = r.Min
		r.PatchValues = append(r.PatchValues, src[pos])
	}
	r.PatchIndices = r.PatchIndices[:numPatches]

	// log2 := types.Log2Range(r.Min, r.Max)
	// fmt.Printf("Encode [%d,%d] => minv=%d maxv=%d log2=%d ex=%d\n", exp.E, exp.F, r.Min, r.Max, log2, len(r.PatchValues))
	// if log2 == 61 {
	// 	for i, v := range r.Encoded {
	// 		if v > E(1679397590) {
	// 			fmt.Printf("WARN wide int %d: src=%v int=%v\n", i, src[i], v)
	// 		}
	// 	}
	// }

	return r
}
