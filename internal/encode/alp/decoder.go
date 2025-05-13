// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package alp

import (
	"slices"
	"sync"

	"blockwatch.cc/knoxdb/internal/encode/alp/avx2"
	"blockwatch.cc/knoxdb/internal/encode/bitpack"
	"blockwatch.cc/knoxdb/pkg/util"
)

type Decoder[T Float, E Int] struct {
	f             T
	e             T
	patch_values  []T
	patch_indices []uint32
	patch_map     map[uint32]T
	_f            uint8
	_e            uint8
	_safe         bool
}

var alpDecFactory = alpDecoderFactory{
	f64Pool: sync.Pool{New: func() any { return new(Decoder[float64, int64]) }},
	f32Pool: sync.Pool{New: func() any { return new(Decoder[float32, int32]) }},
}

type alpDecoderFactory struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
}

func newAlpDecoder[T Float, E Int]() *Decoder[T, E] {
	switch any(T(0)).(type) {
	case float64:
		return alpDecFactory.f64Pool.Get().(*Decoder[T, E])
	case float32:
		return alpDecFactory.f32Pool.Get().(*Decoder[T, E])
	default:
		return nil
	}
}

func putAlpDecoder[T Float, E Int](d *Decoder[T, E]) {
	switch any(T(0)).(type) {
	case float64:
		alpDecFactory.f64Pool.Put(d)
	case float32:
		alpDecFactory.f32Pool.Put(d)
	}
}

func NewDecoder[T Float, E Int](factor, exponent uint8) *Decoder[T, E] {
	c := getConstantPtr[T]()
	d := newAlpDecoder[T, E]()
	d.f = c.F10[factor]
	d.e = c.IF10[exponent]
	d._f = factor
	d._e = exponent
	d._safe = false
	return d
}

func (d *Decoder[T, E]) Close() {
	if d.patch_values != nil {
		d.patch_values = nil
		d.patch_indices = nil
		clear(d.patch_map)
	}
	putAlpDecoder(d)
}

func (d *Decoder[T, E]) WithSafeInt(isSafe bool) *Decoder[T, E] {
	d._safe = isSafe
	return d
}

func (d *Decoder[T, E]) WithExceptions(values []T, pos []uint32) *Decoder[T, E] {
	d.patch_values = values
	d.patch_indices = pos
	return d
}

func (d *Decoder[T, E]) DecodeValue(v E, i int) T {
	if d.patch_values != nil {
		// lazy init patch map on first access
		if d.patch_map == nil {
			d.patch_map = make(map[uint32]T, len(d.patch_values))
			for i, v := range d.patch_values {
				d.patch_map[d.patch_indices[i]] = v
			}
		}
		if e, ok := d.patch_map[uint32(i)]; ok {
			return e
		}
	}
	return d.decode(v)
}

func (d *Decoder[T, E]) decode(v E) T {
	return T(v) * d.f * d.e
}

// Decodes an ALP vector from provided integers. src and dst must have same length.
func (d *Decoder[T, E]) Decode(dst []T, src []E) {
	l := len(src)
	if l == 0 {
		return
	}

	var i int
	if l >= 128 {
		switch util.SizeOf[T]() {
		case 8:
			d64 := util.ReinterpretSlice[T, float64](dst)
			s64 := util.ReinterpretSlice[E, int64](src)
			i += decode64(d64, s64, d._f, d._e, d._safe)
		case 4:
			d32 := util.ReinterpretSlice[T, float32](dst)
			s32 := util.ReinterpretSlice[E, int32](src)
			i += decode32(d32, s32, d._f, d._e, d._safe)
		}
	}

	// tail
	for i < l {
		dst[i] = T(src[i]) * d.f * d.e
		i++
	}

	// patching patch_values
	for i, expPos := range d.patch_indices {
		dst[expPos] = d.patch_values[i]
	}
}

// Scalar decoding of an ALP vector with bit-unpack fusion
// dst[i] = T(unpack + minv) * f * e
func (d *Decoder[T, E]) DecodeFused(dst []T, src []byte, log2 int, minv E) {
	l := len(src)
	if l == 0 {
		return
	}

	// fusion kernel driver is defined either here or in bitpack package
	bitpack.DecodeAlp(dst, src, log2, T(minv), d.f, d.e)

	// patching patch_values
	for i, expPos := range d.patch_indices {
		dst[expPos] = d.patch_values[i]
	}
}

var (
	decode64 = decodeCore[float64, int64]
	decode32 = decodeCore[float32, int32]
)

func init() {
	if util.UseAVX2 {
		decode64 = avx2.Decode64
		decode32 = avx2.Decode32
	}
}

func decodeCore[T Float, E Int](dst []T, src []E, fx, ex uint8, isSafe bool) int {
	l := len(src)
	if l == 0 {
		return 0
	}
	c := getConstantPtr[T]()
	f, e := c.F10[fx], c.IF10[ex]

	_ = dst[l-1]
	var i int
	for range l / 8 {
		dst[i] = T(src[i]) * f * e
		dst[i+1] = T(src[i+1]) * f * e
		dst[i+2] = T(src[i+2]) * f * e
		dst[i+3] = T(src[i+3]) * f * e
		dst[i+4] = T(src[i+4]) * f * e
		dst[i+5] = T(src[i+5]) * f * e
		dst[i+6] = T(src[i+6]) * f * e
		dst[i+7] = T(src[i+7]) * f * e
		i += 8
	}

	return l &^ 7
}

func (d *Decoder[T, E]) DecodeChunk(dst *[128]T, src *[128]E, n, ofs int) {
	// decode values
	if n == 128 {
		switch util.SizeOf[T]() {
		case 8:
			d64 := util.ReinterpretSlice[T, float64](dst[:])
			s64 := util.ReinterpretSlice[E, int64](src[:])
			decode64(d64, s64, d._f, d._e, d._safe)
		case 4:
			d32 := util.ReinterpretSlice[T, float32](dst[:])
			s32 := util.ReinterpretSlice[E, int32](src[:])
			decode32(d32, s32, d._f, d._e, d._safe)
		}
	} else {
		var i int
		for range n {
			dst[i] = T(src[i]) * d.f * d.e
			i++
		}
	}

	// patch patch_values in range
	if len(d.patch_indices) > 0 {
		i, _ := slices.BinarySearch(d.patch_indices, uint32(ofs))
		for i < len(d.patch_indices) {
			p := int(d.patch_indices[i])
			if p >= ofs+128 {
				break
			}
			dst[p-ofs] = d.patch_values[i]
			i++
		}
	}
}
