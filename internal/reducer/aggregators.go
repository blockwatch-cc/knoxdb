// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reducer

import (
	"bytes"
	"encoding"
	"math"
	"strconv"

	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/num"
)

type Bitmap = bitmap.Bitmap // xroar.Bitmap

type Aggregatable interface {
	encoding.BinaryUnmarshaler
	Emit(*bytes.Buffer) error
	Zero() Aggregatable
	Init(Aggregatable)
	Add(Aggregatable) Aggregatable
	Cmp(Aggregatable) int
	Float64() float64
	SetFloat64(float64)
}

// Exported type templates for use in type maps
var (
	// BigIntAgg         = func(scale uint8) Aggregatable { return &BigIntAggregator{scale: scale} }
	I128Agg      = func(scale uint8) Aggregatable { return &Int128Aggregator{scale: scale} }
	I256Agg      = func(scale uint8) Aggregatable { return &Int256Aggregator{scale: scale} }
	BitAgg       = func() Aggregatable { return &BitmapAggregator{} }
	BitAggAnd    = func(src *Bitmap) Aggregatable { return &BitmapAggregator{src: src, fn: bitmap.And} }
	BitAggOr     = func(src *Bitmap) Aggregatable { return &BitmapAggregator{src: src, fn: bitmap.Or} }
	BitAggAndNot = func(src *Bitmap) Aggregatable { return &BitmapAggregator{src: src, fn: bitmap.AndNot} }
)

type BitmapIntersectFunc func(*Bitmap, *Bitmap) *bitmap.Bitmap

// bitmap.Bitmap OR's on add and returns count of bits on emit. When used for
// linear interpolation or other math, we convert calculated float64 values
// to a separate internal int counter.
type BitmapAggregator struct {
	count int
	bits  *Bitmap
	src   *Bitmap
	fn    BitmapIntersectFunc
}

func (b *BitmapAggregator) Init(val Aggregatable) {
	a := val.(*BitmapAggregator)
	b.src = a.src
	b.fn = a.fn
}

func (b BitmapAggregator) IsZero() bool {
	return b.bits == nil
}

func (b BitmapAggregator) Count() int {
	if b.fn == nil {
		if b.IsZero() {
			return b.count
		}
		return b.bits.Count()
	}
	res := b.fn(b.src, b.bits)
	cnt := res.Count()
	// res.Free()
	return cnt
}

func (b *BitmapAggregator) UnmarshalBinary(src []byte) error {
	if b.IsZero() {
		b.bits = bitmap.New()
	}
	return b.bits.UnmarshalBinary(src)
}

func (b BitmapAggregator) Emit(buf *bytes.Buffer) error {
	_, err := buf.WriteString(strconv.Itoa(b.Count()))
	return err
}

func (b BitmapAggregator) Zero() Aggregatable {
	return &BitmapAggregator{
		src: b.src,
		fn:  b.fn,
	}
}

func (b *BitmapAggregator) Add(val Aggregatable) Aggregatable {
	a, ok := val.(*BitmapAggregator)
	if !ok {
		return b
	}
	if b.IsZero() {
		b.count += a.count
		if !a.IsZero() {
			b.bits = a.bits.Clone()
		}
	} else {
		b.count = 0
		b.bits.Or(a.bits)
	}
	return b
}

func (b BitmapAggregator) Cmp(val Aggregatable) int {
	a, ok := val.(*BitmapAggregator)
	if !ok {
		return 0
	}
	acnt, bcnt := a.Count(), b.Count()
	switch {
	case acnt == bcnt:
		return 0
	case acnt < bcnt:
		return 1
	default:
		return -1
	}
}

func (b BitmapAggregator) Float64() float64 {
	return float64(b.Count())
}

func (b *BitmapAggregator) SetFloat64(f64 float64) {
	b.count = int(math.RoundToEven(f64))
}

// BigInt
// type BigIntAggregator struct {
// 	BigInt
// 	scale int
// }

// func (b *BigIntAggregator) Init(val Aggregatable) {
// 	b.scale = val.(*BigIntAggregator).scale
// }

// func (b *BigIntAggregator) UnmarshalBinary(src []byte) error {
// 	return b.BigInt.UnmarshalBinary(src)
// }

// func (b BigIntAggregator) Emit(buf *bytes.Buffer) error {
// 	_, err := buf.WriteString(strconv.Quote(b.BigInt.Decimals(b.scale)))
// 	return err
// }

// func (b BigIntAggregator) Zero() Aggregatable {
// 	return &BigIntAggregator{
// 		scale: b.scale,
// 	}
// }

// func (b *BigIntAggregator) Add(val Aggregatable) Aggregatable {
// 	a, ok := val.(*BigIntAggregator)
// 	if !ok {
// 		return b
// 	}
// 	return &BigIntAggregator{b.BigInt.Add(a.BigInt), b.scale}
// }

// func (b BigIntAggregator) Cmp(val Aggregatable) int {
// 	a, ok := val.(*BigIntAggregator)
// 	if !ok {
// 		return 0
// 	}
// 	return b.BigInt.Big().Cmp(a.BigInt.Big())
// }

// func (b BigIntAggregator) Float64() float64 {
// 	return b.BigInt.Float64(0)
// }

// func (b *BigIntAggregator) SetFloat64(f64 float64) {
// 	f64 = math.RoundToEven(f64)
// 	fs, _, _ := strings.Cut(strconv.FormatFloat(f64, 'f', -1, 64), ".")
// 	bi, _ := ParseBigInt(fs)
// 	b.BigInt = bi
// }

// Int128
type Int128Aggregator struct {
	num.Int128
	scale uint8
}

func (b *Int128Aggregator) Init(val Aggregatable) {
	b.scale = val.(*Int128Aggregator).scale
}

func (b *Int128Aggregator) UnmarshalBinary(_ []byte) error {
	return nil
}

func (b Int128Aggregator) Emit(buf *bytes.Buffer) error {
	_, err := buf.WriteString(strconv.Quote(num.NewDecimal128(b.Int128, b.scale).String()))
	return err
}

func (b Int128Aggregator) Zero() Aggregatable {
	return &Int128Aggregator{num.ZeroInt128, b.scale}
}

func (b *Int128Aggregator) Add(val Aggregatable) Aggregatable {
	a, ok := val.(*Int128Aggregator)
	if !ok {
		return b
	}
	return &Int128Aggregator{b.Int128.Add(a.Int128), b.scale}
}

func (b Int128Aggregator) Cmp(val Aggregatable) int {
	a, ok := val.(*Int128Aggregator)
	if !ok {
		return 0
	}
	return b.Int128.Cmp(a.Int128)
}

func (b Int128Aggregator) Float64() float64 {
	return b.Int128.Float64()
}

func (b *Int128Aggregator) SetFloat64(f64 float64) {
	b.Int128.SetFloat64(f64)
}

// Int256
type Int256Aggregator struct {
	num.Int256
	scale uint8
}

func (b *Int256Aggregator) Init(val Aggregatable) {
	b.scale = val.(*Int256Aggregator).scale
}

func (b *Int256Aggregator) UnmarshalBinary(_ []byte) error {
	return nil
}

func (b Int256Aggregator) Emit(buf *bytes.Buffer) error {
	_, err := buf.WriteString(strconv.Quote(num.NewDecimal256(b.Int256, b.scale).String()))
	return err
}

func (b Int256Aggregator) Zero() Aggregatable {
	return &Int256Aggregator{num.ZeroInt256, b.scale}
}

func (b *Int256Aggregator) Add(val Aggregatable) Aggregatable {
	a, ok := val.(*Int256Aggregator)
	if !ok {
		return b
	}
	return &Int256Aggregator{b.Int256.Add(a.Int256), b.scale}
}

func (b Int256Aggregator) Cmp(val Aggregatable) int {
	a, ok := val.(*Int256Aggregator)
	if !ok {
		return 0
	}
	return b.Int256.Cmp(a.Int256)
}

func (b Int256Aggregator) Float64() float64 {
	return b.Int256.Float64()
}

func (b *Int256Aggregator) SetFloat64(f64 float64) {
	b.Int256.SetFloat64(f64)
}
