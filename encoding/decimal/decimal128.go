// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// half-even rounding mode (IEEE 754-2008 roundTiesToEven)

package decimal

import (
	"fmt"
	// "strconv"
	"strings"

	. "blockwatch.cc/knoxdb/vec"
)

// 38 digits
type Decimal128 struct {
	val   Int128
	scale int
}

// var _ Decimal = (*Decimal128)(nil)

func NewDecimal128(val Int128, scale int) Decimal128 {
	return Decimal128{val: val, scale: scale}
}

func (d Decimal128) IsValid() bool {
	ok, _ := d.Check()
	return ok
}

func (d Decimal128) Check() (bool, error) {
	if d.scale < 0 {
		return false, fmt.Errorf("decimal128: invalid negative scale %d", d.scale)
	}
	if d.scale > MaxDecimal128Precision {
		return false, fmt.Errorf("decimal128: scale %d overflow", d.scale)
	}
	return true, nil
}

func (d Decimal128) Bitsize() int {
	return 128
}

func (d Decimal128) Scale() int {
	return d.scale
}

// TODO, extend to 128bit
func (d Decimal128) Precision() int {
	// for i := range pow10 {
	// 	if abs(d.val[1]) > pow10[i] {
	// 		continue
	// 	}
	// 	return i
	// }
	return 0
}

func (d Decimal128) Clone() Decimal128 {
	return Decimal128{
		val:   d.val,
		scale: d.scale,
	}
}

// TODO, extend to 128bit
func (d Decimal128) Quantize(scale int) Decimal128 {
	if scale == d.scale {
		return d
	}
	if scale > MaxDecimal128Precision {
		scale = MaxDecimal128Precision
	}
	diff := d.scale - scale
	if diff < 0 {
		// d.val[1] *= pow10[-diff]
		// d.scale = scale
	} else {
		// sign := int64(1)
		// if d.val[1] < 0 {
		// 	sign = -1
		// }
		// // IEEE 754-2008 roundTiesToEven
		// rem := d.val[1] % pow10[diff] * sign
		// mid := 5 * pow10[diff-1]
		// d.val[1] /= pow10[diff]
		// if rem > mid || rem == mid && d.val[1]*sign%2 == 1 {
		// 	d.val[1] += sign
		// }
		// d.scale = scale
	}
	return d
}

func (d Decimal128) Int64() int64 {
	return d.val.Int64()
}

func (d Decimal128) Int128() Int128 {
	return d.val
}

func (d Decimal128) Int256() Int256 {
	return d.val.Int256()
}

func (d *Decimal128) SetInt64(value int64, scale int) error {
	if scale > MaxDecimal128Precision {
		return fmt.Errorf("decimal128: scale %d overflow", scale)
	}
	d.scale = scale
	d.val.SetInt64(value)
	return nil
}

func (d *Decimal128) SetInt128(value Int128, scale int) error {
	if scale > MaxDecimal128Precision {
		return fmt.Errorf("decimal128: scale %d overflow", scale)
	}
	d.scale = scale
	d.val = value
	return nil
}

func (d Decimal128) RoundToInt64() int64 {
	return d.Quantize(0).Int64()
}

// TODO, extend to 128bit
func (d Decimal128) Float64() float64 {
	return float64(d.val[1]) / float64(pow10[d.scale])
}

// TODO, extend to 128bit
func (d *Decimal128) SetFloat64(value float64, scale int) error {
	if scale > MaxDecimal128Precision {
		return fmt.Errorf("decimal128: scale %d overflow", scale)
	}

	// ignore overflow/underflow
	d.val.SetFloat64(value)
	d.scale = 0
	*d = d.Quantize(scale)
	return nil
}

func (d Decimal128) String() string {
	s := d.val.String()
	if d.scale == 0 {
		return s
	}
	return s[:len(s)-d.scale] + "." + s[len(s)-d.scale:]
}

func (d Decimal128) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *Decimal128) UnmarshalText(buf []byte) error {
	if len(buf) == 0 {
		d.scale = 0
		d.val = Int128Zero
		return nil
	}

	s := string(buf)

	// handle sign
	var sign string
	switch s[0] {
	case '+', '-':
		sign = string(s[0])
		s = s[1:]
	}

	// find the decimal dot
	dot := strings.IndexByte(s, '.')

	// remove the dot
	scale := len(s) - dot - 1
	if dot < 0 {
		scale = 0
	} else {
		if scale > MaxDecimal128Precision {
			return fmt.Errorf("decimal128: number %s overflows precision", s)
		}
		s = s[:dot] + s[dot+1:]
	}

	// parse number
	i, err := ParseInt128(sign + s)
	if err != nil {
		return fmt.Errorf("decimal128: %v", err)
	}

	d.scale = scale
	d.val = i
	return nil
}

func ParseDecimal128(s string, scale int) (Decimal128, error) {
	dec := NewDecimal128(Int128{}, scale)
	if _, err := dec.Check(); err != nil {
		return dec, err
	}
	if err := dec.UnmarshalText([]byte(s)); err != nil {
		return dec, err
	}
	return dec, nil
}

func (d Decimal128) Eq(b Decimal128) bool {
	return d.scale == b.scale && d.val == b.val
}

func (a Decimal128) Cmp(b Decimal128) int {
	return a.val.Cmp(b.val)
}

func EqualScaleDecimal128(a, b Decimal128) (Decimal128, Decimal128) {
	switch true {
	case a.scale == b.scale:
		return a, b
	case a.scale < b.scale:
		return a, b.Quantize(a.scale)
	default:
		return a.Quantize(b.scale), b
	}
}

func (a Decimal128) Gt(b Decimal128) bool {
	x, y := EqualScaleDecimal128(a, b)
	return x.val.Gt(y.val)
}

func (a Decimal128) Gte(b Decimal128) bool {
	x, y := EqualScaleDecimal128(a, b)
	return x.val.Gte(y.val)
}

func (a Decimal128) Lt(b Decimal128) bool {
	x, y := EqualScaleDecimal128(a, b)
	return x.val.Lt(y.val)
}

func (a Decimal128) Lte(b Decimal128) bool {
	x, y := EqualScaleDecimal128(a, b)
	return x.val.Lte(y.val)
}
