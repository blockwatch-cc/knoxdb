// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package decimal

import (
	"fmt"
	"strings"

	. "blockwatch.cc/knoxdb/vec"
)

var Decimal128Zero = Decimal128{Int128Zero, 0}

// 38 digits
type Decimal128 struct {
	val   Int128
	scale int
}

type Decimal128Slice struct {
	Vec   []Int128
	Scale int
}

func NewDecimal128(val Int128, scale int) Decimal128 {
	return Decimal128{val: val, scale: scale}
}

func (d Decimal128) IsValid() bool {
	ok, _ := d.Check()
	return ok
}

func (d Decimal128) IsZero() bool {
	return d.val.IsZero()
}

func (d Decimal128) Check() (bool, error) {
	if d.scale < 0 {
		return false, fmt.Errorf("decimal128: invalid negative scale %d", d.scale)
	}
	if d.scale > MaxDecimal128Precision {
		return false, fmt.Errorf("decimal128: scale %d overflow", d.scale)
	}
	if d.scale > 0 && !d.val.IsZero() {
		if p := d.val.Precision(); p < d.scale {
			return false, fmt.Errorf("decimal128: scale %d larger than value digits %d", d.scale, p)
		}
	}
	return true, nil
}

func (d Decimal128) Scale() int {
	return d.scale
}

func (d Decimal128) Precision() int {
	if d.val.IsInt64() {
		val := d.val.Int64()
		for i := range pow10 {
			if abs(val) > pow10[i] {
				continue
			}
			return i
		}
	}
	pow := Int128FromInt64(1e18)
	q, r := d.val.Abs().QuoRem(pow)
	for p := 0; ; p += 18 {
		if q.IsZero() {
			for i := r.Int64(); i != 0; i /= 10 {
				p++
			}
			return p
		}
		q, r = q.QuoRem(pow)
	}
	return 0
}

func (d Decimal128) Clone() Decimal128 {
	return Decimal128{
		val:   d.val,
		scale: d.scale,
	}
}

func (d Decimal128) Quantize(scale int) Decimal128 {
	if scale == d.scale {
		return d
	}
	if scale > MaxDecimal128Precision {
		scale = MaxDecimal128Precision
	}
	if d.IsZero() {
		return Decimal128{Int128Zero, scale}
	}
	diff := d.scale - scale
	l := len(pow10)
	if diff < 0 {
		for i := -diff / l; i > 0; i-- {
			d.val = d.val.Mul64(int64(pow10[l-1]))
			diff += l
		}
		d.val = d.val.Mul64(int64(pow10[-diff]))
		d.scale = scale
	} else {
		sign := d.val.Sign()
		y := Int128FromInt64(int64(pow10[diff%l]))
		for i := diff / l; i > 0; i-- {
			y = y.Mul64(int64(pow10[l-1]))
		}
		// IEEE 754-2008 roundTiesToEven
		quo, rem := d.val.QuoRem(y)
		mid := y.Div64(2)
		if rem.Gt(mid) || rem.Eq(mid) && quo[1]%2 == 1 {
			if sign > 0 {
				quo = quo.Add64(1)
			} else {
				quo = quo.Sub64(1)
			}
		}
		d.val = quo
		d.scale = scale
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
	if scale < 0 {
		return fmt.Errorf("decimal128: scale %d underflow", scale)
	}
	if scale > MaxDecimal128Precision {
		return fmt.Errorf("decimal128: scale %d overflow", scale)
	}
	d.scale = scale
	d.val.SetInt64(value)
	return nil
}

func (d *Decimal128) SetInt128(value Int128, scale int) error {
	if scale < 0 {
		return fmt.Errorf("decimal128: scale %d underflow", scale)
	}
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

func (d Decimal128) Float64() float64 {
	f := d.val.Float64()
	scale := d.scale
	l := len(pow10)
	for i := scale / l; i > 0; i-- {
		f /= float64(pow10[l-1])
		scale -= l
	}
	return f / float64(pow10[scale])
}

func (d *Decimal128) SetFloat64(value float64, scale int) error {
	if scale < 0 {
		return fmt.Errorf("decimal128: scale %d underflow", scale)
	}
	if scale > MaxDecimal128Precision {
		return fmt.Errorf("decimal128: scale %d overflow", scale)
	}
	if scale > 0 {
		l := len(pow10)
		for i := scale / l; i > 0; i-- {
			value *= float64(pow10[l-1])
		}
		value *= float64(pow10[scale%l+1])
	}
	d.val.SetFloat64(value)
	d.scale = scale
	return nil
}

func (d Decimal128) String() string {
	s := d.val.String()
	if d.scale == 0 || d.val.IsZero() {
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

func ParseDecimal128(s string) (Decimal128, error) {
	dec := NewDecimal128(Int128{}, 0)
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
