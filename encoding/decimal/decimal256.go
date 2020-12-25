// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package decimal

import (
	"fmt"
	"strings"

	. "blockwatch.cc/knoxdb/vec"
)

var Decimal256Zero = Decimal256{Int256Zero, 0}

// 76 digits
type Decimal256 struct {
	val   Int256
	scale int
}

type Decimal256Slice struct {
	Vec   []Int256
	Scale int
}

func NewDecimal256(val Int256, scale int) Decimal256 {
	return Decimal256{val: val, scale: scale}
}

func (d Decimal256) IsValid() bool {
	ok, _ := d.Check()
	return ok
}

func (d Decimal256) IsZero() bool {
	return d.val.IsZero()
}

func (d Decimal256) Check() (bool, error) {
	if d.scale < 0 {
		return false, fmt.Errorf("decimal256: invalid negative scale %d", d.scale)
	}
	if d.scale > MaxDecimal256Precision {
		return false, fmt.Errorf("decimal256: scale %d overflow", d.scale)
	}
	if d.scale > 0 && !d.val.IsZero() {
		if p := d.val.Precision(); p < d.scale {
			return false, fmt.Errorf("decimal256: scale %d larger than value digits %d", d.scale, p)
		}
	}
	return true, nil
}

func (d Decimal256) Scale() int {
	return d.scale
}

func (d Decimal256) Precision() int {
	if d.val.IsInt64() {
		val := d.val.Int64()
		for i := range pow10 {
			if abs(val) > pow10[i] {
				continue
			}
			return i
		}
	}
	pow := Int256FromInt64(1e18)
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

func (d Decimal256) Clone() Decimal256 {
	return Decimal256{
		val:   d.val,
		scale: d.scale,
	}
}

func (d Decimal256) Quantize(scale int) Decimal256 {
	if scale == d.scale {
		return d
	}
	if scale > MaxDecimal256Precision {
		scale = MaxDecimal256Precision
	}
	if d.IsZero() {
		return Decimal256{Int256Zero, scale}
	}
	diff := d.scale - scale
	l := len(pow10)
	// fmt.Printf("D256 quantize %d->%d\n", d.scale, scale)
	if diff < 0 {
		for i := -diff / l; i > 0; i-- {
			// fmt.Printf("> mul by %d\n", pow10[l-1])
			d.val = d.val.Mul(Int256FromInt64(int64(pow10[l-1])))
			diff += l
		}
		// fmt.Printf("> mul by %d (%s)\n", pow10[-diff], d.val)
		d.val = d.val.Mul(Int256FromInt64(int64(pow10[-diff])))
		// fmt.Printf("> res = %s\n", d.val)
		d.scale = scale
	} else {
		sign := d.val.Sign()
		y := Int256FromInt64(int64(pow10[diff%l]))
		for i := diff / l; i > 0; i-- {
			y = y.Mul(Int256FromInt64(int64(pow10[l-1])))
		}
		// fmt.Printf("> div %s by %s\n", d.val, y)
		// IEEE 754-2008 roundTiesToEven
		quo, rem := d.val.QuoRem(y)
		mid := y.Div(Int256FromInt64(2))
		// fmt.Printf("> quo = %s rem=%s %08x %08x %08x %08x\n", quo, rem, rem[0], rem[1], rem[2], rem[3])
		if rem.Gt(mid) || rem.Eq(mid) && quo[1]%2 == 1 {
			if sign > 0 {
				quo = quo.Add64(1)
			} else {
				quo = quo.Sub64(1)
			}
		}
		d.val = quo
		d.scale = scale
		// fmt.Printf("> res = %s (%d)\n", quo, scale)
	}
	return d
}

func (d Decimal256) Int64() int64 {
	return d.val.Int64()
}

func (d Decimal256) Int128() Int128 {
	return d.val.Int128()
}

func (d Decimal256) Int256() Int256 {
	return d.val
}

func (d *Decimal256) SetInt64(value int64, scale int) error {
	if scale < 0 {
		return fmt.Errorf("decimal256: scale %d underflow", scale)
	}
	if scale > MaxDecimal256Precision {
		return fmt.Errorf("decimal256: scale %d overflow", scale)
	}
	d.scale = scale
	d.val.SetInt64(value)
	return nil
}

func (d *Decimal256) SetInt128(value Int128, scale int) error {
	if scale < 0 {
		return fmt.Errorf("decimal256: scale %d underflow", scale)
	}
	if scale > MaxDecimal256Precision {
		return fmt.Errorf("decimal256: scale %d overflow", scale)
	}
	d.scale = scale
	d.val = value.Int256()
	return nil
}

func (d *Decimal256) SetInt256(value Int256, scale int) error {
	if scale < 0 {
		return fmt.Errorf("decimal256: scale %d underflow", scale)
	}
	if scale > MaxDecimal256Precision {
		return fmt.Errorf("decimal256: scale %d overflow", scale)
	}
	d.scale = scale
	d.val = value
	return nil
}

func (d Decimal256) RoundToInt64() int64 {
	return d.Quantize(0).Int64()
}

func (d Decimal256) Float64() float64 {
	f := d.val.Float64()
	scale := d.scale
	l := len(pow10)
	for i := scale / l; i > 0; i-- {
		f /= float64(pow10[l-1])
		scale -= l
	}
	return f / float64(pow10[scale])
}

func (d *Decimal256) SetFloat64(value float64, scale int) error {
	if scale < 0 {
		return fmt.Errorf("decimal256: scale %d underflow", scale)
	}
	if scale > MaxDecimal256Precision {
		return fmt.Errorf("decimal256: scale %d overflow", scale)
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

func (d Decimal256) String() string {
	s := d.val.String()
	if d.scale == 0 || d.val.IsZero() {
		return s
	}
	return s[:len(s)-d.scale] + "." + s[len(s)-d.scale:]
}

func (d Decimal256) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *Decimal256) UnmarshalText(buf []byte) error {
	if len(buf) == 0 {
		d.scale = 0
		d.val = Int256Zero
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
		if scale > MaxDecimal256Precision {
			return fmt.Errorf("decimal256: number %s overflows precision", s)
		}
		s = s[:dot] + s[dot+1:]
	}

	// parse number
	i, err := ParseInt256(sign + s)
	if err != nil {
		return fmt.Errorf("decimal256: %v", err)
	}

	d.scale = scale
	d.val = i
	return nil
}

func ParseDecimal256(s string) (Decimal256, error) {
	dec := NewDecimal256(Int256{}, 0)
	if _, err := dec.Check(); err != nil {
		return dec, err
	}
	if err := dec.UnmarshalText([]byte(s)); err != nil {
		return dec, err
	}
	return dec, nil
}

func (d Decimal256) Eq(b Decimal256) bool {
	return d.scale == b.scale && d.val == b.val
}

func (a Decimal256) Cmp(b Decimal256) int {
	return a.val.Cmp(b.val)
}

func EqualScaleDecimal256(a, b Decimal256) (Decimal256, Decimal256) {
	switch true {
	case a.scale == b.scale:
		return a, b
	case a.scale < b.scale:
		return a, b.Quantize(a.scale)
	default:
		return a.Quantize(b.scale), b
	}
}

func (a Decimal256) Gt(b Decimal256) bool {
	x, y := EqualScaleDecimal256(a, b)
	return x.val.Gt(y.val)
}

func (a Decimal256) Gte(b Decimal256) bool {
	x, y := EqualScaleDecimal256(a, b)
	return x.val.Gte(y.val)
}

func (a Decimal256) Lt(b Decimal256) bool {
	x, y := EqualScaleDecimal256(a, b)
	return x.val.Lt(y.val)
}

func (a Decimal256) Lte(b Decimal256) bool {
	x, y := EqualScaleDecimal256(a, b)
	return x.val.Lte(y.val)
}
