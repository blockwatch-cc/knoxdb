// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"fmt"
	"math"
	"strings"
)

var ZeroDecimal128 = Decimal128{ZeroInt128, 0}

// 38 digits
type Decimal128 struct {
	val   Int128
	scale uint8
}

type Decimal128Slice struct {
	Int128 []Int128
	Scale  uint8
}

func NewDecimal128(val Int128, scale uint8) Decimal128 {
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
	if d.scale > MaxDecimal128Precision {
		return false, ErrScaleOverflow
	}
	return true, nil
}

func (d Decimal128) Scale() uint8 {
	return d.scale
}

func (d Decimal128) Precision() uint8 {
	switch {
	case d.val.IsInt64():
		v := abs(d.val.Int64())
		for i := range pow10 {
			if v >= pow10[i] {
				continue
			}
			return uint8(i)
		}
	case d.val == MinInt128:
		return 39
	default:
		pow := Int128{0, 1e18}
		q, r := d.val.Abs().QuoRem(pow)
		for p := 0; ; p += 18 {
			if q.IsZero() {
				v := abs(r.Int64())
				for i := range pow10 {
					if v >= pow10[i] {
						continue
					}
					return uint8(p + i)
				}
			}
			q, r = q.QuoRem(pow)
		}
	}
	return 0
}

func (d Decimal128) Clone() Decimal128 {
	return Decimal128{
		val:   d.val,
		scale: d.scale,
	}
}

func (d Decimal128) Quantize(scale uint8) Decimal128 {
	if scale == d.scale {
		return d
	}
	if scale > MaxDecimal128Precision {
		scale = MaxDecimal128Precision
	}
	if d.IsZero() {
		return Decimal128{ZeroInt128, scale}
	}
	diff := int(d.scale) - int(scale)
	l := len(pow10) - 2
	if diff < 0 {
		val := d.val
		for i := -diff / l; i > 0; i-- {
			val = val.Mul64(int64(pow10[l]))
			diff += l
		}
		val = val.Mul64(int64(pow10[-diff]))
		d.val = val
		d.scale = scale
	} else {
		sign := d.val.Sign()
		y := Int128{0, pow10[diff%l]}
		for i := diff / l; i > 0; i-- {
			y = y.Mul64(int64(pow10[l]))
		}
		// IEEE 754-2008 roundTiesToEven
		quo, rem := d.val.QuoRem(y)
		mid := y.Div64(2).Abs()
		rem = rem.Abs()
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

func (d *Decimal128) Set(value Int128) {
	d.val = value
}

func (d *Decimal128) SetScale(scale uint8) {
	d.scale = scale
}

func (d *Decimal128) SetInt64(value int64, scale uint8) error {
	if scale > MaxDecimal128Precision {
		return ErrScaleOverflow
	}
	d.scale = scale
	d.val.SetInt64(value)
	return nil
}

func (d *Decimal128) SetInt128(value Int128, scale uint8) error {
	if scale > MaxDecimal128Precision {
		return ErrScaleOverflow
	}
	d.scale = scale
	d.val = value
	return nil
}

func (d Decimal128) Add64(value int64) Decimal128 {
	d.val = d.val.Add64(uint64(value))
	return d
}

func (d Decimal128) Add(value Decimal128) Decimal128 {
	if d.scale != value.scale {
		value = value.Quantize(d.scale)
	}
	d.val = d.val.Add(value.val)
	return d
}

func (d Decimal128) RoundToInt64() int64 {
	return d.Quantize(0).Int64()
}

func (d Decimal128) Float64() float64 {
	f := d.val.Float64()
	scale := int(d.scale)
	l := len(pow10)
	for i := scale / l; i > 0; i-- {
		f /= float64(pow10[l-1])
		scale -= l
	}
	return f / float64(pow10[scale])
}

func (d *Decimal128) SetFloat64(value float64, scale uint8) error {
	if scale > MaxDecimal128Precision {
		return ErrScaleOverflow
	}
	// handle special cases
	switch {
	case math.IsNaN(value):
		return ErrInvalidFloat64
	case math.IsInf(value, 1):
		return ErrPrecisionOverflow
	case math.IsInf(value, -1):
		return ErrPrecisionUnderflow
	}
	if scale > 0 {
		l := len(pow10) - 1
		for i := int(scale) / l; i > 0; i-- {
			value *= float64(pow10[l])
		}
		value *= float64(pow10[int(scale)%l])
	}
	var i128 Int128
	acc := i128.SetFloat64(value)
	switch acc {
	case Below:
		return ErrPrecisionUnderflow
	case Above:
		return ErrPrecisionOverflow
	}
	d.val = i128
	d.scale = scale
	return nil
}

func (d Decimal128) String() string {
	i := d.val.String()
	switch d.scale {
	case 0:
		return i
	default:
		var b strings.Builder
		b.Grow(int(MaxDecimal128Precision + 2))
		sign := 0
		if i[0] == '-' {
			b.WriteRune('-')
			sign = 1
		}
		diff := int(d.scale) - len(i) + sign
		if diff >= 0 {
			// 0.00001 (scale=5)
			// add leading zeros
			b.WriteString("0.")
			b.WriteString(zeros[:diff])
			b.WriteString(i[sign:])
		} else {
			// 1234.56789 (scale=5)
			b.WriteString(i[sign : len(i)-int(d.scale)])
			b.WriteRune('.')
			b.WriteString(i[len(i)-int(d.scale):])
		}
		return b.String()
	}
}

func (d Decimal128) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *Decimal128) UnmarshalText(buf []byte) error {
	l := len(buf)
	if l == 0 {
		return fmt.Errorf("decimal: empty string")
	}

	var (
		scale             = uint8(l)
		i, dpos, ncount   int
		sawdot, sawdigits bool
		val               Int128
	)

	// handle sign
	var sign int
	switch buf[i] {
	case '+':
		i++
		scale--
	case '-':
		sign = -1
		i++
		scale--
	}

loop:
	for ; i < l; i++ {
		c := buf[i]
		switch {
		case c == '.':
			if sawdot {
				break loop
			}
			if !sawdigits {
				return ErrInvalidDecimal
			}
			sawdot = true
			dpos = ncount
			scale--
			continue
		case '0' <= c && c <= '9':
			sawdigits = true
			if c == '0' && ncount == 0 { // ignore leading zeros
				dpos--
				scale--
				continue
			}
			ncount++
			// value is accumulated as positive int256
			val = val.Mul64(10)
			val = val.Add64(uint64(c - '0'))
			if sign < 0 {
				// Note: since val is +int128, MinInt128 would overflow
				if val[0] > 1<<63 || (val[0] == 1<<63 && val[1] > 0) {
					return ErrPrecisionUnderflow
				}
			} else {
				if val[0] > 1<<63-1 {
					return ErrPrecisionOverflow
				}
			}
			continue
		}
		break
	}
	if !sawdigits || i < l || dpos == ncount {
		return ErrInvalidDecimal
	}

	// adjust scale by dot position
	if sawdot {
		scale -= uint8(dpos)
	} else {
		scale = 0
	}

	// check limits
	if scale > MaxDecimal128Precision {
		return ErrScaleOverflow
	}

	if sign < 0 {
		val = val.Neg()
	}

	d.scale = scale
	d.val = val
	return nil
}

func ParseDecimal128(s string) (Decimal128, error) {
	var dec Decimal128
	err := dec.UnmarshalText([]byte(s))
	return dec, err
}

func EqualScaleDecimal128(a, b Decimal128) (Decimal128, Decimal128) {
	switch {
	case a.scale == b.scale:
		return a, b
	case a.scale < b.scale:
		return a, b.Quantize(a.scale)
	default:
		return a.Quantize(b.scale), b
	}
}

func (a Decimal128) Eq(b Decimal128) bool {
	x, y := EqualScaleDecimal128(a, b)
	return x.val.Eq(y.val)
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

func (a Decimal128) Cmp(b Decimal128) int {
	x, y := EqualScaleDecimal128(a, b)
	return x.val.Cmp(y.val)
}

func CompareDecimal128(a, b Decimal128) int {
	return a.Cmp(b)
}
