// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// half-even rounding mode (IEEE 754-2008 roundTiesToEven)

package decimal

import (
	"fmt"
	// "strconv"
	// "strings"

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

// TODO, extend to 128bit
func (d Decimal128) String() string {
	// switch d.scale {
	// case 0:
	// 	return strconv.FormatInt(d.val[1], 10)
	// default:
	// 	i := strconv.FormatInt(d.val[1]/pow10[d.scale], 10)
	// 	f := strconv.FormatInt(abs(d.val[1])%pow10[d.scale], 10)
	// 	if diff := d.scale - len(f); diff > 0 {
	// 		f = strings.Repeat("0", diff) + f
	// 	}
	// 	return i + "." + f
	// }
	return "Decimal128.String()_todo"
}

func (d Decimal128) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// TODO, extend to 128bit
func (d *Decimal128) UnmarshalText(buf []byte) error {
	// s := string(buf)
	// if !decimalRegexp.Match(buf) {
	// 	return fmt.Errorf("decimal128: invalid decimal string %s", s)
	// }
	// sign := int64(1)
	// switch s[0] {
	// case '+':
	// 	s = s[1:]
	// case '-':
	// 	sign = -1
	// 	s = s[1:]
	// }
	// dot := strings.Index(s, ".")
	// switch dot {
	// case -1:
	// 	// parse number
	// 	i, err := strconv.ParseUint(s, 10, 64)
	// 	if err != nil {
	// 		return fmt.Errorf("decimal128: %v", err)
	// 	}
	// 	if len(s) > MaxDecimal128Precision {
	// 		return fmt.Errorf("decimal128: number %s overflows precision", s)
	// 	}
	// 	d.val = NewInt128(sign, i)
	// 	d.scale = 0

	// default:
	// 	if len(s)-1 > MaxDecimal128Precision {
	// 		return fmt.Errorf("decimal128: number %s overflows precision", s)
	// 	}

	// 	// parse integral part
	// 	i, err := strconv.ParseUint(s[:dot], 10, 64)
	// 	if err != nil {
	// 		return fmt.Errorf("decimal128: integral %v", err)
	// 	}

	// 	// parse fractional digits
	// 	f, err := strconv.ParseUint(s[dot+1:], 10, 64)
	// 	if err != nil {
	// 		return fmt.Errorf("decimal128: fraction %v", err)
	// 	}

	// 	// count leading zeros in fractional part
	// 	lead := 0
	// 	for i := dot + 1; i < len(s); i++ {
	// 		if s[i] != '0' {
	// 			break
	// 		}
	// 		lead++
	// 	}

	// 	d.scale = len(s) - dot - 1
	// 	d.val = NewInt128(sign, i*pow10[d.scale]+f*pow10[lead])
	// }
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
