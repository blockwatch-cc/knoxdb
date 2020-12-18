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

// 76 digits
type Decimal256 struct {
	val   Int256
	scale int
}

// var _ Decimal = (*Decimal256)(nil)

func NewDecimal256(val Int256, scale int) Decimal256 {
	return Decimal256{val: val, scale: scale}
}

func (d Decimal256) IsValid() bool {
	ok, _ := d.Check()
	return ok
}

func (d Decimal256) Check() (bool, error) {
	if d.scale < 0 {
		return false, fmt.Errorf("decimal256: invalid negative scale %d", d.scale)
	}
	if d.scale > MaxDecimal256Precision {
		return false, fmt.Errorf("decimal256: scale %d overflow", d.scale)
	}
	return true, nil
}

func (d Decimal256) Bitsize() int {
	return 256
}

func (d Decimal256) Scale() int {
	return d.scale
}

// TODO, extend to 128bit
func (d Decimal256) Precision() int {
	// for i := range pow10 {
	// 	if abs(d.val[3]) > pow10[i] {
	// 		continue
	// 	}
	// 	return i
	// }
	return 0
}

func (d Decimal256) Clone() Decimal256 {
	return Decimal256{
		val:   d.val,
		scale: d.scale,
	}
}

// TODO, extend to 128bit
func (d Decimal256) Quantize(scale int) Decimal256 {
	if scale == d.scale {
		return d
	}
	// if scale > MaxDecimal256Precision {
	// 	return fmt.Errorf("decimal256: scale %d overflow", scale)
	// }
	// diff := d.scale - scale
	// if diff < 0 {
	// 	d.val[1] *= pow10[-diff]
	// 	d.scale = scale
	// } else {
	// 	sign := int64(1)
	// 	if d.val[1] < 0 {
	// 		sign = -1
	// 	}
	// 	// IEEE 754-2008 roundTiesToEven
	// 	rem := d.val[1] % pow10[diff] * sign
	// 	mid := 5 * pow10[diff-1]
	// 	d.val[1] /= pow10[diff]
	// 	if rem > mid || rem == mid && d.val[1]*sign%2 == 1 {
	// 		d.val[1] += sign
	// 	}
	// 	d.scale = scale
	// }
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
	if scale > MaxDecimal256Precision {
		return fmt.Errorf("decimal256: scale %d overflow", scale)
	}
	d.scale = scale
	d.val.SetInt64(value)
	return nil
}

func (d *Decimal256) SetInt128(value Int128, scale int) error {
	if scale > MaxDecimal128Precision {
		return fmt.Errorf("decimal128: scale %d overflow", scale)
	}
	d.scale = scale
	d.val = value.Int256()
	return nil
}

func (d *Decimal256) SetInt256(value Int256, scale int) error {
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

// TODO, extend to 128bit
func (d Decimal256) Float64() float64 {
	return float64(d.val[3]) / float64(pow10[d.scale])
}

// TODO, extend to 128bit
func (d *Decimal256) SetFloat64(value float64, scale int) error {
	// if scale > MaxDecimal256Precision {
	// 	return fmt.Errorf("decimal256: scale %d overflow", scale)
	// }
	// sign := int64(1)
	// if value < 0 {
	// 	sign = -1
	// }
	// f := value * float64(pow10[scale])
	// i := int64(f)
	// // IEEE 754-2008 roundTiesToEven
	// rem := (f - float64(i)) * float64(sign)
	// if rem > 0.5 || rem == 0.5 && i*sign%2 == 1 {
	// 	i += sign
	// }
	// d.val = Int256{0, 0, 0, i}
	// d.scale = scale
	return nil
}

func (d Decimal256) String() string {
	s := d.val.String()
	if d.scale == 0 {
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

func ParseDecimal256(s string, scale int) (Decimal256, error) {
	dec := NewDecimal256(Int256{}, scale)
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
