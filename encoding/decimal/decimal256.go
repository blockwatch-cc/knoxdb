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

// TODO, extend to 128bit
func (d Decimal256) String() string {
	// switch d.scale {
	// case 0:
	// 	return strconv.FormatInt(d.val[3], 10)
	// default:
	// 	i := strconv.FormatInt(d.val[3]/pow10[d.scale], 10)
	// 	f := strconv.FormatInt(abs(d.val[3])%pow10[d.scale], 10)
	// 	if diff := d.scale - len(f); diff > 0 {
	// 		f = strings.Repeat("0", diff) + f
	// 	}
	// 	return i + "." + f
	// }
	return "Decimal256_String()_todo"
}

func (d Decimal256) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// TODO, extend to 256bit
func (d *Decimal256) UnmarshalText(buf []byte) error {
	// s := string(buf)
	// if !decimalRegexp.Match(buf) {
	// 	return fmt.Errorf("decimal256: invalid decimal string %s", s)
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
	// 		return fmt.Errorf("decimal256: %v", err)
	// 	}
	// 	if len(s) > MaxDecimal256Precision {
	// 		return fmt.Errorf("decimal256: number %s overflows precision", s)
	// 	}
	// 	d.val = NewInt256(sign, uint64(sign), uint64(sign), i)
	// 	d.scale = 0

	// default:
	// 	if len(s)-1 > MaxDecimal256Precision {
	// 		return fmt.Errorf("decimal256: number %s overflows precision", s)
	// 	}

	// 	// parse integral part
	// 	i, err := strconv.ParseUint(s[:dot], 10, 64)
	// 	if err != nil {
	// 		return fmt.Errorf("decimal256: integral %v", err)
	// 	}

	// 	// parse fractional digits
	// 	f, err := strconv.ParseUint(s[dot+1:], 10, 64)
	// 	if err != nil {
	// 		return fmt.Errorf("decimal256: fraction %v", err)
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
	// 	d.val = NewInt256(sign, uint64(sign), uint64(sign), i*pow10[d.scale]+f*pow10[lead])
	// }
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
