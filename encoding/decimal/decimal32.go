// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package decimal

import (
	"fmt"
	"strconv"
	"strings"

	. "blockwatch.cc/knoxdb/vec"
)

var Decimal32Zero = Decimal32{0, 0}

// 9 digits
type Decimal32 struct {
	val   int32
	scale int
}

type Decimal32Slice struct {
	Int32 []int32
	Scale int
}

func NewDecimal32(val int32, scale int) Decimal32 {
	return Decimal32{val: val, scale: scale}
}

func (d Decimal32) IsValid() bool {
	ok, _ := d.Check()
	return ok
}

func (d Decimal32) IsZero() bool {
	return d.val == 0
}

func (d Decimal32) Check() (bool, error) {
	if d.scale < 0 {
		return false, fmt.Errorf("decimal32: invalid negative scale %d", d.scale)
	}
	if d.scale > MaxDecimal32Precision {
		return false, fmt.Errorf("decimal32: scale %d overflow", d.scale)
	}
	if d.scale > 0 && d.val > 0 {
		if p := digits64(int64(d.val)); p < d.scale {
			return false, fmt.Errorf("decimal32: scale %d larger than value digits %d", d.scale, p)
		}
	}
	return true, nil
}

func (d Decimal32) Scale() int {
	return d.scale
}

func (d Decimal32) Precision() int {
	return digits64(int64(d.val))
}

func (d Decimal32) Clone() Decimal32 {
	return Decimal32{
		val:   d.val,
		scale: d.scale,
	}
}

func (d Decimal32) Quantize(scale int) Decimal32 {
	if scale == d.scale {
		return d
	}
	if scale > MaxDecimal32Precision {
		scale = MaxDecimal32Precision
	}
	if d.IsZero() {
		return Decimal32{0, scale}
	}
	diff := d.scale - scale
	if diff < 0 {
		d.val *= int32(pow10[-diff])
		d.scale = scale
	} else {
		sign := int32(1)
		if d.val < 0 {
			sign = -1
		}
		// IEEE 754-2008 roundTiesToEven
		rem := d.val % int32(pow10[diff]) * sign
		mid := 5 * int32(pow10[diff-1])
		d.val /= int32(pow10[diff])
		if rem > mid || rem == mid && d.val*sign%2 == 1 {
			d.val += sign
		}
		d.scale = scale
	}
	return d
}

func (d Decimal32) Int32() int32 {
	return d.val
}

func (d Decimal32) Int64() int64 {
	return int64(d.val)
}

func (d Decimal32) Int128() Int128 {
	return Int128{uint64(d.val >> 63), uint64(d.val)}
}

func (d Decimal32) Int256() Int256 {
	return Int256{uint64(d.val >> 63), uint64(d.val >> 63), uint64(d.val >> 63), uint64(d.val)}
}

func (d *Decimal32) SetInt64(value int64, scale int) error {
	if scale < 0 {
		return fmt.Errorf("decimal32: scale %d underflow", scale)
	}
	if scale > MaxDecimal32Precision {
		return fmt.Errorf("decimal32: scale %d overflow", scale)
	}
	d.scale = scale
	d.val = int32(value)
	return nil
}

func (d Decimal32) RoundToInt64() int64 {
	return d.Quantize(0).Int64()
}

func (d Decimal32) Float64() float64 {
	return float64(d.val) / float64(pow10[d.scale])
}

func (d *Decimal32) SetFloat64(value float64, scale int) error {
	if scale < 0 {
		return fmt.Errorf("decimal32: scale %d underflow", scale)
	}
	if scale > MaxDecimal32Precision {
		return fmt.Errorf("decimal32: scale %d overflow", scale)
	}
	sign := int64(1)
	if value < 0 {
		sign = -1
	}
	f := value * float64(pow10[scale])
	i := int64(f)
	// IEEE 754-2008 roundTiesToEven
	rem := (f - float64(i)) * float64(sign)
	if rem > 0.5 || rem == 0.5 && i*sign%2 == 1 {
		i += sign
	}
	d.val = int32(i)
	d.scale = scale
	return nil
}

func (d Decimal32) String() string {
	switch d.scale {
	case 0:
		return strconv.FormatInt(int64(d.val), 10)
	default:
		i := strconv.FormatInt(int64(d.val)/int64(pow10[d.scale]), 10)
		f := strconv.FormatUint(abs(int64(d.val))%pow10[d.scale], 10)
		if diff := d.scale - len(f); diff > 0 {
			f = strings.Repeat("0", diff) + f
		}
		return i + "." + f
	}
}

func (d Decimal32) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *Decimal32) UnmarshalText(buf []byte) error {
	s := string(buf)

	// handle sign
	sign := int32(1)
	switch s[0] {
	case '+':
		s = s[1:]
	case '-':
		sign = -1
		s = s[1:]
	}

	// find the decimal dot
	dot := strings.IndexByte(s, '.')

	// remove the dot
	scale := len(s) - dot - 1
	if dot < 0 {
		scale = 0
	} else {
		if scale > MaxDecimal32Precision {
			return fmt.Errorf("decimal32: number %s overflows precision", s)
		}
		s = s[:dot] + s[dot+1:]
	}

	// parse number
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return fmt.Errorf("decimal32: %v", err)
	}

	d.scale = scale
	d.val = int32(i) * sign

	return nil
}

func ParseDecimal32(s string) (Decimal32, error) {
	dec := NewDecimal32(0, 0)
	if _, err := dec.Check(); err != nil {
		return dec, err
	}
	if err := dec.UnmarshalText([]byte(s)); err != nil {
		return dec, err
	}
	return dec, nil
}

func (d Decimal32) Eq(b Decimal32) bool {
	return d.scale == b.scale && d.val == b.val
}

func (a Decimal32) Cmp(b Decimal32) int {
	switch true {
	case a.Lt(b):
		return -1
	case a.Eq(b):
		return 0
	default:
		return 1
	}
}

func EqualScaleDecimal32(a, b Decimal32) (Decimal32, Decimal32) {
	switch true {
	case a.scale == b.scale:
		return a, b
	case a.scale < b.scale:
		return a, b.Quantize(a.scale)
	default:
		return a.Quantize(b.scale), b
	}
}

func (a Decimal32) Gt(b Decimal32) bool {
	x, y := EqualScaleDecimal32(a, b)
	return x.val > y.val
}

func (a Decimal32) Gte(b Decimal32) bool {
	x, y := EqualScaleDecimal32(a, b)
	return x.val >= y.val
}

func (a Decimal32) Lt(b Decimal32) bool {
	x, y := EqualScaleDecimal32(a, b)
	return x.val < y.val
}

func (a Decimal32) Lte(b Decimal32) bool {
	x, y := EqualScaleDecimal32(a, b)
	return x.val <= y.val
}
