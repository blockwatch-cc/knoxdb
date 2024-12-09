// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

var ZeroDecimal64 = Decimal64{0, 0}

// 18 digits
type Decimal64 struct {
	val   int64
	scale uint8
}

type Decimal64Slice struct {
	Int64 []int64
	Scale uint8
}

func NewDecimal64(val int64, scale uint8) Decimal64 {
	return Decimal64{val: val, scale: scale}
}

func (d Decimal64) IsValid() bool {
	ok, _ := d.Check()
	return ok
}

func (d Decimal64) IsZero() bool {
	return d.val == 0
}

func (d Decimal64) Check() (bool, error) {
	if d.scale > MaxDecimal64Precision {
		return false, ErrScaleOverflow
	}
	return true, nil
}

func (d Decimal64) Scale() uint8 {
	return d.scale
}

func (d Decimal64) Precision() uint8 {
	return uint8(digits64(d.val))
}

func (d Decimal64) Clone() Decimal64 {
	return Decimal64{
		val:   d.val,
		scale: d.scale,
	}
}

func (d Decimal64) Quantize(scale uint8) Decimal64 {
	if scale == d.scale {
		return d
	}
	if scale > MaxDecimal64Precision {
		scale = MaxDecimal64Precision
	}
	if d.IsZero() {
		return Decimal64{0, scale}
	}
	diff := int(d.scale) - int(scale)
	if diff < 0 {
		d.val *= int64(pow10[-diff])
		d.scale = scale
	} else {
		sign := int64(1)
		if d.val < 0 {
			sign = -1
		}
		// IEEE 754-2008 roundTiesToEven
		rem := d.val % int64(pow10[diff]) * sign
		mid := int64(5 * pow10[diff-1])
		d.val /= int64(pow10[diff])
		if rem > mid || rem == mid && d.val*sign%2 == 1 {
			d.val += sign
		}
		d.scale = scale
	}
	return d
}

func (d Decimal64) Int64() int64 {
	return d.val
}

func (d Decimal64) Int128() Int128 {
	return Int128FromInt64(d.val)
}

func (d Decimal64) Int256() Int256 {
	return Int256FromInt64(d.val)
}

func (d *Decimal64) SetInt64(value int64, scale uint8) error {
	if scale > MaxDecimal64Precision {
		return ErrScaleOverflow
	}
	d.scale = scale
	d.val = value
	return nil
}

func (d Decimal64) Add64(value int64) Decimal64 {
	d.val += value
	return d
}

func (d Decimal64) Add(value Decimal64) Decimal64 {
	if d.scale != value.scale {
		value = value.Quantize(d.scale)
	}
	d.val += value.val
	return d
}

func (d Decimal64) RoundToInt64() int64 {
	return d.Quantize(0).Int64()
}

func (d Decimal64) Float64() float64 {
	return float64(d.val) / float64(pow10[d.scale])
}

func (d *Decimal64) Set(value int64) {
	d.val = value
}

func (d *Decimal64) SetScale(scale uint8) {
	d.scale = scale
}

func (d *Decimal64) SetFloat64(value float64, scale uint8) error {
	if scale > MaxDecimal64Precision {
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
	sign := int64(1)
	if value < 0 {
		sign = -1
		value = -value
	}
	f := value * float64(pow10[scale])
	// IEEE 754-2008 roundTiesToEven
	i := uint64(math.RoundToEven(f))
	// check against min/max
	if i > 1<<63-1 {
		if sign > 0 {
			return ErrPrecisionOverflow
		} else {
			return ErrPrecisionUnderflow
		}
	}
	d.val = int64(i) * sign
	d.scale = scale
	return nil
}

func (d Decimal64) String() string {
	switch d.scale {
	case 0:
		return strconv.FormatInt(d.val, 10)
	default:
		var b strings.Builder
		b.Grow(int(MaxDecimal64Precision + 2))
		if d.val>>63 != 0 {
			b.WriteRune('-')
		}
		i := strconv.FormatUint(abs(d.val), 10)
		diff := int(d.scale) - len(i)
		if diff >= 0 {
			// 0.00001 (scale=5)
			// add leading zeros
			b.WriteString("0.")
			b.WriteString(zeros[:diff])
			b.WriteString(i)
		} else {
			// 1234.56789 (scale=5)
			b.WriteString(i[:len(i)-int(d.scale)])
			b.WriteRune('.')
			b.WriteString(i[len(i)-int(d.scale):])
		}
		return b.String()
	}
}

func (d Decimal64) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *Decimal64) UnmarshalText(buf []byte) error {
	if len(buf) == 0 {
		return fmt.Errorf("decimal: empty string")
	}

	scale := uint8(len(buf))
	var (
		i, dpos, ncount   int
		sawdot, sawdigits bool
		val               uint64
	)

	// handle sign
	sign := int64(1)
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
	for ; i < len(buf); i++ {
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
			val *= 10
			val += uint64(c - '0')
			continue
		}
		break
	}
	if !sawdigits || i < len(buf) || dpos == ncount {
		return ErrInvalidDecimal
	}

	// adjust scale by dot position
	if sawdot {
		scale -= uint8(dpos)
	} else {
		scale = 0
	}

	// check limits
	if scale > MaxDecimal64Precision {
		return ErrScaleOverflow
	}
	if sign > 0 {
		if val > 1<<63-1 {
			return ErrPrecisionOverflow
		}
	} else {
		if val > 1<<63 {
			return ErrPrecisionUnderflow
		}
	}

	d.scale = scale
	d.val = int64(val) * sign
	return nil
}

func ParseDecimal64(s string) (Decimal64, error) {
	var dec Decimal64
	err := dec.UnmarshalText([]byte(s))
	return dec, err
}

func EqualScaleDecimal64(a, b Decimal64) (Decimal64, Decimal64) {
	switch {
	case a.scale == b.scale:
		return a, b
	case a.scale < b.scale:
		return a, b.Quantize(a.scale)
	default:
		return a.Quantize(b.scale), b
	}
}

func (a Decimal64) Eq(b Decimal64) bool {
	x, y := EqualScaleDecimal64(a, b)
	return x.val == y.val
}

func (a Decimal64) Gt(b Decimal64) bool {
	x, y := EqualScaleDecimal64(a, b)
	return x.val > y.val
}

func (a Decimal64) Gte(b Decimal64) bool {
	x, y := EqualScaleDecimal64(a, b)
	return x.val >= y.val
}

func (a Decimal64) Lt(b Decimal64) bool {
	x, y := EqualScaleDecimal64(a, b)
	return x.val < y.val
}

func (a Decimal64) Lte(b Decimal64) bool {
	x, y := EqualScaleDecimal64(a, b)
	return x.val <= y.val
}

func (a Decimal64) Cmp(b Decimal64) int {
	switch {
	case a.Lt(b):
		return -1
	case a.Eq(b):
		return 0
	default:
		return 1
	}
}

func CompareDecimal64(a, b Decimal64) int {
	return a.Cmp(b)
}
