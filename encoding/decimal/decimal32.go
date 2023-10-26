// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package decimal

import (
	"fmt"
	"math"
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
		return false, ErrScaleUnderflow
	}
	if d.scale > MaxDecimal32Precision {
		return false, ErrScaleOverflow
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
	if scale < 0 {
		scale = 0
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
	return Int128{uint64(d.val >> 31), uint64(d.val)}
}

func (d Decimal32) Int256() Int256 {
	return Int256{uint64(d.val >> 31), uint64(d.val >> 31), uint64(d.val >> 31), uint64(d.val)}
}

func (d *Decimal32) Set(value int32) {
	d.val = value
}

func (d *Decimal32) SetScale(scale int) {
	d.scale = scale
}

func (d *Decimal32) SetInt64(value int64, scale int) error {
	if scale < 0 {
		return ErrScaleUnderflow
	}
	if scale > MaxDecimal32Precision {
		return ErrScaleOverflow
	}
	d.scale = scale
	d.val = int32(value)
	return nil
}

func (d Decimal32) Add64(value int64) Decimal32 {
	d.val += int32(value)
	return d
}

func (d Decimal32) Add(value Decimal32) Decimal32 {
	if d.scale != value.scale {
		value = value.Quantize(d.scale)
	}
	d.val += value.val
	return d
}

func (d Decimal32) RoundToInt64() int64 {
	return d.Quantize(0).Int64()
}

func (d Decimal32) Float64() float64 {
	return float64(d.val) / float64(pow10[d.scale])
}

func (d *Decimal32) SetFloat64(value float64, scale int) error {
	if scale < 0 {
		return ErrScaleUnderflow
	}
	if scale > MaxDecimal32Precision {
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
	}
	f := value * float64(pow10[scale])
	i := int64(f)
	// IEEE 754-2008 roundTiesToEven
	rem := (f - float64(i)) * float64(sign)
	if rem > 0.5 || rem == 0.5 && i*sign%2 == 1 {
		i += sign
	}

	// check against min/max
	if sign > 0 {
		if i > 1<<31-1 {
			return ErrPrecisionOverflow
		}
	} else {
		if i < -1<<31 {
			return ErrPrecisionUnderflow
		}
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
		var b strings.Builder
		b.Grow(MaxDecimal32Precision + 2)
		if d.val>>31 != 0 {
			b.WriteRune('-')
		}
		i := strconv.FormatUint(abs(int64(d.val)), 10)
		diff := d.scale - len(i)
		if diff >= 0 {
			// 0.00001 (scale=5)
			// add leading zeros
			b.WriteString("0.")
			b.WriteString(zeros[:diff])
			b.WriteString(i)
		} else {
			// 1234.56789 (scale=5)
			b.WriteString(i[:len(i)-d.scale])
			b.WriteRune('.')
			b.WriteString(i[len(i)-d.scale:])
		}
		return b.String()
	}
}

func (d Decimal32) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *Decimal32) UnmarshalText(buf []byte) error {
	if len(buf) == 0 {
		return fmt.Errorf("decimal: empty string")
	}

	scale := len(buf)
	var (
		i, dpos, ncount   int
		sawdot, sawdigits bool
		val               uint64
	)

	// handle sign
	sign := int32(1)
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
		switch c := buf[i]; true {
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
		scale -= dpos
	} else {
		scale = 0
	}

	// check limits
	if scale > MaxDecimal32Precision {
		return ErrScaleOverflow
	}
	if sign > 0 {
		if val > 1<<31-1 {
			return ErrPrecisionOverflow
		}
	} else {
		if val > 1<<31 {
			return ErrPrecisionUnderflow
		}
	}

	d.scale = scale
	d.val = int32(val) * sign
	return nil
}

func ParseDecimal32(s string) (Decimal32, error) {
	var dec Decimal32
	err := dec.UnmarshalText([]byte(s))
	return dec, err
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

func (a Decimal32) Eq(b Decimal32) bool {
	x, y := EqualScaleDecimal32(a, b)
	return x.val == y.val
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
