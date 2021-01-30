// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package decimal

import (
	"fmt"
	"math"
	"strings"

	. "blockwatch.cc/knoxdb/vec"
)

var Decimal256Zero = Decimal256{ZeroInt256, 0}

// 76 digits
type Decimal256 struct {
	val   Int256
	scale int
}

type Decimal256Slice struct {
	Int256 []Int256
	Scale  int
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
		return false, ErrScaleUnderflow
	}
	if d.scale > MaxDecimal256Precision {
		return false, ErrScaleOverflow
	}
	return true, nil
}

func (d Decimal256) Scale() int {
	return d.scale
}

func (d Decimal256) Precision() int {
	switch {
	case d.val.IsInt64():
		val := abs(d.val.Int64())
		for i := range pow10 {
			if val >= pow10[i] {
				continue
			}
			return i
		}
	case d.val == MinInt256:
		return 77
	default:
		pow := Int256{0, 0, 0, 1e18}
		q, r := d.val.Abs().QuoRem(pow)
		// fmt.Printf("PREC val %s (%x %x %x %x) abs %s (%x %x %x %x)\n",
		// 	d.val, d.val[0], d.val[1], d.val[2], d.val[3],
		// 	d.val.Abs(), d.val.Abs()[0], d.val.Abs()[1], d.val.Abs()[2], d.val.Abs()[3],
		// )
		for p := 0; ; p += 18 {
			// fmt.Printf("PREC noint %s %% %s\n", q, r)
			if q.IsZero() {
				v := abs(r.Int64())
				for i := range pow10 {
					if v >= pow10[i] {
						continue
					}
					return p + i
				}
			}
			q, r = q.QuoRem(pow)
		}
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
	if scale < 0 {
		scale = 0
	}
	if d.IsZero() {
		return Decimal256{ZeroInt256, scale}
	}
	diff := d.scale - scale
	l := len(pow10) - 2
	// fmt.Printf("D256 quantize %d->%d\n", d.scale, scale)
	if diff < 0 {
		val := d.val
		for i := -diff / l; i > 0; i-- {
			// fmt.Printf("> mul by %d\n", pow10[l])
			val = val.Mul(Int256{0, 0, 0, pow10[l]})
			diff += l
		}
		// fmt.Printf("> mul by %d (%s)\n", pow10[-diff], val)
		val = val.Mul(Int256{0, 0, 0, pow10[-diff]})
		// fmt.Printf("> res = %s\n", val)
		d.val = val
		d.scale = scale
	} else {
		sign := d.val.Sign()
		y := Int256{0, 0, 0, pow10[diff%l]}
		for i := diff / l; i > 0; i-- {
			y = y.Mul(Int256{0, 0, 0, pow10[l]})
		}
		// fmt.Printf("> div %s by %s\n", d.val, y)
		// IEEE 754-2008 roundTiesToEven
		quo, rem := d.val.QuoRem(y)
		mid := y.Div(Int256{0, 0, 0, 2}).Abs()
		rem = rem.Abs()
		// fmt.Printf("> quo = %s rem=%s %08x %08x %08x %08x mid=%s %08x %08x %08x %08x \n",
		// 	quo,
		// 	rem, rem[0], rem[1], rem[2], rem[3],
		// 	mid, mid[0], mid[1], mid[2], mid[3],
		// )
		if rem.Gt(mid) || rem.Eq(mid) && quo[3]%2 == 1 {
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
		return ErrScaleUnderflow
	}
	if scale > MaxDecimal256Precision {
		return ErrScaleOverflow
	}
	d.scale = scale
	d.val.SetInt64(value)
	return nil
}

func (d *Decimal256) SetInt128(value Int128, scale int) error {
	if scale < 0 {
		return ErrScaleUnderflow
	}
	if scale > MaxDecimal256Precision {
		return ErrScaleOverflow
	}
	d.scale = scale
	d.val = value.Int256()
	return nil
}

func (d *Decimal256) SetInt256(value Int256, scale int) error {
	if scale < 0 {
		return ErrScaleUnderflow
	}
	if scale > MaxDecimal256Precision {
		return ErrScaleOverflow
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
		return ErrScaleUnderflow
	}
	if scale > MaxDecimal256Precision {
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
		for i := scale / l; i > 0; i-- {
			value *= float64(pow10[l])
		}
		value *= float64(pow10[scale%l])
	}
	var i256 Int256
	acc := i256.SetFloat64(value)
	switch acc {
	case Below:
		return ErrPrecisionUnderflow
	case Above:
		return ErrPrecisionOverflow
	}
	d.val = i256
	d.scale = scale
	return nil
}

func (d Decimal256) String() string {
	i := d.val.String()
	switch d.scale {
	case 0:
		return i
	default:
		var b strings.Builder
		b.Grow(MaxDecimal256Precision + 2)
		sign := 0
		if i[0] == '-' {
			b.WriteRune('-')
			sign = 1
		}
		diff := d.scale - len(i) + sign
		if diff >= 0 {
			// 0.00001 (scale=5)
			// add leading zeros
			b.WriteString("0.")
			b.WriteString(zeros[:diff])
			b.WriteString(i[sign:])
		} else {
			// 1234.56789 (scale=5)
			b.WriteString(i[sign : len(i)-d.scale])
			b.WriteRune('.')
			b.WriteString(i[len(i)-d.scale:])
		}
		return b.String()
	}
}

func (d Decimal256) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *Decimal256) UnmarshalText(buf []byte) error {
	if len(buf) == 0 {
		return fmt.Errorf("decimal: empty string")
	}

	scale := len(buf)
	var (
		i, dpos, ncount   int
		sawdot, sawdigits bool
		val               Int256
		ten               = Int256{0, 0, 0, 10}
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
			// value is accumulated as positive int256
			// since val is +int256, MinInt256 would overflow
			val = val.Mul(ten)
			val = val.Add64(uint64(c - '0'))
			if sign < 0 {
				// check for negative overflow
				if val[0] > 1<<63 || (val[0] == 1<<63 && (val[1]|val[2]|val[3]) > 0) {
					return ErrPrecisionUnderflow
				}
			} else {
				// check for positive overflow
				if val[0] > 1<<63-1 {
					return ErrPrecisionOverflow
				}
			}
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
	if scale > MaxDecimal256Precision {
		return ErrScaleOverflow
	}

	if sign < 0 {
		val = val.Neg()
	}

	d.scale = scale
	d.val = val
	return nil
}

func ParseDecimal256(s string) (Decimal256, error) {
	var dec Decimal256
	err := dec.UnmarshalText([]byte(s))
	return dec, err
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

func (a Decimal256) Eq(b Decimal256) bool {
	x, y := EqualScaleDecimal256(a, b)
	return x.val.Eq(y.val)
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

func (a Decimal256) Cmp(b Decimal256) int {
	x, y := EqualScaleDecimal256(a, b)
	return x.val.Cmp(y.val)
}
