// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"bytes"
	"math"
	"math/big"
	"math/bits"
	"strings"
)

// A variable length sequence of bytes representing an unsigned Big integer
// number with unlimited precision. Negative bigints are unsupported for
// binary marshaling to enable native comparisons operators on encoded
// binary data (data is stored as big-endian variable length byte slice).
type Big big.Int

var BigZero = NewBig(0)

func NewBig(i int64) Big {
	var z Big
	z.SetInt64(i)
	return z
}

func NewFromBigInt(b *big.Int) Big {
	var z Big
	z.SetBig(b)
	return z
}

func NewBigFromBytes(buf []byte) Big {
	var z Big
	(*big.Int)(&z).SetBytes(buf)
	return z
}

func (z Big) Big() *big.Int {
	return (*big.Int)(&z)
}

func (z Big) AsInt128() Int128 {
	var i128 Int128
	words := (*big.Int)(&z).Bits()
	if bits.UintSize == 64 {
		switch len(words) {
		case 0:
			// zero
		case 1:
			i128[1] = 0
			i128[1] = uint64(words[0])
		default:
			i128[0] = uint64(words[1])
			i128[1] = uint64(words[0])
		}
	} else {
		switch len(words) {
		case 0:
			// zero
		case 1:
			i128[1] = uint64(words[0])
		case 2:
			i128[1] = uint64(words[1])<<32 | uint64(words[0])
		case 3:
			i128[0] = uint64(words[2])
			i128[1] = uint64(words[1])<<32 | uint64(words[0])
		default:
			i128[0] = uint64(words[3])<<32 | uint64(words[2])
			i128[1] = uint64(words[1])<<32 | uint64(words[0])
		}
	}
	return i128
}

func (z Big) AsInt256() Int256 {
	var i256 Int256
	words := (*big.Int)(&z).Bits()
	if bits.UintSize == 64 {
		len := len(words)
		if len > 0 {
			i256[3] = uint64(words[0])
		}
		if len > 1 {
			i256[2] = uint64(words[1])
		}
		if len > 2 {
			i256[1] = uint64(words[2])
		}
		if len > 3 {
			i256[0] = uint64(words[3])
		}
	} else {
		len := len(words)
		if len > 0 {
			i256[3] = uint64(words[0])
		}
		if len > 1 {
			i256[3] |= uint64(words[1]) << 32
		}
		if len > 2 {
			i256[2] = uint64(words[2])
		}
		if len > 3 {
			i256[2] |= uint64(words[3]) << 32
		}
		if len > 4 {
			i256[1] = uint64(words[4])
		}
		if len > 5 {
			i256[1] |= uint64(words[5]) << 32
		}
		if len > 6 {
			i256[0] = uint64(words[6])
		}
		if len > 7 {
			i256[0] |= uint64(words[7]) << 32
		}
	}
	return i256
}

func (z Big) Equal(x Big) bool {
	return z.Big().Cmp(x.Big()) == 0
}

func (z Big) IsBigZero() bool {
	return len((*big.Int)(&z).Bits()) == 0
}

func (z Big) Cmp(b Big) int {
	return (*big.Int)(&z).Cmp((*big.Int)(&b))
}

func BigCmp(a, b Big) int {
	return a.Cmp(b)
}

func (z Big) IsLess(b Big) bool {
	return z.Cmp(b) < 0
}

func (z Big) IsLessEqual(b Big) bool {
	return z.Cmp(b) <= 0
}

func (z Big) Int64() int64 {
	return (*big.Int)(&z).Int64()
}

func (z *Big) SetBig(b *big.Int) *Big {
	(*big.Int)(z).Set(b)
	return z
}

func (z *Big) SetInt64(i int64) *Big {
	(*big.Int)(z).SetInt64(i)
	return z
}

func (z Big) Clone() Big {
	var x Big
	x.SetBig(z.Big())
	return x
}

func (z *Big) UnmarshalBinary(buf []byte) error {
	(*big.Int)(z).SetBytes(buf)
	return nil
}

func (z *Big) DecodeBuffer(buf *bytes.Buffer) error {
	(*big.Int)(z).SetBytes(buf.Bytes())
	return nil
}

func (z Big) Bytes() []byte {
	return (*big.Int)(&z).Bytes()
}

func (z Big) MarshalBinary() ([]byte, error) {
	return z.Bytes(), nil
}

func (z Big) EncodeBuffer(buf *bytes.Buffer) error {
	buf.Write(z.Bytes())
	return nil
}

func ParseBig(s string) (Big, error) {
	var z Big
	err := (*big.Int)(&z).UnmarshalText([]byte(s))
	return z, err
}

func MustParseBig(s string) Big {
	z, err := ParseBig(s)
	if err != nil {
		panic(err)
	}
	return z
}

// Set implements the flags.Value interface for use in command line argument parsing.
func (z *Big) Set(val string) (err error) {
	*z, err = ParseBig(val)
	return
}

func (z Big) MarshalText() ([]byte, error) {
	return (*big.Int)(&z).MarshalText()
}

func (z *Big) UnmarshalText(d []byte) error {
	return (*big.Int)(z).UnmarshalText(d)
}

func (z Big) String() string {
	return (*big.Int)(&z).Text(10)
}

func (z Big) Decimals(d int) string {
	s := z.String()
	if d <= 0 {
		return s
	}
	var sig string
	if z.IsNeg() {
		sig = "-"
		s = s[1:]
	}
	l := len(s)
	if l <= d {
		s = strings.Repeat("0", d-l+1) + s
	}
	l = len(s)
	return sig + s[:l-d] + "." + s[l-d:]
}

func (z Big) IsNeg() bool {
	return z.Big().Sign() < 0
}

func (z Big) Neg() Big {
	var n Big
	n.SetBig(new(big.Int).Neg(z.Big()))
	return n
}

func (z Big) Add(y Big) Big {
	var x Big
	x.SetBig(new(big.Int).Add(z.Big(), y.Big()))
	return x
}

func (z Big) Sub(y Big) Big {
	var x Big
	x.SetBig(new(big.Int).Sub(z.Big(), y.Big()))
	return x
}

func (z Big) Mul(y Big) Big {
	var x Big
	x.SetBig(new(big.Int).Mul(z.Big(), y.Big()))
	return x
}

func (z Big) Div(y Big) Big {
	var x Big
	if !y.IsBigZero() {
		x.SetBig(new(big.Int).Div(z.Big(), y.Big()))
	}
	return x
}

func (z Big) CeilDiv(y Big) Big {
	var x Big
	if !y.IsBigZero() {
		d, m := new(big.Int).DivMod(z.Big(), y.Big(), new(big.Int))
		x.SetBig(d)
		x = x.Add64(int64(m.Cmp(BigZero.Big())))
	}
	return x
}

func (z Big) Add64(y int64) Big {
	var x Big
	x.SetBig(new(big.Int).Add(z.Big(), big.NewInt(y)))
	return x
}

func (z Big) Sub64(y int64) Big {
	var x Big
	x.SetBig(new(big.Int).Sub(z.Big(), big.NewInt(y)))
	return x
}

func (z Big) Mul64(y int64) Big {
	var x Big
	x.SetBig(new(big.Int).Mul(z.Big(), big.NewInt(y)))
	return x
}

func (z Big) Div64(y int64) Big {
	var x Big
	if y != 0 {
		x.SetBig(new(big.Int).Div(z.Big(), big.NewInt(y)))
	}
	return x
}

func (z Big) AddU64(y uint64) Big {
	var x Big
	x.SetBig(new(big.Int).Add(z.Big(), big.NewInt(0).SetUint64(y)))
	return x
}

func (z Big) SubU64(y uint64) Big {
	var x Big
	x.SetBig(new(big.Int).Sub(z.Big(), big.NewInt(0).SetUint64(y)))
	return x
}

func (z Big) MulU64(y uint64) Big {
	var x Big
	x.SetBig(new(big.Int).Mul(z.Big(), big.NewInt(0).SetUint64(y)))
	return x
}

func (z Big) DivU64(y uint64) Big {
	var x Big
	if y != 0 {
		x.SetBig(new(big.Int).Div(z.Big(), big.NewInt(0).SetUint64(y)))
	}
	return x
}

func (z Big) Scale(n int) Big {
	var x Big
	if n == 0 {
		x.SetBig(z.Big())
	} else {
		if n < 0 {
			factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-n)), nil)
			x.SetBig(factor.Div(z.Big(), factor))
		} else {
			factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
			x.SetBig(factor.Mul(z.Big(), factor))
		}
	}
	return x
}

func (z Big) CeilScale(n int) Big {
	var x Big
	if n == 0 {
		x.SetBig(z.Big())
	} else {
		if n < 0 {
			factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-n)), nil)
			f, m := factor.DivMod(z.Big(), factor, new(big.Int))
			x.SetBig(f)
			x = x.Add64(int64(m.Cmp(BigZero.Big())))
		} else {
			factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
			x.SetBig(factor.Mul(z.Big(), factor))
		}
	}
	return x
}

func (z Big) Float64(dec int) float64 {
	f64, _ := new(big.Float).SetInt(z.Big()).Float64()
	switch {
	case dec == 0:
		return f64
	case dec < 0:
		factor := math.Pow10(-dec)
		return f64 / factor
	default:
		factor := math.Pow10(dec)
		return f64 * factor
	}
}

func (z Big) Lsh(n uint) Big {
	return NewFromBigInt(new(big.Int).Lsh(z.Big(), n))
}

func (z Big) Rsh(n uint) Big {
	return NewFromBigInt(new(big.Int).Rsh(z.Big(), n))
}

func MaxBig(args ...Big) Big {
	var m Big
	for _, z := range args {
		if m.Cmp(z) < 0 {
			m = z
		}
	}
	return m
}

func MinBig(args ...Big) Big {
	switch len(args) {
	case 0:
		return Big{}
	case 1:
		return args[0]
	default:
		m := args[0]
		for _, z := range args[1:] {
			if m.Cmp(z) > 0 {
				m = z
			}
		}
		return m
	}
}
