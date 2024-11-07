// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// https://github.com/chfast/intx
// https://github.com/holiman/uint256

package num

import (
	"encoding/binary"
	"math"
	"math/bits"
	"strconv"
	"strings"

	"blockwatch.cc/knoxdb/pkg/slicex"
)

var (
	ZeroInt256 = Int256{0, 0, 0, 0}
	OneInt256  = Int256{0, 0, 0, 1}
	MaxInt256  = Int256{0x7FFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF}
	MinInt256  = Int256{0x8000000000000000, 0x0, 0x0, 0x0}
)

// Big-Endian format [0] = Hi .. [3] = Lo
type Int256 [4]uint64
type Uint256 [4]uint64

func NewInt256() Int256 {
	return ZeroInt256
}

func Int256FromInt64(in int64) Int256 {
	var z Int256
	z.SetInt64(in)
	return z
}

func Int256From2Int64(in0, in1 int64) Int256 {
	var z Int128
	z[0], z[1] = uint64(in0), uint64(in1)
	return Int256FromInt128(z)
}

func Int256From4Int64(in0, in1, in2, in3 int64) Int256 {
	var z Int256
	z[0], z[1], z[2], z[3] = uint64(in0), uint64(in1), uint64(in2), uint64(in3)
	return z
}

func Int256FromInt128(in Int128) Int256 {
	var z Int256
	z.SetInt128(in)
	return z
}

func Int256FromBytes(in []byte) Int256 {
	_ = in[31] // bounds check hint to compiler; see golang.org/issue/14808
	var x Int256
	x[0] = binary.BigEndian.Uint64(in[0:8])
	x[1] = binary.BigEndian.Uint64(in[8:16])
	x[2] = binary.BigEndian.Uint64(in[16:24])
	x[3] = binary.BigEndian.Uint64(in[24:32])
	return x
}

func (x Int256) Bytes32() [32]byte {
	// The PutUint64()s are inlined and we get 4x (load, bswap, store) instructions.
	var b [32]byte
	binary.BigEndian.PutUint64(b[0:8], x[0])
	binary.BigEndian.PutUint64(b[8:16], x[1])
	binary.BigEndian.PutUint64(b[16:24], x[2])
	binary.BigEndian.PutUint64(b[24:32], x[3])
	return b
}

func (x Int256) Bytes() []byte {
	b32 := x.Bytes32()
	return b32[:]
}

// IsInt64 reports whether x can be represented as a int64.
func (x Int256) IsInt64() bool {
	return (x[0]|x[1]|x[2]|x[3]>>63) == 0 || (^x[0]|^x[1]|^x[2]) == 0
}

func (x Int256) IsInt128() bool {
	return (x[0]|x[1]|x[2]>>63) == 0 || (^x[0]|^x[1]) == 0
}

// IsZero returns true if x == 0
func (x Int256) IsZero() bool {
	return (x[0] | x[1] | x[2] | x[3]) == 0
}

// Sign returns:
//
//	-1 if x <  0
//	 0 if x == 0
//	+1 if x >  0
//
// Where x is interpreted as a two's complement signed number
func (x Int256) Sign() int {
	if x.IsZero() {
		return 0
	}
	if x[0] < 0x8000000000000000 {
		return 1
	}
	return -1
}

func (x Int256) Int64() int64 {
	return int64(x[3])
}

func (x Int256) Int128() Int128 {
	return Int128{x[2], x[3]}
}

func (x Int256) Float64() float64 {
	sign := x[0] & 0x8000000000000000
	if sign > 0 {
		x = x.Neg()
	}
	bl := uint(x.BitLen())
	exp := 1023 + uint64(bl) - 1
	var frac uint64

	if bl <= 53 {
		frac = x[3] << (53 - bl)
	} else {
		frac = x.Rsh(bl - 53)[3] // TODO: optimize
	}

	return math.Float64frombits(sign | exp<<52 | (frac & 0x000fffffffffffff))
}

func (x *Int256) SetInt64(y int64) {
	sign := uint64(y >> 63)
	x[0], x[1], x[2], x[3] = sign, sign, sign, uint64(y)
}

func (x *Int256) SetInt128(y Int128) {
	sign := uint64(int64(y[0]) >> 63)
	x[0], x[1], x[2], x[3] = sign, sign, y[0], y[1]
}

func (x *Int256) SetFloat64(y float64) Accuracy {
	// handle special cases
	switch {
	case y == 0:
		*x = ZeroInt256
		return Exact
	case math.IsNaN(y):
		*x = ZeroInt256
		return Exact
	case math.IsInf(y, 1):
		*x = MaxInt256
		return Above
	case math.IsInf(y, -1):
		*x = MinInt256
		return Below
	case math.Abs(y) < 0:
		*x = ZeroInt256
		return Below
	}

	// we're only interested in the integer part, rounded to nearest even
	y = math.RoundToEven(y)

	// at this point we have
	// - no non-integer numbers
	// - no subnormals
	// - no specials like Inf and NaN
	// - potentially too large integers

	// IEEE 754-1985 double precision floating point format
	//
	// 1 sign bit (sign = -1^sign)
	// 11 exponent bits (2^(exp-1023); 0 = subnormal, 1..2047 = regular, 2048 = inf)
	// 52 fractional
	//
	ybits := math.Float64bits(y)
	sign := 1 - int64((ybits>>63)<<1)
	exp := uint(ybits >> 52 & 0x07ff)
	frac := (ybits & 0x000fffffffffffff) | 0x1<<52 // also add leading 1
	shift := exp - 1023                            // -1023 to normalize

	// since we have no fractional numbers, shift is always >= 0
	// check if we can express the number in 256 bits
	if shift > 255 {
		*x = MaxInt256
		return Above
	}

	var z Int256
	if shift <= 52 {
		z[3] = frac >> (52 - shift)
	} else {
		z[3] = frac
		z = z.Lsh(shift - 52)
	}

	if sign < 0 {
		if z[0] > 1<<63 || (z[0] == 1<<63 && (z[1]|z[2]|z[3]) > 0) {
			*x = MinInt256
			return Below
		}
		*x = z.Neg()
	} else {
		if z[0] > 1<<63 || (z[0] == 1<<63 && (z[1]|z[2]|z[3]) > 0) {
			*x = MaxInt256
			return Above
		}
		// correct saturated MaxInt256
		if z[0] > 1<<63-1 {
			z = z.Sub64(1)
		}
		*x = z
	}

	return Exact
}

func (x Int256) Precision() int {
	switch {
	case x.IsInt64():
		var p int
		for i := x.Int64(); i != 0; i /= 10 {
			p++
		}
		return p
	case x == MinInt256:
		return 77
	default:
		pow := Int256FromInt64(1e18)
		q, r := x.Abs().QuoRem(pow)
		for p := 0; ; p += 18 {
			if q.IsZero() {
				for i := r.Int64(); i != 0; i /= 10 {
					p++
				}
				return p
			}
			q, r = q.QuoRem(pow)
		}
	}
}

// log10(2^128) < 78
const i256str = "000000000000000000000000000000000000000000000000000000000000000000000000000000"

func (x Int256) String() string {
	if x.IsZero() {
		return "0"
	}
	buf := []byte(i256str)
	var b strings.Builder
	b.Grow(80)
	if x.Sign() < 0 {
		b.WriteRune('-')
		x = x.Neg()
	}
	for i := len(buf); ; i -= 19 {
		q, r := x.uQuoRem64(1e19) // largest power of 10 that fits in a uint64
		var n int
		for ; r != 0; r /= 10 {
			n++
			buf[i-n] += byte(r % 10)
		}
		if q.IsZero() {
			b.Write(buf[i-n:])
			return b.String()
		}
		x = q
	}
}

func ParseInt256(s string) (Int256, error) {
	if len(s) == 0 {
		return ZeroInt256, nil
	}
	sign := int64(0)
	switch s[0] {
	case '+':
		s = s[1:]
	case '-':
		sign = -1
		s = s[1:]
	}

	l := len(s)
	switch {
	case l == 0:
		return ZeroInt256, nil
	case l < 19:
		i, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return ZeroInt256, err
		}
		return Int256{
			uint64(sign >> 63),
			uint64(sign >> 63),
			uint64(sign >> 63),
			i ^ uint64(sign) - uint64(sign),
		}, nil
	default:
		var i Int256
		for start, step := 0, (l+17)/18-1; step >= 0; step-- {
			end := l - step*18
			n, err := strconv.ParseUint(s[start:end], 10, 64)
			if err != nil {
				return ZeroInt256, err
			}
			if start == 0 {
				i = Int256FromInt64(int64(n))
			} else {
				i = i.Mul(Int256{0, 0, 0, 1e18}).Add64(n)
			}
			start = end
		}
		if sign < 0 {
			i = i.Neg()
		}
		return i, nil
	}
}

func MustParseInt256(s string) Int256 {
	i, err := ParseInt256(s)
	if err != nil {
		panic(err)
	}
	return i
}

func (x Int256) MarshalText() ([]byte, error) {
	return []byte(x.String()), nil
}

func (x *Int256) UnmarshalText(buf []byte) error {
	z, err := ParseInt256(string(buf))
	if err != nil {
		return err
	}
	*x = z
	return nil
}

// BitLen returns the number of bits required to represent z
func (x Int256) BitLen() int {
	switch {
	case x[0] != 0:
		return 192 + bits.Len64(x[0])
	case x[1] != 0:
		return 128 + bits.Len64(x[1])
	case x[2] != 0:
		return 64 + bits.Len64(x[2])
	default:
		return bits.Len64(x[3])
	}
}

// Abs interprets x as a two's complement signed number,
// and returns its absolute value
//
//	Abs(0)        = 0
//	Abs(1)        = 1
//	Abs(2**255)   = -2**255
//	Abs(2**256-1) = -1
func (x Int256) Abs() Int256 {
	if x[0] < 0x8000000000000000 {
		return x
	}
	return ZeroInt256.Sub(x)
}

// Neg returns -x mod 2**256.
func (x Int256) Neg() Int256 {
	return ZeroInt256.Sub(x)
}

// Not returns ^x.
func (x Int256) Not() Int256 {
	x[0], x[1], x[2], x[3] = ^x[0], ^x[1], ^x[2], ^x[3]
	return x
}

func (x Int256) Or(y Int256) Int256 {
	x[0] = x[0] | y[0]
	x[1] = x[1] | y[1]
	x[2] = x[2] | y[2]
	x[3] = x[3] | y[3]
	return x
}

func (x Int256) And(y Int256) Int256 {
	x[0] = x[0] & y[0]
	x[1] = x[1] & y[1]
	x[2] = x[2] & y[2]
	x[3] = x[3] & y[3]
	return x
}

func (x Int256) Xor(y Int256) Int256 {
	x[0] = x[0] ^ y[0]
	x[1] = x[1] ^ y[1]
	x[2] = x[2] ^ y[2]
	x[3] = x[3] ^ y[3]
	return x
}

// Lsh returns x << n.
func (x Int256) Lsh(n uint) Int256 {
	// n % 64 == 0
	if n&0x3f == 0 {
		switch n {
		case 0:
			return x
		case 64:
			return x.lsh64()
		case 128:
			return x.lsh128()
		case 192:
			return x.lsh192()
		default:
			return ZeroInt256
		}
	}
	var (
		a, b uint64
	)
	// Big swaps first
	switch {
	case n > 192:
		if n > 256 {
			return ZeroInt256
		}
		x = x.lsh192()
		n -= 192
		goto sh192
	case n > 128:
		x = x.lsh128()
		n -= 128
		goto sh128
	case n > 64:
		x = x.lsh64()
		n -= 64
		goto sh64
	}

	// remaining shifts
	a = x[3] >> (64 - n)
	x[3] = x[3] << n

sh64:
	b = x[2] >> (64 - n)
	x[2] = (x[2] << n) | a

sh128:
	a = x[1] >> (64 - n)
	x[1] = (x[1] << n) | b

sh192:
	x[0] = (x[0] << n) | a

	return x
}

// SRsh (Signed/Arithmetic right shift)
// considers z to be a signed integer, during right-shift
// and returns x >> n.
func (x Int256) Rsh(n uint) Int256 {
	if n%64 == 0 {
		switch n {
		case 0:
			return x
		case 64:
			return x.rsh64()
		case 128:
			return x.rsh128()
		case 192:
			return x.rsh192()
		default:
			if x[0]>>63 == 0 {
				return ZeroInt256
			}
			return MinInt256
		}
	}
	var (
		a uint64 = math.MaxUint64 << (64 - n%64)
	)
	// Big swaps first
	switch {
	case n > 192:
		if n > 256 {
			if x[0]>>63 == 0 {
				return ZeroInt256
			}
			return MinInt256
		}
		x = x.rsh192()
		n -= 192
		goto sh192
	case n > 128:
		x = x.rsh128()
		n -= 128
		goto sh128
	case n > 64:
		x = x.rsh64()
		n -= 64
		goto sh64
	}

	// remaining shifts
	x[0], a = (x[0]>>n)|a, x[0]<<(64-n)

sh64:
	x[1], a = (x[1]>>n)|a, x[1]<<(64-n)

sh128:
	x[2], a = (x[2]>>n)|a, x[2]<<(64-n)

sh192:
	x[3] = (x[3] >> n) | a

	return x
}

func (x Int256) lsh64() Int256 {
	x[0], x[1], x[2], x[3] = x[1], x[2], x[3], 0
	return x
}
func (x Int256) lsh128() Int256 {
	x[0], x[1], x[2], x[3] = x[2], x[3], 0, 0
	return x
}
func (x Int256) lsh192() Int256 {
	x[0], x[1], x[2], x[3] = x[3], 0, 0, 0
	return x
}
func (x Int256) rsh64() Int256 {
	sign := uint64(x[0] >> 63)
	x[0], x[1], x[2], x[3] = sign, x[0], x[1], x[2]
	return x
}
func (x Int256) rsh128() Int256 {
	sign := uint64(x[0] >> 63)
	x[0], x[1], x[2], x[3] = sign, sign, x[0], x[1]
	return x
}
func (x Int256) rsh192() Int256 {
	sign := uint64(x[0] >> 63)
	x[0], x[1], x[2], x[3] = sign, sign, sign, x[0]
	return x
}

// Add returns the sum x+y
func (x Int256) Add(y Int256) (z Int256) {
	var carry uint64
	z[3], carry = bits.Add64(x[3], y[3], 0)
	z[2], carry = bits.Add64(x[2], y[2], carry)
	z[1], carry = bits.Add64(x[1], y[1], carry)
	z[0], _ = bits.Add64(x[0], y[0], carry)
	return
}

func (x Int256) Add64(y uint64) (z Int256) {
	var carry uint64
	z[3], carry = bits.Add64(x[3], y, 0)
	z[2], carry = bits.Add64(x[2], 0, carry)
	z[1], carry = bits.Add64(x[1], 0, carry)
	z[0] = x[0] + carry
	return
}

// AddOverflow returns the sum x+y, and returns whether overflow occurred
func (x Int256) AddOverflow(y Int256) (Int256, bool) {
	var (
		carry uint64
		z     Int256
	)
	z[3], carry = bits.Add64(x[3], y[3], 0)
	z[2], carry = bits.Add64(x[2], y[2], carry)
	z[1], carry = bits.Add64(x[1], y[1], carry)
	z[0], carry = bits.Add64(x[0], y[0], carry)
	overflow := carry != 0
	if x.Sign() < 0 {
		overflow = overflow || z[0] > 1<<63 || (z[0] == 1<<63 && (z[1]|z[2]|z[3] > 0))
	} else {
		overflow = overflow || z[0] > 1<<63-1
	}
	return z, overflow
}

// Sub returns the difference x-y
func (x Int256) Sub(y Int256) Int256 {
	var (
		carry uint64
		z     Int256
	)
	z[3], carry = bits.Sub64(x[3], y[3], 0)
	z[2], carry = bits.Sub64(x[2], y[2], carry)
	z[1], carry = bits.Sub64(x[1], y[1], carry)
	z[0], _ = bits.Sub64(x[0], y[0], carry)
	return z
}

// SubOverflow returns the difference x-y and returns true if the operation underflowed
func (x Int256) SubOverflow(y Int256) (Int256, bool) {
	var (
		carry uint64
		z     Int256
	)
	z[3], carry = bits.Sub64(x[3], y[3], 0)
	z[2], carry = bits.Sub64(x[2], y[2], carry)
	z[1], carry = bits.Sub64(x[1], y[1], carry)
	z[0], carry = bits.Sub64(x[0], y[0], carry)
	overflow := carry != 0
	if x.Sign() < 0 {
		overflow = overflow || z[0] > 1<<63 || (z[0] == 1<<63 && (z[1]|z[2]|z[3] > 0))
	} else {
		overflow = overflow || z[0] > 1<<63-1
	}
	return z, overflow
}

// Sub64 returns the difference x - y, where y is a uint64
func (x Int256) Sub64(y uint64) Int256 {
	var carry uint64
	z := x

	if z[3], carry = bits.Sub64(x[3], y, carry); carry == 0 {
		return z
	}
	if z[2], carry = bits.Sub64(x[2], 0, carry); carry == 0 {
		return z
	}
	if z[1], carry = bits.Sub64(x[1], 0, carry); carry == 0 {
		return z
	}
	z[0]--
	return z
}

func (x Int256) Cmp(y Int256) int {
	if x == y {
		return 0
	} else if x.Lt(y) {
		return -1
	} else {
		return 1
	}
}

func Compare256(a, b Int256) int {
	return a.Cmp(b)
}

func (x Int256) Eq(y Int256) bool {
	return x == y
}

func (x Int256) Lt(y Int256) bool {
	xSign := x.Sign()
	ySign := y.Sign()

	switch {
	case xSign >= 0 && ySign < 0:
		return false
	case xSign < 0 && ySign >= 0:
		return true
	default:
		return x.ult(y)
	}
}

// ult returns true if x < y
func (x Int256) ult(y Int256) bool {
	// x < y <=> x - y < 0 i.e. when subtraction overflows.
	_, carry := bits.Sub64(x[3], y[3], 0)
	_, carry = bits.Sub64(x[2], y[2], carry)
	_, carry = bits.Sub64(x[1], y[1], carry)
	_, carry = bits.Sub64(x[0], y[0], carry)
	return carry != 0
}

func (x Int256) Gt(y Int256) bool {
	xSign := x.Sign()
	ySign := y.Sign()

	switch {
	case xSign >= 0 && ySign < 0:
		return true
	case xSign < 0 && ySign >= 0:
		return false
	default:
		return y.ult(x)
	}
}

func (x Int256) Lte(y Int256) bool {
	return x == y || x.Lt(y)
}

func (x Int256) Gte(y Int256) bool {
	return x == y || x.Gt(y)
}

func Min256(x, y Int256) Int256 {
	if x.Lt(y) {
		return x
	}
	return y
}

func Max256(x, y Int256) Int256 {
	if y.Lt(x) {
		return x
	}
	return y
}

func (x Int256) Uint256() Uint256 {
	return Uint256{x[0], x[1], x[2], x[3]}
}

func (x Uint256) Int256() Int256 {
	return Int256{x[0], x[1], x[2], x[3]}
}

func (x Uint256) Gte(y Uint256) bool {
	return !x.Int256().ult(y.Int256())
}

func Int256Intersect(a, b []Int256) []Int256 {
	x := slicex.NewOrderedBytes(nil).SetUnique()
	y := slicex.NewOrderedBytes(nil).SetUnique()
	for _, v := range a {
		x.Insert(v.Bytes())
	}
	for _, v := range b {
		y.Insert(v.Bytes())
	}
	z := x.Intersect(y)
	res := make([]Int256, z.Len())
	for i, v := range z.Values {
		res[i] = Int256FromBytes(v)
	}
	return res
}

func Int256Union(a, b []Int256) []Int256 {
	x := slicex.NewOrderedBytes(nil).SetUnique()
	y := slicex.NewOrderedBytes(nil).SetUnique()
	for _, v := range a {
		x.Insert(v.Bytes())
	}
	for _, v := range b {
		y.Insert(v.Bytes())
	}
	z := x.Union(y)
	res := make([]Int256, z.Len())
	for i, v := range z.Values {
		res[i] = Int256FromBytes(v)
	}
	return res
}

func Int256Difference(a, b []Int256) []Int256 {
	x := slicex.NewOrderedBytes(nil).SetUnique()
	y := slicex.NewOrderedBytes(nil).SetUnique()
	for _, v := range a {
		x.Insert(v.Bytes())
	}
	for _, v := range b {
		y.Insert(v.Bytes())
	}
	z := x.Difference(y)
	res := make([]Int256, z.Len())
	for i, v := range z.Values {
		res[i] = Int256FromBytes(v)
	}
	return res
}
