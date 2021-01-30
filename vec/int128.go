// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// https://github.com/lukechampine/uint128
// https://github.com/holiman/uint256

package vec

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	// "fmt"
)

var ErrInvalidNumber = errors.New("vec: invalid number")

type Accuracy int8

const (
	Below Accuracy = -1
	Exact Accuracy = 0
	Above Accuracy = +1
)

func (a Accuracy) String() string {
	switch a {
	case Below:
		return "below"
	case Above:
		return "above"
	default:
		return "exact"
	}
}

var (
	ZeroInt128 = Int128{0, 0}
	OneInt128  = Int128{0, 1}
	MaxInt128  = Int128{0x7FFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF}
	MinInt128  = Int128{0x8000000000000000, 0x0}
)

// Big-Endian format [0] = Hi, [1] = Lo
type Int128 [2]uint64

func NewInt128() Int128 {
	return ZeroInt128
}

func Int128FromInt64(in int64) Int128 {
	var z Int128
	z.SetInt64(in)
	return z
}

func Int128FromBytes(in []byte) Int128 {
	_ = in[15] // bounds check hint to compiler; see golang.org/issue/14808
	var x Int128
	x[0] = binary.BigEndian.Uint64(in[0:8])
	x[1] = binary.BigEndian.Uint64(in[8:16])
	return x
}

func (x Int128) Bytes16() [16]byte {
	// The PutUint64()s are inlined and we get 4x (load, bswap, store) instructions.
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], x[0])
	binary.BigEndian.PutUint64(b[8:16], x[1])
	return b
}

// IsInt64 reports whether x can be represented as a int64.
func (x Int128) IsInt64() bool {
	return (x[0]|x[1]>>63) == 0 || ^x[0] == 0
}

// IsZero returns true if x == 0
func (x Int128) IsZero() bool {
	return (x[0] | x[1]) == 0
}

// Sign returns:
//	-1 if x <  0
//	 0 if x == 0
//	+1 if x >  0
// Where x is interpreted as a two's complement signed number
func (x Int128) Sign() int {
	if x.IsZero() {
		return 0
	}
	if x[0] < 0x8000000000000000 {
		return 1
	}
	return -1
}

func (x Int128) Int64() int64 {
	return int64(x[1])
}

func (x Int128) Int256() Int256 {
	sign := uint64(int64(x[0]) >> 63)
	return Int256{sign, sign, x[0], x[1]}
}

func (x Int128) Float64() float64 {
	sign := x[0] & 0x8000000000000000
	if sign > 0 {
		x = x.Neg()
	}
	bl := uint(x.BitLen())
	exp := 1023 + uint64(bl) - 1
	var frac uint64

	if bl <= 53 {
		frac = x[1] << (53 - bl)
	} else {
		frac = x.Rsh(bl - 53)[1] // TODO: optimize
	}

	return math.Float64frombits(sign | exp<<52 | (frac & 0x000fffffffffffff))
}

func (x *Int128) SetInt64(y int64) {
	x[0], x[1] = uint64(y>>63), uint64(y)
}

func (x *Int128) SetFloat64(y float64) Accuracy {
	// handle special cases
	switch {
	case y == 0:
		*x = ZeroInt128
		return Exact
	case math.IsNaN(y):
		*x = ZeroInt128
		return Exact
	case math.IsInf(y, 1):
		*x = MaxInt128
		return Above
	case math.IsInf(y, -1):
		*x = MinInt128
		return Below
	case math.Abs(y) < 0:
		*x = ZeroInt128
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
	// check if we can express the number in 128 bits
	if shift > 127 {
		*x = MaxInt128
		return Above
	}

	var z Int128
	if shift <= 52 {
		z[1] = frac >> (52 - shift)
	} else {
		z[1] = frac
		z = z.Lsh(shift - 52)
	}

	if sign < 0 {
		if z[0] > 1<<63 || (z[0] == 1<<63 && z[1] > 0) {
			*x = MinInt128
			return Below
		}
		*x = z.Neg()
	} else {
		if z[0] > 1<<63 || (z[0] == 1<<63 && z[1] > 0) {
			*x = MaxInt128
			return Above
		}
		// correct saturated MaxInt128
		if z[0] > 1<<63-1 {
			z[0]--
			z[1]--
		}
		*x = z
	}

	return Exact
}

func (x Int128) Precision() int {
	switch {
	case x.IsInt64():
		var p int
		for i := x.Int64(); i != 0; i /= 10 {
			p++
		}
		return p
	case x == MinInt128:
		return 39
	default:
		pow := Int128{0, 1e18}
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
		return 0
	}
}

// log10(2^64) < 40
const i128str = "0000000000000000000000000000000000000000"

func (x Int128) String() string {
	if x.IsZero() {
		return "0"
	}
	buf := []byte(i128str)
	var b strings.Builder
	b.Grow(40)
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

func ParseInt128(s string) (Int128, error) {
	if len(s) == 0 {
		return ZeroInt128, ErrInvalidNumber
	}
	sign := int64(0)
	var i int
	switch s[0] {
	case '+':
		i++
	case '-':
		sign = -1
		i++
	}

	l := len(s) - i
	switch {
	case l == 0:
		return ZeroInt128, ErrInvalidNumber
	case l < 19:
		n, err := strconv.ParseUint(s[i:], 10, 64)
		if err != nil {
			return ZeroInt128, err
		}
		return Int128{uint64(sign >> 63), n ^ uint64(sign) - uint64(sign)}, nil
	default:
		var r Int128
		for start, step := i, (l+17)/18-1; step >= 0; step-- {
			end := l - step*18
			n, err := strconv.ParseUint(s[start:end], 10, 64)
			if err != nil {
				return ZeroInt128, err
			}
			if start == 0 {
				r[1] = n
			} else {
				r = r.Mul64(1e18).Add64(n)
			}
			start = end
		}
		if sign < 0 {
			r = r.Neg()
		}
		return r, nil
	}
}

func MustParseInt128(s string) Int128 {
	i, err := ParseInt128(s)
	if err != nil {
		panic(err)
	}
	return i
}

func (x Int128) MarshalText() ([]byte, error) {
	return []byte(x.String()), nil
}

func (x *Int128) UnmarshalText(buf []byte) error {
	z, err := ParseInt128(string(buf))
	if err != nil {
		return err
	}
	*x = z
	return nil
}

// BitLen returns the number of bits required to represent x
func (x Int128) BitLen() int {
	switch {
	case x[0] != 0:
		return 64 + bits.Len64(x[0])
	default:
		return bits.Len64(x[1])
	}
}

// Abs interprets x as a two's complement signed number,
// and returns its absolute value
//   Abs(0)        = 0
//   Abs(1)        = 1
//   Abs(2**127)   = -2**127
//   Abs(2**128-1) = -1
func (x Int128) Abs() Int128 {
	if x[0] < 0x8000000000000000 {
		return x
	}
	return ZeroInt128.Sub(x)
}

// Neg returns -x mod 2**128.
func (x Int128) Neg() Int128 {
	return ZeroInt128.Sub(x)
}

// Not sets z = ^x and returns z.
func (x Int128) Not() Int128 {
	x[0], x[1] = ^x[0], ^x[1]
	return x
}

func (x Int128) Or(y Int128) Int128 {
	x[0] = x[0] | y[0]
	x[1] = x[1] | y[1]
	return x
}

func (x Int128) And(y Int128) Int128 {
	x[0] = x[0] & y[0]
	x[1] = x[1] & y[1]
	return x
}

func (x Int128) Xor(y Int128) Int128 {
	x[0] = x[0] ^ y[0]
	x[1] = x[1] ^ y[1]
	return x
}

// Lsh returns u<<n.
func (x Int128) Lsh(n uint) Int128 {
	if n > 64 {
		x[0] = x[1] << (n - 64)
		x[1] = 0
	} else {
		x[0] = x[0]<<n | x[1]>>(64-n)
		x[1] = x[1] << n
	}
	return x
}

// Rsh returns u>>n.
func (x Int128) Rsh(n uint) Int128 {
	if n > 64 {
		x[1] = x[0] >> (n - 64)
		x[0] = 0
	} else {
		x[1] = x[1]>>n | x[0]<<(64-n)
		x[0] = x[0] >> n
	}
	return x
}

// Add returns the sum x+y
func (x Int128) Add(y Int128) Int128 {
	var (
		carry uint64
		z     Int128
	)
	z[1], carry = bits.Add64(x[1], y[1], 0)
	z[0], _ = bits.Add64(x[0], y[0], carry)
	return z
}

// AddOverflow returns the sum x+y, and returns whether overflow occurred
func (x Int128) AddOverflow(y Int128) (Int128, bool) {
	var (
		carry uint64
		z     Int128
	)
	sign := x.Sign()
	z[1], carry = bits.Add64(x[1], y[1], 0)
	z[0], carry = bits.Add64(x[0], y[0], carry)
	overflow := carry != 0
	if sign < 0 {
		overflow = overflow || z[0] > 1<<63 || (z[0] == 1<<63 && z[1] > 0)
	} else {
		overflow = overflow || z[0] > 1<<63-1
	}
	return z, overflow
}

func (x Int128) Add64(y uint64) (z Int128) {
	var carry uint64
	z[1], carry = bits.Add64(x[1], y, 0)
	z[0] = x[0] + carry
	return
}

func (x Int128) Add64Overflow(y uint64) (Int128, bool) {
	var (
		carry uint64
		z     Int128
	)
	z[1], carry = bits.Add64(x[1], y, 0)
	z[0], carry = bits.Add64(x[0], 0, carry)
	overflow := carry != 0
	if x.Sign() < 0 {
		overflow = overflow || z[0] > 1<<63 || (z[0] == 1<<63 && z[1] > 0)
	} else {
		overflow = overflow || z[0] > 1<<63-1
	}
	return z, overflow
}

// Sub returns the difference x-y
func (x Int128) Sub(y Int128) Int128 {
	var (
		carry uint64
		z     Int128
	)
	z[1], carry = bits.Sub64(x[1], y[1], 0)
	z[0], _ = bits.Sub64(x[0], y[0], carry)
	return z
}

// SubOverflow returns the difference x-y and returns true if the operation underflowed
func (x Int128) SubOverflow(y Int128) (Int128, bool) {
	var (
		carry uint64
		z     Int128
	)
	z[1], carry = bits.Sub64(x[1], y[1], 0)
	z[0], carry = bits.Sub64(x[0], y[0], carry)
	overflow := carry != 0
	if x.Sign() < 0 {
		overflow = overflow || z[0] > 1<<63 || (z[0] == 1<<63 && z[1] > 0)
	} else {
		overflow = overflow || z[0] > 1<<63-1
	}
	return z, overflow
}

// Sub64 returns the difference x - y, where y is a uint64
func (x Int128) Sub64(y uint64) Int128 {
	var carry uint64
	z := x

	if z[1], carry = bits.Sub64(x[1], y, carry); carry == 0 {
		return z
	}
	z[0]--
	return z
}

func (x Int128) Cmp(y Int128) int {
	if x == y {
		return 0
	} else if x.Lt(y) {
		return -1
	} else {
		return 1
	}
}

func (x Int128) Eq(y Int128) bool {
	return x == y
}

func (x Int128) Lt(y Int128) bool {
	xSign := x.Sign()
	ySign := y.Sign()

	switch {
	case xSign >= 0 && ySign < 0:
		return false
	case xSign < 0 && ySign >= 0:
		return true
	default:
		return x[0] < y[0] || x[0] == y[0] && x[1] < y[1]
		// _, carry := bits.Sub64(x[1], y[1], 0)
		// _, carry = bits.Sub64(x[0], y[0], carry)
		// return carry != 0
	}
}

func (x Int128) Gt(y Int128) bool {
	xSign := x.Sign()
	ySign := y.Sign()

	switch {
	case xSign >= 0 && ySign < 0:
		return true
	case xSign < 0 && ySign >= 0:
		return false
	default:
		return x[0] > y[0] || x[0] == y[0] && x[1] > y[1]
	}
}

func (x Int128) Lte(y Int128) bool {
	return x == y || x.Lt(y)
}

func (x Int128) Gte(y Int128) bool {
	return x == y || x.Gt(y)
}

func Min128(x, y Int128) Int128 {
	if x.Lt(y) {
		return x
	}
	return y
}

func Max128(x, y Int128) Int128 {
	if y.Lt(x) {
		return x
	}
	return y
}

// Match helpers
func MatchInt128Equal(src []Int128, val Int128, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt128Equal(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchInt128NotEqual(src []Int128, val Int128, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt128NotEqual(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchInt128LessThan(src []Int128, val Int128, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt128LessThan(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchInt128LessThanEqual(src []Int128, val Int128, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt128LessThanEqual(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchInt128GreaterThan(src []Int128, val Int128, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt128GreaterThan(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchInt128GreaterThanEqual(src []Int128, val Int128, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt128GreaterThanEqual(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchInt128Between(src []Int128, a, b Int128, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt128Between(src, a, b, bits.Bytes(), mask.Bytes()))
	return bits
}

type Int128Slice []Int128

func (s *Int128Slice) Unique() {
	*s = UniqueInt128Slice(*s)
}

func (s *Int128Slice) AddUnique(val Int128) bool {
	idx := s.Index(val, 0)
	if idx > -1 {
		return false
	}
	*s = append(*s, val)
	s.Sort()
	return true
}

func (s *Int128Slice) Remove(val Int128) bool {
	idx := s.Index(val, 0)
	if idx < 0 {
		return false
	}
	*s = append((*s)[:idx], (*s)[idx+1:]...)
	return true
}

func (s *Int128Slice) RemoveZeros() int {
	var n int
	for i, v := range *s {
		if v.IsZero() {
			continue
		}
		(*s)[n] = (*s)[i]
		n++
	}
	(*s) = (*s)[:n]
	return n
}

func (s Int128Slice) Contains(val Int128) bool {
	// empty s cannot contain values
	if len(s) == 0 {
		return false
	}

	// s is sorted, check against first (min) and last (max) entries
	if s[0].Gt(val) || s[len(s)-1].Lt(val) {
		return false
	}

	// for dense slices (continuous, no dups) compute offset directly
	if ofs := int(val.Sub(s[0]).Int64()); ofs >= 0 && ofs < len(s) && s[ofs] == val {
		return true
	}

	// use binary search to find value in sorted s
	i := sort.Search(len(s), func(i int) bool { return s[i].Gte(val) })
	if i < len(s) && s[i] == val {
		return true
	}

	return false
}

func (s Int128Slice) Index(val Int128, last int) int {
	if len(s) <= last {
		return -1
	}

	// search for value in slice starting at last index
	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if val.Lt(min) || val.Gt(max) {
		return -1
	}

	// for dense slices (values are continuous) compute offset directly
	if l == int(max.Sub(min).Int64())+1 {
		return int(val.Sub(min).Int64()) + last
	}

	// for sparse slices, use binary search (slice is sorted)
	idx := sort.Search(l, func(i int) bool { return slice[i].Gte(val) })
	if idx < l && slice[idx] == val {
		return idx + last
	}
	return -1
}

func (s Int128Slice) MinMax() (Int128, Int128) {
	var min, max Int128

	switch l := len(s); l {
	case 0:
		// nothing
	case 1:
		min, max = s[0], s[0]
	default:
		// If there is more than one element, then initialize min and max
		if s[0].Lt(s[1]) {
			max = s[0]
			min = s[1]
		} else {
			max = s[1]
			min = s[0]
		}

		for i := 2; i < l; i++ {
			if s[i].Gt(max) {
				max = s[i]
			} else if s[i].Lt(min) {
				min = s[i]
			}
		}
	}

	return min, max
}

// ContainsRange returns true when slice s contains any values between
// from and to. Note that from/to do not necessarily have to be members
// themselves, but some intermediate values are. Slice s is expected
// to be sorted and from must be less than or equal to to.
func (s Int128Slice) ContainsRange(from, to Int128) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	// Case A
	if to.Lt(s[0]) {
		return false
	}
	// shortcut for B.1
	if to == s[0] {
		return true
	}
	// Case E
	if from.Gt(s[n-1]) {
		return false
	}
	// shortcut for D.3
	if from == s[n-1] {
		return true
	}
	// Case B-D
	// search if lower interval bound is within slice
	min := sort.Search(n, func(i int) bool {
		return s[i].Gte(from)
	})
	// exit when from was found (no need to check if min < n)
	if s[min] == from {
		return true
	}
	// continue search for upper interval bound in the remainder of the slice
	max := sort.Search(n-min, func(i int) bool {
		return s[i+min].Gte(to)
	})
	max = max + min

	// exit when to was found (also solves case C1a)
	if max < n && s[max] == to {
		return true
	}

	// range is contained iff min < max; note that from/to do not necessarily
	// have to be members, but some intermediate values are
	return min < max
}

func (s Int128Slice) Intersect(x, out Int128Slice) Int128Slice {
	if out == nil {
		out = make(Int128Slice, 0, min(len(x), len(s)))
	}
	count := 0
	for i, j, il, jl := 0, 0, len(x), len(s); i < il && j < jl; {
		if x[i].Lt(s[j]) {
			i++
			continue
		}
		if x[i].Gt(s[j]) {
			j++
			continue
		}
		if count > 0 {
			// skip duplicates
			last := out[count-1]
			if last == x[i] {
				i++
				continue
			}
			if last == s[j] {
				j++
				continue
			}
		}
		if i == il || j == jl {
			break
		}
		if x[i] == s[j] {
			out = append(out, x[i])
			count++
			i++
			j++
		}
	}
	return out

}
func (s Int128Slice) MatchEqual(val Int128, bits, mask *BitSet) *BitSet {
	return MatchInt128Equal(s, val, bits, nil)
}
