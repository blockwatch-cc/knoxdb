// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bignum

import (
	"math/bits"
)

// Mul returns x*y with wraparound semantics,
func (x Int128) Mul(y Int128) (z Int128) {
	xSign := x.Sign()
	ySign := y.Sign()
	if xSign < 0 {
		x = x.Neg()
	}
	if ySign < 0 {
		y = y.Neg()
	}
	z[0], z[1] = bits.Mul64(x[1], y[1])
	_, p1 := bits.Mul64(x[0], y[1])
	_, p3 := bits.Mul64(x[1], y[0])
	z[0], _ = bits.Add64(z[0], p1, 0)
	z[0], _ = bits.Add64(z[0], p3, 0)
	if xSign != ySign {
		z = z.Neg()
	}
	return
}

func (x Int128) MulOverflow(y Int128) (Int128, bool) {
	xSign := x.Sign()
	ySign := y.Sign()
	if xSign < 0 {
		x = x.Neg()
	}
	if ySign < 0 {
		y = y.Neg()
	}
	var (
		z      Int128
		c1, c2 uint64
	)
	z[0], z[1] = bits.Mul64(x[1], y[1])
	o1, p1 := bits.Mul64(x[0], y[1])
	o2, p3 := bits.Mul64(x[1], y[0])
	z[0], _ = bits.Add64(z[0], p1, 0)
	z[0], _ = bits.Add64(z[0], p3, 0)
	if xSign != ySign {
		z = z.Neg()
	}
	return z, o1 != 0 || o2 != 0 || c1 != 0 || c2 != 0
}

// Mul64 returns x*y.
func (x Int128) Mul64(y int64) (z Int128) {
	switch y {
	case 0:
		return ZeroInt128
	case 1:
		return x
	case -1:
		return x.Neg()
	}
	xSign := x.Sign()
	if xSign < 0 {
		x = x.Neg()
	}
	var ySign int
	if y > 0 {
		ySign = 1
	} else {
		y = -y
		ySign = -1
	}
	z = x.uMul64(uint64(y))
	if xSign != ySign {
		z = z.Neg()
	}
	return
}

func (x Int128) uMul64(y uint64) (z Int128) {
	z[0], z[1] = bits.Mul64(x[1], y)
	_, p1 := bits.Mul64(x[0], y)
	z[0], _ = bits.Add64(z[0], p1, 0)
	return
}

func (x Int128) Mul64Overflow(y int64) (Int128, bool) {
	switch y {
	case 0:
		return ZeroInt128, false
	case 1:
		return x, false
	case -1:
		return x.Neg(), false
	}
	xSign := x.Sign()
	if xSign < 0 {
		x = x.Neg()
	}
	var ySign int
	if y > 0 {
		ySign = 1
	} else {
		y = -y
		ySign = -1
	}
	z, overflow := x.uMul64Overflow(uint64(y))
	if xSign != ySign {
		z = z.Neg()
		overflow = overflow || z[0] > 1<<63
	} else {
		overflow = overflow || z[0] > 1<<63-1
	}
	return z, overflow
}

func (x Int128) uMul64Overflow(y uint64) (Int128, bool) {
	var (
		z     Int128
		carry uint64
	)
	z[0], z[1] = bits.Mul64(x[1], y)
	_, p1 := bits.Mul64(x[0], y)
	z[0], carry = bits.Add64(z[0], p1, 0)
	return z, carry != 0
}

func (n Int128) Div(d Int128) Int128 {
	if n.Sign() > 0 {
		if d.Sign() > 0 {
			// pos / pos
			q, _ := n.uQuoRem(d)
			return q
		} else {
			// pos / neg
			q, _ := n.uQuoRem(d.Neg())
			return q.Neg()
		}
	}

	if d.Sign() < 0 {
		// neg / neg
		q, _ := n.Neg().uQuoRem(d.Neg())
		return q
	}
	// neg / pos
	q, _ := n.Neg().uQuoRem(d)
	return q.Neg()
}

// Div64 returns u/v.
func (n Int128) Div64(d int64) Int128 {
	if d == 0 {
		return ZeroInt128
	}
	nSign := n.Sign()
	if nSign < 0 {
		n = n.Neg()
	}
	var dSign int
	if d > 0 {
		dSign = 1
	} else {
		d = -d
		dSign = -1
	}
	q, _ := n.uQuoRem64(uint64(d))
	if nSign != dSign {
		q = q.Neg()
	}
	return q
}

func (n Int128) QuoRem(d Int128) (Int128, Int128) {
	if n.Sign() > 0 {
		if d.Sign() > 0 {
			// pos / pos
			return n.uQuoRem(d)
		} else {
			// pos / neg
			q, r := n.uQuoRem(d.Neg())
			return q.Neg(), r.Neg()
		}
	}

	if d.Sign() < 0 {
		// neg / neg
		return n.Neg().uQuoRem(d.Neg())
	}
	// neg / pos
	q, r := n.Neg().uQuoRem(d)
	return q.Neg(), r.Neg()
}

// QuoRem returns q = u/v and r = u%y.
func (x Int128) uQuoRem(y Int128) (q, r Int128) {
	if y[0] == 0 {
		var r64 uint64
		q, r64 = x.uQuoRem64(y[1])
		r.SetInt64(int64(r64))
	} else {
		// generate a "trial quotient," guaranteed to be within 1 of the actual
		// quotient, then adjust.
		n := uint(bits.LeadingZeros64(y[0]))
		v1 := y.Lsh(n)
		u1 := x.Rsh(1)
		tq, _ := bits.Div64(u1[0], u1[1], v1[0])
		tq >>= 63 - n
		if tq != 0 {
			tq--
		}
		q.SetInt64(int64(tq))
		// calculate remainder using trial quotient, then adjust if remainder is
		// greater than divisor
		r = x.Sub(y.uMul64(tq))
		if r.Cmp(y) >= 0 {
			q = q.Add64(1)
			r = r.Sub(y)
		}
	}
	return
}

// QuoRem64 returns q = u/v and r = u%v.
func (x Int128) uQuoRem64(y uint64) (q Int128, r uint64) {
	if x[0] < y {
		q[1], r = bits.Div64(x[0], x[1], y)
	} else {
		q[0], r = bits.Div64(0, x[0], y)
		q[1], r = bits.Div64(r, x[1], y)
	}
	return
}
