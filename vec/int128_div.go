// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

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
	z[1] += x[0]*y[1] + x[1]*y[0]
	if xSign != ySign {
		z = z.Neg()
	}
	return
}

// Mul64 returns x*y.
func (x Int128) Mul64(y int64) (z Int128) {
	if y == 0 {
		return Int128Zero
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
		return Int128Zero
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
