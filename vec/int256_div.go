// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import "math/bits"

// Mul returns the product x*y
func (x Int256) Mul(y Int256) Int256 {
	var (
		res              Int256
		carry            uint64
		res1, res2, res3 uint64
	)

	carry, res[3] = bits.Mul64(x[3], y[3])
	carry, res1 = umulHop(carry, x[2], y[3])
	carry, res2 = umulHop(carry, x[1], y[3])
	res3 = x[0]*y[3] + carry

	carry, res[2] = umulHop(res1, x[3], y[2])
	carry, res2 = umulStep(res2, x[2], y[2], carry)
	res3 = res3 + x[1]*y[2] + carry

	carry, res[1] = umulHop(res2, x[3], y[1])
	res3 = res3 + x[2]*y[1] + carry

	res[0] = res3 + x[3]*y[0]

	return res
}

// FIXME: IEEE 754-2008 roundTiesToEven
//
// Div interprets n and d as two's complement signed integers,
// does a signed division on the two operands.
// If d == 0, returns 0
func (n Int256) Div(d Int256) Int256 {
	if n.Sign() > 0 {
		if d.Sign() > 0 {
			// pos / pos
			return n.udiv(d)
		} else {
			// pos / neg
			return n.udiv(d.Neg()).Neg()
		}
	}

	if d.Sign() < 0 {
		// neg / neg
		return n.Neg().udiv(d.Neg())
	}
	// neg / pos
	return n.Neg().udiv(d).Neg()
}

// Div returns the quotient x/y.
func (x Int256) udiv(y Int256) Int256 {
	if y.IsZero() || y.Abs().Gt(x.Abs()) {
		return ZeroInt256
	}
	if x.Abs().Eq(y.Abs()) {
		if x.Sign() != y.Sign() {
			return OneInt256.Neg()
		}
		return OneInt256
	}
	// Shortcut some cases
	if x.IsInt64() {
		return Int256FromInt64(x.Int64() / y.Int64())
	}

	// At this point, we know
	// x/y ; x > y > 0

	// flip order for embedded algorithm
	x[3], x[2], x[1], x[0] = x[0], x[1], x[2], x[3]
	y[3], y[2], y[1], y[0] = y[0], y[1], y[2], y[3]

	var quot Int256
	udivrem(quot[:], x[:], y)

	// flip result order
	quot[3], quot[2], quot[1], quot[0] = quot[0], quot[1], quot[2], quot[3]
	return quot
}

// Mod returns the modulus x%y for y != 0.
// If y == 0, returns 0. OBS: differs from other math libraries
func (x Int256) Mod(y Int256) Int256 {
	if x.IsZero() || y.IsZero() {
		return ZeroInt256
	}
	switch x.Abs().Cmp(y.Abs()) {
	case -1:
		// x < y
		return x
	case 0:
		// x == y
		return ZeroInt256 // They are equal
	}

	// At this point:
	// x != 0
	// y != 0
	// x > y

	// Shortcut trivial case
	if x.IsInt64() {
		return Int256FromInt64(x.Int64() % y.Int64())
	}

	// flip order for embedded algorithm
	x[3], x[2], x[1], x[0] = x[0], x[1], x[2], x[3]
	y[3], y[2], y[1], y[0] = y[0], y[1], y[2], y[3]

	var quot Int256
	rem := udivrem(quot[:], x[:], y)

	// rem[3], rem[2], rem[1], rem[0] = rem[0], rem[1], rem[2], rem[3]
	return rem
}

func (x Int256) QuoRem(y Int256) (Int256, Int256) {
	if x.IsZero() || y.IsZero() {
		return ZeroInt256, ZeroInt256
	}
	switch x.Abs().Cmp(y.Abs()) {
	case -1:
		// x < y
		return ZeroInt256, x
	case 0:
		// x == y
		if x.Sign() != y.Sign() {
			return OneInt256.Neg(), ZeroInt256
		}
		return OneInt256, ZeroInt256 // They are equal
	}

	// At this point:
	// x != 0
	// y != 0
	// x > y

	// Shortcut trivial case
	if x.IsInt64() {
		return Int256FromInt64(x.Int64() / y.Int64()), Int256FromInt64(x.Int64() % y.Int64())
	}

	// flip order for embedded algorithm
	x[3], x[2], x[1], x[0] = x[0], x[1], x[2], x[3]
	y[3], y[2], y[1], y[0] = y[0], y[1], y[2], y[3]

	var quot Int256
	rem := udivrem(quot[:], x[:], y)

	// flip result order
	quot[3], quot[2], quot[1], quot[0] = quot[0], quot[1], quot[2], quot[3]
	return quot, rem
}

// umulStep computes (hi * 2^64 + lo) = z + (x * y) + carry.
func umulStep(z, x, y, carry uint64) (hi, lo uint64) {
	hi, lo = bits.Mul64(x, y)
	lo, carry = bits.Add64(lo, carry, 0)
	hi, _ = bits.Add64(hi, 0, carry)
	lo, carry = bits.Add64(lo, z, 0)
	hi, _ = bits.Add64(hi, 0, carry)
	return hi, lo
}

// umulHop computes (hi * 2^64 + lo) = z + (x * y)
func umulHop(z, x, y uint64) (hi, lo uint64) {
	hi, lo = bits.Mul64(x, y)
	lo, carry := bits.Add64(lo, z, 0)
	hi, _ = bits.Add64(hi, 0, carry)
	return hi, lo
}

// addTo computes x += y.
// Requires len(x) >= len(y).
func addTo(x, y []uint64) uint64 {
	var carry uint64
	for i := 0; i < len(y); i++ {
		x[i], carry = bits.Add64(x[i], y[i], carry)
	}
	return carry
}

// subMulTo computes x -= y * multiplier.
// Requires len(x) >= len(y).
func subMulTo(x, y []uint64, multiplier uint64) uint64 {
	var borrow uint64
	for i := 0; i < len(y); i++ {
		s, carry1 := bits.Sub64(x[i], borrow, 0)
		ph, pl := bits.Mul64(y[i], multiplier)
		t, carry2 := bits.Sub64(s, pl, 0)
		x[i] = t
		borrow = ph + carry1 + carry2
	}
	return borrow
}

// udivremBy1 divides u by single normalized word d and produces both quotient and remainder.
// The quotient is stored in provided quot.
func udivremBy1(quot, u []uint64, d uint64) (rem uint64) {
	reciprocal := reciprocal2by1(d)
	rem = u[len(u)-1] // Set the top word as remainder.
	for j := len(u) - 2; j >= 0; j-- {
		quot[j], rem = udivrem2by1(rem, u[j], d, reciprocal)
	}
	return rem
}

// reciprocal2by1 computes <^d, ^0> / d.
func reciprocal2by1(d uint64) uint64 {
	reciprocal, _ := bits.Div64(^d, ^uint64(0), d)
	return reciprocal
}

// udivrem2by1 divides <uh, ul> / d and produces both quotient and remainder.
// It uses the provided d's reciprocal.
// Implementation ported from https://github.com/chfast/intx and is based on
// "Improved division by invariant integers", Algorithm 4.
func udivrem2by1(uh, ul, d, reciprocal uint64) (quot, rem uint64) {
	qh, ql := bits.Mul64(reciprocal, uh)
	ql, carry := bits.Add64(ql, ul, 0)
	qh, _ = bits.Add64(qh, uh, carry)
	qh++

	r := ul - qh*d

	if r > ql {
		qh--
		r += d
	}

	if r >= d {
		qh++
		r -= d
	}

	return qh, r
}

// udivremKnuth implements the division of u by normalized multiple word d from the Knuth's division algorithm.
// The quotient is stored in provided quot - len(u)-len(d) words.
// Updates u to contain the remainder - len(d) words.
func udivremKnuth(quot, u, d []uint64) {
	dh := d[len(d)-1]
	dl := d[len(d)-2]
	reciprocal := reciprocal2by1(dh)

	for j := len(u) - len(d) - 1; j >= 0; j-- {
		u2 := u[j+len(d)]
		u1 := u[j+len(d)-1]
		u0 := u[j+len(d)-2]

		var qhat, rhat uint64
		if u2 >= dh { // Division overflows.
			qhat = ^uint64(0)
			// TODO: Add "qhat one to big" adjustment (not needed for correctness, but helps avoiding "add back" case).
		} else {
			qhat, rhat = udivrem2by1(u2, u1, dh, reciprocal)
			ph, pl := bits.Mul64(qhat, dl)
			if ph > rhat || (ph == rhat && pl > u0) {
				qhat--
				// TODO: Add "qhat one to big" adjustment (not needed for correctness, but helps avoiding "add back" case).
			}
		}

		// Multiply and subtract.
		borrow := subMulTo(u[j:], d, qhat)
		u[j+len(d)] = u2 - borrow
		if u2 < borrow { // Too much subtracted, add back.
			qhat--
			u[j+len(d)] += addTo(u[j:], d)
		}

		quot[j] = qhat // Store quotient digit.
	}
}

// udivrem divides u by d and produces both quotient and remainder.
// The quotient is stored in provided quot - len(u)-len(d)+1 words.
// It loosely follows the Knuth's division algorithm (sometimes referenced as "schoolbook" division) using 64-bit words.
// See Knuth, Volume 2, section 4.3.1, Algorithm D.
func udivrem(quot, u []uint64, d Int256) (rem Int256) {
	var dLen int
	for i := len(d) - 1; i >= 0; i-- {
		if d[i] != 0 {
			dLen = i + 1
			break
		}
	}

	shift := uint(bits.LeadingZeros64(d[dLen-1]))

	var dnStorage Int256
	dn := dnStorage[:dLen]
	for i := dLen - 1; i > 0; i-- {
		dn[i] = (d[i] << shift) | (d[i-1] >> (64 - shift))
	}
	dn[0] = d[0] << shift

	var uLen int
	for i := len(u) - 1; i >= 0; i-- {
		if u[i] != 0 {
			uLen = i + 1
			break
		}
	}

	var unStorage [9]uint64
	un := unStorage[:uLen+1]
	un[uLen] = u[uLen-1] >> (64 - shift)
	for i := uLen - 1; i > 0; i-- {
		un[i] = (u[i] << shift) | (u[i-1] >> (64 - shift))
	}
	un[0] = u[0] << shift

	// TODO: Skip the highest word of numerator if not significant.

	if dLen == 1 {
		r := udivremBy1(quot, un, dn[0])
		rem = ZeroInt256
		rem[3] = r >> shift
		return rem
	}

	udivremKnuth(quot, un, dn)

	for i := 0; i < dLen-1; i++ {
		rem[i] = (un[i] >> shift) | (un[i+1] << (64 - shift))
	}
	rem[dLen-1] = un[dLen-1] >> shift

	return rem
}

func (x Int256) uQuoRem64(y uint64) (q Int256, r uint64) {
	q[0], r = bits.Div64(0, x[0], y)
	q[1], r = bits.Div64(r, x[1], y)
	q[2], r = bits.Div64(r, x[2], y)
	q[3], r = bits.Div64(r, x[3], y)
	return
}
