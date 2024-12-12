// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"math"
	"strconv"
	"testing"
)

func n256(hi int64, lo ...uint64) Int256 {
	switch {
	case len(lo) == 0 || lo[0] == 0:
		return Int256FromInt64(hi)
	default:
		i256 := Int256FromInt64(hi)
		for i := 0; i < len(lo); i++ {
			mul := Int256FromInt64(int64(pow10[digits64(int64(lo[i]))-1]))
			i256 = i256.Mul(mul)
		}
		return i256
	}
}

func e256(n int) Int256 {
	i := OneInt256
	for ; n > 18; n -= 18 {
		i = i.Mul(Int256FromInt64(int64(pow10[18])))
	}
	return i.Mul(Int256FromInt64(int64(pow10[n])))
}

func TestDecimal256Numbers(t *testing.T) {
	type test struct {
		name  string
		in    Int256
		scale uint8
		prec  uint8
		err   error
	}
	makePrecisionTest256 := func(n int) (tests []test) {
		for i := 0; i < n; i++ {
			tests = append(tests, test{
				name:  "10e" + strconv.Itoa(i),
				in:    e256(i),
				scale: 0,
				prec:  uint8(i + 1),
			})
		}
		return
	}

	var tests = []test{
		// regular
		{name: "0", in: ZeroInt256, scale: 0, prec: 0},
		{name: "-0", in: ZeroInt256, scale: 0, prec: 0},
		{name: "1234.56789", in: n256(123456789), scale: 5, prec: 9},
		{name: "-1234.56789", in: n256(-123456789), scale: 5, prec: 9},
		{name: "23.5", in: n256(235), scale: 1, prec: 3},
		{name: "-23.5", in: n256(-235), scale: 1, prec: 3},
		{name: "23.51", in: n256(2351), scale: 2, prec: 4},
		{name: "-23.51", in: n256(-2351), scale: 2, prec: 4},
		// invalid
		{name: "+scale", in: n256(1234567891234567891, 0), scale: 77, prec: 78, err: ErrScaleOverflow},
		{name: "+scale-", in: n256(-1234567891234567891, 0), scale: 77, prec: 78, err: ErrScaleOverflow},
		{name: "-scale", in: n256(1), scale: 255, prec: 2, err: ErrScaleOverflow},
		// // extremes
		{name: "MAX", in: MaxInt256, scale: 76, prec: 77},
		{name: "MIN", in: MinInt256, scale: 76, prec: 77},
	}
	// precision
	tests = append(tests, makePrecisionTest256(77)...)

	for _, test := range tests {
		dec := NewDecimal256(test.in, test.scale)
		ok, err := dec.Check()
		if ok != (err == nil) {
			t.Errorf("%s: expected ok == !err", test.name)
		}
		if test.err != nil {
			if err != test.err {
				t.Errorf("%s: expected error %T, got %s", test.name, test.err, err)
			}
			return
		}
		if got, want := dec.Scale(), test.scale; got != want {
			t.Errorf("%s: scale error exp %d, got %d", test.name, want, got)
		}
		if got, want := dec.Precision(), test.prec; got != want {
			t.Errorf("%s: precision error exp %d, got %d", test.name, want, got)
		}
		if got, want := dec.Int256(), test.in; got != want {
			t.Errorf("%s: value error exp %x, got %x", test.name, want.Bytes32(), got.Bytes32())
		}
	}
}

func TestDecimal256Parse(t *testing.T) {
	type test struct {
		name  string
		in    string
		out   Int256
		scale uint8
		prec  uint8
		iserr bool
		str   string
	}

	makePrecisionTest256 := func(
		n int,
		pre, suf string,
		outFn func(i int) Int256,
		scaleFn func(i int) int,
		precFn func(i int) int,
	) (tests []test) {
		for i := 1; i <= n; i++ {
			tests = append(tests, test{
				in:    pre + z(i) + suf,
				out:   outFn(i),
				scale: uint8(scaleFn(i)),
				prec:  uint8(precFn(i)),
			})
		}
		return
	}

	var tests = []test{
		// regular
		{in: "0", out: ZeroInt256, scale: 0, prec: 0},
		{in: "+0", out: ZeroInt256, scale: 0, prec: 0, str: "0"},
		{in: "-0", out: ZeroInt256, scale: 0, prec: 0, str: "0"},
		{in: "1234.56789", out: n256(123456789), scale: 5, prec: 9},
		{in: "-1234.56789", out: n256(-123456789), scale: 5, prec: 9},
		{in: "+1234.56789", out: n256(123456789), scale: 5, prec: 9, str: "1234.56789"},
		{in: "23.5", out: n256(235), scale: 1, prec: 3},
		{in: "-23.5", out: n256(-235), scale: 1, prec: 3},
		{in: "23.51", out: n256(2351), scale: 2, prec: 4},
		{in: "-23.51", out: n256(-2351), scale: 2, prec: 4},
		// extremes
		{name: "MaxInt256", in: "5.7896044618658097711785492504343953926634992332820282019728792003956564819967", out: MaxInt256, scale: 76, prec: 77},
		{name: "MinInt256", in: "-5.7896044618658097711785492504343953926634992332820282019728792003956564819968", out: MinInt256, scale: 76, prec: 77},
		{name: "Small+", in: "0.0000000000000000000000000000000000000000000000000000000000000000000000000001", out: n256(1), scale: 76, prec: 1},
		{name: "Small-", in: "-0.0000000000000000000000000000000000000000000000000000000000000000000000000001", out: n256(-1), scale: 76, prec: 1},
		// unusual
		{name: "lead-0", in: "00.1", out: n256(1), scale: 1, prec: 1, str: "0.1"},
		// other values generated below
		// invalid
		{name: "empty", in: "", iserr: true},
		{name: "double dot", in: "1..2", iserr: true},
		{name: "end dot", in: "12.", iserr: true},
		{name: "start dot", in: ".12", iserr: true},
		{name: "wrong comma", in: "1,2", iserr: true},
		{name: "double minus", in: "--12", iserr: true},
		{name: "double plus", in: "++12", iserr: true},
		{name: "plus/minus", in: "+-12", iserr: true},
		{name: "wrong prefix", in: "~12", iserr: true},
		{name: "int256+1 overflow", in: "57896044618658097711785492504343953926634992332820282019728792003956564819968", iserr: true},
		{name: "int256.1 overflow", in: "5.7896044618658097711785492504343953926634992332820282019728792003956564819968", iserr: true},
		{name: "int256+N overflow", in: "100000000000000000000000000000000000000000000000000000000000000000000000000000", iserr: true},
		{name: "int256-1 underflow", in: "-57896044618658097711785492504343953926634992332820282019728792003956564819969", iserr: true},
		{name: "int256.1 underflow", in: "-5.7896044618658097711785492504343953926634992332820282019728792003956564819969", iserr: true},
		{name: "int256-N underflow", in: "-100000000000000000000000000000000000000000000000000000000000000000000000000000", iserr: true},
		{name: "pos scale 77", in: "0.00000000000000000000000000000000000000000000000000000000000000000000000000001", iserr: true},
		{name: "neg scale 77", in: "-0.00000000000000000000000000000000000000000000000000000000000000000000000000001", iserr: true},
		// precision
		{in: "1", out: e256(0), scale: 0, prec: 1},
		// other values generated below
		{in: "1.0", out: e256(1), scale: 1, prec: 2},
		// other values generated below
		{in: "0.1", out: n256(1), scale: 1, prec: 1},
		// other values generated below
	}

	// unusual
	tests = append(tests, makePrecisionTest256(38, "0.", "", func(i int) Int256 { return ZeroInt256 }, func(i int) int { return i }, func(i int) int { return 0 })...)
	// precision 0.000
	tests = append(tests, makePrecisionTest256(38, "1.", "", e256, func(i int) int { return i }, func(i int) int { return i + 1 })...)
	// precision 1000.0
	tests = append(tests, makePrecisionTest256(37, "1", ".0", func(i int) Int256 { return e256(i + 1) }, func(i int) int { return 1 }, func(i int) int { return i + 2 })...)
	// precision 0.00001
	tests = append(tests, makePrecisionTest256(37, "0.", "1", func(i int) Int256 { return n256(1) }, func(i int) int { return i + 1 }, func(i int) int { return 1 })...)

	for _, test := range tests {
		name := test.name
		if name == "" {
			name = test.in
		}
		dec, err := ParseDecimal256(test.in)
		if test.iserr {
			if err == nil {
				t.Fatalf("%s: expected error, got none", name)
			}
			return
		}
		if !test.iserr && (err != nil) {
			t.Errorf("%s: expected no error, got %s", name, err)
			return
		}
		if got, want := dec.Int256(), test.out; got != want {
			t.Errorf("%s: value error exp %x, got %x", name, want.Bytes32(), got.Bytes32())
		}
		if got, want := dec.Scale(), test.scale; got != want {
			t.Errorf("%s: scale error exp %d, got %d", name, want, got)
		}
		if got, want := dec.Precision(), test.prec; got != want {
			t.Errorf("%s: precision error exp %d, got %d", name, want, got)
		}
		if !test.iserr {
			exp := test.str
			if exp == "" {
				exp = test.in
			}
			if got, want := dec.String(), exp; got != want {
				t.Errorf("%s: string error exp %s, got %s", name, want, got)
			}
		}
	}
}

func TestDecimal256SetFloat(t *testing.T) {
	var tests = []struct {
		name  string
		in    float64
		out   Int256
		scale uint8
		prec  uint8
		iserr bool
	}{
		// regular
		{name: "0", in: 0.0, out: n256(0), scale: 0, prec: 0},
		{name: "-0", in: -1 * 0.0, out: n256(0), scale: 0, prec: 0},
		{name: "1234.56789", in: 1234.56789, out: n256(123456789), scale: 5, prec: 9},
		{name: "-1234.56789", in: -1234.56789, out: n256(-123456789), scale: 5, prec: 9},
		{name: "+1234.56789", in: 1234.56789, out: n256(123456789), scale: 5, prec: 9},
		{name: "23.5", in: 23.5, out: n256(235), scale: 1, prec: 3},
		{name: "-23.5", in: -23.5, out: n256(-235), scale: 1, prec: 3},
		{name: "23.51", in: 23.51, out: n256(2351), scale: 2, prec: 4},
		{name: "-23.51", in: -23.51, out: n256(-2351), scale: 2, prec: 4},
		// extremes
		// Note: MaxInt64-1 does not fit into float64
		// nearest float to MaxInt64 is MaxInt64+1, so we must use Int128{0, 1<<63}
		// nearest float to MinInt64 is MinInt64
		// nearest float to MaxInt128 is MaxInt128+1, so we must use Int128{0, 0, 1<<63, 0}
		// nearest float to MinInt128 is MinInt128
		{name: "MaxInt64", in: 9.223372036854775807, out: Int256{0, 0, 0, 1 << 63}, scale: 18, prec: 19},
		{name: "MinInt64", in: -9.223372036854775808, out: Int256FromInt64(-1 << 63), scale: 18, prec: 19},
		{name: "MaxInt128", in: 1.70141183460469231731687303715884105727, out: Int256{0, 0, 1 << 63, 0}, scale: 38, prec: 39},
		{name: "MinInt128", in: -1.70141183460469231731687303715884105727, out: Int256FromInt128(MinInt128), scale: 38, prec: 39},
		{name: "MaxInt256", in: 5.7896044618658097711785492504343953926634992332820282019728792003956564819967, out: MaxInt256, scale: 76, prec: 77},
		{name: "MinInt256", in: -5.7896044618658097711785492504343953926634992332820282019728792003956564819967, out: MinInt256, scale: 76, prec: 77},
		// max safe integer (53 bit precision)
		{name: "9.007199254740991", in: 9.007199254740991, out: n256(1<<53 - 1), scale: 15, prec: 16},
		{name: "-9.007199254740992", in: -9.007199254740992, out: n256(-1 << 53), scale: 15, prec: 16},
		{name: "0.000000000000000001", in: 0.000000000000000001, out: n256(1), scale: 18, prec: 1},
		{name: "-0.000000000000000001", in: -0.000000000000000001, out: n256(-1), scale: 18, prec: 1},
		// unusual
		{name: "0.0", in: 0.0, out: n256(0), scale: 0, prec: 0},
		// round to nearest even
		{name: "24.5", in: 24.5, out: n256(24), scale: 0, prec: 2},
		{name: "23.5", in: 23.5, out: n256(24), scale: 0, prec: 2},
		// invalid
		{name: "-scale", in: 1.0, scale: 255, iserr: true},
		{name: ">scale", in: 1.0, scale: 77, iserr: true},
		{name: "NaN", in: math.NaN(), iserr: true},
		{name: "+Inf", in: math.Inf(+1), iserr: true},
		{name: "-Inf", in: math.Inf(-1), iserr: true},
		// Note: float64 rounds down to nearest power of 2 which in these cases
		// is = Min/MaxInt256; for this reason use the next representable float64 value
		{name: "int256+1 overflow", in: 57896044618658110567289846576266158262331731062121102197352742266299247230976.0, scale: 0, iserr: true},
		{name: "int256.1 overflow", in: 5.7896044618658110567289846576266158262331731062121102197352742266299247230976, scale: 76, iserr: true},
		{name: "int256+N overflow", in: 1000000000000000000000000000000000000000000000000000000000000000000000000000000.0, scale: 0, iserr: true},
		{name: "int256-1 underflow", in: -57896044618658110567289846576266158262331731062121102197352742266299247230976.0, scale: 0, iserr: true},
		{name: "int256.1 underflow", in: -5.7896044618658110567289846576266158262331731062121102197352742266299247230976, scale: 76, iserr: true},
		{name: "int256-N underflow", in: -1000000000000000000000000000000000000000000000000000000000000000000000000000000.0, scale: 0, iserr: true},
		// not error cases, will be rounded to nearest even
		// {name: "pos scale 19", in: 0.0000000000000000001, iserr: true},
		// {name: "neg scale 19", in: -0.0000000000000000001, iserr: true},
	}

	for _, test := range tests {
		var dec Decimal256
		err := dec.SetFloat64(test.in, test.scale)
		if test.iserr {
			if err == nil {
				t.Fatalf("%s: expected error, got none", test.name)
			}
			return
		}
		if !test.iserr && (err != nil) {
			t.Fatalf("%s: expected no error, got %s", test.name, err)
			return
		}
		if got, want := dec.Int256(), test.out; got != want {
			t.Errorf("%s: value error exp %x, got %x", test.name, want.Bytes32(), got.Bytes32())
		}
		if got, want := dec.Scale(), test.scale; got != want {
			t.Errorf("%s: scale error exp %d, got %d", test.name, want, got)
		}
		if got, want := dec.Precision(), test.prec; got != want {
			t.Errorf("%s: precision error exp %d, got %d", test.name, want, got)
		}
	}
}

func TestDecimal256Quantize(t *testing.T) {
	var tests = []struct {
		name    string
		in      Int256
		scale   uint8
		quant   uint8
		out     Int256
		isover  bool
		isunder bool
	}{
		// regular no-change
		{name: "no-change_24.51", in: n256(2451), scale: 2, quant: 2, out: n256(2451)},
		{name: "no+change_24.51", in: n256(-2451), scale: 2, quant: 2, out: n256(-2451)},
		// regular down
		{name: "down1+24.51", in: n256(2451), scale: 2, quant: 1, out: n256(245)},
		{name: "down1-24.51", in: n256(-2451), scale: 2, quant: 1, out: n256(-245)},
		{name: "down2+24.51", in: n256(2451), scale: 2, quant: 0, out: n256(25)},
		{name: "down2-24.51", in: n256(-2451), scale: 2, quant: 0, out: n256(-25)},
		{name: "down1+24.5", in: n256(245), scale: 1, quant: 0, out: n256(24)},
		{name: "down1-24.5", in: n256(-245), scale: 1, quant: 0, out: n256(-24)},
		{name: "down2+23.51", in: n256(2351), scale: 2, quant: 0, out: n256(24)},
		{name: "down2-23.51", in: n256(-2351), scale: 2, quant: 0, out: n256(-24)},
		{name: "down1+23.5", in: n256(235), scale: 1, quant: 0, out: n256(24)},
		{name: "down1-23.5", in: n256(-235), scale: 1, quant: 0, out: n256(-24)},
		// regular up
		{name: "up1+24.51", in: n256(2451), scale: 2, quant: 3, out: n256(24510)},
		{name: "up1-24.51", in: n256(-2451), scale: 2, quant: 3, out: n256(-24510)},
		// invalid scales are clipped
		{name: "neg_scale", in: n256(15), scale: 1, quant: 255, out: Int256FromInt64(15).Mul(e256(75)), isover: true},
		{name: "big_scale", in: n256(15), scale: 1, quant: 76, out: Int256FromInt64(15).Mul(e256(75)), isover: true},
	}

	for _, test := range tests {
		dec := NewDecimal256(test.in, test.scale)
		res := dec.Quantize(test.quant)
		if got, want := res.Int256(), test.out; got != want {
			t.Errorf("%s: value error exp %x, got %x", test.name, want.Bytes32(), got.Bytes32())
		}
		switch {
		case test.isover:
			if got, want := res.Scale(), MaxDecimal256Precision; got != want {
				t.Errorf("%s: scale error exp %d, got %d", test.name, want, got)
			}
		case test.isunder:
			if got, want := res.Scale(), uint8(0); got != want {
				t.Errorf("%s: scale error exp %d, got %d", test.name, want, got)
			}
		default:
			if got, want := res.Scale(), test.quant; got != want {
				t.Errorf("%s: scale error exp %d, got %d", test.name, want, got)
			}
		}
	}
}

func TestDecimal256Round(t *testing.T) {
	var tests = []struct {
		name  string
		in    Int256
		scale uint8
		out   int64
	}{
		// regular
		{name: "0", in: n256(0), scale: 2, out: 0},
		{name: "+24.51", in: n256(2451), scale: 2, out: 25},
		{name: "-24.51", in: n256(-2451), scale: 2, out: -25},
		{name: "+24.5", in: n256(245), scale: 1, out: 24},
		{name: "-24.5", in: n256(-245), scale: 1, out: -24},
		{name: "+23.51", in: n256(2351), scale: 2, out: 24},
		{name: "-23.51", in: n256(-2351), scale: 2, out: -24},
		{name: "+23.5", in: n256(235), scale: 1, out: 24},
		{name: "-23.5", in: n256(-235), scale: 1, out: -24},
	}

	for _, test := range tests {
		dec := NewDecimal256(test.in, test.scale)
		if got, want := dec.RoundToInt64(), test.out; got != want {
			t.Errorf("%s: value error exp %d, got %d", test.name, want, got)
		}
	}
}

func TestDecimal256Compare(t *testing.T) {
	// Scales
	//   same
	//   a<b
	//   a>b
	// Functions
	//   Eq
	//   Gt
	//   Gte
	//   Lt
	//   Lte
	var tests = []struct {
		name string
		a    Int256
		b    Int256
		x    uint8  // scale A
		y    uint8  // scale B
		res  string // [01] for EQ, LT, LTE, GT, GTE
	}{
		// same scale, same sign
		{name: "=A+=B+", a: n256(1), b: n256(1), x: 1, y: 1, res: "10101"},
		{name: "=A+<B+", a: n256(1), b: n256(2), x: 1, y: 1, res: "01100"},
		{name: "=A+>B+", a: n256(2), b: n256(1), x: 1, y: 1, res: "00011"},
		// same scale, A-, B+
		{name: "=A-<B+", a: n256(-1), b: n256(2), x: 1, y: 1, res: "01100"},
		{name: "=A->B+", a: n256(-2), b: n256(1), x: 1, y: 1, res: "01100"},
		// same scale, A+, B-
		{name: "=A+<B-", a: n256(1), b: n256(-2), x: 1, y: 1, res: "00011"},
		{name: "=A+>B-", a: n256(2), b: n256(-1), x: 1, y: 1, res: "00011"},
		// same scale, A-, B-
		{name: "=A-=B-", a: n256(-1), b: n256(-1), x: 1, y: 1, res: "10101"},
		{name: "=A->B+", a: n256(-1), b: n256(-2), x: 1, y: 1, res: "00011"},
		{name: "=A-<B+", a: n256(-2), b: n256(-1), x: 1, y: 1, res: "01100"},

		// a<b scale, same sign
		{name: "<A+=B+", a: n256(1), b: n256(10), x: 1, y: 2, res: "10101"},
		{name: "<A+<B+", a: n256(1), b: n256(20), x: 1, y: 2, res: "01100"},
		{name: "<A+>B+", a: n256(2), b: n256(10), x: 1, y: 2, res: "00011"},
		// a<b scale, A-, B+
		{name: "<A-<B+", a: n256(-1), b: n256(20), x: 1, y: 2, res: "01100"},
		{name: "<A->B+", a: n256(-2), b: n256(10), x: 1, y: 2, res: "01100"},
		// a<b scale, A+, B-
		{name: "<A+<B-", a: n256(1), b: n256(-20), x: 1, y: 2, res: "00011"},
		{name: "<A+>B-", a: n256(2), b: n256(-10), x: 1, y: 2, res: "00011"},
		// a<b scale, A-, B-
		{name: "<A-=B-", a: n256(-1), b: n256(-10), x: 1, y: 2, res: "10101"},
		{name: "<A->B+", a: n256(-1), b: n256(-20), x: 1, y: 2, res: "00011"},
		{name: "<A-<B+", a: n256(-2), b: n256(-10), x: 1, y: 2, res: "01100"},

		// a>b scale, same sign
		{name: ">A+=B+", a: n256(100), b: n256(10), x: 3, y: 2, res: "10101"},
		{name: ">A+<B+", a: n256(100), b: n256(20), x: 3, y: 2, res: "01100"},
		{name: ">A+>B+", a: n256(200), b: n256(10), x: 3, y: 2, res: "00011"},
		// a>b scale, A-, B+
		{name: ">A-<B+", a: n256(-100), b: n256(20), x: 3, y: 2, res: "01100"},
		{name: ">A->B+", a: n256(-200), b: n256(10), x: 3, y: 2, res: "01100"},
		// a>b scale, A+, B-
		{name: ">A+<B-", a: n256(100), b: n256(-20), x: 3, y: 2, res: "00011"},
		{name: ">A+>B-", a: n256(200), b: n256(-10), x: 3, y: 2, res: "00011"},
		// a>b scale, A-, B-
		{name: ">A-=B-", a: n256(-100), b: n256(-10), x: 3, y: 2, res: "10101"},
		{name: ">A->B+", a: n256(-100), b: n256(-20), x: 3, y: 2, res: "00011"},
		{name: ">A-<B+", a: n256(-200), b: n256(-10), x: 3, y: 2, res: "01100"},
	}

	comp := func(s string) []bool {
		b := make([]bool, len(s))
		for i := range s {
			b[i] = s[i] == '1'
		}
		return b
	}

	for _, test := range tests {
		A := NewDecimal256(test.a, test.x)
		B := NewDecimal256(test.b, test.y)
		cmp := comp(test.res)
		if got, want := A.Eq(B), cmp[0]; got != want {
			t.Errorf("%s: equal error exp %t, got %t", test.name, want, got)
		}
		if got, want := A.Lt(B), cmp[1]; got != want {
			t.Errorf("%s: lt error exp %t, got %t", test.name, want, got)
		}
		if got, want := A.Lte(B), cmp[2]; got != want {
			t.Errorf("%s: lte error exp %t, got %t", test.name, want, got)
		}
		if got, want := A.Gt(B), cmp[3]; got != want {
			t.Errorf("%s: gt error exp %t, got %t", test.name, want, got)
		}
		if got, want := A.Gte(B), cmp[4]; got != want {
			t.Errorf("%s: gte error exp %t, got %t", test.name, want, got)
		}
	}
}

var parse256Benchmarks = []string{
	"1.0",
	"1.000000000",
	"100000000.0",
	"0.000000001",
}

var marshal256Benchmarks = []struct {
	f float64
	s uint8
}{
	{f: 1.0, s: 1},
	{f: 1.000000000, s: 9},
	{f: 100000000.0, s: 1},
	{f: 0.000000001, s: 9},
}

func BenchmarkParseDecimal256(b *testing.B) {
	for _, v := range parse256Benchmarks {
		b.Run(v, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(len(v)))
			for i := 0; i < b.N; i++ {
				_, _ = ParseDecimal256(v)
			}
		})
	}
}

func BenchmarkMarshalDecimal256(b *testing.B) {
	for _, v := range marshal256Benchmarks {
		b.Run(strconv.FormatFloat(v.f, 'f', -1, 64), func(b *testing.B) {
			var dec Decimal256
			dec.SetFloat64(v.f, v.s)
			b.ResetTimer()
			b.SetBytes(8)
			for i := 0; i < b.N; i++ {
				_ = dec.String()
			}
		})
	}
}
