// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package decimal

import (
	"math"
	"strconv"
	"testing"
)

func TestDecimal64Numbers(t *testing.T) {
	var tests = []struct {
		name  string
		in    int64
		scale int
		prec  int
		err   error
	}{
		// regular
		{name: "0", in: 0, scale: 0, prec: 0},
		{name: "-0", in: 0, scale: 0, prec: 0},
		{name: "1234.56789", in: 123456789, scale: 5, prec: 9},
		{name: "-1234.56789", in: -123456789, scale: 5, prec: 9},
		{name: "23.5", in: 235, scale: 1, prec: 3},
		{name: "-23.5", in: -235, scale: 1, prec: 3},
		{name: "23.51", in: 2351, scale: 2, prec: 4},
		{name: "-23.51", in: -2351, scale: 2, prec: 4},
		// invalid
		{name: "0.1234567891234567891", in: 1234567891234567891, scale: 19, prec: 19, err: ErrScaleOverflow},
		{name: "-0.1234567891234567891", in: -1234567891234567891, scale: 19, prec: 19, err: ErrScaleOverflow},
		{name: "-scale", in: 11, scale: -1, prec: 2, err: ErrScaleUnderflow},
		// extremes
		{name: "9.223372036854775807", in: math.MaxInt64, scale: 18, prec: 19},
		{name: "-9.223372036854775808", in: math.MinInt64, scale: 18, prec: 19},
		// precision
		{name: "1", in: 1, scale: 0, prec: 1},
		{name: "10", in: 10, scale: 0, prec: 2},
		{name: "100", in: 100, scale: 0, prec: 3},
		{name: "1000", in: 1000, scale: 0, prec: 4},
		{name: "10000", in: 10000, scale: 0, prec: 5},
		{name: "100000", in: 100000, scale: 0, prec: 6},
		{name: "1000000", in: 1000000, scale: 0, prec: 7},
		{name: "10000000", in: 10000000, scale: 0, prec: 8},
		{name: "100000000", in: 100000000, scale: 0, prec: 9},
		{name: "1000000000", in: 1000000000, scale: 0, prec: 10},
		{name: "10000000000", in: 10000000000, scale: 0, prec: 11},
		{name: "100000000000", in: 100000000000, scale: 0, prec: 12},
		{name: "1000000000000", in: 1000000000000, scale: 0, prec: 13},
		{name: "10000000000000", in: 10000000000000, scale: 0, prec: 14},
		{name: "100000000000000", in: 100000000000000, scale: 0, prec: 15},
		{name: "1000000000000000", in: 1000000000000000, scale: 0, prec: 16},
		{name: "10000000000000000", in: 10000000000000000, scale: 0, prec: 17},
		{name: "100000000000000000", in: 100000000000000000, scale: 0, prec: 18},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dec := NewDecimal64(test.in, test.scale)
			ok, err := dec.Check()
			if ok != (err == nil) {
				t.Errorf("expected ok == !err\n")
			}
			if test.err != nil {
				if err != test.err {
					t.Errorf("expected error %T, got %s\n", test.err, err)
				}
				return
			}
			if got, want := dec.Scale(), test.scale; got != want {
				t.Errorf("scale error exp %d, got %d\n", want, got)
			}
			if got, want := dec.Precision(), test.prec; got != want {
				t.Errorf("precision error exp %d, got %d\n", want, got)
			}
			if got, want := dec.Int64(), test.in; got != want {
				t.Errorf("value error exp %d, got %d\n", want, got)
			}
		})
	}
}

func TestDecimal64Parse(t *testing.T) {
	var tests = []struct {
		name  string
		in    string
		out   int64
		scale int
		prec  int
		iserr bool
		str   string
	}{
		// regular
		{in: "0", out: 0, scale: 0, prec: 0},
		{in: "+0", out: 0, scale: 0, prec: 0, str: "0"},
		{in: "-0", out: 0, scale: 0, prec: 0, str: "0"},
		{in: "1234.56789", out: 123456789, scale: 5, prec: 9},
		{in: "-1234.56789", out: -123456789, scale: 5, prec: 9},
		{in: "+1234.56789", out: 123456789, scale: 5, prec: 9, str: "1234.56789"},
		{in: "23.5", out: 235, scale: 1, prec: 3},
		{in: "-23.5", out: -235, scale: 1, prec: 3},
		{in: "23.51", out: 2351, scale: 2, prec: 4},
		{in: "-23.51", out: -2351, scale: 2, prec: 4},
		// extremes
		{in: "9.223372036854775807", out: math.MaxInt64, scale: 18, prec: 19},
		{in: "-9.223372036854775808", out: math.MinInt64, scale: 18, prec: 19},
		{in: "0.000000000000000001", out: 1, scale: 18, prec: 1},
		{in: "-0.000000000000000001", out: -1, scale: 18, prec: 1},
		// unusual
		{name: "lead-0", in: "00.1", out: 1, scale: 1, prec: 1, str: "0.1"},
		{in: "0.0", out: 0, scale: 1, prec: 0},
		{in: "0.00", out: 0, scale: 2, prec: 0},
		{in: "0.000", out: 0, scale: 3, prec: 0},
		{in: "0.0000", out: 0, scale: 4, prec: 0},
		{in: "0.00000", out: 0, scale: 5, prec: 0},
		{in: "0.000000", out: 0, scale: 6, prec: 0},
		{in: "0.0000000", out: 0, scale: 7, prec: 0},
		{in: "0.00000000", out: 0, scale: 8, prec: 0},
		{in: "0.000000000", out: 0, scale: 9, prec: 0},
		{in: "0.0000000000", out: 0, scale: 10, prec: 0},
		{in: "0.00000000000", out: 0, scale: 11, prec: 0},
		{in: "0.000000000000", out: 0, scale: 12, prec: 0},
		{in: "0.0000000000000", out: 0, scale: 13, prec: 0},
		{in: "0.00000000000000", out: 0, scale: 14, prec: 0},
		{in: "0.000000000000000", out: 0, scale: 15, prec: 0},
		{in: "0.0000000000000000", out: 0, scale: 16, prec: 0},
		{in: "0.00000000000000000", out: 0, scale: 17, prec: 0},
		{in: "0.000000000000000000", out: 0, scale: 18, prec: 0},
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
		{name: "int64+1 overflow", in: "9223372036854775808", iserr: true},
		{name: "int64.1 overflow", in: "9.223372036854775808", iserr: true},
		{name: "int64+N overflow", in: "10000000000000000000", iserr: true},
		{name: "int64-1 underflow", in: "-9223372036854775809", iserr: true},
		{name: "int64.1 underflow", in: "-9.223372036854775809", iserr: true},
		{name: "int64-N underflow", in: "-10000000000000000000", iserr: true},
		{name: "pos scale 19", in: "0.0000000000000000001", iserr: true},
		{name: "neg scale 19", in: "-0.0000000000000000001", iserr: true},
		// precision
		{in: "1", out: 1, scale: 0, prec: 1},
		{in: "1.0", out: 10, scale: 1, prec: 2},
		{in: "1.00", out: 100, scale: 2, prec: 3},
		{in: "1.000", out: 1000, scale: 3, prec: 4},
		{in: "1.0000", out: 10000, scale: 4, prec: 5},
		{in: "1.00000", out: 100000, scale: 5, prec: 6},
		{in: "1.000000", out: 1000000, scale: 6, prec: 7},
		{in: "1.0000000", out: 10000000, scale: 7, prec: 8},
		{in: "1.00000000", out: 100000000, scale: 8, prec: 9},
		{in: "1.000000000", out: 1000000000, scale: 9, prec: 10},
		{in: "1.0000000000", out: 10000000000, scale: 10, prec: 11},
		{in: "1.00000000000", out: 100000000000, scale: 11, prec: 12},
		{in: "1.000000000000", out: 1000000000000, scale: 12, prec: 13},
		{in: "1.0000000000000", out: 10000000000000, scale: 13, prec: 14},
		{in: "1.00000000000000", out: 100000000000000, scale: 14, prec: 15},
		{in: "1.000000000000000", out: 1000000000000000, scale: 15, prec: 16},
		{in: "1.0000000000000000", out: 10000000000000000, scale: 16, prec: 17},
		{in: "1.00000000000000000", out: 100000000000000000, scale: 17, prec: 18},
		{in: "1.000000000000000000", out: 1000000000000000000, scale: 18, prec: 19},
		{in: "1.0", out: 10, scale: 1, prec: 2},
		{in: "10.0", out: 100, scale: 1, prec: 3},
		{in: "100.0", out: 1000, scale: 1, prec: 4},
		{in: "1000.0", out: 10000, scale: 1, prec: 5},
		{in: "10000.0", out: 100000, scale: 1, prec: 6},
		{in: "100000.0", out: 1000000, scale: 1, prec: 7},
		{in: "1000000.0", out: 10000000, scale: 1, prec: 8},
		{in: "10000000.0", out: 100000000, scale: 1, prec: 9},
		{in: "100000000.0", out: 1000000000, scale: 1, prec: 10},
		{in: "1000000000.0", out: 10000000000, scale: 1, prec: 11},
		{in: "10000000000.0", out: 100000000000, scale: 1, prec: 12},
		{in: "100000000000.0", out: 1000000000000, scale: 1, prec: 13},
		{in: "1000000000000.0", out: 10000000000000, scale: 1, prec: 14},
		{in: "10000000000000.0", out: 100000000000000, scale: 1, prec: 15},
		{in: "100000000000000.0", out: 1000000000000000, scale: 1, prec: 16},
		{in: "1000000000000000.0", out: 10000000000000000, scale: 1, prec: 17},
		{in: "10000000000000000.0", out: 100000000000000000, scale: 1, prec: 18},
		{in: "100000000000000000.0", out: 1000000000000000000, scale: 1, prec: 19},
		{in: "0.1", out: 1, scale: 1, prec: 1},
		{in: "0.01", out: 1, scale: 2, prec: 1},
		{in: "0.001", out: 1, scale: 3, prec: 1},
		{in: "0.0001", out: 1, scale: 4, prec: 1},
		{in: "0.00001", out: 1, scale: 5, prec: 1},
		{in: "0.000001", out: 1, scale: 6, prec: 1},
		{in: "0.0000001", out: 1, scale: 7, prec: 1},
		{in: "0.00000001", out: 1, scale: 8, prec: 1},
		{in: "0.000000001", out: 1, scale: 9, prec: 1},
		{in: "0.0000000001", out: 1, scale: 10, prec: 1},
		{in: "0.00000000001", out: 1, scale: 11, prec: 1},
		{in: "0.000000000001", out: 1, scale: 12, prec: 1},
		{in: "0.0000000000001", out: 1, scale: 13, prec: 1},
		{in: "0.00000000000001", out: 1, scale: 14, prec: 1},
		{in: "0.000000000000001", out: 1, scale: 15, prec: 1},
		{in: "0.0000000000000001", out: 1, scale: 16, prec: 1},
		{in: "0.00000000000000001", out: 1, scale: 17, prec: 1},
		{in: "0.000000000000000001", out: 1, scale: 18, prec: 1},
	}

	for _, test := range tests {
		name := test.name
		if name == "" {
			name = test.in
		}
		t.Run(name, func(t *testing.T) {
			dec, err := ParseDecimal64(test.in)
			if test.iserr {
				if err == nil {
					t.Fatalf("expected error, got none\n")
				}
				return
			}
			if !test.iserr && (err != nil) {
				t.Errorf("expected no error, got %s\n", err)
				return
			}
			if got, want := dec.Int64(), test.out; got != want {
				t.Errorf("value error exp %d, got %d\n", want, got)
			}
			if got, want := dec.Scale(), test.scale; got != want {
				t.Errorf("scale error exp %d, got %d\n", want, got)
			}
			if got, want := dec.Precision(), test.prec; got != want {
				t.Errorf("precision error exp %d, got %d\n", want, got)
			}
			if !test.iserr {
				exp := test.str
				if exp == "" {
					exp = test.in
				}
				if got, want := dec.String(), exp; got != want {
					t.Errorf("string error exp %s, got %s\n", want, got)
				}
			}
		})
	}
}

func TestDecimal64SetFloat(t *testing.T) {
	var tests = []struct {
		name  string
		in    float64
		out   int64
		scale int
		prec  int
		iserr bool
	}{
		// regular
		{name: "0", in: 0.0, out: 0, scale: 0, prec: 0},
		{name: "-0", in: -0.0, out: 0, scale: 0, prec: 0},
		{name: "1234.56789", in: 1234.56789, out: 123456789, scale: 5, prec: 9},
		{name: "-1234.56789", in: -1234.56789, out: -123456789, scale: 5, prec: 9},
		{name: "+1234.56789", in: 1234.56789, out: 123456789, scale: 5, prec: 9},
		{name: "23.5", in: 23.5, out: 235, scale: 1, prec: 3},
		{name: "-23.5", in: -23.5, out: -235, scale: 1, prec: 3},
		{name: "23.51", in: 23.51, out: 2351, scale: 2, prec: 4},
		{name: "-23.51", in: -23.51, out: -2351, scale: 2, prec: 4},
		// extremes
		// MaxInt64 does not fit into float64, MinInt64 is properly truncated
		{name: "9.223372036854775807", in: 9.223372036854775807, out: math.MaxInt64, scale: 18, prec: 19, iserr: true},
		{name: "-9.223372036854775808", in: -9.223372036854775808, out: math.MinInt64, scale: 18, prec: 19, iserr: true},
		// max safe integer (53 bit precision)
		{name: "9.007199254740991", in: 9.007199254740991, out: 1<<53 - 1, scale: 15, prec: 16},
		{name: "-9.007199254740992", in: -9.007199254740992, out: -1 << 53, scale: 15, prec: 16},
		{name: "0.000000000000000001", in: 0.000000000000000001, out: 1, scale: 18, prec: 1},
		{name: "-0.000000000000000001", in: -0.000000000000000001, out: -1, scale: 18, prec: 1},
		// unusual
		{name: "0.0", in: 0.0, out: 0, scale: 0, prec: 0},
		// round to nearest even
		{name: "24.5", in: 24.5, out: 24, scale: 0, prec: 2},
		{name: "23.5", in: 23.5, out: 24, scale: 0, prec: 2},
		// invalid
		{name: "-scale", in: 1.0, scale: -1, iserr: true},
		{name: ">scale", in: 1.0, scale: 19, iserr: true},
		{name: "NaN", in: math.NaN(), iserr: true},
		{name: "+Inf", in: math.Inf(+1), iserr: true},
		{name: "-Inf", in: math.Inf(-1), iserr: true},
		{name: "int64+1 overflow", in: 9223372036854775808.0, scale: 0, iserr: true},
		{name: "int64.1 overflow", in: 9.223372036854775808, scale: 18, iserr: true},
		{name: "int64+N overflow", in: 10000000000000000000.0, scale: 0, iserr: true},
		{name: "int64-1 underflow", in: -9223372036854775809.0, scale: 0, iserr: true},
		{name: "int64.1 underflow", in: -9.223372036854775809, scale: 18, iserr: true},
		{name: "int64-N underflow", in: -10000000000000000000.0, scale: 0, iserr: true},
		// not error cases, will be rounded to nearest even
		// {name: "pos scale 19", in: 0.0000000000000000001, iserr: true},
		// {name: "neg scale 19", in: -0.0000000000000000001, iserr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var dec Decimal64
			err := dec.SetFloat64(test.in, test.scale)
			if test.iserr {
				if err == nil {
					t.Fatalf("expected error, got none\n")
				}
				return
			}
			if !test.iserr && (err != nil) {
				t.Fatalf("expected no error, got %s\n", err)
				return
			}
			if got, want := dec.Int64(), test.out; got != want {
				t.Errorf("value error exp %d, got %d\n", want, got)
			}
			if got, want := dec.Scale(), test.scale; got != want {
				t.Errorf("scale error exp %d, got %d\n", want, got)
			}
			if got, want := dec.Precision(), test.prec; got != want {
				t.Errorf("precision error exp %d, got %d\n", want, got)
			}
		})
	}
}

func TestDecimal64Quantize(t *testing.T) {
	var tests = []struct {
		name    string
		in      int64
		scale   int
		quant   int
		out     int64
		isover  bool
		isunder bool
	}{
		// regular no-change
		{name: "no-change_24.51", in: 2451, scale: 2, quant: 2, out: 2451},
		{name: "no+change_24.51", in: -2451, scale: 2, quant: 2, out: -2451},
		// regular down
		{name: "down1+24.51", in: 2451, scale: 2, quant: 1, out: 245},
		{name: "down1-24.51", in: -2451, scale: 2, quant: 1, out: -245},
		{name: "down2+24.51", in: 2451, scale: 2, quant: 0, out: 25},
		{name: "down2-24.51", in: -2451, scale: 2, quant: 0, out: -25},
		{name: "down1+24.5", in: 245, scale: 1, quant: 0, out: 24},
		{name: "down1-24.5", in: -245, scale: 1, quant: 0, out: -24},
		{name: "down2+23.51", in: 2351, scale: 2, quant: 0, out: 24},
		{name: "down2-23.51", in: -2351, scale: 2, quant: 0, out: -24},
		{name: "down1+23.5", in: 235, scale: 1, quant: 0, out: 24},
		{name: "down1-23.5", in: -235, scale: 1, quant: 0, out: -24},
		// regular up
		{name: "up1+24.51", in: 2451, scale: 2, quant: 3, out: 24510},
		{name: "up1-24.51", in: -2451, scale: 2, quant: 3, out: -24510},
		// invalid scales are clipped
		{name: "neg_scale", in: 15, scale: 1, quant: -1, out: 2, isunder: true},
		{name: "big_scale", in: 15, scale: 1, quant: 20, out: 1500000000000000000, isover: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dec := NewDecimal64(test.in, test.scale)
			res := dec.Quantize(test.quant)
			if got, want := res.Int64(), test.out; got != want {
				t.Errorf("value error exp %d, got %d\n", want, got)
			}
			switch true {
			case test.isover:
				if got, want := res.Scale(), MaxDecimal64Precision; got != want {
					t.Errorf("scale error exp %d, got %d\n", want, got)
				}
			case test.isunder:
				if got, want := res.Scale(), 0; got != want {
					t.Errorf("scale error exp %d, got %d\n", want, got)
				}
			default:
				if got, want := res.Scale(), test.quant; got != want {
					t.Errorf("scale error exp %d, got %d\n", want, got)
				}
			}
		})
	}
}

func TestDecimal64Round(t *testing.T) {
	var tests = []struct {
		name  string
		in    int64
		scale int
		out   int64
	}{
		// regular
		{name: "0", in: 0, scale: 2, out: 0},
		{name: "+24.51", in: 2451, scale: 2, out: 25},
		{name: "-24.51", in: -2451, scale: 2, out: -25},
		{name: "+24.5", in: 245, scale: 1, out: 24},
		{name: "-24.5", in: -245, scale: 1, out: -24},
		{name: "+23.51", in: 2351, scale: 2, out: 24},
		{name: "-23.51", in: -2351, scale: 2, out: -24},
		{name: "+23.5", in: 235, scale: 1, out: 24},
		{name: "-23.5", in: -235, scale: 1, out: -24},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dec := NewDecimal64(test.in, test.scale)
			if got, want := dec.RoundToInt64(), test.out; got != want {
				t.Errorf("value error exp %d, got %d\n", want, got)
			}
		})
	}
}

func TestDecimal64Compare(t *testing.T) {
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
		a    int64
		b    int64
		x    int    // scale A
		y    int    // scale B
		res  string // [01] for EQ, LT, LTE, GT, GTE
	}{
		// same scale, same sign
		{name: "=A+=B+", a: 1, b: 1, x: 1, y: 1, res: "10101"},
		{name: "=A+<B+", a: 1, b: 2, x: 1, y: 1, res: "01100"},
		{name: "=A+>B+", a: 2, b: 1, x: 1, y: 1, res: "00011"},
		// same scale, A-, B+
		{name: "=A-<B+", a: -1, b: 2, x: 1, y: 1, res: "01100"},
		{name: "=A->B+", a: -2, b: 1, x: 1, y: 1, res: "01100"},
		// same scale, A+, B-
		{name: "=A+<B-", a: 1, b: -2, x: 1, y: 1, res: "00011"},
		{name: "=A+>B-", a: 2, b: -1, x: 1, y: 1, res: "00011"},
		// same scale, A-, B-
		{name: "=A-=B-", a: -1, b: -1, x: 1, y: 1, res: "10101"},
		{name: "=A->B+", a: -1, b: -2, x: 1, y: 1, res: "00011"},
		{name: "=A-<B+", a: -2, b: -1, x: 1, y: 1, res: "01100"},

		// a<b scale, same sign
		{name: "<A+=B+", a: 1, b: 10, x: 1, y: 2, res: "10101"},
		{name: "<A+<B+", a: 1, b: 20, x: 1, y: 2, res: "01100"},
		{name: "<A+>B+", a: 2, b: 10, x: 1, y: 2, res: "00011"},
		// a<b scale, A-, B+
		{name: "<A-<B+", a: -1, b: 20, x: 1, y: 2, res: "01100"},
		{name: "<A->B+", a: -2, b: 10, x: 1, y: 2, res: "01100"},
		// a<b scale, A+, B-
		{name: "<A+<B-", a: 1, b: -20, x: 1, y: 2, res: "00011"},
		{name: "<A+>B-", a: 2, b: -10, x: 1, y: 2, res: "00011"},
		// a<b scale, A-, B-
		{name: "<A-=B-", a: -1, b: -10, x: 1, y: 2, res: "10101"},
		{name: "<A->B+", a: -1, b: -20, x: 1, y: 2, res: "00011"},
		{name: "<A-<B+", a: -2, b: -10, x: 1, y: 2, res: "01100"},

		// a>b scale, same sign
		{name: ">A+=B+", a: 100, b: 10, x: 3, y: 2, res: "10101"},
		{name: ">A+<B+", a: 100, b: 20, x: 3, y: 2, res: "01100"},
		{name: ">A+>B+", a: 200, b: 10, x: 3, y: 2, res: "00011"},
		// a>b scale, A-, B+
		{name: ">A-<B+", a: -100, b: 20, x: 3, y: 2, res: "01100"},
		{name: ">A->B+", a: -200, b: 10, x: 3, y: 2, res: "01100"},
		// a>b scale, A+, B-
		{name: ">A+<B-", a: 100, b: -20, x: 3, y: 2, res: "00011"},
		{name: ">A+>B-", a: 200, b: -10, x: 3, y: 2, res: "00011"},
		// a>b scale, A-, B-
		{name: ">A-=B-", a: -100, b: -10, x: 3, y: 2, res: "10101"},
		{name: ">A->B+", a: -100, b: -20, x: 3, y: 2, res: "00011"},
		{name: ">A-<B+", a: -200, b: -10, x: 3, y: 2, res: "01100"},
	}

	comp := func(s string) []bool {
		b := make([]bool, len(s))
		for i := range s {
			b[i] = s[i] == '1'
		}
		return b
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			A := NewDecimal64(test.a, test.x)
			B := NewDecimal64(test.b, test.y)
			cmp := comp(test.res)
			if got, want := A.Eq(B), cmp[0]; got != want {
				t.Errorf("equal error exp %t, got %t\n", want, got)
			}
			if got, want := A.Lt(B), cmp[1]; got != want {
				t.Errorf("lt error exp %t, got %t\n", want, got)
			}
			if got, want := A.Lte(B), cmp[2]; got != want {
				t.Errorf("lte error exp %t, got %t\n", want, got)
			}
			if got, want := A.Gt(B), cmp[3]; got != want {
				t.Errorf("gt error exp %t, got %t\n", want, got)
			}
			if got, want := A.Gte(B), cmp[4]; got != want {
				t.Errorf("gte error exp %t, got %t\n", want, got)
			}
		})
	}
}

var parse64Benchmarks = []string{
	"1.0",
	"1.000000000",
	"100000000.0",
	"0.000000001",
}

var marshal64Benchmarks = []struct {
	f float64
	s int
}{
	{f: 1.0, s: 1},
	{f: 1.000000000, s: 9},
	{f: 100000000.0, s: 1},
	{f: 0.000000001, s: 9},
}

func BenchmarkParseDecimal64(B *testing.B) {
	for _, v := range parse64Benchmarks {
		B.Run(v, func(B *testing.B) {
			B.ResetTimer()
			B.SetBytes(int64(len(v)))
			for i := 0; i < B.N; i++ {
				_, _ = ParseDecimal64(v)
			}
		})
	}
}

func BenchmarkMarshalDecimal64(B *testing.B) {
	for _, v := range marshal64Benchmarks {
		B.Run(strconv.FormatFloat(v.f, 'f', -1, 64), func(B *testing.B) {
			var dec Decimal64
			dec.SetFloat64(v.f, v.s)
			B.ResetTimer()
			B.SetBytes(8)
			for i := 0; i < B.N; i++ {
				_ = dec.String()
			}
		})
	}
}
