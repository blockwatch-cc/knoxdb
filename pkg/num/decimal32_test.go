// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"math"
	"strconv"
	"testing"
)

func TestDecimal32Numbers(t *testing.T) {
	var tests = []struct {
		name  string
		in    int32
		scale uint8
		prec  uint8
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
		{name: "0.1234567891", in: 1234567891, scale: 10, prec: 10, err: ErrScaleOverflow},
		{name: "-0.1234567891", in: -1234567891, scale: 10, prec: 10, err: ErrScaleOverflow},
		{name: "-scale", in: 11, scale: 255, prec: 2, err: ErrScaleOverflow},
		// extremes
		{name: "2.147483647", in: math.MaxInt32, scale: 9, prec: 10},
		{name: "-2.147483647", in: math.MinInt32, scale: 9, prec: 10},
		// precision
		{name: "2", in: 2, scale: 0, prec: 1},
		{name: "20", in: 20, scale: 0, prec: 2},
		{name: "200", in: 200, scale: 0, prec: 3},
		{name: "2000", in: 2000, scale: 0, prec: 4},
		{name: "20000", in: 20000, scale: 0, prec: 5},
		{name: "200000", in: 200000, scale: 0, prec: 6},
		{name: "2000000", in: 2000000, scale: 0, prec: 7},
		{name: "20000000", in: 20000000, scale: 0, prec: 8},
		{name: "200000000", in: 200000000, scale: 0, prec: 9},
		{name: "2000000000", in: 2000000000, scale: 0, prec: 10},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dec := NewDecimal32(test.in, test.scale)
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
			if got, want := dec.Int32(), test.in; got != want {
				t.Errorf("value error exp %d, got %d\n", want, got)
			}
		})
	}
}

func TestDecimal32Parse(t *testing.T) {
	var tests = []struct {
		name  string
		in    string
		out   int32
		scale uint8
		prec  uint8
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
		{in: "2.147483647", out: math.MaxInt32, scale: 9, prec: 10},
		{in: "-2.147483648", out: math.MinInt32, scale: 9, prec: 10},
		{in: "0.000000001", out: 1, scale: 9, prec: 1},
		{in: "-0.000000001", out: -1, scale: 9, prec: 1},
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
		{name: "int32+1 overflow", in: "2147483648", iserr: true},
		{name: "int32.1 overflow", in: "2.147483648", iserr: true},
		{name: "int32+N overflow", in: "20000000000", iserr: true},
		{name: "int32-1 underflow", in: "-2147483649", iserr: true},
		{name: "int32.1 underflow", in: "-2.147483649", iserr: true},
		{name: "int32-N underflow", in: "-20000000000", iserr: true},
		{name: "pos scale 10", in: "0.0000000001", iserr: true},
		{name: "neg scale 10", in: "-0.0000000001", iserr: true},
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
		{in: "1.0", out: 10, scale: 1, prec: 2},
		{in: "10.0", out: 100, scale: 1, prec: 3},
		{in: "100.0", out: 1000, scale: 1, prec: 4},
		{in: "1000.0", out: 10000, scale: 1, prec: 5},
		{in: "10000.0", out: 100000, scale: 1, prec: 6},
		{in: "100000.0", out: 1000000, scale: 1, prec: 7},
		{in: "1000000.0", out: 10000000, scale: 1, prec: 8},
		{in: "10000000.0", out: 100000000, scale: 1, prec: 9},
		{in: "100000000.0", out: 1000000000, scale: 1, prec: 10},
		{in: "0.1", out: 1, scale: 1, prec: 1},
		{in: "0.01", out: 1, scale: 2, prec: 1},
		{in: "0.001", out: 1, scale: 3, prec: 1},
		{in: "0.0001", out: 1, scale: 4, prec: 1},
		{in: "0.00001", out: 1, scale: 5, prec: 1},
		{in: "0.000001", out: 1, scale: 6, prec: 1},
		{in: "0.0000001", out: 1, scale: 7, prec: 1},
		{in: "0.00000001", out: 1, scale: 8, prec: 1},
		{in: "0.000000001", out: 1, scale: 9, prec: 1},
	}

	for _, test := range tests {
		name := test.name
		if name == "" {
			name = test.in
		}
		t.Run(name, func(t *testing.T) {
			dec, err := ParseDecimal32(test.in)
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
			if got, want := dec.Int32(), test.out; got != want {
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

func TestDecimal32SetFloat(t *testing.T) {
	var tests = []struct {
		name  string
		in    float64
		out   int32
		scale uint8
		prec  uint8
		iserr bool
	}{
		// regular
		{name: "0", in: 0.0, out: 0, scale: 0, prec: 0},
		{name: "-0", in: -1 * 0.0, out: 0, scale: 0, prec: 0},
		{name: "1234.56789", in: 1234.56789, out: 123456789, scale: 5, prec: 9},
		{name: "-1234.56789", in: -1234.56789, out: -123456789, scale: 5, prec: 9},
		{name: "+1234.56789", in: 1234.56789, out: 123456789, scale: 5, prec: 9},
		{name: "23.5", in: 23.5, out: 235, scale: 1, prec: 3},
		{name: "-23.5", in: -23.5, out: -235, scale: 1, prec: 3},
		{name: "23.51", in: 23.51, out: 2351, scale: 2, prec: 4},
		{name: "-23.51", in: -23.51, out: -2351, scale: 2, prec: 4},
		// extremes
		{name: "2.147483647", in: 2.147483647, out: math.MaxInt32, scale: 9, prec: 10},
		{name: "-2.147483648", in: -2.147483648, out: math.MinInt32, scale: 9, prec: 10},
		{name: "0.000000001", in: 0.000000001, out: 1, scale: 9, prec: 1},
		{name: "-0.000000001", in: -0.000000001, out: -1, scale: 9, prec: 1},
		// unusual
		{name: "0.0", in: 0.0, out: 0, scale: 0, prec: 0},
		// round to nearest even
		{name: "24.5", in: 24.5, out: 24, scale: 0, prec: 2},
		{name: "23.5", in: 23.5, out: 24, scale: 0, prec: 2},
		// invalid
		{name: "-scale", in: 1.0, scale: 255, iserr: true},
		{name: ">scale", in: 1.0, scale: 11, iserr: true},
		{name: "NaN", in: math.NaN(), iserr: true},
		{name: "+Inf", in: math.Inf(+1), iserr: true},
		{name: "-Inf", in: math.Inf(-1), iserr: true},
		{name: "int32+1 overflow", in: 2147483648.0, scale: 0, iserr: true},
		{name: "int32.1 overflow", in: 2.147483648, scale: 10, iserr: true},
		{name: "int32+N overflow", in: 20000000000.0, scale: 0, iserr: true},
		{name: "int32-1 underflow", in: -2147483649.0, scale: 0, iserr: true},
		{name: "int32.1 underflow", in: -2.147483649, scale: 10, iserr: true},
		{name: "int32-N underflow", in: -20000000000.0, scale: 0, iserr: true},
		// not error cases, will be rounded to nearest even
		// {name: "pos scale 10", in: 0.0000000001, iserr: true},
		// {name: "neg scale 10", in: -0.0000000001, iserr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var dec Decimal32
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
			if got, want := dec.Int32(), test.out; got != want {
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

func TestDecimal32Quantize(t *testing.T) {
	var tests = []struct {
		name    string
		in      int32
		scale   uint8
		quant   uint8
		out     int32
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
		{name: "down1_24.5", in: 245, scale: 1, quant: 0, out: 24},
		{name: "down1-24.5", in: -245, scale: 1, quant: 0, out: -24},
		{name: "down2+23.51", in: 2351, scale: 2, quant: 0, out: 24},
		{name: "down2-23.51", in: -2351, scale: 2, quant: 0, out: -24},
		{name: "down1+23.5", in: 235, scale: 1, quant: 0, out: 24},
		{name: "down1-23.5", in: -235, scale: 1, quant: 0, out: -24},
		// regular up
		{name: "up1+24.51", in: 2451, scale: 2, quant: 3, out: 24510},
		{name: "up1-24.51", in: -2451, scale: 2, quant: 3, out: -24510},
		// invalid scales are clipped
		{name: "neg_scale", in: 15, scale: 1, quant: 255, out: 1500000000, isover: true},
		{name: "big_scale", in: 15, scale: 1, quant: 11, out: 1500000000, isover: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dec := NewDecimal32(test.in, test.scale)
			res := dec.Quantize(test.quant)
			if got, want := res.Int32(), test.out; got != want {
				t.Errorf("value error exp %d, got %d\n", want, got)
			}
			switch {
			case test.isover:
				if got, want := res.Scale(), MaxDecimal32Precision; got != want {
					t.Errorf("scale error exp %d, got %d\n", want, got)
				}
			case test.isunder:
				if got, want := res.Scale(), uint8(0); got != want {
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

func TestDecimal32Round(t *testing.T) {
	var tests = []struct {
		name  string
		in    int32
		scale uint8
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
			dec := NewDecimal32(test.in, test.scale)
			if got, want := dec.RoundToInt64(), test.out; got != want {
				t.Errorf("value error exp %d, got %d\n", want, got)
			}
		})
	}
}

func TestDecimal32Compare(t *testing.T) {
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
		a    int32
		b    int32
		x    uint8  // scale A
		y    uint8  // scale B
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
			A := NewDecimal32(test.a, test.x)
			B := NewDecimal32(test.b, test.y)
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

var parse32Benchmarks = []string{
	"1.0",
	"1.000000000",
	"100000000.0",
	"0.000000001",
}

var marshal32Benchmarks = []struct {
	f float64
	s uint8
}{
	{f: 1.0, s: 1},
	{f: 1.000000000, s: 9},
	{f: 100000000.0, s: 1},
	{f: 0.000000001, s: 9},
}

func BenchmarkParseDecimal32(b *testing.B) {
	for _, v := range parse32Benchmarks {
		b.Run(v, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(len(v)))
			for i := 0; i < b.N; i++ {
				_, _ = ParseDecimal32(v)
			}
		})
	}
}

func BenchmarkParseFloat64(b *testing.B) {
	for _, v := range parse32Benchmarks {
		b.Run(v, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(len(v)))
			for i := 0; i < b.N; i++ {
				_, _ = strconv.ParseFloat(v, 64)
			}
		})
	}
}

func BenchmarkMarshalDecimal32(b *testing.B) {
	for _, v := range marshal32Benchmarks {
		b.Run(strconv.FormatFloat(v.f, 'f', -1, 64), func(b *testing.B) {
			var dec Decimal32
			dec.SetFloat64(v.f, v.s)
			b.ResetTimer()
			b.SetBytes(8)
			for i := 0; i < b.N; i++ {
				_ = dec.String()
			}
		})
	}
}

func BenchmarkMarshalFloat64(b *testing.B) {
	for _, v := range marshal32Benchmarks {
		b.Run(strconv.FormatFloat(v.f, 'f', -1, 64), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(8)
			for i := 0; i < b.N; i++ {
				_ = strconv.FormatFloat(v.f, 'f', int(v.s), 64)
			}
		})
	}
}
