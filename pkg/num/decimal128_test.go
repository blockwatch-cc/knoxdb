// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

func n128(hi int64, lo ...uint64) Int128 {
	switch {
	case len(lo) == 0 || lo[0] == 0:
		return Int128FromInt64(hi)
	default:
		i128 := Int128FromInt64(hi)
		for i := 0; i < len(lo); i++ {
			i128 = i128.Mul64(int64(pow10[digits64(int64(lo[i]))-1]))
		}
		return i128
	}
}

func e128(n int) Int128 {
	i := OneInt128
	for ; n > 18; n -= 18 {
		i = i.Mul64(int64(pow10[18]))
	}
	return i.Mul64(int64(pow10[n]))
}

func z(n int) string {
	return strings.Repeat("0", n)
}

func TestDecimal128Numbers(t *testing.T) {
	var tests = []struct {
		name  string
		in    Int128
		scale uint8
		prec  uint8
		err   error
	}{
		// regular
		{name: "0", in: ZeroInt128, scale: 0, prec: 0},
		{name: "-0", in: ZeroInt128, scale: 0, prec: 0},
		{name: "1234.56789", in: n128(123456789), scale: 5, prec: 9},
		{name: "-1234.56789", in: n128(-123456789), scale: 5, prec: 9},
		{name: "23.5", in: n128(235), scale: 1, prec: 3},
		{name: "-23.5", in: n128(-235), scale: 1, prec: 3},
		{name: "23.51", in: n128(2351), scale: 2, prec: 4},
		{name: "-23.51", in: n128(-2351), scale: 2, prec: 4},
		// invalid
		{name: "+scale", in: n128(1234567891234567891, 0), scale: 39, prec: 40, err: ErrScaleOverflow},
		{name: "+scale-", in: n128(-1234567891234567891, 0), scale: 39, prec: 40, err: ErrScaleOverflow},
		{name: "-scale", in: n128(1), scale: 255, prec: 2, err: ErrScaleOverflow},
		// extremes
		{name: "MAX", in: MaxInt128, scale: 38, prec: 39},
		{name: "MIN", in: MinInt128, scale: 38, prec: 39},
		// precision
		{name: "10e0", in: n128(1), scale: 0, prec: 1},
		{name: "10e1", in: e128(1), scale: 0, prec: 2},
		{name: "10e2", in: e128(2), scale: 0, prec: 3},
		{name: "10e3", in: e128(3), scale: 0, prec: 4},
		{name: "10e4", in: e128(4), scale: 0, prec: 5},
		{name: "10e5", in: e128(5), scale: 0, prec: 6},
		{name: "10e6", in: e128(6), scale: 0, prec: 7},
		{name: "10e7", in: e128(7), scale: 0, prec: 8},
		{name: "10e8", in: e128(8), scale: 0, prec: 9},
		{name: "10e9", in: e128(9), scale: 0, prec: 10},
		{name: "10e10", in: e128(10), scale: 0, prec: 11},
		{name: "10e11", in: e128(11), scale: 0, prec: 12},
		{name: "10e12", in: e128(12), scale: 0, prec: 13},
		{name: "10e13", in: e128(13), scale: 0, prec: 14},
		{name: "10e14", in: e128(14), scale: 0, prec: 15},
		{name: "10e15", in: e128(15), scale: 0, prec: 16},
		{name: "10e16", in: e128(16), scale: 0, prec: 17},
		{name: "10e17", in: e128(17), scale: 0, prec: 18},
		{name: "10e18", in: e128(18), scale: 0, prec: 19},
		{name: "10e19", in: e128(19), scale: 0, prec: 20},
		{name: "10e20", in: e128(20), scale: 0, prec: 21},
		{name: "10e21", in: e128(21), scale: 0, prec: 22},
		{name: "10e22", in: e128(22), scale: 0, prec: 23},
		{name: "10e23", in: e128(23), scale: 0, prec: 24},
		{name: "10e24", in: e128(24), scale: 0, prec: 25},
		{name: "10e25", in: e128(25), scale: 0, prec: 26},
		{name: "10e26", in: e128(26), scale: 0, prec: 27},
		{name: "10e27", in: e128(27), scale: 0, prec: 28},
		{name: "10e28", in: e128(28), scale: 0, prec: 29},
		{name: "10e29", in: e128(29), scale: 0, prec: 30},
		{name: "10e30", in: e128(30), scale: 0, prec: 31},
		{name: "10e31", in: e128(31), scale: 0, prec: 32},
		{name: "10e32", in: e128(32), scale: 0, prec: 33},
		{name: "10e33", in: e128(33), scale: 0, prec: 34},
		{name: "10e34", in: e128(34), scale: 0, prec: 35},
		{name: "10e35", in: e128(35), scale: 0, prec: 36},
		{name: "10e36", in: e128(36), scale: 0, prec: 37},
		{name: "10e37", in: e128(37), scale: 0, prec: 38},
		{name: "10e38", in: e128(38), scale: 0, prec: 39},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dec := NewDecimal128(test.in, test.scale)
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
			t.Logf("i128 = %s\n", dec)
			if got, want := dec.Scale(), test.scale; got != want {
				t.Errorf("scale error exp %d, got %d\n", want, got)
			}
			if got, want := dec.Precision(), test.prec; got != want {
				t.Errorf("precision error exp %d, got %d\n", want, got)
			}
			if got, want := dec.Int128(), test.in; got != want {
				t.Errorf("value error exp %d, got %d\n", want, got)
			}
		})
	}
}

func TestDecimal128Parse(t *testing.T) {
	var tests = []struct {
		name  string
		in    string
		out   Int128
		scale uint8
		prec  uint8
		iserr bool
		str   string
	}{
		// regular
		{in: "0", out: ZeroInt128, scale: 0, prec: 0},
		{in: "+0", out: ZeroInt128, scale: 0, prec: 0, str: "0"},
		{in: "-0", out: ZeroInt128, scale: 0, prec: 0, str: "0"},
		{in: "1234.56789", out: n128(123456789), scale: 5, prec: 9},
		{in: "-1234.56789", out: n128(-123456789), scale: 5, prec: 9},
		{in: "+1234.56789", out: n128(123456789), scale: 5, prec: 9, str: "1234.56789"},
		{in: "23.5", out: n128(235), scale: 1, prec: 3},
		{in: "-23.5", out: n128(-235), scale: 1, prec: 3},
		{in: "23.51", out: n128(2351), scale: 2, prec: 4},
		{in: "-23.51", out: n128(-2351), scale: 2, prec: 4},
		// extremes
		{name: "MaxInt128", in: "1.70141183460469231731687303715884105727", out: MaxInt128, scale: 38, prec: 39},
		{name: "MinInt128", in: "-1.70141183460469231731687303715884105728", out: MinInt128, scale: 38, prec: 39},
		{name: "Small+", in: "0.00000000000000000000000000000000000001", out: n128(1), scale: 38, prec: 1},
		{name: "Small-", in: "-0.00000000000000000000000000000000000001", out: n128(-1), scale: 38, prec: 1},
		// unusual
		{name: "lead-0", in: "00.1", out: n128(1), scale: 1, prec: 1, str: "0.1"},
		{in: "0." + z(1), out: ZeroInt128, scale: 1, prec: 0},
		{in: "0." + z(2), out: ZeroInt128, scale: 2, prec: 0},
		{in: "0." + z(3), out: ZeroInt128, scale: 3, prec: 0},
		{in: "0." + z(4), out: ZeroInt128, scale: 4, prec: 0},
		{in: "0." + z(5), out: ZeroInt128, scale: 5, prec: 0},
		{in: "0." + z(6), out: ZeroInt128, scale: 6, prec: 0},
		{in: "0." + z(7), out: ZeroInt128, scale: 7, prec: 0},
		{in: "0." + z(8), out: ZeroInt128, scale: 8, prec: 0},
		{in: "0." + z(9), out: ZeroInt128, scale: 9, prec: 0},
		{in: "0." + z(10), out: ZeroInt128, scale: 10, prec: 0},
		{in: "0." + z(11), out: ZeroInt128, scale: 11, prec: 0},
		{in: "0." + z(12), out: ZeroInt128, scale: 12, prec: 0},
		{in: "0." + z(13), out: ZeroInt128, scale: 13, prec: 0},
		{in: "0." + z(14), out: ZeroInt128, scale: 14, prec: 0},
		{in: "0." + z(15), out: ZeroInt128, scale: 15, prec: 0},
		{in: "0." + z(16), out: ZeroInt128, scale: 16, prec: 0},
		{in: "0." + z(17), out: ZeroInt128, scale: 17, prec: 0},
		{in: "0." + z(18), out: ZeroInt128, scale: 18, prec: 0},
		{in: "0." + z(19), out: ZeroInt128, scale: 19, prec: 0},
		{in: "0." + z(20), out: ZeroInt128, scale: 20, prec: 0},
		{in: "0." + z(21), out: ZeroInt128, scale: 21, prec: 0},
		{in: "0." + z(22), out: ZeroInt128, scale: 22, prec: 0},
		{in: "0." + z(23), out: ZeroInt128, scale: 23, prec: 0},
		{in: "0." + z(24), out: ZeroInt128, scale: 24, prec: 0},
		{in: "0." + z(25), out: ZeroInt128, scale: 25, prec: 0},
		{in: "0." + z(26), out: ZeroInt128, scale: 26, prec: 0},
		{in: "0." + z(27), out: ZeroInt128, scale: 27, prec: 0},
		{in: "0." + z(28), out: ZeroInt128, scale: 28, prec: 0},
		{in: "0." + z(29), out: ZeroInt128, scale: 29, prec: 0},
		{in: "0." + z(30), out: ZeroInt128, scale: 30, prec: 0},
		{in: "0." + z(31), out: ZeroInt128, scale: 31, prec: 0},
		{in: "0." + z(32), out: ZeroInt128, scale: 32, prec: 0},
		{in: "0." + z(33), out: ZeroInt128, scale: 33, prec: 0},
		{in: "0." + z(34), out: ZeroInt128, scale: 34, prec: 0},
		{in: "0." + z(35), out: ZeroInt128, scale: 35, prec: 0},
		{in: "0." + z(36), out: ZeroInt128, scale: 36, prec: 0},
		{in: "0." + z(37), out: ZeroInt128, scale: 37, prec: 0},
		{in: "0." + z(38), out: ZeroInt128, scale: 38, prec: 0},
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
		{name: "int128+1 overflow", in: "170141183460469231731687303715884105728", iserr: true},
		{name: "int128.1 overflow", in: "1.70141183460469231731687303715884105728", iserr: true},
		{name: "int128+N overflow", in: "1000000000000000000000000000000000000000", iserr: true},
		{name: "int128-1 underflow", in: "-170141183460469231731687303715884105729", iserr: true},
		{name: "int128.1 underflow", in: "-1.70141183460469231731687303715884105729", iserr: true},
		{name: "int128-N underflow", in: "-1000000000000000000000000000000000000000", iserr: true},
		{name: "pos scale 39", in: "0.000000000000000000000000000000000000001", iserr: true},
		{name: "neg scale 39", in: "-0.000000000000000000000000000000000000001", iserr: true},
		// precision
		{in: "1", out: e128(0), scale: 0, prec: 1},
		{in: "1." + z(1), out: e128(1), scale: 1, prec: 2},
		{in: "1." + z(2), out: e128(2), scale: 2, prec: 3},
		{in: "1." + z(3), out: e128(3), scale: 3, prec: 4},
		{in: "1." + z(4), out: e128(4), scale: 4, prec: 5},
		{in: "1." + z(5), out: e128(5), scale: 5, prec: 6},
		{in: "1." + z(6), out: e128(6), scale: 6, prec: 7},
		{in: "1." + z(7), out: e128(7), scale: 7, prec: 8},
		{in: "1." + z(8), out: e128(8), scale: 8, prec: 9},
		{in: "1." + z(9), out: e128(9), scale: 9, prec: 10},
		{in: "1." + z(10), out: e128(10), scale: 10, prec: 11},
		{in: "1." + z(11), out: e128(11), scale: 11, prec: 12},
		{in: "1." + z(12), out: e128(12), scale: 12, prec: 13},
		{in: "1." + z(13), out: e128(13), scale: 13, prec: 14},
		{in: "1." + z(14), out: e128(14), scale: 14, prec: 15},
		{in: "1." + z(15), out: e128(15), scale: 15, prec: 16},
		{in: "1." + z(16), out: e128(16), scale: 16, prec: 17},
		{in: "1." + z(17), out: e128(17), scale: 17, prec: 18},
		{in: "1." + z(18), out: e128(18), scale: 18, prec: 19},
		{in: "1." + z(19), out: e128(19), scale: 19, prec: 20},
		{in: "1." + z(20), out: e128(20), scale: 20, prec: 21},
		{in: "1." + z(21), out: e128(21), scale: 21, prec: 22},
		{in: "1." + z(22), out: e128(22), scale: 22, prec: 23},
		{in: "1." + z(23), out: e128(23), scale: 23, prec: 24},
		{in: "1." + z(24), out: e128(24), scale: 24, prec: 25},
		{in: "1." + z(25), out: e128(25), scale: 25, prec: 26},
		{in: "1." + z(26), out: e128(26), scale: 26, prec: 27},
		{in: "1." + z(27), out: e128(27), scale: 27, prec: 28},
		{in: "1." + z(28), out: e128(28), scale: 28, prec: 29},
		{in: "1." + z(29), out: e128(29), scale: 29, prec: 30},
		{in: "1." + z(30), out: e128(30), scale: 30, prec: 31},
		{in: "1." + z(31), out: e128(31), scale: 31, prec: 32},
		{in: "1." + z(32), out: e128(32), scale: 32, prec: 33},
		{in: "1." + z(33), out: e128(33), scale: 33, prec: 34},
		{in: "1." + z(34), out: e128(34), scale: 34, prec: 35},
		{in: "1." + z(35), out: e128(35), scale: 35, prec: 36},
		{in: "1." + z(36), out: e128(36), scale: 36, prec: 37},
		{in: "1." + z(37), out: e128(37), scale: 37, prec: 38},
		{in: "1." + z(38), out: e128(38), scale: 38, prec: 39},
		{in: "1.0", out: e128(1), scale: 1, prec: 2},
		{in: "1" + z(1) + ".0", out: e128(2), scale: 1, prec: 3},
		{in: "1" + z(2) + ".0", out: e128(3), scale: 1, prec: 4},
		{in: "1" + z(3) + ".0", out: e128(4), scale: 1, prec: 5},
		{in: "1" + z(4) + ".0", out: e128(5), scale: 1, prec: 6},
		{in: "1" + z(5) + ".0", out: e128(6), scale: 1, prec: 7},
		{in: "1" + z(6) + ".0", out: e128(7), scale: 1, prec: 8},
		{in: "1" + z(7) + ".0", out: e128(8), scale: 1, prec: 9},
		{in: "1" + z(8) + ".0", out: e128(9), scale: 1, prec: 10},
		{in: "1" + z(9) + ".0", out: e128(10), scale: 1, prec: 11},
		{in: "1" + z(10) + ".0", out: e128(11), scale: 1, prec: 12},
		{in: "1" + z(11) + ".0", out: e128(12), scale: 1, prec: 13},
		{in: "1" + z(12) + ".0", out: e128(13), scale: 1, prec: 14},
		{in: "1" + z(13) + ".0", out: e128(14), scale: 1, prec: 15},
		{in: "1" + z(14) + ".0", out: e128(15), scale: 1, prec: 16},
		{in: "1" + z(15) + ".0", out: e128(16), scale: 1, prec: 17},
		{in: "1" + z(16) + ".0", out: e128(17), scale: 1, prec: 18},
		{in: "1" + z(17) + ".0", out: e128(18), scale: 1, prec: 19},
		{in: "1" + z(18) + ".0", out: e128(19), scale: 1, prec: 20},
		{in: "1" + z(19) + ".0", out: e128(20), scale: 1, prec: 21},
		{in: "1" + z(20) + ".0", out: e128(21), scale: 1, prec: 22},
		{in: "1" + z(21) + ".0", out: e128(22), scale: 1, prec: 23},
		{in: "1" + z(22) + ".0", out: e128(23), scale: 1, prec: 24},
		{in: "1" + z(23) + ".0", out: e128(24), scale: 1, prec: 25},
		{in: "1" + z(24) + ".0", out: e128(25), scale: 1, prec: 26},
		{in: "1" + z(25) + ".0", out: e128(26), scale: 1, prec: 27},
		{in: "1" + z(26) + ".0", out: e128(27), scale: 1, prec: 28},
		{in: "1" + z(27) + ".0", out: e128(28), scale: 1, prec: 29},
		{in: "1" + z(28) + ".0", out: e128(29), scale: 1, prec: 30},
		{in: "1" + z(29) + ".0", out: e128(30), scale: 1, prec: 31},
		{in: "1" + z(30) + ".0", out: e128(31), scale: 1, prec: 32},
		{in: "1" + z(31) + ".0", out: e128(32), scale: 1, prec: 33},
		{in: "1" + z(32) + ".0", out: e128(33), scale: 1, prec: 34},
		{in: "1" + z(33) + ".0", out: e128(34), scale: 1, prec: 35},
		{in: "1" + z(34) + ".0", out: e128(35), scale: 1, prec: 36},
		{in: "1" + z(35) + ".0", out: e128(36), scale: 1, prec: 37},
		{in: "1" + z(36) + ".0", out: e128(37), scale: 1, prec: 38},
		{in: "1" + z(37) + ".0", out: e128(38), scale: 1, prec: 39},
		{in: "0.1", out: n128(1), scale: 1, prec: 1},
		{in: "0." + z(1) + "1", out: n128(1), scale: 2, prec: 1},
		{in: "0." + z(2) + "1", out: n128(1), scale: 3, prec: 1},
		{in: "0." + z(3) + "1", out: n128(1), scale: 4, prec: 1},
		{in: "0." + z(4) + "1", out: n128(1), scale: 5, prec: 1},
		{in: "0." + z(5) + "1", out: n128(1), scale: 6, prec: 1},
		{in: "0." + z(6) + "1", out: n128(1), scale: 7, prec: 1},
		{in: "0." + z(7) + "1", out: n128(1), scale: 8, prec: 1},
		{in: "0." + z(8) + "1", out: n128(1), scale: 9, prec: 1},
		{in: "0." + z(9) + "1", out: n128(1), scale: 10, prec: 1},
		{in: "0." + z(10) + "1", out: n128(1), scale: 11, prec: 1},
		{in: "0." + z(11) + "1", out: n128(1), scale: 12, prec: 1},
		{in: "0." + z(12) + "1", out: n128(1), scale: 13, prec: 1},
		{in: "0." + z(13) + "1", out: n128(1), scale: 14, prec: 1},
		{in: "0." + z(14) + "1", out: n128(1), scale: 15, prec: 1},
		{in: "0." + z(15) + "1", out: n128(1), scale: 16, prec: 1},
		{in: "0." + z(16) + "1", out: n128(1), scale: 17, prec: 1},
		{in: "0." + z(17) + "1", out: n128(1), scale: 18, prec: 1},
		{in: "0." + z(18) + "1", out: n128(1), scale: 19, prec: 1},
		{in: "0." + z(19) + "1", out: n128(1), scale: 20, prec: 1},
		{in: "0." + z(20) + "1", out: n128(1), scale: 21, prec: 1},
		{in: "0." + z(21) + "1", out: n128(1), scale: 22, prec: 1},
		{in: "0." + z(22) + "1", out: n128(1), scale: 23, prec: 1},
		{in: "0." + z(23) + "1", out: n128(1), scale: 24, prec: 1},
		{in: "0." + z(24) + "1", out: n128(1), scale: 25, prec: 1},
		{in: "0." + z(25) + "1", out: n128(1), scale: 26, prec: 1},
		{in: "0." + z(26) + "1", out: n128(1), scale: 27, prec: 1},
		{in: "0." + z(27) + "1", out: n128(1), scale: 28, prec: 1},
		{in: "0." + z(28) + "1", out: n128(1), scale: 29, prec: 1},
		{in: "0." + z(29) + "1", out: n128(1), scale: 30, prec: 1},
		{in: "0." + z(30) + "1", out: n128(1), scale: 31, prec: 1},
		{in: "0." + z(31) + "1", out: n128(1), scale: 32, prec: 1},
		{in: "0." + z(32) + "1", out: n128(1), scale: 33, prec: 1},
		{in: "0." + z(33) + "1", out: n128(1), scale: 34, prec: 1},
		{in: "0." + z(34) + "1", out: n128(1), scale: 35, prec: 1},
		{in: "0." + z(35) + "1", out: n128(1), scale: 36, prec: 1},
		{in: "0." + z(36) + "1", out: n128(1), scale: 37, prec: 1},
		{in: "0." + z(37) + "1", out: n128(1), scale: 38, prec: 1},
	}

	for _, test := range tests {
		name := test.name
		if name == "" {
			name = test.in
		}
		t.Run(name, func(t *testing.T) {
			dec, err := ParseDecimal128(test.in)
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
			if got, want := dec.Int128(), test.out; got != want {
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

func TestDecimal128SetFloat(t *testing.T) {
	var tests = []struct {
		name  string
		in    float64
		out   Int128
		scale uint8
		prec  uint8
		iserr bool
	}{
		// regular
		{name: "0", in: 0.0, out: n128(0), scale: 0, prec: 0},
		{name: "-0", in: -1 * 0.0, out: n128(0), scale: 0, prec: 0},
		{name: "1234.56789", in: 1234.56789, out: n128(123456789), scale: 5, prec: 9},
		{name: "-1234.56789", in: -1234.56789, out: n128(-123456789), scale: 5, prec: 9},
		{name: "+1234.56789", in: 1234.56789, out: n128(123456789), scale: 5, prec: 9},
		{name: "23.5", in: 23.5, out: n128(235), scale: 1, prec: 3},
		{name: "-23.5", in: -23.5, out: n128(-235), scale: 1, prec: 3},
		{name: "23.51", in: 23.51, out: n128(2351), scale: 2, prec: 4},
		{name: "-23.51", in: -23.51, out: n128(-2351), scale: 2, prec: 4},
		// extremes
		// Note: MaxInt64-1 does not fit into float64
		// nearest float to MaxInt64 is MaxInt64+1, so we must use Int128{0, 1<<63}
		// nearest float to MinInt64 is MinInt64
		// nearest float to MaxInt64 is MaxInt128+1, but we correct for this case internally
		// nearest float to MinInt128 is MinInt128
		{name: "MaxInt64", in: 9.223372036854775807, out: Int128{0, 1 << 63}, scale: 18, prec: 19},
		{name: "MinInt64", in: -9.223372036854775808, out: Int128FromInt64(-1 << 63), scale: 18, prec: 19},
		{name: "MaxInt128", in: 1.70141183460469231731687303715884105727, out: MaxInt128, scale: 38, prec: 39},
		{name: "MinInt128", in: -1.70141183460469231731687303715884105727, out: MinInt128, scale: 38, prec: 39},
		// max safe integer (53 bit precision)
		{name: "9.007199254740991", in: 9.007199254740991, out: n128(1<<53 - 1), scale: 15, prec: 16},
		{name: "-9.007199254740992", in: -9.007199254740992, out: n128(-1 << 53), scale: 15, prec: 16},
		{name: "0.000000000000000001", in: 0.000000000000000001, out: n128(1), scale: 18, prec: 1},
		{name: "-0.000000000000000001", in: -0.000000000000000001, out: n128(-1), scale: 18, prec: 1},
		// unusual
		{name: "0.0", in: 0.0, out: n128(0), scale: 0, prec: 0},
		// round to nearest even
		{name: "24.5", in: 24.5, out: n128(24), scale: 0, prec: 2},
		{name: "23.5", in: 23.5, out: n128(24), scale: 0, prec: 2},
		// invalid
		{name: "-scale", in: 1.0, scale: 255, iserr: true},
		{name: ">scale", in: 1.0, scale: 39, iserr: true},
		{name: "NaN", in: math.NaN(), iserr: true},
		{name: "+Inf", in: math.Inf(+1), iserr: true},
		{name: "-Inf", in: math.Inf(-1), iserr: true},
		// Note: float64 rounds down to nearest power of 2 which in these cases
		// is = Min/MaxInt128; for this reason use the next representable float64 value
		{name: "int128+1 overflow", in: 170141183460469269510619166673045815296.0, scale: 0, iserr: true},
		{name: "int128.1 overflow", in: 1.70141183460469269510619166673045815296, scale: 38, iserr: true},
		{name: "int128+N overflow", in: 1000000000000000000000000000000000000000.0, scale: 0, iserr: true},
		{name: "int128-1 underflow", in: -170141183460469269510619166673045815296.0, scale: 0, iserr: true},
		{name: "int128.1 underflow", in: -1.70141183460469269510619166673045815296, scale: 38, iserr: true},
		{name: "int128-N underflow", in: -1000000000000000000000000000000000000000.0, scale: 0, iserr: true},
		// not error cases, will be rounded to nearest even
		// {name: "pos scale 19", in: 0.0000000000000000001, iserr: true},
		// {name: "neg scale 19", in: -0.0000000000000000001, iserr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var dec Decimal128
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
			if got, want := dec.Int128(), test.out; got != want {
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

func TestDecimal128Quantize(t *testing.T) {
	var tests = []struct {
		name    string
		in      Int128
		scale   uint8
		quant   uint8
		out     Int128
		isover  bool
		isunder bool
	}{
		// regular no-change
		{name: "no-change_24.51", in: n128(2451), scale: 2, quant: 2, out: n128(2451)},
		{name: "no+change_24.51", in: n128(-2451), scale: 2, quant: 2, out: n128(-2451)},
		// regular down
		{name: "down1+24.51", in: n128(2451), scale: 2, quant: 1, out: n128(245)},
		{name: "down1-24.51", in: n128(-2451), scale: 2, quant: 1, out: n128(-245)},
		{name: "down2+24.51", in: n128(2451), scale: 2, quant: 0, out: n128(25)},
		{name: "down2-24.51", in: n128(-2451), scale: 2, quant: 0, out: n128(-25)},
		{name: "down1+24.5", in: n128(245), scale: 1, quant: 0, out: n128(24)},
		{name: "down1-24.5", in: n128(-245), scale: 1, quant: 0, out: n128(-24)},
		{name: "down2+23.51", in: n128(2351), scale: 2, quant: 0, out: n128(24)},
		{name: "down2-23.51", in: n128(-2351), scale: 2, quant: 0, out: n128(-24)},
		{name: "down1+23.5", in: n128(235), scale: 1, quant: 0, out: n128(24)},
		{name: "down1-23.5", in: n128(-235), scale: 1, quant: 0, out: n128(-24)},
		// regular up
		{name: "up1+24.51", in: n128(2451), scale: 2, quant: 3, out: n128(24510)},
		{name: "up1-24.51", in: n128(-2451), scale: 2, quant: 3, out: n128(-24510)},
		// invalid scales are clipped
		{name: "neg_scale", in: n128(15), scale: 1, quant: 255, out: Int128FromInt64(15).Mul(e128(37)), isover: true},
		{name: "big_scale", in: n128(15), scale: 1, quant: 39, out: Int128FromInt64(15).Mul(e128(37)), isover: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dec := NewDecimal128(test.in, test.scale)
			res := dec.Quantize(test.quant)
			if got, want := res.Int128(), test.out; got != want {
				t.Errorf("value error exp %d, got %d\n", want, got)
			}
			switch {
			case test.isover:
				if got, want := res.Scale(), MaxDecimal128Precision; got != want {
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

func TestDecimal128Round(t *testing.T) {
	var tests = []struct {
		name  string
		in    Int128
		scale uint8
		out   int64
	}{
		// regular
		{name: "0", in: n128(0), scale: 2, out: 0},
		{name: "+24.51", in: n128(2451), scale: 2, out: 25},
		{name: "-24.51", in: n128(-2451), scale: 2, out: -25},
		{name: "+24.5", in: n128(245), scale: 1, out: 24},
		{name: "-24.5", in: n128(-245), scale: 1, out: -24},
		{name: "+23.51", in: n128(2351), scale: 2, out: 24},
		{name: "-23.51", in: n128(-2351), scale: 2, out: -24},
		{name: "+23.5", in: n128(235), scale: 1, out: 24},
		{name: "-23.5", in: n128(-235), scale: 1, out: -24},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dec := NewDecimal128(test.in, test.scale)
			t.Logf("RES %s \n", dec)
			if got, want := dec.RoundToInt64(), test.out; got != want {
				t.Errorf("value error exp %d, got %d\n", want, got)
			}
		})
	}
}

func TestDecimal128Compare(t *testing.T) {
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
		a    Int128
		b    Int128
		x    uint8  // scale A
		y    uint8  // scale B
		res  string // [01] for EQ, LT, LTE, GT, GTE
	}{
		// same scale, same sign
		{name: "=A+=B+", a: n128(1), b: n128(1), x: 1, y: 1, res: "10101"},
		{name: "=A+<B+", a: n128(1), b: n128(2), x: 1, y: 1, res: "01100"},
		{name: "=A+>B+", a: n128(2), b: n128(1), x: 1, y: 1, res: "00011"},
		// same scale, A-, B+
		{name: "=A-<B+", a: n128(-1), b: n128(2), x: 1, y: 1, res: "01100"},
		{name: "=A->B+", a: n128(-2), b: n128(1), x: 1, y: 1, res: "01100"},
		// same scale, A+, B-
		{name: "=A+<B-", a: n128(1), b: n128(-2), x: 1, y: 1, res: "00011"},
		{name: "=A+>B-", a: n128(2), b: n128(-1), x: 1, y: 1, res: "00011"},
		// same scale, A-, B-
		{name: "=A-=B-", a: n128(-1), b: n128(-1), x: 1, y: 1, res: "10101"},
		{name: "=A->B+", a: n128(-1), b: n128(-2), x: 1, y: 1, res: "00011"},
		{name: "=A-<B+", a: n128(-2), b: n128(-1), x: 1, y: 1, res: "01100"},

		// a<b scale, same sign
		{name: "<A+=B+", a: n128(1), b: n128(10), x: 1, y: 2, res: "10101"},
		{name: "<A+<B+", a: n128(1), b: n128(20), x: 1, y: 2, res: "01100"},
		{name: "<A+>B+", a: n128(2), b: n128(10), x: 1, y: 2, res: "00011"},
		// a<b scale, A-, B+
		{name: "<A-<B+", a: n128(-1), b: n128(20), x: 1, y: 2, res: "01100"},
		{name: "<A->B+", a: n128(-2), b: n128(10), x: 1, y: 2, res: "01100"},
		// a<b scale, A+, B-
		{name: "<A+<B-", a: n128(1), b: n128(-20), x: 1, y: 2, res: "00011"},
		{name: "<A+>B-", a: n128(2), b: n128(-10), x: 1, y: 2, res: "00011"},
		// a<b scale, A-, B-
		{name: "<A-=B-", a: n128(-1), b: n128(-10), x: 1, y: 2, res: "10101"},
		{name: "<A->B+", a: n128(-1), b: n128(-20), x: 1, y: 2, res: "00011"},
		{name: "<A-<B+", a: n128(-2), b: n128(-10), x: 1, y: 2, res: "01100"},

		// a>b scale, same sign
		{name: ">A+=B+", a: n128(100), b: n128(10), x: 3, y: 2, res: "10101"},
		{name: ">A+<B+", a: n128(100), b: n128(20), x: 3, y: 2, res: "01100"},
		{name: ">A+>B+", a: n128(200), b: n128(10), x: 3, y: 2, res: "00011"},
		// a>b scale, A-, B+
		{name: ">A-<B+", a: n128(-100), b: n128(20), x: 3, y: 2, res: "01100"},
		{name: ">A->B+", a: n128(-200), b: n128(10), x: 3, y: 2, res: "01100"},
		// a>b scale, A+, B-
		{name: ">A+<B-", a: n128(100), b: n128(-20), x: 3, y: 2, res: "00011"},
		{name: ">A+>B-", a: n128(200), b: n128(-10), x: 3, y: 2, res: "00011"},
		// a>b scale, A-, B-
		{name: ">A-=B-", a: n128(-100), b: n128(-10), x: 3, y: 2, res: "10101"},
		{name: ">A->B+", a: n128(-100), b: n128(-20), x: 3, y: 2, res: "00011"},
		{name: ">A-<B+", a: n128(-200), b: n128(-10), x: 3, y: 2, res: "01100"},
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
			A := NewDecimal128(test.a, test.x)
			B := NewDecimal128(test.b, test.y)
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

var parse128Benchmarks = []string{
	"1.0",
	"1.000000000",
	"100000000.0",
	"0.000000001",
}

var marshal128Benchmarks = []struct {
	f float64
	s uint8
}{
	{f: 1.0, s: 1},
	{f: 1.000000000, s: 9},
	{f: 100000000.0, s: 1},
	{f: 0.000000001, s: 9},
}

func BenchmarkParseDecimal128(b *testing.B) {
	for _, v := range parse128Benchmarks {
		b.Run(v, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(len(v)))
			for i := 0; i < b.N; i++ {
				_, _ = ParseDecimal128(v)
			}
		})
	}
}

func BenchmarkMarshalDecimal128(b *testing.B) {
	for _, v := range marshal128Benchmarks {
		b.Run(strconv.FormatFloat(v.f, 'f', -1, 64), func(b *testing.B) {
			var dec Decimal128
			dec.SetFloat64(v.f, v.s)
			b.ResetTimer()
			b.SetBytes(8)
			for i := 0; i < b.N; i++ {
				_ = dec.String()
			}
		})
	}
}
