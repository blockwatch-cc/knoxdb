// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"fmt"
	"math"
	"math/bits"
	"math/rand"

	"golang.org/x/exp/slices"
)

func RandFloat64Slice(n, u int) []float64 {
	s := make([]float64, n*u)
	for i := 0; i < n; i++ {
		s[i] = rand.Float64()
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

type Float64MatchTest struct {
	Name   string
	Slice  []float64
	Match  float64
	Match2 float64
	Result []byte
	Count  int64
}

var (
	f64_s0 = []float64{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	f64_eq_m0    float64 = 5
	f64_eq_res_0         = []byte{0x6a, 0x1e, 0x48, 0x2c}

	f64_ne_m0    float64 = 5
	f64_ne_res_0         = []byte{0x95, 0xe1, 0xb7, 0xd3}

	f64_lt_mat_0 float64 = 5
	f64_lt_res_0         = []byte{0x05, 0x21, 0x27, 0x01}

	f64_le_mat_0 float64 = 5
	f64_le_res_0         = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	f64_gt_mat_0 float64 = 5
	f64_gt_res_0         = []byte{0x90, 0xc0, 0x90, 0xd2}

	f64_ge_mat_0 float64 = 5
	f64_ge_res_0         = []byte{0xfa, 0xde, 0xd8, 0xfe}

	f64_bw_mat_0a float64 = 5
	f64_bw_mat_0b float64 = 10
	f64_bw_res_0          = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	// positive int values only
	f64_s1 = []float64{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 500000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	f64_eq_res_1         = []byte{0x41, 0x42, 0xc4, 0x0e}
	f64_eq_mat_1 float64 = 5

	f64_ne_res_1         = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	f64_ne_mat_1 float64 = 5

	f64_lt_res_1         = []byte{0x0e, 0x00, 0x00, 0x00}
	f64_lt_mat_1 float64 = 5

	f64_le_res_1         = []byte{0x4f, 0x42, 0xc4, 0x0e}
	f64_le_mat_1 float64 = 5

	f64_gt_res_1         = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	f64_gt_mat_1 float64 = 5

	f64_ge_res_1         = []byte{0xf1, 0xff, 0xff, 0xff}
	f64_ge_mat_1 float64 = 5

	f64_bw_res_1          = []byte{0xf1, 0x42, 0xc4, 0x0e}
	f64_bw_mat_1a float64 = 5
	f64_bw_mat_1b float64 = 10

	// negative and positive values mixed
	f64_s2 = []float64{
		-5.12, 2.5, -3.1, 5.45,
		7.125, 8.2, 9.4, -10.25,
		15.25, 50.25, 55.25, 500.25,
		1000.25, -500000.25, 113.25, 12.25,
		31.25, 32.25, 33.25, 34.25,
		35, -36, 37.25, 38.25,
		39.25, 40.25, -41.25, 42.25,
		43.25, 44.25, 45.25, -46.25,
	}
	f64_eq_res_2         = []byte{0x01, 0x0, 0x0, 0x0}
	f64_eq_mat_2 float64 = -5.12

	f64_ne_res_2         = []byte{0xfe, 0xff, 0xff, 0xff}
	f64_ne_mat_2 float64 = -5.12

	f64_lt_res_2         = []byte{0x80, 0x20, 0x20, 0x84}
	f64_lt_mat_2 float64 = -5.12

	f64_le_res_2         = []byte{0x81, 0x20, 0x20, 0x84}
	f64_le_mat_2 float64 = -5.12

	f64_gt_res_2         = []byte{0x7e, 0xdf, 0xdf, 0x7b}
	f64_gt_mat_2 float64 = -5.12

	f64_ge_res_2         = []byte{0x7f, 0xdf, 0xdf, 0x7b}
	f64_ge_mat_2 float64 = -5.12

	f64_bw_res_2          = []byte{0x7f, 0x00, 0x00, 0x00}
	f64_bw_mat_2a float64 = -5.12
	f64_bw_mat_2b float64 = 10

	// extreme values
	f64_s3 = []float64{
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
	}
	f64_eq_res_3         = []byte{0x44, 0x44, 0x44, 0x44}
	f64_eq_mat_3 float64 = math.MaxFloat64

	f64_ne_res_3         = []byte{0xbb, 0xbb, 0xbb, 0xbb}
	f64_ne_mat_3 float64 = math.MaxFloat64

	f64_lt_res_3         = []byte{0xbb, 0xbb, 0xbb, 0xbb}
	f64_lt_mat_3 float64 = math.MaxFloat64

	f64_le_res_3         = []byte{0xff, 0xff, 0xff, 0xff}
	f64_le_mat_3 float64 = math.MaxFloat64

	f64_gt_res_3         = []byte{0x0, 0x0, 0x0, 0x0}
	f64_gt_mat_3 float64 = math.MaxFloat64

	f64_ge_res_3         = []byte{0x44, 0x44, 0x44, 0x44}
	f64_ge_mat_3 float64 = math.MaxFloat64

	f64_bw_res_3          = []byte{0x55, 0x55, 0x55, 0x55}
	f64_bw_mat_3a float64 = math.MaxFloat32
	f64_bw_mat_3b float64 = math.MaxFloat64

	// NaN/Inf values
	f64_s4 = []float64{
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
	}
	f64_eq_res_4         = []byte{0x0, 0x0, 0x0, 0x0}
	f64_eq_mat_4 float64 = math.NaN()

	f64_ne_res_4         = []byte{0xff, 0xff, 0xff, 0xff}
	f64_ne_mat_4 float64 = math.NaN()

	f64_lt_res_4         = []byte{0x0, 0x0, 0x0, 0x0}
	f64_lt_mat_4 float64 = math.NaN()

	f64_le_res_4         = []byte{0x0, 0x0, 0x0, 0x0}
	f64_le_mat_4 float64 = math.NaN()

	f64_gt_res_4         = []byte{0x0, 0x0, 0x0, 0x0}
	f64_gt_mat_4 float64 = math.NaN()

	f64_ge_res_4         = []byte{0x0, 0x0, 0x0, 0x0}
	f64_ge_mat_4 float64 = math.NaN()

	f64_bw_res_4          = []byte{0x0, 0x0, 0x0, 0x0}
	f64_bw_mat_4a float64 = math.NaN()
	f64_bw_mat_4b float64 = math.NaN()
)

// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mkF64(name string, src []float64, match, match2 float64, result []byte, length int) Float64MatchTest {
	if len(src)%8 != 0 {
		panic(fmt.Errorf("f64 %s: length of slice has to be a multiple of 8", name))
	}
	if len(result) != bitFieldLen(len(src)) {
		panic(fmt.Errorf("f64 %s: length of slice and length of result does not match", name))
	}

	// create new src at requested length
	src = slices.Clone(src)
	l := length
	for l > len(src) {
		src = append(src, src...)
	}
	src = src[:l]

	// create new result at requested length
	result = slices.Clone(result)
	l = bitFieldLen(length)
	for l > len(result) {
		result = append(result, result...)
	}
	result = result[:l]

	// clear the last unused bits
	if length%8 != 0 {
		result[len(result)-1] &= 0xff >> (8 - length%8)
	}

	// count number of ones
	var cnt int
	for _, v := range result {
		cnt += bits.OnesCount8(v)
	}
	return Float64MatchTest{
		Name:   name,
		Slice:  src,
		Match:  match,
		Match2: match2,
		Result: result,
		Count:  int64(cnt),
	}
}

// -----------------------------------------------------------------------------
// Equal Testcases
var Float64EqualCases = []Float64MatchTest{
	{"l0", make([]float64, 0), f64_eq_mat_1, 0, []byte{}, 0},
	{"nil", nil, f64_eq_mat_1, 0, []byte{}, 0},
	mkF64("vec1", f64_s0, f64_eq_m0, 0, f64_eq_res_0, 32),
	mkF64("vec2", f64_s0, f64_eq_m0, 0, f64_eq_res_0, 64),
	mkF64("l32", f64_s1, f64_eq_mat_1, 0, f64_eq_res_1, 32),
	mkF64("l64", append(f64_s1, f64_s0...), f64_eq_mat_1, 0, append(f64_eq_res_1, f64_eq_res_0...), 64),
	mkF64("l128", append(f64_s1, f64_s0...), f64_eq_mat_1, 0, append(f64_eq_res_1, f64_eq_res_0...), 128),
	mkF64("l127", append(f64_s1, f64_s0...), f64_eq_mat_1, 0, append(f64_eq_res_1, f64_eq_res_0...), 127),
	mkF64("l63", f64_s1, f64_eq_mat_1, 0, f64_eq_res_1, 63),
	mkF64("l31", f64_s1, f64_eq_mat_1, 0, f64_eq_res_1, 31),
	mkF64("l23", f64_s1, f64_eq_mat_1, 0, f64_eq_res_1, 23),
	mkF64("l15", f64_s1, f64_eq_mat_1, 0, f64_eq_res_1, 15),
	mkF64("l7", f64_s1, f64_eq_mat_1, 0, f64_eq_res_1, 7),
	mkF64("neg64", f64_s2, f64_eq_mat_2, 0, f64_eq_res_2, 64),
	mkF64("neg32", f64_s2, f64_eq_mat_2, 0, f64_eq_res_2, 32),
	mkF64("neg31", f64_s2, f64_eq_mat_2, 0, f64_eq_res_2, 31),
	mkF64("ext64", f64_s3, f64_eq_mat_3, 0, f64_eq_res_3, 64),
	mkF64("ext32", f64_s3, f64_eq_mat_3, 0, f64_eq_res_3, 32),
	mkF64("ext31", f64_s3, f64_eq_mat_3, 0, f64_eq_res_3, 31),
	mkF64("nan31", f64_s4, f64_eq_mat_4, 0, f64_eq_res_4, 31),
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
var Float64NotEqualCases = []Float64MatchTest{
	{"l0", make([]float64, 0), f64_ne_mat_1, 0, []byte{}, 0},
	{"nil", nil, f64_ne_mat_1, 0, []byte{}, 0},
	mkF64("vec1", f64_s0, f64_ne_m0, 0, f64_ne_res_0, 32),
	mkF64("vec2", f64_s0, f64_ne_m0, 0, f64_ne_res_0, 64),
	mkF64("l32", f64_s1, f64_ne_mat_1, 0, f64_ne_res_1, 32),
	mkF64("l64", append(f64_s1, f64_s0...), f64_ne_mat_1, 0, append(f64_ne_res_1, f64_ne_res_0...), 64),
	mkF64("l128", append(f64_s1, f64_s0...), f64_ne_mat_1, 0, append(f64_ne_res_1, f64_ne_res_0...), 128),
	mkF64("l127", append(f64_s1, f64_s0...), f64_ne_mat_1, 0, append(f64_ne_res_1, f64_ne_res_0...), 127),
	mkF64("l63", f64_s1, f64_ne_mat_1, 0, f64_ne_res_1, 63),
	mkF64("l31", f64_s1, f64_ne_mat_1, 0, f64_ne_res_1, 31),
	mkF64("l23", f64_s1, f64_ne_mat_1, 0, f64_ne_res_1, 23),
	mkF64("l15", f64_s1, f64_ne_mat_1, 0, f64_ne_res_1, 15),
	mkF64("l7", f64_s1, f64_ne_mat_1, 0, f64_ne_res_1, 7),
	mkF64("neg64", f64_s2, f64_ne_mat_2, 0, f64_ne_res_2, 64),
	mkF64("neg32", f64_s2, f64_ne_mat_2, 0, f64_ne_res_2, 32),
	mkF64("neg31", f64_s2, f64_ne_mat_2, 0, f64_ne_res_2, 31),
	mkF64("ext64", f64_s3, f64_ne_mat_3, 0, f64_ne_res_3, 64),
	mkF64("ext32", f64_s3, f64_ne_mat_3, 0, f64_ne_res_3, 32),
	mkF64("ext31", f64_s3, f64_ne_mat_3, 0, f64_ne_res_3, 31),
	mkF64("nan64", f64_s4, f64_ne_mat_4, 0, f64_ne_res_4, 64),
	mkF64("nan32", f64_s4, f64_ne_mat_4, 0, f64_ne_res_4, 32),
	mkF64("nan31", f64_s4, f64_ne_mat_4, 0, f64_ne_res_4, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var Float64LessCases = []Float64MatchTest{
	{"l0", make([]float64, 0), f64_lt_mat_1, 0, []byte{}, 0},
	{"nil", nil, f64_lt_mat_1, 0, []byte{}, 0},
	mkF64("vec1", f64_s0, f64_lt_mat_0, 0, f64_lt_res_0, 32),
	mkF64("vec2", f64_s0, f64_lt_mat_0, 0, f64_lt_res_0, 64),
	mkF64("l32", f64_s1, f64_lt_mat_1, 0, f64_lt_res_1, 32),
	mkF64("l64", append(f64_s1, f64_s0...), f64_lt_mat_1, 0, append(f64_lt_res_1, f64_lt_res_0...), 64),
	mkF64("l128", append(f64_s1, f64_s0...), f64_lt_mat_1, 0, append(f64_lt_res_1, f64_lt_res_0...), 128),
	mkF64("l127", append(f64_s1, f64_s0...), f64_lt_mat_1, 0, append(f64_lt_res_1, f64_lt_res_0...), 127),
	mkF64("l63", f64_s1, f64_lt_mat_1, 0, f64_lt_res_1, 63),
	mkF64("l31", f64_s1, f64_lt_mat_1, 0, f64_lt_res_1, 31),
	mkF64("l23", f64_s1, f64_lt_mat_1, 0, f64_lt_res_1, 23),
	mkF64("l15", f64_s1, f64_lt_mat_1, 0, f64_lt_res_1, 15),
	mkF64("l7", f64_s1, f64_lt_mat_1, 0, f64_lt_res_1, 7),
	mkF64("neg64", f64_s2, f64_lt_mat_2, 0, f64_lt_res_2, 64),
	mkF64("neg32", f64_s2, f64_lt_mat_2, 0, f64_lt_res_2, 32),
	mkF64("neg31", f64_s2, f64_lt_mat_2, 0, f64_lt_res_2, 31),
	mkF64("ext64", f64_s3, f64_lt_mat_3, 0, f64_lt_res_3, 64),
	mkF64("ext32", f64_s3, f64_lt_mat_3, 0, f64_lt_res_3, 32),
	mkF64("ext31", f64_s3, f64_lt_mat_3, 0, f64_lt_res_3, 31),
	mkF64("nan64", f64_s4, f64_lt_mat_4, 0, f64_lt_res_4, 64),
	mkF64("nan32", f64_s4, f64_lt_mat_4, 0, f64_lt_res_4, 32),
	mkF64("nan31", f64_s4, f64_lt_mat_4, 0, f64_lt_res_4, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var Float64LessEqualCases = []Float64MatchTest{
	{"l0", make([]float64, 0), f64_le_mat_1, 0, []byte{}, 0},
	{"nil", nil, f64_le_mat_1, 0, []byte{}, 0},
	mkF64("vec1", f64_s0, f64_le_mat_0, 0, f64_le_res_0, 32),
	mkF64("vec2", f64_s0, f64_le_mat_0, 0, f64_le_res_0, 64),
	mkF64("l32", f64_s1, f64_le_mat_1, 0, f64_le_res_1, 32),
	mkF64("l64", append(f64_s1, f64_s0...), f64_le_mat_1, 0, append(f64_le_res_1, f64_le_res_0...), 64),
	mkF64("l128", append(f64_s1, f64_s0...), f64_le_mat_1, 0, append(f64_le_res_1, f64_le_res_0...), 128),
	mkF64("l127", append(f64_s1, f64_s0...), f64_le_mat_1, 0, append(f64_le_res_1, f64_le_res_0...), 127),
	mkF64("l63", f64_s1, f64_le_mat_1, 0, f64_le_res_1, 63),
	mkF64("l31", f64_s1, f64_le_mat_1, 0, f64_le_res_1, 31),
	mkF64("l23", f64_s1, f64_le_mat_1, 0, f64_le_res_1, 23),
	mkF64("l15", f64_s1, f64_le_mat_1, 0, f64_le_res_1, 15),
	mkF64("l7", f64_s1, f64_le_mat_1, 0, f64_le_res_1, 7),
	mkF64("neg64", f64_s2, f64_le_mat_2, 0, f64_le_res_2, 64),
	mkF64("neg32", f64_s2, f64_le_mat_2, 0, f64_le_res_2, 32),
	mkF64("neg31", f64_s2, f64_le_mat_2, 0, f64_le_res_2, 31),
	mkF64("ext64", f64_s3, f64_le_mat_3, 0, f64_le_res_3, 64),
	mkF64("ext32", f64_s3, f64_le_mat_3, 0, f64_le_res_3, 32),
	mkF64("ext31", f64_s3, f64_le_mat_3, 0, f64_le_res_3, 31),
	mkF64("nan64", f64_s4, f64_le_mat_4, 0, f64_le_res_4, 64),
	mkF64("nan32", f64_s4, f64_le_mat_4, 0, f64_le_res_4, 32),
	mkF64("nan31", f64_s4, f64_le_mat_4, 0, f64_le_res_4, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var Float64GreaterCases = []Float64MatchTest{
	{"l0", make([]float64, 0), f64_gt_mat_1, 0, []byte{}, 0},
	{"nil", nil, f64_gt_mat_1, 0, []byte{}, 0},
	mkF64("vec1", f64_s0, f64_gt_mat_0, 0, f64_gt_res_0, 32),
	mkF64("vec2", f64_s0, f64_gt_mat_0, 0, f64_gt_res_0, 64),
	mkF64("l32", f64_s1, f64_gt_mat_1, 0, f64_gt_res_1, 32),
	mkF64("l64", append(f64_s1, f64_s0...), f64_gt_mat_1, 0, append(f64_gt_res_1, f64_gt_res_0...), 64),
	mkF64("l128", append(f64_s1, f64_s0...), f64_gt_mat_1, 0, append(f64_gt_res_1, f64_gt_res_0...), 128),
	mkF64("l127", append(f64_s1, f64_s0...), f64_gt_mat_1, 0, append(f64_gt_res_1, f64_gt_res_0...), 127),
	mkF64("l63", f64_s1, f64_gt_mat_1, 0, f64_gt_res_1, 63),
	mkF64("l31", f64_s1, f64_gt_mat_1, 0, f64_gt_res_1, 31),
	mkF64("l23", f64_s1, f64_gt_mat_1, 0, f64_gt_res_1, 23),
	mkF64("l15", f64_s1, f64_gt_mat_1, 0, f64_gt_res_1, 15),
	mkF64("l7", f64_s1, f64_gt_mat_1, 0, f64_gt_res_1, 7),
	mkF64("neg64", f64_s2, f64_gt_mat_2, 0, f64_gt_res_2, 64),
	mkF64("neg32", f64_s2, f64_gt_mat_2, 0, f64_gt_res_2, 32),
	mkF64("neg31", f64_s2, f64_gt_mat_2, 0, f64_gt_res_2, 31),
	mkF64("ext64", f64_s3, f64_gt_mat_3, 0, f64_gt_res_3, 64),
	mkF64("ext32", f64_s3, f64_gt_mat_3, 0, f64_gt_res_3, 32),
	mkF64("ext31", f64_s3, f64_gt_mat_3, 0, f64_gt_res_3, 31),
	mkF64("nan64", f64_s4, f64_gt_mat_4, 0, f64_gt_res_4, 64),
	mkF64("nan32", f64_s4, f64_gt_mat_4, 0, f64_gt_res_4, 32),
	mkF64("nan31", f64_s4, f64_gt_mat_4, 0, f64_gt_res_4, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var Float64GreaterEqualCases = []Float64MatchTest{
	{"l0", make([]float64, 0), f64_ge_mat_1, 0, []byte{}, 0},
	{"nil", nil, f64_ge_mat_1, 0, []byte{}, 0},
	mkF64("vec1", f64_s0, f64_ge_mat_0, 0, f64_ge_res_0, 32),
	mkF64("vec2", f64_s0, f64_ge_mat_0, 0, f64_ge_res_0, 64),
	mkF64("l32", f64_s1, f64_ge_mat_1, 0, f64_ge_res_1, 32),
	mkF64("l64", append(f64_s1, f64_s0...), f64_ge_mat_1, 0, append(f64_ge_res_1, f64_ge_res_0...), 64),
	mkF64("l128", append(f64_s1, f64_s0...), f64_ge_mat_1, 0, append(f64_ge_res_1, f64_ge_res_0...), 128),
	mkF64("l127", append(f64_s1, f64_s0...), f64_ge_mat_1, 0, append(f64_ge_res_1, f64_ge_res_0...), 127),
	mkF64("l63", f64_s1, f64_ge_mat_1, 0, f64_ge_res_1, 63),
	mkF64("l31", f64_s1, f64_ge_mat_1, 0, f64_ge_res_1, 31),
	mkF64("l23", f64_s1, f64_ge_mat_1, 0, f64_ge_res_1, 23),
	mkF64("l15", f64_s1, f64_ge_mat_1, 0, f64_ge_res_1, 15),
	mkF64("l7", f64_s1, f64_ge_mat_1, 0, f64_ge_res_1, 7),
	mkF64("neg64", f64_s2, f64_ge_mat_2, 0, f64_ge_res_2, 64),
	mkF64("neg32", f64_s2, f64_ge_mat_2, 0, f64_ge_res_2, 32),
	mkF64("neg31", f64_s2, f64_ge_mat_2, 0, f64_ge_res_2, 31),
	mkF64("ext64", f64_s3, f64_ge_mat_3, 0, f64_ge_res_3, 64),
	mkF64("ext32", f64_s3, f64_ge_mat_3, 0, f64_ge_res_3, 32),
	mkF64("ext31", f64_s3, f64_ge_mat_3, 0, f64_ge_res_3, 31),
	mkF64("nan64", f64_s4, f64_ge_mat_4, 0, f64_ge_res_4, 64),
	mkF64("nan32", f64_s4, f64_ge_mat_4, 0, f64_ge_res_4, 32),
	mkF64("nan31", f64_s4, f64_ge_mat_4, 0, f64_ge_res_4, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var Float64BetweenCases = []Float64MatchTest{
	{"l0", make([]float64, 0), f64_bw_mat_1a, 0, []byte{}, 0},
	{"nil", nil, f64_bw_mat_1a, 0, []byte{}, 0},
	mkF64("vec1", f64_s0, f64_bw_mat_0a, f64_bw_mat_0b, f64_bw_res_0, 32),
	mkF64("vec2", f64_s0, f64_bw_mat_0a, f64_bw_mat_0b, f64_bw_res_0, 64),
	mkF64("l32", f64_s1, f64_bw_mat_1a, f64_bw_mat_1b, f64_bw_res_1, 32),
	mkF64("l64", append(f64_s1, f64_s0...), f64_bw_mat_1a, f64_bw_mat_1b, append(f64_bw_res_1, f64_bw_res_0...), 64),
	mkF64("l128", append(f64_s1, f64_s0...), f64_bw_mat_1a, f64_bw_mat_1b, append(f64_bw_res_1, f64_bw_res_0...), 128),
	mkF64("l127", append(f64_s1, f64_s0...), f64_bw_mat_1a, f64_bw_mat_1b, append(f64_bw_res_1, f64_bw_res_0...), 127),
	mkF64("l63", f64_s1, f64_bw_mat_1a, f64_bw_mat_1b, f64_bw_res_1, 63),
	mkF64("l31", f64_s1, f64_bw_mat_1a, f64_bw_mat_1b, f64_bw_res_1, 31),
	mkF64("l23", f64_s1, f64_bw_mat_1a, f64_bw_mat_1b, f64_bw_res_1, 23),
	mkF64("l15", f64_s1, f64_bw_mat_1a, f64_bw_mat_1b, f64_bw_res_1, 15),
	mkF64("l7", f64_s1, f64_bw_mat_1a, f64_bw_mat_1b, f64_bw_res_1, 7),
	mkF64("neg64", f64_s2, f64_bw_mat_2a, f64_bw_mat_2b, f64_bw_res_2, 64),
	mkF64("neg32", f64_s2, f64_bw_mat_2a, f64_bw_mat_2b, f64_bw_res_2, 32),
	mkF64("neg31", f64_s2, f64_bw_mat_2a, f64_bw_mat_2b, f64_bw_res_2, 31),
	mkF64("ext64", f64_s3, f64_bw_mat_3a, f64_bw_mat_3b, f64_bw_res_3, 64),
	mkF64("ext32", f64_s3, f64_bw_mat_3a, f64_bw_mat_3b, f64_bw_res_3, 32),
	mkF64("ext31", f64_s3, f64_bw_mat_3a, f64_bw_mat_3b, f64_bw_res_3, 31),
	mkF64("nan64", f64_s4, f64_bw_mat_4a, f64_bw_mat_4b, f64_bw_res_4, 64),
	mkF64("nan32", f64_s4, f64_bw_mat_4a, f64_bw_mat_4b, f64_bw_res_4, 32),
	mkF64("nan31", f64_s4, f64_bw_mat_4a, f64_bw_mat_4b, f64_bw_res_4, 31),
}
