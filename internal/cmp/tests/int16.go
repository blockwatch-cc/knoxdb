// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

import (
	"fmt"
	"math"
	"math/bits"
	"slices"
)

var (
	i16_s0 = []int16{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	i16_eq_mat_0 int16 = 5
	i16_eq_res_0       = []byte{0x6a, 0x1e, 0x48, 0x2c}

	i16_ne_mat_0 int16 = 5
	i16_ne_res_0       = []byte{0x95, 0xe1, 0xb7, 0xd3}

	i16_lt_mat_0 int16 = 5
	i16_lt_res_0       = []byte{0x05, 0x21, 0x27, 0x01}

	i16_le_mat_0 int16 = 5
	i16_le_res_0       = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	i16_gt_mat_0 int16 = 5
	i16_gt_res_0       = []byte{0x90, 0xc0, 0x90, 0xd2}

	i16_ge_mat_0 int16 = 5
	i16_ge_res_0       = []byte{0xfa, 0xde, 0xd8, 0xfe}

	i16_bw_mat_0a int16 = 5
	i16_bw_mat_0b int16 = 10
	i16_bw_res_0        = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	// positive values only
	i16_s1 = []int16{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 5000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	i16_eq_res_1       = []byte{0x41, 0x42, 0xc4, 0x0e}
	i16_eq_mat_1 int16 = 5

	i16_ne_res_1       = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	i16_ne_mat_1 int16 = 5

	i16_lt_res_1       = []byte{0x0e, 0x00, 0x00, 0x00}
	i16_lt_mat_1 int16 = 5

	i16_le_res_1       = []byte{0x4f, 0x42, 0xc4, 0x0e}
	i16_le_mat_1 int16 = 5

	i16_gt_res_1       = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	i16_gt_mat_1 int16 = 5

	i16_ge_res_1       = []byte{0xf1, 0xff, 0xff, 0xff}
	i16_ge_mat_1 int16 = 5

	i16_bw_res_1        = []byte{0xf1, 0x42, 0xc4, 0x0e}
	i16_bw_mat_1a int16 = 5
	i16_bw_mat_1b int16 = 10

	// negative and positive values mixed
	i16_s2 = []int16{
		-5, 2, -3, 5,
		7, 8, 9, -10,
		15, 50, 55, 500,
		1000, -5000, 113, 12,
		31, 32, 33, 34,
		35, -36, 37, 38,
		39, 40, -41, 42,
		43, 44, 45, -46,
	}
	i16_eq_res_2       = []byte{0x01, 0x0, 0x0, 0x0}
	i16_eq_mat_2 int16 = -5

	i16_ne_res_2       = []byte{0xfe, 0xff, 0xff, 0xff}
	i16_ne_mat_2 int16 = -5

	i16_lt_res_2       = []byte{0x87, 0x20, 0x20, 0x84}
	i16_lt_mat_2 int16 = 5

	i16_le_res_2       = []byte{0x8f, 0x20, 0x20, 0x84}
	i16_le_mat_2 int16 = 5

	i16_gt_res_2       = []byte{0x70, 0xdf, 0xdf, 0x7b}
	i16_gt_mat_2 int16 = 5

	i16_ge_res_2       = []byte{0x78, 0xdf, 0xdf, 0x7b}
	i16_ge_mat_2 int16 = 5

	i16_bw_res_2        = []byte{0x78, 0x00, 0x00, 0x00}
	i16_bw_mat_2a int16 = 5
	i16_bw_mat_2b int16 = 10

	// extreme values
	i16_s3 = []int16{
		0, 0,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		0, 0,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		0, 0,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		0, 0,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
	}
	i16_eq_res_3       = []byte{0x80, 0x80, 0x80, 0x80}
	i16_eq_mat_3 int16 = math.MinInt16

	i16_ne_res_3       = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	i16_ne_mat_3 int16 = math.MinInt16

	i16_lt_res_3       = []byte{0x0, 0x0, 0x0, 0x0}
	i16_lt_mat_3 int16 = math.MinInt16

	i16_le_res_3       = []byte{0x80, 0x80, 0x80, 0x80}
	i16_le_mat_3 int16 = math.MinInt16

	i16_gt_res_3       = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	i16_gt_mat_3 int16 = math.MinInt16

	i16_ge_res_3       = []byte{0xff, 0xff, 0xff, 0xff}
	i16_ge_mat_3 int16 = math.MinInt16

	i16_bw_res_3        = []byte{0x50, 0x50, 0x50, 0x50}
	i16_bw_mat_3a int16 = math.MaxInt8
	i16_bw_mat_3b int16 = math.MaxInt16

	i16_bw_res_4        = []byte{0xff, 0xff, 0xff, 0xff}
	i16_bw_mat_4a int16 = math.MinInt16
	i16_bw_mat_4b int16 = math.MaxInt16
)

// creates an uint16 test case from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mkI16(name string, src []int16, match, match2 int16, result []byte, length int) MatchTest[int16] {
	if len(src)%8 != 0 {
		panic(fmt.Errorf("i16 %s: length of slice has to be a multiple of 8", name))
	}
	if len(result) != BitFieldLen(len(src)) {
		panic(fmt.Errorf("i16 %s: length of slice and length of result does not match", name))
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
	l = BitFieldLen(length)
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
	return MatchTest[int16]{
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
var Int16EqualCases = []MatchTest[int16]{
	{"l0", make([]int16, 0), i16_eq_mat_1, 0, []byte{}, 0},
	{"nil", nil, i16_eq_mat_1, 0, []byte{}, 0},
	mkI16("vec1", i16_s0, i16_eq_mat_0, 0, i16_eq_res_0, 32),
	mkI16("vec2", i16_s0, i16_eq_mat_0, 0, i16_eq_res_0, 256),
	mkI16("l32", i16_s1, i16_eq_mat_1, 0, i16_eq_res_1, 32),
	mkI16("l64", append(i16_s1, i16_s0...), i16_eq_mat_1, 0, append(i16_eq_res_1, i16_eq_res_0...), 64),
	mkI16("l128", append(i16_s1, i16_s0...), i16_eq_mat_1, 0, append(i16_eq_res_1, i16_eq_res_0...), 128),
	mkI16("l256", append(i16_s1, i16_s0...), i16_eq_mat_1, 0, append(i16_eq_res_1, i16_eq_res_0...), 256),
	mkI16("l512", append(i16_s1, i16_s0...), i16_eq_mat_1, 0, append(i16_eq_res_1, i16_eq_res_0...), 512),
	mkI16("l511", append(i16_s1, i16_s0...), i16_eq_mat_1, 0, append(i16_eq_res_1, i16_eq_res_0...), 511),
	mkI16("l255", append(i16_s1, i16_s0...), i16_eq_mat_1, 0, append(i16_eq_res_1, i16_eq_res_0...), 255),
	mkI16("l127", i16_s1, i16_eq_mat_1, 0, i16_eq_res_1, 127),
	mkI16("l63", i16_s1, i16_eq_mat_1, 0, i16_eq_res_1, 63),
	mkI16("l31", i16_s1, i16_eq_mat_1, 0, i16_eq_res_1, 31),
	mkI16("l23", i16_s1, i16_eq_mat_1, 0, i16_eq_res_1, 23),
	mkI16("l15", i16_s1, i16_eq_mat_1, 0, i16_eq_res_1, 15),
	mkI16("l7", i16_s1, i16_eq_mat_1, 0, i16_eq_res_1, 7),
	mkI16("neg256", i16_s2, i16_eq_mat_2, 0, i16_eq_res_2, 256),
	mkI16("neg32", i16_s2, i16_eq_mat_2, 0, i16_eq_res_2, 32),
	mkI16("neg31", i16_s2, i16_eq_mat_2, 0, i16_eq_res_2, 31),
	mkI16("ext256", i16_s3, i16_eq_mat_3, 0, i16_eq_res_3, 256),
	mkI16("ext32", i16_s3, i16_eq_mat_3, 0, i16_eq_res_3, 32),
	mkI16("ext31", i16_s3, i16_eq_mat_3, 0, i16_eq_res_3, 31),
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
var Int16NotEqualCases = []MatchTest[int16]{
	{"l0", make([]int16, 0), i16_ne_mat_1, 0, []byte{}, 0},
	{"nil", nil, i16_ne_mat_1, 0, []byte{}, 0},
	mkI16("vec1", i16_s0, i16_ne_mat_0, 0, i16_ne_res_0, 32),
	mkI16("vec2", i16_s0, i16_ne_mat_0, 0, i16_ne_res_0, 256),
	mkI16("l32", i16_s1, i16_ne_mat_1, 0, i16_ne_res_1, 32),
	mkI16("l64", append(i16_s1, i16_s0...), i16_ne_mat_1, 0, append(i16_ne_res_1, i16_ne_res_0...), 64),
	mkI16("l128", append(i16_s1, i16_s0...), i16_ne_mat_1, 0, append(i16_ne_res_1, i16_ne_res_0...), 128),
	mkI16("l256", append(i16_s1, i16_s0...), i16_ne_mat_1, 0, append(i16_ne_res_1, i16_ne_res_0...), 256),
	mkI16("l512", append(i16_s1, i16_s0...), i16_ne_mat_1, 0, append(i16_ne_res_1, i16_ne_res_0...), 512),
	mkI16("l511", append(i16_s1, i16_s0...), i16_ne_mat_1, 0, append(i16_ne_res_1, i16_ne_res_0...), 511),
	mkI16("l255", append(i16_s1, i16_s0...), i16_ne_mat_1, 0, append(i16_ne_res_1, i16_ne_res_0...), 255),
	mkI16("l127", i16_s1, i16_ne_mat_1, 0, i16_ne_res_1, 127),
	mkI16("l63", i16_s1, i16_ne_mat_1, 0, i16_ne_res_1, 63),
	mkI16("l31", i16_s1, i16_ne_mat_1, 0, i16_ne_res_1, 31),
	mkI16("l23", i16_s1, i16_ne_mat_1, 0, i16_ne_res_1, 23),
	mkI16("l15", i16_s1, i16_ne_mat_1, 0, i16_ne_res_1, 15),
	mkI16("l7", i16_s1, i16_ne_mat_1, 0, i16_ne_res_1, 7),
	mkI16("neg256", i16_s2, i16_ne_mat_2, 0, i16_ne_res_2, 256),
	mkI16("neg32", i16_s2, i16_ne_mat_2, 0, i16_ne_res_2, 32),
	mkI16("neg31", i16_s2, i16_ne_mat_2, 0, i16_ne_res_2, 31),
	mkI16("ext256", i16_s3, i16_ne_mat_3, 0, i16_ne_res_3, 256),
	mkI16("ext32", i16_s3, i16_ne_mat_3, 0, i16_ne_res_3, 32),
	mkI16("ext31", i16_s3, i16_ne_mat_3, 0, i16_ne_res_3, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var Int16LessCases = []MatchTest[int16]{
	{"l0", make([]int16, 0), i16_lt_mat_1, 0, []byte{}, 0},
	{"nil", nil, i16_lt_mat_1, 0, []byte{}, 0},
	mkI16("vec1", i16_s0, i16_lt_mat_0, 0, i16_lt_res_0, 32),
	mkI16("vec2", i16_s0, i16_lt_mat_0, 0, i16_lt_res_0, 256),
	mkI16("l32", i16_s1, i16_lt_mat_1, 0, i16_lt_res_1, 32),
	mkI16("l64", append(i16_s1, i16_s0...), i16_lt_mat_1, 0, append(i16_lt_res_1, i16_lt_res_0...), 64),
	mkI16("l128", append(i16_s1, i16_s0...), i16_lt_mat_1, 0, append(i16_lt_res_1, i16_lt_res_0...), 128),
	mkI16("l256", append(i16_s1, i16_s0...), i16_lt_mat_1, 0, append(i16_lt_res_1, i16_lt_res_0...), 256),
	mkI16("l512", append(i16_s1, i16_s0...), i16_lt_mat_1, 0, append(i16_lt_res_1, i16_lt_res_0...), 512),
	mkI16("l511", append(i16_s1, i16_s0...), i16_lt_mat_1, 0, append(i16_lt_res_1, i16_lt_res_0...), 511),
	mkI16("l255", append(i16_s1, i16_s0...), i16_lt_mat_1, 0, append(i16_lt_res_1, i16_lt_res_0...), 255),
	mkI16("l127", i16_s1, i16_lt_mat_1, 0, i16_lt_res_1, 127),
	mkI16("l63", i16_s1, i16_lt_mat_1, 0, i16_lt_res_1, 63),
	mkI16("l31", i16_s1, i16_lt_mat_1, 0, i16_lt_res_1, 31),
	mkI16("l23", i16_s1, i16_lt_mat_1, 0, i16_lt_res_1, 23),
	mkI16("l15", i16_s1, i16_lt_mat_1, 0, i16_lt_res_1, 15),
	mkI16("l7", i16_s1, i16_lt_mat_1, 0, i16_lt_res_1, 7),
	mkI16("neg256", i16_s2, i16_lt_mat_2, 0, i16_lt_res_2, 256),
	mkI16("neg32", i16_s2, i16_lt_mat_2, 0, i16_lt_res_2, 32),
	mkI16("neg31", i16_s2, i16_lt_mat_2, 0, i16_lt_res_2, 31),
	mkI16("ext256", i16_s3, i16_lt_mat_3, 0, i16_lt_res_3, 256),
	mkI16("ext32", i16_s3, i16_lt_mat_3, 0, i16_lt_res_3, 32),
	mkI16("ext31", i16_s3, i16_lt_mat_3, 0, i16_lt_res_3, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var Int16LessEqualCases = []MatchTest[int16]{
	{"l0", make([]int16, 0), i16_le_mat_1, 0, []byte{}, 0},
	{"nil", nil, i16_le_mat_1, 0, []byte{}, 0},
	mkI16("vec1", i16_s0, i16_le_mat_0, 0, i16_le_res_0, 32),
	mkI16("vec2", i16_s0, i16_le_mat_0, 0, i16_le_res_0, 256),
	mkI16("l32", i16_s1, i16_le_mat_1, 0, i16_le_res_1, 32),
	mkI16("l64", append(i16_s1, i16_s0...), i16_le_mat_1, 0, append(i16_le_res_1, i16_le_res_0...), 64),
	mkI16("l128", append(i16_s1, i16_s0...), i16_le_mat_1, 0, append(i16_le_res_1, i16_le_res_0...), 128),
	mkI16("l256", append(i16_s1, i16_s0...), i16_le_mat_1, 0, append(i16_le_res_1, i16_le_res_0...), 256),
	mkI16("l512", append(i16_s1, i16_s0...), i16_le_mat_1, 0, append(i16_le_res_1, i16_le_res_0...), 512),
	mkI16("l511", append(i16_s1, i16_s0...), i16_le_mat_1, 0, append(i16_le_res_1, i16_le_res_0...), 511),
	mkI16("l255", append(i16_s1, i16_s0...), i16_le_mat_1, 0, append(i16_le_res_1, i16_le_res_0...), 255),
	mkI16("l127", i16_s1, i16_le_mat_1, 0, i16_le_res_1, 127),
	mkI16("l63", i16_s1, i16_le_mat_1, 0, i16_le_res_1, 63),
	mkI16("l31", i16_s1, i16_le_mat_1, 0, i16_le_res_1, 31),
	mkI16("l23", i16_s1, i16_le_mat_1, 0, i16_le_res_1, 23),
	mkI16("l15", i16_s1, i16_le_mat_1, 0, i16_le_res_1, 15),
	mkI16("l7", i16_s1, i16_le_mat_1, 0, i16_le_res_1, 7),
	mkI16("neg256", i16_s2, i16_le_mat_2, 0, i16_le_res_2, 256),
	mkI16("neg32", i16_s2, i16_le_mat_2, 0, i16_le_res_2, 32),
	mkI16("neg31", i16_s2, i16_le_mat_2, 0, i16_le_res_2, 31),
	mkI16("ext256", i16_s3, i16_le_mat_3, 0, i16_le_res_3, 256),
	mkI16("ext32", i16_s3, i16_le_mat_3, 0, i16_le_res_3, 32),
	mkI16("ext31", i16_s3, i16_le_mat_3, 0, i16_le_res_3, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var Int16GreaterCases = []MatchTest[int16]{
	{"l0", make([]int16, 0), i16_gt_mat_1, 0, []byte{}, 0},
	{"nil", nil, i16_gt_mat_1, 0, []byte{}, 0},
	mkI16("vec1", i16_s0, i16_gt_mat_0, 0, i16_gt_res_0, 32),
	mkI16("vec2", i16_s0, i16_gt_mat_0, 0, i16_gt_res_0, 256),
	mkI16("l32", i16_s1, i16_gt_mat_1, 0, i16_gt_res_1, 32),
	mkI16("l64", append(i16_s1, i16_s0...), i16_gt_mat_1, 0, append(i16_gt_res_1, i16_gt_res_0...), 64),
	mkI16("l128", append(i16_s1, i16_s0...), i16_gt_mat_1, 0, append(i16_gt_res_1, i16_gt_res_0...), 128),
	mkI16("l256", append(i16_s1, i16_s0...), i16_gt_mat_1, 0, append(i16_gt_res_1, i16_gt_res_0...), 256),
	mkI16("l512", append(i16_s1, i16_s0...), i16_gt_mat_1, 0, append(i16_gt_res_1, i16_gt_res_0...), 512),
	mkI16("l511", append(i16_s1, i16_s0...), i16_gt_mat_1, 0, append(i16_gt_res_1, i16_gt_res_0...), 511),
	mkI16("l255", append(i16_s1, i16_s0...), i16_gt_mat_1, 0, append(i16_gt_res_1, i16_gt_res_0...), 255),
	mkI16("l127", i16_s1, i16_gt_mat_1, 0, i16_gt_res_1, 127),
	mkI16("l63", i16_s1, i16_gt_mat_1, 0, i16_gt_res_1, 63),
	mkI16("l31", i16_s1, i16_gt_mat_1, 0, i16_gt_res_1, 31),
	mkI16("l23", i16_s1, i16_gt_mat_1, 0, i16_gt_res_1, 23),
	mkI16("l15", i16_s1, i16_gt_mat_1, 0, i16_gt_res_1, 15),
	mkI16("l7", i16_s1, i16_gt_mat_1, 0, i16_gt_res_1, 7),
	mkI16("neg256", i16_s2, i16_gt_mat_2, 0, i16_gt_res_2, 256),
	mkI16("neg32", i16_s2, i16_gt_mat_2, 0, i16_gt_res_2, 32),
	mkI16("neg31", i16_s2, i16_gt_mat_2, 0, i16_gt_res_2, 31),
	mkI16("ext256", i16_s3, i16_gt_mat_3, 0, i16_gt_res_3, 256),
	mkI16("ext32", i16_s3, i16_gt_mat_3, 0, i16_gt_res_3, 32),
	mkI16("ext31", i16_s3, i16_gt_mat_3, 0, i16_gt_res_3, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var Int16GreaterEqualCases = []MatchTest[int16]{
	{"l0", make([]int16, 0), i16_ge_mat_1, 0, []byte{}, 0},
	{"nil", nil, i16_ge_mat_1, 0, []byte{}, 0},
	mkI16("vec1", i16_s0, i16_ge_mat_0, 0, i16_ge_res_0, 32),
	mkI16("vec2", i16_s0, i16_ge_mat_0, 0, i16_ge_res_0, 256),
	mkI16("l32", i16_s1, i16_ge_mat_1, 0, i16_ge_res_1, 32),
	mkI16("l64", append(i16_s1, i16_s0...), i16_ge_mat_1, 0, append(i16_ge_res_1, i16_ge_res_0...), 64),
	mkI16("l128", append(i16_s1, i16_s0...), i16_ge_mat_1, 0, append(i16_ge_res_1, i16_ge_res_0...), 128),
	mkI16("l256", append(i16_s1, i16_s0...), i16_ge_mat_1, 0, append(i16_ge_res_1, i16_ge_res_0...), 256),
	mkI16("l512", append(i16_s1, i16_s0...), i16_ge_mat_1, 0, append(i16_ge_res_1, i16_ge_res_0...), 512),
	mkI16("l511", append(i16_s1, i16_s0...), i16_ge_mat_1, 0, append(i16_ge_res_1, i16_ge_res_0...), 511),
	mkI16("l255", append(i16_s1, i16_s0...), i16_ge_mat_1, 0, append(i16_ge_res_1, i16_ge_res_0...), 255),
	mkI16("l127", i16_s1, i16_ge_mat_1, 0, i16_ge_res_1, 127),
	mkI16("l63", i16_s1, i16_ge_mat_1, 0, i16_ge_res_1, 63),
	mkI16("l31", i16_s1, i16_ge_mat_1, 0, i16_ge_res_1, 31),
	mkI16("l23", i16_s1, i16_ge_mat_1, 0, i16_ge_res_1, 23),
	mkI16("l15", i16_s1, i16_ge_mat_1, 0, i16_ge_res_1, 15),
	mkI16("l7", i16_s1, i16_ge_mat_1, 0, i16_ge_res_1, 7),
	mkI16("neg256", i16_s2, i16_ge_mat_2, 0, i16_ge_res_2, 256),
	mkI16("neg32", i16_s2, i16_ge_mat_2, 0, i16_ge_res_2, 32),
	mkI16("neg31", i16_s2, i16_ge_mat_2, 0, i16_ge_res_2, 31),
	mkI16("ext256", i16_s3, i16_ge_mat_3, 0, i16_ge_res_3, 256),
	mkI16("ext32", i16_s3, i16_ge_mat_3, 0, i16_ge_res_3, 32),
	mkI16("ext31", i16_s3, i16_ge_mat_3, 0, i16_ge_res_3, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var Int16BetweenCases = []MatchTest[int16]{
	{"l0", make([]int16, 0), i16_bw_mat_1a, i16_bw_mat_1b, []byte{}, 0},
	{"nil", nil, i16_bw_mat_1a, i16_bw_mat_1b, []byte{}, 0},
	mkI16("vec1", i16_s0, i16_bw_mat_0a, i16_bw_mat_0b, i16_bw_res_0, 32),
	mkI16("vec2", i16_s0, i16_bw_mat_0a, i16_bw_mat_0b, i16_bw_res_0, 256),
	mkI16("l32", i16_s1, i16_bw_mat_1a, i16_bw_mat_1b, i16_bw_res_1, 32),
	mkI16("l64", append(i16_s1, i16_s0...), i16_bw_mat_1a, i16_bw_mat_1b, append(i16_bw_res_1, i16_bw_res_0...), 64),
	mkI16("l128", append(i16_s1, i16_s0...), i16_bw_mat_1a, i16_bw_mat_1b, append(i16_bw_res_1, i16_bw_res_0...), 128),
	mkI16("l256", append(i16_s1, i16_s0...), i16_bw_mat_1a, i16_bw_mat_1b, append(i16_bw_res_1, i16_bw_res_0...), 256),
	mkI16("l512", append(i16_s1, i16_s0...), i16_bw_mat_1a, i16_bw_mat_1b, append(i16_bw_res_1, i16_bw_res_0...), 512),
	mkI16("l511", append(i16_s1, i16_s0...), i16_bw_mat_1a, i16_bw_mat_1b, append(i16_bw_res_1, i16_bw_res_0...), 511),
	mkI16("l255", append(i16_s1, i16_s0...), i16_bw_mat_1a, i16_bw_mat_1b, append(i16_bw_res_1, i16_bw_res_0...), 255),
	mkI16("l127", i16_s1, i16_bw_mat_1a, i16_bw_mat_1b, i16_bw_res_1, 127),
	mkI16("l63", i16_s1, i16_bw_mat_1a, i16_bw_mat_1b, i16_bw_res_1, 63),
	mkI16("l31", i16_s1, i16_bw_mat_1a, i16_bw_mat_1b, i16_bw_res_1, 31),
	mkI16("l23", i16_s1, i16_bw_mat_1a, i16_bw_mat_1b, i16_bw_res_1, 23),
	mkI16("l15", i16_s1, i16_bw_mat_1a, i16_bw_mat_1b, i16_bw_res_1, 15),
	mkI16("l7", i16_s1, i16_bw_mat_1a, i16_bw_mat_1b, i16_bw_res_1, 7),
	mkI16("neg256", i16_s2, i16_bw_mat_2a, i16_bw_mat_2b, i16_bw_res_2, 256),
	mkI16("neg32", i16_s2, i16_bw_mat_2a, i16_bw_mat_2b, i16_bw_res_2, 32),
	mkI16("neg31", i16_s2, i16_bw_mat_2a, i16_bw_mat_2b, i16_bw_res_2, 31),
	mkI16("ext256", i16_s3, i16_bw_mat_3a, i16_bw_mat_3b, i16_bw_res_3, 256),
	mkI16("ext32", i16_s3, i16_bw_mat_3a, i16_bw_mat_3b, i16_bw_res_3, 32),
	mkI16("ext31", i16_s3, i16_bw_mat_3a, i16_bw_mat_3b, i16_bw_res_3, 31),
	mkI16("full", i16_s3, i16_bw_mat_4a, i16_bw_mat_4b, i16_bw_res_4, 32),
}
