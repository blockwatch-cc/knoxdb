// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

import (
	"fmt"
	"math"
	"math/bits"

	"golang.org/x/exp/slices"
)

var (
	i32_s0 = []int32{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	i32_eq_mat_0 int32 = 5
	i32_eq_res_0       = []byte{0x6a, 0x1e, 0x48, 0x2c}

	i32_ne_mat_0 int32 = 5
	i32_ne_res_0       = []byte{0x95, 0xe1, 0xb7, 0xd3}

	i32_lt_mat_0 int32 = 5
	i32_lt_res_0       = []byte{0x05, 0x21, 0x27, 0x01}

	i32_le_mat_0 int32 = 5
	i32_le_res_0       = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	i32_gt_mat_0 int32 = 5
	i32_gt_res_0       = []byte{0x90, 0xc0, 0x90, 0xd2}

	i32_ge_mat_0 int32 = 5
	i32_ge_res_0       = []byte{0xfa, 0xde, 0xd8, 0xfe}

	i32_bw_mat_0a int32 = 5
	i32_bw_mat_0b int32 = 10
	i32_bw_res_0        = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	// positive values only
	i32_s1 = []int32{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 500000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	i32_eq_res_1       = []byte{0x41, 0x42, 0xc4, 0x0e}
	i32_eq_mat_1 int32 = 5

	i32_ne_res_1       = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	i32_ne_mat_1 int32 = 5

	i32_lt_res_1       = []byte{0x0e, 0x00, 0x00, 0x00}
	i32_lt_mat_1 int32 = 5

	i32_le_res_1       = []byte{0x4f, 0x42, 0xc4, 0x0e}
	i32_le_mat_1 int32 = 5

	i32_gt_res_1       = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	i32_gt_mat_1 int32 = 5

	i32_ge_res_1       = []byte{0xf1, 0xff, 0xff, 0xff}
	i32_ge_mat_1 int32 = 5

	i32_bw_res_1        = []byte{0xf1, 0x42, 0xc4, 0x0e}
	i32_bw_mat_1a int32 = 5
	i32_bw_mat_1b int32 = 10

	// negative and positive values mixed
	i32_s2 = []int32{
		-5, 2, -3, 5,
		7, 8, 9, -10,
		15, 50, 55, 500,
		1000, -500000, 113, 12,
		31, 32, 33, 34,
		35, -36, 37, 38,
		39, 40, -41, 42,
		43, 44, 45, -46,
	}
	i32_eq_res_2       = []byte{0x01, 0x0, 0x0, 0x0}
	i32_eq_mat_2 int32 = -5

	i32_ne_res_2       = []byte{0xfe, 0xff, 0xff, 0xff}
	i32_ne_mat_2 int32 = -5

	i32_lt_res_2       = []byte{0x87, 0x20, 0x20, 0x84}
	i32_lt_mat_2 int32 = 5

	i32_le_res_2       = []byte{0x8f, 0x20, 0x20, 0x84}
	i32_le_mat_2 int32 = 5

	i32_gt_res_2       = []byte{0x70, 0xdf, 0xdf, 0x7b}
	i32_gt_mat_2 int32 = 5

	i32_ge_res_2       = []byte{0x78, 0xdf, 0xdf, 0x7b}
	i32_ge_mat_2 int32 = 5

	i32_bw_res_2        = []byte{0x78, 0x00, 0x00, 0x00}
	i32_bw_mat_2a int32 = 5
	i32_bw_mat_2b int32 = 10

	// extreme values
	i32_s3 = []int32{
		0, 0,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		0, 0,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		0, 0,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		0, 0,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
	}
	i32_eq_res_3       = []byte{0x80, 0x80, 0x80, 0x80}
	i32_eq_mat_3 int32 = math.MinInt32

	i32_ne_res_3       = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	i32_ne_mat_3 int32 = math.MinInt32

	i32_lt_res_3       = []byte{0x0, 0x0, 0x0, 0x00}
	i32_lt_mat_3 int32 = math.MinInt32

	i32_le_res_3       = []byte{0x80, 0x80, 0x80, 0x80}
	i32_le_mat_3 int32 = math.MinInt32

	i32_gt_res_3       = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	i32_gt_mat_3 int32 = math.MinInt32

	i32_ge_res_3       = []byte{0xff, 0xff, 0xff, 0xff}
	i32_ge_mat_3 int32 = math.MinInt32

	i32_bw_res_3        = []byte{0x50, 0x50, 0x50, 0x50}
	i32_bw_mat_3a int32 = math.MaxInt16
	i32_bw_mat_3b int32 = math.MaxInt32

	i32_bw_res_4        = []byte{0xff, 0xff, 0xff, 0xff}
	i32_bw_mat_4a int32 = math.MinInt32
	i32_bw_mat_4b int32 = math.MaxInt32
)

// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mkI32(name string, src []int32, match, match2 int32, result []byte, length int) MatchTest[int32] {
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
	return MatchTest[int32]{
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
var Int32EqualCases = []MatchTest[int32]{
	{"l0", make([]int32, 0), i32_eq_mat_1, 0, []byte{}, 0},
	{"nil", nil, i32_eq_mat_1, 0, []byte{}, 0},
	mkI32("vec1", i32_s0, i32_eq_mat_0, 0, i32_eq_res_0, 32),
	mkI32("vec2", i32_s0, i32_eq_mat_0, 0, i32_eq_res_0, 128),
	mkI32("l32", i32_s1, i32_eq_mat_1, 0, i32_eq_res_1, 32),
	mkI32("l64", append(i32_s1, i32_s0...), i32_eq_mat_1, 0, append(i32_eq_res_1, i32_eq_res_0...), 64),
	mkI32("l128", append(i32_s1, i32_s0...), i32_eq_mat_1, 0, append(i32_eq_res_1, i32_eq_res_0...), 128),
	mkI32("l256", append(i32_s1, i32_s0...), i32_eq_mat_1, 0, append(i32_eq_res_1, i32_eq_res_0...), 256),
	mkI32("l255", append(i32_s1, i32_s0...), i32_eq_mat_1, 0, append(i32_eq_res_1, i32_eq_res_0...), 255),
	mkI32("l127", i32_s1, i32_eq_mat_1, 0, i32_eq_res_1, 127),
	mkI32("l63", i32_s1, i32_eq_mat_1, 0, i32_eq_res_1, 63),
	mkI32("l31", i32_s1, i32_eq_mat_1, 0, i32_eq_res_1, 31),
	mkI32("l23", i32_s1, i32_eq_mat_1, 0, i32_eq_res_1, 23),
	mkI32("l15", i32_s1, i32_eq_mat_1, 0, i32_eq_res_1, 15),
	mkI32("l7", i32_s1, i32_eq_mat_1, 0, i32_eq_res_1, 7),
	mkI32("neg128", i32_s2, i32_eq_mat_2, 0, i32_eq_res_2, 128),
	mkI32("neg32", i32_s2, i32_eq_mat_2, 0, i32_eq_res_2, 32),
	mkI32("neg31", i32_s2, i32_eq_mat_2, 0, i32_eq_res_2, 31),
	mkI32("ext128", i32_s3, i32_eq_mat_3, 0, i32_eq_res_3, 128),
	mkI32("ext32", i32_s3, i32_eq_mat_3, 0, i32_eq_res_3, 32),
	mkI32("ext31", i32_s3, i32_eq_mat_3, 0, i32_eq_res_3, 31),
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
var Int32NotEqualCases = []MatchTest[int32]{
	{"l0", make([]int32, 0), i32_ne_mat_1, 0, []byte{}, 0},
	{"nil", nil, i32_ne_mat_1, 0, []byte{}, 0},
	mkI32("vec1", i32_s0, i32_ne_mat_0, 0, i32_ne_res_0, 32),
	mkI32("vec2", i32_s0, i32_ne_mat_0, 0, i32_ne_res_0, 128),
	mkI32("l32", i32_s1, i32_ne_mat_1, 0, i32_ne_res_1, 32),
	mkI32("l64", append(i32_s1, i32_s0...), i32_ne_mat_1, 0, append(i32_ne_res_1, i32_ne_res_0...), 64),
	mkI32("l128", append(i32_s1, i32_s0...), i32_ne_mat_1, 0, append(i32_ne_res_1, i32_ne_res_0...), 128),
	mkI32("l256", append(i32_s1, i32_s0...), i32_ne_mat_1, 0, append(i32_ne_res_1, i32_ne_res_0...), 256),
	mkI32("l255", append(i32_s1, i32_s0...), i32_ne_mat_1, 0, append(i32_ne_res_1, i32_ne_res_0...), 255),
	mkI32("l127", i32_s1, i32_ne_mat_1, 0, i32_ne_res_1, 127),
	mkI32("l63", i32_s1, i32_ne_mat_1, 0, i32_ne_res_1, 63),
	mkI32("l31", i32_s1, i32_ne_mat_1, 0, i32_ne_res_1, 31),
	mkI32("l23", i32_s1, i32_ne_mat_1, 0, i32_ne_res_1, 23),
	mkI32("l15", i32_s1, i32_ne_mat_1, 0, i32_ne_res_1, 15),
	mkI32("l7", i32_s1, i32_ne_mat_1, 0, i32_ne_res_1, 7),
	mkI32("neg128", i32_s2, i32_ne_mat_2, 0, i32_ne_res_2, 128),
	mkI32("neg32", i32_s2, i32_ne_mat_2, 0, i32_ne_res_2, 32),
	mkI32("neg31", i32_s2, i32_ne_mat_2, 0, i32_ne_res_2, 31),
	mkI32("ext128", i32_s3, i32_ne_mat_3, 0, i32_ne_res_3, 128),
	mkI32("ext32", i32_s3, i32_ne_mat_3, 0, i32_ne_res_3, 32),
	mkI32("ext31", i32_s3, i32_ne_mat_3, 0, i32_ne_res_3, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var Int32LessCases = []MatchTest[int32]{
	{"l0", make([]int32, 0), i32_lt_mat_1, 0, []byte{}, 0},
	{"nil", nil, i32_lt_mat_1, 0, []byte{}, 0},
	mkI32("vec1", i32_s0, i32_lt_mat_0, 0, i32_lt_res_0, 32),
	mkI32("vec2", i32_s0, i32_lt_mat_0, 0, i32_lt_res_0, 128),
	mkI32("l32", i32_s1, i32_lt_mat_1, 0, i32_lt_res_1, 32),
	mkI32("l64", append(i32_s1, i32_s0...), i32_lt_mat_1, 0, append(i32_lt_res_1, i32_lt_res_0...), 64),
	mkI32("l128", append(i32_s1, i32_s0...), i32_lt_mat_1, 0, append(i32_lt_res_1, i32_lt_res_0...), 128),
	mkI32("l256", append(i32_s1, i32_s0...), i32_lt_mat_1, 0, append(i32_lt_res_1, i32_lt_res_0...), 256),
	mkI32("l255", append(i32_s1, i32_s0...), i32_lt_mat_1, 0, append(i32_lt_res_1, i32_lt_res_0...), 255),
	mkI32("l127", i32_s1, i32_lt_mat_1, 0, i32_lt_res_1, 127),
	mkI32("l63", i32_s1, i32_lt_mat_1, 0, i32_lt_res_1, 63),
	mkI32("l31", i32_s1, i32_lt_mat_1, 0, i32_lt_res_1, 31),
	mkI32("l23", i32_s1, i32_lt_mat_1, 0, i32_lt_res_1, 23),
	mkI32("l15", i32_s1, i32_lt_mat_1, 0, i32_lt_res_1, 15),
	mkI32("l7", i32_s1, i32_lt_mat_1, 0, i32_lt_res_1, 7),
	mkI32("neg128", i32_s2, i32_lt_mat_2, 0, i32_lt_res_2, 128),
	mkI32("neg32", i32_s2, i32_lt_mat_2, 0, i32_lt_res_2, 32),
	mkI32("neg31", i32_s2, i32_lt_mat_2, 0, i32_lt_res_2, 31),
	mkI32("ext128", i32_s3, i32_lt_mat_3, 0, i32_lt_res_3, 128),
	mkI32("ext32", i32_s3, i32_lt_mat_3, 0, i32_lt_res_3, 32),
	mkI32("ext31", i32_s3, i32_lt_mat_3, 0, i32_lt_res_3, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var Int32LessEqualCases = []MatchTest[int32]{
	{"l0", make([]int32, 0), i32_le_mat_1, 0, []byte{}, 0},
	{"nil", nil, i32_le_mat_1, 0, []byte{}, 0},
	mkI32("vec1", i32_s0, i32_le_mat_0, 0, i32_le_res_0, 32),
	mkI32("vec2", i32_s0, i32_le_mat_0, 0, i32_le_res_0, 128),
	mkI32("l32", i32_s1, i32_le_mat_1, 0, i32_le_res_1, 32),
	mkI32("l64", append(i32_s1, i32_s0...), i32_le_mat_1, 0, append(i32_le_res_1, i32_le_res_0...), 64),
	mkI32("l128", append(i32_s1, i32_s0...), i32_le_mat_1, 0, append(i32_le_res_1, i32_le_res_0...), 128),
	mkI32("l256", append(i32_s1, i32_s0...), i32_le_mat_1, 0, append(i32_le_res_1, i32_le_res_0...), 256),
	mkI32("l255", append(i32_s1, i32_s0...), i32_le_mat_1, 0, append(i32_le_res_1, i32_le_res_0...), 255),
	mkI32("l127", i32_s1, i32_le_mat_1, 0, i32_le_res_1, 127),
	mkI32("l63", i32_s1, i32_le_mat_1, 0, i32_le_res_1, 63),
	mkI32("l31", i32_s1, i32_le_mat_1, 0, i32_le_res_1, 31),
	mkI32("l23", i32_s1, i32_le_mat_1, 0, i32_le_res_1, 23),
	mkI32("l15", i32_s1, i32_le_mat_1, 0, i32_le_res_1, 15),
	mkI32("l7", i32_s1, i32_le_mat_1, 0, i32_le_res_1, 7),
	mkI32("neg128", i32_s2, i32_le_mat_2, 0, i32_le_res_2, 128),
	mkI32("neg32", i32_s2, i32_le_mat_2, 0, i32_le_res_2, 32),
	mkI32("neg31", i32_s2, i32_le_mat_2, 0, i32_le_res_2, 31),
	mkI32("ext128", i32_s3, i32_le_mat_3, 0, i32_le_res_3, 128),
	mkI32("ext32", i32_s3, i32_le_mat_3, 0, i32_le_res_3, 32),
	mkI32("ext31", i32_s3, i32_le_mat_3, 0, i32_le_res_3, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var Int32GreaterCases = []MatchTest[int32]{
	{"l0", make([]int32, 0), i32_gt_mat_1, 0, []byte{}, 0},
	{"nil", nil, i32_gt_mat_1, 0, []byte{}, 0},
	mkI32("vec1", i32_s0, i32_gt_mat_0, 0, i32_gt_res_0, 32),
	mkI32("vec2", i32_s0, i32_gt_mat_0, 0, i32_gt_res_0, 128),
	mkI32("l32", i32_s1, i32_gt_mat_1, 0, i32_gt_res_1, 32),
	mkI32("l64", append(i32_s1, i32_s0...), i32_gt_mat_1, 0, append(i32_gt_res_1, i32_gt_res_0...), 64),
	mkI32("l128", append(i32_s1, i32_s0...), i32_gt_mat_1, 0, append(i32_gt_res_1, i32_gt_res_0...), 128),
	mkI32("l256", append(i32_s1, i32_s0...), i32_gt_mat_1, 0, append(i32_gt_res_1, i32_gt_res_0...), 256),
	mkI32("l255", append(i32_s1, i32_s0...), i32_gt_mat_1, 0, append(i32_gt_res_1, i32_gt_res_0...), 255),
	mkI32("l127", i32_s1, i32_gt_mat_1, 0, i32_gt_res_1, 127),
	mkI32("l63", i32_s1, i32_gt_mat_1, 0, i32_gt_res_1, 63),
	mkI32("l31", i32_s1, i32_gt_mat_1, 0, i32_gt_res_1, 31),
	mkI32("l23", i32_s1, i32_gt_mat_1, 0, i32_gt_res_1, 23),
	mkI32("l15", i32_s1, i32_gt_mat_1, 0, i32_gt_res_1, 15),
	mkI32("l7", i32_s1, i32_gt_mat_1, 0, i32_gt_res_1, 7),
	mkI32("neg128", i32_s2, i32_gt_mat_2, 0, i32_gt_res_2, 128),
	mkI32("neg32", i32_s2, i32_gt_mat_2, 0, i32_gt_res_2, 32),
	mkI32("neg31", i32_s2, i32_gt_mat_2, 0, i32_gt_res_2, 31),
	mkI32("ext128", i32_s3, i32_gt_mat_3, 0, i32_gt_res_3, 128),
	mkI32("ext32", i32_s3, i32_gt_mat_3, 0, i32_gt_res_3, 32),
	mkI32("ext31", i32_s3, i32_gt_mat_3, 0, i32_gt_res_3, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var Int32GreaterEqualCases = []MatchTest[int32]{
	{"l0", make([]int32, 0), i32_ge_mat_1, 0, []byte{}, 0},
	{"nil", nil, i32_ge_mat_1, 0, []byte{}, 0},
	mkI32("vec1", i32_s0, i32_ge_mat_0, 0, i32_ge_res_0, 32),
	mkI32("vec2", i32_s0, i32_ge_mat_0, 0, i32_ge_res_0, 128),
	mkI32("l32", i32_s1, i32_ge_mat_1, 0, i32_ge_res_1, 32),
	mkI32("l64", append(i32_s1, i32_s0...), i32_ge_mat_1, 0, append(i32_ge_res_1, i32_ge_res_0...), 64),
	mkI32("l128", append(i32_s1, i32_s0...), i32_ge_mat_1, 0, append(i32_ge_res_1, i32_ge_res_0...), 128),
	mkI32("l256", append(i32_s1, i32_s0...), i32_ge_mat_1, 0, append(i32_ge_res_1, i32_ge_res_0...), 256),
	mkI32("l255", append(i32_s1, i32_s0...), i32_ge_mat_1, 0, append(i32_ge_res_1, i32_ge_res_0...), 255),
	mkI32("l127", i32_s1, i32_ge_mat_1, 0, i32_ge_res_1, 127),
	mkI32("l63", i32_s1, i32_ge_mat_1, 0, i32_ge_res_1, 63),
	mkI32("l31", i32_s1, i32_ge_mat_1, 0, i32_ge_res_1, 31),
	mkI32("l23", i32_s1, i32_ge_mat_1, 0, i32_ge_res_1, 23),
	mkI32("l15", i32_s1, i32_ge_mat_1, 0, i32_ge_res_1, 15),
	mkI32("l7", i32_s1, i32_ge_mat_1, 0, i32_ge_res_1, 7),
	mkI32("neg128", i32_s2, i32_ge_mat_2, 0, i32_ge_res_2, 128),
	mkI32("neg32", i32_s2, i32_ge_mat_2, 0, i32_ge_res_2, 32),
	mkI32("neg31", i32_s2, i32_ge_mat_2, 0, i32_ge_res_2, 31),
	mkI32("ext128", i32_s3, i32_ge_mat_3, 0, i32_ge_res_3, 128),
	mkI32("ext32", i32_s3, i32_ge_mat_3, 0, i32_ge_res_3, 32),
	mkI32("ext31", i32_s3, i32_ge_mat_3, 0, i32_ge_res_3, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var Int32BetweenCases = []MatchTest[int32]{
	{"l0", make([]int32, 0), i32_bw_mat_1a, i32_bw_mat_1b, []byte{}, 0},
	{"nil", nil, i32_bw_mat_1a, i32_bw_mat_1b, []byte{}, 0},
	mkI32("vec1", i32_s0, i32_bw_mat_0a, i32_bw_mat_0b, i32_bw_res_0, 32),
	mkI32("vec2", i32_s0, i32_bw_mat_0a, i32_bw_mat_0b, i32_bw_res_0, 128),
	mkI32("l32", i32_s1, i32_bw_mat_1a, i32_bw_mat_1b, i32_bw_res_1, 32),
	mkI32("l64", append(i32_s1, i32_s0...), i32_bw_mat_1a, i32_bw_mat_1b, append(i32_bw_res_1, i32_bw_res_0...), 64),
	mkI32("l128", append(i32_s1, i32_s0...), i32_bw_mat_1a, i32_bw_mat_1b, append(i32_bw_res_1, i32_bw_res_0...), 128),
	mkI32("l256", append(i32_s1, i32_s0...), i32_bw_mat_1a, i32_bw_mat_1b, append(i32_bw_res_1, i32_bw_res_0...), 256),
	mkI32("l255", append(i32_s1, i32_s0...), i32_bw_mat_1a, i32_bw_mat_1b, append(i32_bw_res_1, i32_bw_res_0...), 255),
	mkI32("l127", i32_s1, i32_bw_mat_1a, i32_bw_mat_1b, i32_bw_res_1, 127),
	mkI32("l63", i32_s1, i32_bw_mat_1a, i32_bw_mat_1b, i32_bw_res_1, 63),
	mkI32("l31", i32_s1, i32_bw_mat_1a, i32_bw_mat_1b, i32_bw_res_1, 31),
	mkI32("l23", i32_s1, i32_bw_mat_1a, i32_bw_mat_1b, i32_bw_res_1, 23),
	mkI32("l15", i32_s1, i32_bw_mat_1a, i32_bw_mat_1b, i32_bw_res_1, 15),
	mkI32("l7", i32_s1, i32_bw_mat_1a, i32_bw_mat_1b, i32_bw_res_1, 7),
	mkI32("neg128", i32_s2, i32_bw_mat_2a, i32_bw_mat_2b, i32_bw_res_2, 128),
	mkI32("neg32", i32_s2, i32_bw_mat_2a, i32_bw_mat_2b, i32_bw_res_2, 32),
	mkI32("neg31", i32_s2, i32_bw_mat_2a, i32_bw_mat_2b, i32_bw_res_2, 31),
	mkI32("ext128", i32_s3, i32_bw_mat_3a, i32_bw_mat_3b, i32_bw_res_3, 128),
	mkI32("ext32", i32_s3, i32_bw_mat_3a, i32_bw_mat_3b, i32_bw_res_3, 32),
	mkI32("ext31", i32_s3, i32_bw_mat_3a, i32_bw_mat_3b, i32_bw_res_3, 31),
	mkI32("full", i32_s3, i32_bw_mat_4a, i32_bw_mat_4b, i32_bw_res_4, 32),
}
