// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"fmt"
	"math"
	"math/bits"
	"slices"
)

var (
	i64_s0 = []int64{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	i64_eq_mat_0 int64 = 5
	i64_eq_res_0       = []byte{0x6a, 0x1e, 0x48, 0x2c}

	i64_ne_mat_0 int64 = 5
	i64_ne_res_0       = []byte{0x95, 0xe1, 0xb7, 0xd3}

	i64_lt_mat_0 int64 = 5
	i64_lt_res_0       = []byte{0x05, 0x21, 0x27, 0x01}

	i64_le_mat_0 int64 = 5
	i64_le_res_0       = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	i64_gt_mat_0 int64 = 5
	i64_gt_res_0       = []byte{0x90, 0xc0, 0x90, 0xd2}

	i64_ge_mat_0 int64 = 5
	i64_ge_res_0       = []byte{0xfa, 0xde, 0xd8, 0xfe}

	i64_bw_mat_0a int64 = 5
	i64_bw_mat_0b int64 = 10
	i64_bw_res_0        = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	// positive values only
	i64_s1 = []int64{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 500000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	i64_eq_res_1       = []byte{0x41, 0x42, 0xc4, 0x0e}
	i64_eq_mat_1 int64 = 5

	i64_ne_res_1       = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	i64_ne_mat_1 int64 = 5

	i64_lt_res_1       = []byte{0x0e, 0x00, 0x00, 0x00}
	i64_lt_mat_1 int64 = 5

	i64_le_res_1       = []byte{0x4f, 0x42, 0xc4, 0x0e}
	i64_le_mat_1 int64 = 5

	i64_gt_res_1       = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	i64_gt_mat_1 int64 = 5

	i64_ge_res_1       = []byte{0xf1, 0xff, 0xff, 0xff}
	i64_ge_mat_1 int64 = 5

	i64_bw_res_1        = []byte{0xf1, 0x42, 0xc4, 0x0e}
	i64_bw_mat_1a int64 = 5
	i64_bw_mat_1b int64 = 10

	// negative and positive values mixed
	i64_s2 = []int64{
		-5, 2, -3, 5,
		7, 8, 9, -10,
		15, 50, 55, 500,
		1000, -500000, 113, 12,
		31, 32, 33, 34,
		35, -36, 37, 38,
		39, 40, -41, 42,
		43, 44, 45, -46,
	}
	i64_eq_res_2       = []byte{0x01, 0x0, 0x0, 0x0}
	i64_eq_mat_2 int64 = -5

	i64_ne_res_2       = []byte{0xfe, 0xff, 0xff, 0xff}
	i64_ne_mat_2 int64 = -5

	i64_lt_res_2       = []byte{0x87, 0x20, 0x20, 0x84}
	i64_lt_mat_2 int64 = 5

	i64_le_res_2       = []byte{0x8f, 0x20, 0x20, 0x84}
	i64_le_mat_2 int64 = 5

	i64_gt_res_2       = []byte{0x70, 0xdf, 0xdf, 0x7b}
	i64_gt_mat_2 int64 = 5

	i64_ge_res_2       = []byte{0x78, 0xdf, 0xdf, 0x7b}
	i64_ge_mat_2 int64 = 5

	i64_bw_res_2        = []byte{0x78, 0x00, 0x00, 0x00}
	i64_bw_mat_2a int64 = 5
	i64_bw_mat_2b int64 = 10

	// extreme values
	i64_s3 = []int64{
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		math.MaxInt64, math.MinInt64,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		math.MaxInt64, math.MinInt64,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		math.MaxInt64, math.MinInt64,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		math.MaxInt64, math.MinInt64,
	}
	i64_eq_res_3       = []byte{0x80, 0x80, 0x80, 0x80}
	i64_eq_mat_3 int64 = math.MinInt64

	i64_ne_res_3       = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	i64_ne_mat_3 int64 = math.MinInt64

	i64_lt_res_3       = []byte{0x0, 0x0, 0x0, 0x0}
	i64_lt_mat_3 int64 = math.MinInt64

	i64_le_res_3       = []byte{0x80, 0x80, 0x80, 0x80}
	i64_le_mat_3 int64 = math.MinInt64

	i64_gt_res_3       = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	i64_gt_mat_3 int64 = math.MinInt64

	i64_ge_res_3       = []byte{0xff, 0xff, 0xff, 0xff}
	i64_ge_mat_3 int64 = math.MinInt64

	i64_bw_res_3        = []byte{0x50, 0x50, 0x50, 0x50}
	i64_bw_mat_3a int64 = math.MaxInt32
	i64_bw_mat_3b int64 = math.MaxInt64

	i64_bw_res_4        = []byte{0xff, 0xff, 0xff, 0xff}
	i64_bw_mat_4a int64 = math.MinInt64
	i64_bw_mat_4b int64 = math.MaxInt64
)

// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mkI64(name string, src []int64, match, match2 int64, result []byte, length int) MatchTest[int64] {
	if len(src)%8 != 0 {
		panic(fmt.Errorf("i64 %s: length of slice has to be a multiple of 8", name))
	}
	if len(result) != BitFieldLen(len(src)) {
		panic(fmt.Errorf("i64 %s: length of slice and length of result does not match", name))
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
	return MatchTest[int64]{
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
var Int64EqualCases = []MatchTest[int64]{
	{"l0", make([]int64, 0), i64_eq_mat_1, 0, []byte{}, 0},
	{"nil", nil, i64_eq_mat_1, 0, []byte{}, 0},
	mkI64("vec1", i64_s0, i64_eq_mat_0, 0, i64_eq_res_0, 32),
	mkI64("vec2", i64_s0, i64_eq_mat_0, 0, i64_eq_res_0, 64),
	mkI64("l32", i64_s1, i64_eq_mat_1, 0, i64_eq_res_1, 32),
	mkI64("l64", append(i64_s1, i64_s0...), i64_eq_mat_1, 0, append(i64_eq_res_1, i64_eq_res_0...), 64),
	mkI64("l128", append(i64_s1, i64_s0...), i64_eq_mat_1, 0, append(i64_eq_res_1, i64_eq_res_0...), 128),
	mkI64("l127", i64_s1, i64_eq_mat_1, 0, i64_eq_res_1, 127),
	mkI64("l63", i64_s1, i64_eq_mat_1, 0, i64_eq_res_1, 63),
	mkI64("l31", i64_s1, i64_eq_mat_1, 0, i64_eq_res_1, 31),
	mkI64("l23", i64_s1, i64_eq_mat_1, 0, i64_eq_res_1, 23),
	mkI64("l15", i64_s1, i64_eq_mat_1, 0, i64_eq_res_1, 15),
	mkI64("l7", i64_s1, i64_eq_mat_1, 0, i64_eq_res_1, 7),
	mkI64("neg64", i64_s2, i64_eq_mat_2, 0, i64_eq_res_2, 64),
	mkI64("neg32", i64_s2, i64_eq_mat_2, 0, i64_eq_res_2, 32),
	mkI64("neg31", i64_s2, i64_eq_mat_2, 0, i64_eq_res_2, 31),
	mkI64("ext64", i64_s3, i64_eq_mat_3, 0, i64_eq_res_3, 64),
	mkI64("ext32", i64_s3, i64_eq_mat_3, 0, i64_eq_res_3, 32),
	mkI64("ext31", i64_s3, i64_eq_mat_3, 0, i64_eq_res_3, 31),
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
var Int64NotEqualCases = []MatchTest[int64]{
	{"l0", make([]int64, 0), i64_ne_mat_1, 0, []byte{}, 0},
	{"nil", nil, i64_ne_mat_1, 0, []byte{}, 0},
	mkI64("vec1", i64_s0, i64_ne_mat_0, 0, i64_ne_res_0, 32),
	mkI64("vec2", i64_s0, i64_ne_mat_0, 0, i64_ne_res_0, 64),
	mkI64("l32", i64_s1, i64_ne_mat_1, 0, i64_ne_res_1, 32),
	mkI64("l64", append(i64_s1, i64_s0...), i64_ne_mat_1, 0, append(i64_ne_res_1, i64_ne_res_0...), 64),
	mkI64("l128", append(i64_s1, i64_s0...), i64_ne_mat_1, 0, append(i64_ne_res_1, i64_ne_res_0...), 128),
	mkI64("l127", i64_s1, i64_ne_mat_1, 0, i64_ne_res_1, 127),
	mkI64("l63", i64_s1, i64_ne_mat_1, 0, i64_ne_res_1, 63),
	mkI64("l31", i64_s1, i64_ne_mat_1, 0, i64_ne_res_1, 31),
	mkI64("l23", i64_s1, i64_ne_mat_1, 0, i64_ne_res_1, 23),
	mkI64("l15", i64_s1, i64_ne_mat_1, 0, i64_ne_res_1, 15),
	mkI64("l7", i64_s1, i64_ne_mat_1, 0, i64_ne_res_1, 7),
	mkI64("neg64", i64_s2, i64_ne_mat_2, 0, i64_ne_res_2, 64),
	mkI64("neg32", i64_s2, i64_ne_mat_2, 0, i64_ne_res_2, 32),
	mkI64("neg31", i64_s2, i64_ne_mat_2, 0, i64_ne_res_2, 31),
	mkI64("ext64", i64_s3, i64_ne_mat_3, 0, i64_ne_res_3, 64),
	mkI64("ext32", i64_s3, i64_ne_mat_3, 0, i64_ne_res_3, 32),
	mkI64("ext31", i64_s3, i64_ne_mat_3, 0, i64_ne_res_3, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var Int64LessCases = []MatchTest[int64]{
	{"l0", make([]int64, 0), i64_lt_mat_1, 0, []byte{}, 0},
	{"nil", nil, i64_lt_mat_1, 0, []byte{}, 0},
	mkI64("vec1", i64_s0, i64_lt_mat_0, 0, i64_lt_res_0, 32),
	mkI64("vec2", i64_s0, i64_lt_mat_0, 0, i64_lt_res_0, 64),
	mkI64("l32", i64_s1, i64_lt_mat_1, 0, i64_lt_res_1, 32),
	mkI64("l64", append(i64_s1, i64_s0...), i64_lt_mat_1, 0, append(i64_lt_res_1, i64_lt_res_0...), 64),
	mkI64("l128", append(i64_s1, i64_s0...), i64_lt_mat_1, 0, append(i64_lt_res_1, i64_lt_res_0...), 128),
	mkI64("l127", i64_s1, i64_lt_mat_1, 0, i64_lt_res_1, 127),
	mkI64("l63", i64_s1, i64_lt_mat_1, 0, i64_lt_res_1, 63),
	mkI64("l31", i64_s1, i64_lt_mat_1, 0, i64_lt_res_1, 31),
	mkI64("l23", i64_s1, i64_lt_mat_1, 0, i64_lt_res_1, 23),
	mkI64("l15", i64_s1, i64_lt_mat_1, 0, i64_lt_res_1, 15),
	mkI64("l7", i64_s1, i64_lt_mat_1, 0, i64_lt_res_1, 7),
	mkI64("neg64", i64_s2, i64_lt_mat_2, 0, i64_lt_res_2, 64),
	mkI64("neg32", i64_s2, i64_lt_mat_2, 0, i64_lt_res_2, 32),
	mkI64("neg31", i64_s2, i64_lt_mat_2, 0, i64_lt_res_2, 31),
	mkI64("ext64", i64_s3, i64_lt_mat_3, 0, i64_lt_res_3, 64),
	mkI64("ext32", i64_s3, i64_lt_mat_3, 0, i64_lt_res_3, 32),
	mkI64("ext31", i64_s3, i64_lt_mat_3, 0, i64_lt_res_3, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var Int64LessEqualCases = []MatchTest[int64]{
	{"l0", make([]int64, 0), i64_le_mat_1, 0, []byte{}, 0},
	{"nil", nil, i64_le_mat_1, 0, []byte{}, 0},
	mkI64("vec1", i64_s0, i64_le_mat_0, 0, i64_le_res_0, 32),
	mkI64("vec2", i64_s0, i64_le_mat_0, 0, i64_le_res_0, 64),
	mkI64("l32", i64_s1, i64_le_mat_1, 0, i64_le_res_1, 32),
	mkI64("l64", append(i64_s1, i64_s0...), i64_le_mat_1, 0, append(i64_le_res_1, i64_le_res_0...), 64),
	mkI64("l128", append(i64_s1, i64_s0...), i64_le_mat_1, 0, append(i64_le_res_1, i64_le_res_0...), 128),
	mkI64("l127", i64_s1, i64_le_mat_1, 0, i64_le_res_1, 127),
	mkI64("l63", i64_s1, i64_le_mat_1, 0, i64_le_res_1, 63),
	mkI64("l31", i64_s1, i64_le_mat_1, 0, i64_le_res_1, 31),
	mkI64("l23", i64_s1, i64_le_mat_1, 0, i64_le_res_1, 23),
	mkI64("l15", i64_s1, i64_le_mat_1, 0, i64_le_res_1, 15),
	mkI64("l7", i64_s1, i64_le_mat_1, 0, i64_le_res_1, 7),
	mkI64("neg64", i64_s2, i64_le_mat_2, 0, i64_le_res_2, 64),
	mkI64("neg32", i64_s2, i64_le_mat_2, 0, i64_le_res_2, 32),
	mkI64("neg31", i64_s2, i64_le_mat_2, 0, i64_le_res_2, 31),
	mkI64("ext64", i64_s3, i64_le_mat_3, 0, i64_le_res_3, 64),
	mkI64("ext32", i64_s3, i64_le_mat_3, 0, i64_le_res_3, 32),
	mkI64("ext31", i64_s3, i64_le_mat_3, 0, i64_le_res_3, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var Int64GreaterCases = []MatchTest[int64]{
	{"l0", make([]int64, 0), i64_gt_mat_1, 0, []byte{}, 0},
	{"nil", nil, i64_gt_mat_1, 0, []byte{}, 0},
	mkI64("vec1", i64_s0, i64_gt_mat_0, 0, i64_gt_res_0, 32),
	mkI64("vec2", i64_s0, i64_gt_mat_0, 0, i64_gt_res_0, 64),
	mkI64("l32", i64_s1, i64_gt_mat_1, 0, i64_gt_res_1, 32),
	mkI64("l64", append(i64_s1, i64_s0...), i64_gt_mat_1, 0, append(i64_gt_res_1, i64_gt_res_0...), 64),
	mkI64("l128", append(i64_s1, i64_s0...), i64_gt_mat_1, 0, append(i64_gt_res_1, i64_gt_res_0...), 128),
	mkI64("l127", i64_s1, i64_gt_mat_1, 0, i64_gt_res_1, 127),
	mkI64("l63", i64_s1, i64_gt_mat_1, 0, i64_gt_res_1, 63),
	mkI64("l31", i64_s1, i64_gt_mat_1, 0, i64_gt_res_1, 31),
	mkI64("l23", i64_s1, i64_gt_mat_1, 0, i64_gt_res_1, 23),
	mkI64("l15", i64_s1, i64_gt_mat_1, 0, i64_gt_res_1, 15),
	mkI64("l7", i64_s1, i64_gt_mat_1, 0, i64_gt_res_1, 7),
	mkI64("neg64", i64_s2, i64_gt_mat_2, 0, i64_gt_res_2, 64),
	mkI64("neg32", i64_s2, i64_gt_mat_2, 0, i64_gt_res_2, 32),
	mkI64("neg31", i64_s2, i64_gt_mat_2, 0, i64_gt_res_2, 31),
	mkI64("ext64", i64_s3, i64_gt_mat_3, 0, i64_gt_res_3, 64),
	mkI64("ext32", i64_s3, i64_gt_mat_3, 0, i64_gt_res_3, 32),
	mkI64("ext31", i64_s3, i64_gt_mat_3, 0, i64_gt_res_3, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var Int64GreaterEqualCases = []MatchTest[int64]{
	{"l0", make([]int64, 0), i64_ge_mat_1, 0, []byte{}, 0},
	{"nil", nil, i64_ge_mat_1, 0, []byte{}, 0},
	mkI64("vec1", i64_s0, i64_ge_mat_0, 0, i64_ge_res_0, 32),
	mkI64("vec2", i64_s0, i64_ge_mat_0, 0, i64_ge_res_0, 64),
	mkI64("l32", i64_s1, i64_ge_mat_1, 0, i64_ge_res_1, 32),
	mkI64("l64", append(i64_s1, i64_s0...), i64_ge_mat_1, 0, append(i64_ge_res_1, i64_ge_res_0...), 64),
	mkI64("l128", append(i64_s1, i64_s0...), i64_ge_mat_1, 0, append(i64_ge_res_1, i64_ge_res_0...), 128),
	mkI64("l127", i64_s1, i64_ge_mat_1, 0, i64_ge_res_1, 127),
	mkI64("l63", i64_s1, i64_ge_mat_1, 0, i64_ge_res_1, 63),
	mkI64("l31", i64_s1, i64_ge_mat_1, 0, i64_ge_res_1, 31),
	mkI64("l23", i64_s1, i64_ge_mat_1, 0, i64_ge_res_1, 23),
	mkI64("l15", i64_s1, i64_ge_mat_1, 0, i64_ge_res_1, 15),
	mkI64("l7", i64_s1, i64_ge_mat_1, 0, i64_ge_res_1, 7),
	mkI64("neg64", i64_s2, i64_ge_mat_2, 0, i64_ge_res_2, 64),
	mkI64("neg32", i64_s2, i64_ge_mat_2, 0, i64_ge_res_2, 32),
	mkI64("neg31", i64_s2, i64_ge_mat_2, 0, i64_ge_res_2, 31),
	mkI64("ext64", i64_s3, i64_ge_mat_3, 0, i64_ge_res_3, 64),
	mkI64("ext32", i64_s3, i64_ge_mat_3, 0, i64_ge_res_3, 32),
	mkI64("ext31", i64_s3, i64_ge_mat_3, 0, i64_ge_res_3, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var Int64BetweenCases = []MatchTest[int64]{
	{"l0", make([]int64, 0), i64_bw_mat_1a, i64_bw_mat_1b, []byte{}, 0},
	{"nil", nil, i64_bw_mat_1a, i64_bw_mat_1b, []byte{}, 0},
	mkI64("vec1", i64_s0, i64_bw_mat_0a, i64_bw_mat_0b, i64_bw_res_0, 32),
	mkI64("vec2", i64_s0, i64_bw_mat_0a, i64_bw_mat_0b, i64_bw_res_0, 64),
	mkI64("l32", i64_s1, i64_bw_mat_1a, i64_bw_mat_1b, i64_bw_res_1, 32),
	mkI64("l64", append(i64_s1, i64_s0...), i64_bw_mat_1a, i64_bw_mat_1b, append(i64_bw_res_1, i64_bw_res_0...), 64),
	mkI64("l128", append(i64_s1, i64_s0...), i64_bw_mat_1a, i64_bw_mat_1b, append(i64_bw_res_1, i64_bw_res_0...), 128),
	mkI64("l127", i64_s1, i64_bw_mat_1a, i64_bw_mat_1b, i64_bw_res_1, 127),
	mkI64("l63", i64_s1, i64_bw_mat_1a, i64_bw_mat_1b, i64_bw_res_1, 63),
	mkI64("l31", i64_s1, i64_bw_mat_1a, i64_bw_mat_1b, i64_bw_res_1, 31),
	mkI64("l23", i64_s1, i64_bw_mat_1a, i64_bw_mat_1b, i64_bw_res_1, 23),
	mkI64("l15", i64_s1, i64_bw_mat_1a, i64_bw_mat_1b, i64_bw_res_1, 15),
	mkI64("l7", i64_s1, i64_bw_mat_1a, i64_bw_mat_1b, i64_bw_res_1, 7),
	mkI64("neg64", i64_s2, i64_bw_mat_2a, i64_bw_mat_2b, i64_bw_res_2, 64),
	mkI64("neg32", i64_s2, i64_bw_mat_2a, i64_bw_mat_2b, i64_bw_res_2, 32),
	mkI64("neg31", i64_s2, i64_bw_mat_2a, i64_bw_mat_2b, i64_bw_res_2, 31),
	mkI64("ext64", i64_s3, i64_bw_mat_3a, i64_bw_mat_3b, i64_bw_res_3, 64),
	mkI64("ext32", i64_s3, i64_bw_mat_3a, i64_bw_mat_3b, i64_bw_res_3, 32),
	mkI64("ext31", i64_s3, i64_bw_mat_3a, i64_bw_mat_3b, i64_bw_res_3, 31),
	mkI64("full", i64_s3, i64_bw_mat_4a, i64_bw_mat_4b, i64_bw_res_4, 32),
}
