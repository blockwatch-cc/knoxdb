// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

import (
	"fmt"
	"math"
	"math/bits"
	"math/rand"

	"golang.org/x/exp/slices"
)

func RandInt8Slice(n, u int) []int8 {
	s := make([]int8, n*u)
	for i := 0; i < n; i++ {
		s[i] = int8(rand.Intn(math.MaxInt8 + 1))
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

type Int8MatchTest struct {
	Name   string
	Slice  []int8
	Match  int8 // used for every test
	Match2 int8 // used for between tests
	Result []byte
	Count  int64
}

var (
	i8_s0 = []int8{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	i8_eq_mat_0 int8 = 5
	i8_eq_res_0      = []byte{0x6a, 0x1e, 0x48, 0x2c}

	i8_ne_mat_0 int8 = 5
	i8_ne_res_0      = []byte{0x95, 0xe1, 0xb7, 0xd3}

	i8_lt_mat_0 int8 = 5
	i8_lt_res_0      = []byte{0x05, 0x21, 0x27, 0x01}

	i8_le_mat_0 int8 = 5
	i8_le_res_0      = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	i8_gt_mat_0 int8 = 5
	i8_gt_res_0      = []byte{0x90, 0xc0, 0x90, 0xd2}

	i8_ge_mat_0 int8 = 5
	i8_ge_res_0      = []byte{0xfa, 0xde, 0xd8, 0xfe}

	i8_bw_mat_0a int8 = 5
	i8_bw_mat_0b int8 = 10
	i8_bw_res_0       = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	// positive values only
	i8_s1 = []int8{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 50,
		100, 50, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	i8_eq_res_1      = []byte{0x41, 0x42, 0xc4, 0x0e}
	i8_eq_mat_1 int8 = 5

	i8_ne_res_1      = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	i8_ne_mat_1 int8 = 5

	i8_lt_res_1      = []byte{0x0e, 0x00, 0x00, 0x00}
	i8_lt_mat_1 int8 = 5

	i8_le_res_1      = []byte{0x4f, 0x42, 0xc4, 0x0e}
	i8_le_mat_1 int8 = 5

	i8_gt_res_1      = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	i8_gt_mat_1 int8 = 5

	i8_ge_res_1      = []byte{0xf1, 0xff, 0xff, 0xff}
	i8_ge_mat_1 int8 = 5

	i8_bw_res_1       = []byte{0xf1, 0x42, 0xc4, 0x0e}
	i8_bw_mat_1a int8 = 5
	i8_bw_mat_1b int8 = 10

	// negative and positive values mixed
	i8_s2 = []int8{
		-5, 2, -3, 5,
		7, 8, 9, -10,
		15, 50, 55, 50,
		100, -50, 113, 12,
		31, 32, 33, 34,
		35, -36, 37, 38,
		39, 40, -41, 42,
		43, 44, 45, -46,
	}
	i8_eq_res_2      = []byte{0x01, 0x0, 0x0, 0x0}
	i8_eq_mat_2 int8 = -5

	i8_ne_res_2      = []byte{0xfe, 0xff, 0xff, 0xff}
	i8_ne_mat_2 int8 = -5

	i8_lt_res_2      = []byte{0x87, 0x20, 0x20, 0x84}
	i8_lt_mat_2 int8 = 5

	i8_le_res_2      = []byte{0x8f, 0x20, 0x20, 0x84}
	i8_le_mat_2 int8 = 5

	i8_gt_res_2      = []byte{0x70, 0xdf, 0xdf, 0x7b}
	i8_gt_mat_2 int8 = 5

	i8_ge_res_2      = []byte{0x78, 0xdf, 0xdf, 0x7b}
	i8_ge_mat_2 int8 = 5

	i8_bw_res_2       = []byte{0x78, 0x00, 0x00, 0x00}
	i8_bw_mat_2a int8 = 5
	i8_bw_mat_2b int8 = 10

	// extreme values
	i8_s3 = []int8{
		0, 0,
		math.MaxInt8 / 4, math.MinInt8 / 4,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		0, 0,
		math.MaxInt8 / 4, math.MinInt8 / 4,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		0, 0,
		math.MaxInt8 / 4, math.MinInt8 / 4,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		0, 0,
		math.MaxInt8 / 4, math.MinInt8 / 4,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
	}
	i8_eq_res_3      = []byte{0x80, 0x80, 0x80, 0x80}
	i8_eq_mat_3 int8 = math.MinInt8

	i8_ne_res_3      = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	i8_ne_mat_3 int8 = math.MinInt8

	i8_lt_res_3      = []byte{0x0, 0x0, 0x0, 0x0}
	i8_lt_mat_3 int8 = math.MinInt8

	i8_le_res_3      = []byte{0x80, 0x80, 0x80, 0x80}
	i8_le_mat_3 int8 = math.MinInt8

	i8_gt_res_3      = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	i8_gt_mat_3 int8 = math.MinInt8

	i8_ge_res_3      = []byte{0xff, 0xff, 0xff, 0xff}
	i8_ge_mat_3 int8 = math.MinInt8

	i8_bw_res_3       = []byte{0x50, 0x50, 0x50, 0x50}
	i8_bw_mat_3a int8 = math.MaxInt8 / 2
	i8_bw_mat_3b int8 = math.MaxInt8
)

// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mkI8(name string, src []int8, match, match2 int8, result []byte, length int) Int8MatchTest {
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
	return Int8MatchTest{
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
var Int8EqualCases = []Int8MatchTest{
	{"l0", make([]int8, 0), i8_eq_mat_1, 0, []byte{}, 0},
	{"nil", nil, i8_eq_mat_1, 0, []byte{}, 0},
	mkI8("vec1", i8_s0, i8_eq_mat_0, 0, i8_eq_res_0, 32),
	mkI8("vec2", i8_s0, i8_eq_mat_0, 0, i8_eq_res_0, 512),
	mkI8("l32", i8_s1, i8_eq_mat_1, 0, i8_eq_res_1, 32),
	mkI8("l64", append(i8_s1, i8_s0...), i8_eq_mat_1, 0, append(i8_eq_res_1, i8_eq_res_0...), 64),
	mkI8("l128", append(i8_s1, i8_s0...), i8_eq_mat_1, 0, append(i8_eq_res_1, i8_eq_res_0...), 128),
	mkI8("l256", append(i8_s1, i8_s0...), i8_eq_mat_1, 0, append(i8_eq_res_1, i8_eq_res_0...), 256),
	mkI8("l512", append(i8_s1, i8_s0...), i8_eq_mat_1, 0, append(i8_eq_res_1, i8_eq_res_0...), 512),
	mkI8("l1024", append(i8_s1, i8_s0...), i8_eq_mat_1, 0, append(i8_eq_res_1, i8_eq_res_0...), 1024),
	mkI8("l1023", append(i8_s1, i8_s0...), i8_eq_mat_1, 0, append(i8_eq_res_1, i8_eq_res_0...), 1023),
	mkI8("l511", append(i8_s1, i8_s0...), i8_eq_mat_1, 0, append(i8_eq_res_1, i8_eq_res_0...), 511),
	mkI8("l255", append(i8_s1, i8_s0...), i8_eq_mat_1, 0, append(i8_eq_res_1, i8_eq_res_0...), 255),
	mkI8("l127", i8_s1, i8_eq_mat_1, 0, i8_eq_res_1, 127),
	mkI8("l63", i8_s1, i8_eq_mat_1, 0, i8_eq_res_1, 63),
	mkI8("l31", i8_s1, i8_eq_mat_1, 0, i8_eq_res_1, 31),
	mkI8("l23", i8_s1, i8_eq_mat_1, 0, i8_eq_res_1, 23),
	mkI8("l15", i8_s1, i8_eq_mat_1, 0, i8_eq_res_1, 15),
	mkI8("l7", i8_s1, i8_eq_mat_1, 0, i8_eq_res_1, 7),
	mkI8("neg512", i8_s2, i8_eq_mat_2, 0, i8_eq_res_2, 512),
	mkI8("neg32", i8_s2, i8_eq_mat_2, 0, i8_eq_res_2, 32),
	mkI8("neg31", i8_s2, i8_eq_mat_2, 0, i8_eq_res_2, 31),
	mkI8("ext512", i8_s3, i8_eq_mat_3, 0, i8_eq_res_3, 512),
	mkI8("ext32", i8_s3, i8_eq_mat_3, 0, i8_eq_res_3, 32),
	mkI8("ext31", i8_s3, i8_eq_mat_3, 0, i8_eq_res_3, 31),
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
var Int8NotEqualCases = []Int8MatchTest{
	{"l0", make([]int8, 0), i8_ne_mat_1, 0, []byte{}, 0},
	{"nil", nil, i8_ne_mat_1, 0, []byte{}, 0},
	mkI8("vec1", i8_s0, i8_ne_mat_0, 0, i8_ne_res_0, 32),
	mkI8("vec2", i8_s0, i8_ne_mat_0, 0, i8_ne_res_0, 512),
	mkI8("l32", i8_s1, i8_ne_mat_1, 0, i8_ne_res_1, 32),
	mkI8("l64", append(i8_s1, i8_s0...), i8_ne_mat_1, 0, append(i8_ne_res_1, i8_ne_res_0...), 64),
	mkI8("l128", append(i8_s1, i8_s0...), i8_ne_mat_1, 0, append(i8_ne_res_1, i8_ne_res_0...), 128),
	mkI8("l256", append(i8_s1, i8_s0...), i8_ne_mat_1, 0, append(i8_ne_res_1, i8_ne_res_0...), 256),
	mkI8("l512", append(i8_s1, i8_s0...), i8_ne_mat_1, 0, append(i8_ne_res_1, i8_ne_res_0...), 512),
	mkI8("l1024", append(i8_s1, i8_s0...), i8_ne_mat_1, 0, append(i8_ne_res_1, i8_ne_res_0...), 1024),
	mkI8("l1023", append(i8_s1, i8_s0...), i8_ne_mat_1, 0, append(i8_ne_res_1, i8_ne_res_0...), 1023),
	mkI8("l511", append(i8_s1, i8_s0...), i8_ne_mat_1, 0, append(i8_ne_res_1, i8_ne_res_0...), 511),
	mkI8("l255", append(i8_s1, i8_s0...), i8_ne_mat_1, 0, append(i8_ne_res_1, i8_ne_res_0...), 255),
	mkI8("l127", i8_s1, i8_ne_mat_1, 0, i8_ne_res_1, 127),
	mkI8("l63", i8_s1, i8_ne_mat_1, 0, i8_ne_res_1, 63),
	mkI8("l31", i8_s1, i8_ne_mat_1, 0, i8_ne_res_1, 31),
	mkI8("l23", i8_s1, i8_ne_mat_1, 0, i8_ne_res_1, 23),
	mkI8("l15", i8_s1, i8_ne_mat_1, 0, i8_ne_res_1, 15),
	mkI8("l7", i8_s1, i8_ne_mat_1, 0, i8_ne_res_1, 7),
	mkI8("neg512", i8_s2, i8_ne_mat_2, 0, i8_ne_res_2, 512),
	mkI8("neg32", i8_s2, i8_ne_mat_2, 0, i8_ne_res_2, 32),
	mkI8("neg31", i8_s2, i8_ne_mat_2, 0, i8_ne_res_2, 31),
	mkI8("ext512", i8_s3, i8_ne_mat_3, 0, i8_ne_res_3, 512),
	mkI8("ext32", i8_s3, i8_ne_mat_3, 0, i8_ne_res_3, 32),
	mkI8("ext31", i8_s3, i8_ne_mat_3, 0, i8_ne_res_3, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var Int8LessCases = []Int8MatchTest{
	{"l0", make([]int8, 0), i8_lt_mat_1, 0, []byte{}, 0},
	{"nil", nil, i8_lt_mat_1, 0, []byte{}, 0},
	mkI8("vec1", i8_s0, i8_lt_mat_0, 0, i8_lt_res_0, 32),
	mkI8("vec2", i8_s0, i8_lt_mat_0, 0, i8_lt_res_0, 512),
	mkI8("l32", i8_s1, i8_lt_mat_1, 0, i8_lt_res_1, 32),
	mkI8("l64", append(i8_s1, i8_s0...), i8_lt_mat_1, 0, append(i8_lt_res_1, i8_lt_res_0...), 64),
	mkI8("l128", append(i8_s1, i8_s0...), i8_lt_mat_1, 0, append(i8_lt_res_1, i8_lt_res_0...), 128),
	mkI8("l256", append(i8_s1, i8_s0...), i8_lt_mat_1, 0, append(i8_lt_res_1, i8_lt_res_0...), 256),
	mkI8("l512", append(i8_s1, i8_s0...), i8_lt_mat_1, 0, append(i8_lt_res_1, i8_lt_res_0...), 512),
	mkI8("l1024", append(i8_s1, i8_s0...), i8_lt_mat_1, 0, append(i8_lt_res_1, i8_lt_res_0...), 1024),
	mkI8("l1023", append(i8_s1, i8_s0...), i8_lt_mat_1, 0, append(i8_lt_res_1, i8_lt_res_0...), 1023),
	mkI8("l511", append(i8_s1, i8_s0...), i8_lt_mat_1, 0, append(i8_lt_res_1, i8_lt_res_0...), 511),
	mkI8("l255", append(i8_s1, i8_s0...), i8_lt_mat_1, 0, append(i8_lt_res_1, i8_lt_res_0...), 255),
	mkI8("l127", i8_s1, i8_lt_mat_1, 0, i8_lt_res_1, 127),
	mkI8("l63", i8_s1, i8_lt_mat_1, 0, i8_lt_res_1, 63),
	mkI8("l31", i8_s1, i8_lt_mat_1, 0, i8_lt_res_1, 31),
	mkI8("l23", i8_s1, i8_lt_mat_1, 0, i8_lt_res_1, 23),
	mkI8("l15", i8_s1, i8_lt_mat_1, 0, i8_lt_res_1, 15),
	mkI8("l7", i8_s1, i8_lt_mat_1, 0, i8_lt_res_1, 7),
	mkI8("neg512", i8_s2, i8_lt_mat_2, 0, i8_lt_res_2, 512),
	mkI8("neg32", i8_s2, i8_lt_mat_2, 0, i8_lt_res_2, 32),
	mkI8("neg31", i8_s2, i8_lt_mat_2, 0, i8_lt_res_2, 31),
	mkI8("ext512", i8_s3, i8_lt_mat_3, 0, i8_lt_res_3, 512),
	mkI8("ext32", i8_s3, i8_lt_mat_3, 0, i8_lt_res_3, 32),
	mkI8("ext31", i8_s3, i8_lt_mat_3, 0, i8_lt_res_3, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var Int8LessEqualCases = []Int8MatchTest{
	{"l0", make([]int8, 0), i8_le_mat_1, 0, []byte{}, 0},
	{"nil", nil, i8_le_mat_1, 0, []byte{}, 0},
	mkI8("vec1", i8_s0, i8_le_mat_0, 0, i8_le_res_0, 32),
	mkI8("vec2", i8_s0, i8_le_mat_0, 0, i8_le_res_0, 512),
	mkI8("l32", i8_s1, i8_le_mat_1, 0, i8_le_res_1, 32),
	mkI8("l64", append(i8_s1, i8_s0...), i8_le_mat_1, 0, append(i8_le_res_1, i8_le_res_0...), 64),
	mkI8("l128", append(i8_s1, i8_s0...), i8_le_mat_1, 0, append(i8_le_res_1, i8_le_res_0...), 128),
	mkI8("l256", append(i8_s1, i8_s0...), i8_le_mat_1, 0, append(i8_le_res_1, i8_le_res_0...), 256),
	mkI8("l512", append(i8_s1, i8_s0...), i8_le_mat_1, 0, append(i8_le_res_1, i8_le_res_0...), 512),
	mkI8("l1024", append(i8_s1, i8_s0...), i8_le_mat_1, 0, append(i8_le_res_1, i8_le_res_0...), 1024),
	mkI8("l1023", append(i8_s1, i8_s0...), i8_le_mat_1, 0, append(i8_le_res_1, i8_le_res_0...), 1023),
	mkI8("l511", append(i8_s1, i8_s0...), i8_le_mat_1, 0, append(i8_le_res_1, i8_le_res_0...), 511),
	mkI8("l255", append(i8_s1, i8_s0...), i8_le_mat_1, 0, append(i8_le_res_1, i8_le_res_0...), 255),
	mkI8("l127", i8_s1, i8_le_mat_1, 0, i8_le_res_1, 127),
	mkI8("l63", i8_s1, i8_le_mat_1, 0, i8_le_res_1, 63),
	mkI8("l31", i8_s1, i8_le_mat_1, 0, i8_le_res_1, 31),
	mkI8("l23", i8_s1, i8_le_mat_1, 0, i8_le_res_1, 23),
	mkI8("l15", i8_s1, i8_le_mat_1, 0, i8_le_res_1, 15),
	mkI8("l7", i8_s1, i8_le_mat_1, 0, i8_le_res_1, 7),
	mkI8("neg512", i8_s2, i8_le_mat_2, 0, i8_le_res_2, 512),
	mkI8("neg32", i8_s2, i8_le_mat_2, 0, i8_le_res_2, 32),
	mkI8("neg31", i8_s2, i8_le_mat_2, 0, i8_le_res_2, 31),
	mkI8("ext512", i8_s3, i8_le_mat_3, 0, i8_le_res_3, 512),
	mkI8("ext32", i8_s3, i8_le_mat_3, 0, i8_le_res_3, 32),
	mkI8("ext31", i8_s3, i8_le_mat_3, 0, i8_le_res_3, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var Int8GreaterCases = []Int8MatchTest{
	{"l0", make([]int8, 0), i8_gt_mat_1, 0, []byte{}, 0},
	{"nil", nil, i8_gt_mat_1, 0, []byte{}, 0},
	mkI8("vec1", i8_s0, i8_gt_mat_0, 0, i8_gt_res_0, 32),
	mkI8("vec2", i8_s0, i8_gt_mat_0, 0, i8_gt_res_0, 512),
	mkI8("l32", i8_s1, i8_gt_mat_1, 0, i8_gt_res_1, 32),
	mkI8("l64", append(i8_s1, i8_s0...), i8_gt_mat_1, 0, append(i8_gt_res_1, i8_gt_res_0...), 64),
	mkI8("l128", append(i8_s1, i8_s0...), i8_gt_mat_1, 0, append(i8_gt_res_1, i8_gt_res_0...), 128),
	mkI8("l256", append(i8_s1, i8_s0...), i8_gt_mat_1, 0, append(i8_gt_res_1, i8_gt_res_0...), 256),
	mkI8("l512", append(i8_s1, i8_s0...), i8_gt_mat_1, 0, append(i8_gt_res_1, i8_gt_res_0...), 512),
	mkI8("l1024", append(i8_s1, i8_s0...), i8_gt_mat_1, 0, append(i8_gt_res_1, i8_gt_res_0...), 1024),
	mkI8("l1023", append(i8_s1, i8_s0...), i8_gt_mat_1, 0, append(i8_gt_res_1, i8_gt_res_0...), 1023),
	mkI8("l511", append(i8_s1, i8_s0...), i8_gt_mat_1, 0, append(i8_gt_res_1, i8_gt_res_0...), 511),
	mkI8("l255", append(i8_s1, i8_s0...), i8_gt_mat_1, 0, append(i8_gt_res_1, i8_gt_res_0...), 255),
	mkI8("l127", i8_s1, i8_gt_mat_1, 0, i8_gt_res_1, 127),
	mkI8("l63", i8_s1, i8_gt_mat_1, 0, i8_gt_res_1, 63),
	mkI8("l31", i8_s1, i8_gt_mat_1, 0, i8_gt_res_1, 31),
	mkI8("l23", i8_s1, i8_gt_mat_1, 0, i8_gt_res_1, 23),
	mkI8("l15", i8_s1, i8_gt_mat_1, 0, i8_gt_res_1, 15),
	mkI8("l7", i8_s1, i8_gt_mat_1, 0, i8_gt_res_1, 7),
	mkI8("neg512", i8_s2, i8_gt_mat_2, 0, i8_gt_res_2, 512),
	mkI8("neg32", i8_s2, i8_gt_mat_2, 0, i8_gt_res_2, 32),
	mkI8("neg31", i8_s2, i8_gt_mat_2, 0, i8_gt_res_2, 31),
	mkI8("ext512", i8_s3, i8_gt_mat_3, 0, i8_gt_res_3, 512),
	mkI8("ext32", i8_s3, i8_gt_mat_3, 0, i8_gt_res_3, 32),
	mkI8("ext31", i8_s3, i8_gt_mat_3, 0, i8_gt_res_3, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var Int8GreaterEqualCases = []Int8MatchTest{
	{"l0", make([]int8, 0), i8_ge_mat_1, 0, []byte{}, 0},
	{"nil", nil, i8_ge_mat_1, 0, []byte{}, 0},
	mkI8("vec1", i8_s0, i8_ge_mat_0, 0, i8_ge_res_0, 32),
	mkI8("vec2", i8_s0, i8_ge_mat_0, 0, i8_ge_res_0, 512),
	mkI8("l32", i8_s1, i8_ge_mat_1, 0, i8_ge_res_1, 32),
	mkI8("l64", append(i8_s1, i8_s0...), i8_ge_mat_1, 0, append(i8_ge_res_1, i8_ge_res_0...), 64),
	mkI8("l128", append(i8_s1, i8_s0...), i8_ge_mat_1, 0, append(i8_ge_res_1, i8_ge_res_0...), 128),
	mkI8("l256", append(i8_s1, i8_s0...), i8_ge_mat_1, 0, append(i8_ge_res_1, i8_ge_res_0...), 256),
	mkI8("l512", append(i8_s1, i8_s0...), i8_ge_mat_1, 0, append(i8_ge_res_1, i8_ge_res_0...), 512),
	mkI8("l1024", append(i8_s1, i8_s0...), i8_ge_mat_1, 0, append(i8_ge_res_1, i8_ge_res_0...), 1024),
	mkI8("l1023", append(i8_s1, i8_s0...), i8_ge_mat_1, 0, append(i8_ge_res_1, i8_ge_res_0...), 1023),
	mkI8("l511", append(i8_s1, i8_s0...), i8_ge_mat_1, 0, append(i8_ge_res_1, i8_ge_res_0...), 511),
	mkI8("l255", append(i8_s1, i8_s0...), i8_ge_mat_1, 0, append(i8_ge_res_1, i8_ge_res_0...), 255),
	mkI8("l127", i8_s1, i8_ge_mat_1, 0, i8_ge_res_1, 127),
	mkI8("l63", i8_s1, i8_ge_mat_1, 0, i8_ge_res_1, 63),
	mkI8("l31", i8_s1, i8_ge_mat_1, 0, i8_ge_res_1, 31),
	mkI8("l23", i8_s1, i8_ge_mat_1, 0, i8_ge_res_1, 23),
	mkI8("l15", i8_s1, i8_ge_mat_1, 0, i8_ge_res_1, 15),
	mkI8("l7", i8_s1, i8_ge_mat_1, 0, i8_ge_res_1, 7),
	mkI8("neg512", i8_s2, i8_ge_mat_2, 0, i8_ge_res_2, 512),
	mkI8("neg32", i8_s2, i8_ge_mat_2, 0, i8_ge_res_2, 32),
	mkI8("neg31", i8_s2, i8_ge_mat_2, 0, i8_ge_res_2, 31),
	mkI8("ext512", i8_s3, i8_ge_mat_3, 0, i8_ge_res_3, 512),
	mkI8("ext32", i8_s3, i8_ge_mat_3, 0, i8_ge_res_3, 32),
	mkI8("ext31", i8_s3, i8_ge_mat_3, 0, i8_ge_res_3, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var Int8BetweenCases = []Int8MatchTest{
	{"l0", make([]int8, 0), i8_bw_mat_1a, i8_bw_mat_1b, []byte{}, 0},
	{"nil", nil, i8_bw_mat_1a, i8_bw_mat_1b, []byte{}, 0},
	mkI8("vec1", i8_s0, i8_bw_mat_0a, i8_bw_mat_0b, i8_bw_res_0, 32),
	mkI8("vec2", i8_s0, i8_bw_mat_0a, i8_bw_mat_0b, i8_bw_res_0, 512),
	mkI8("l32", i8_s1, i8_bw_mat_1a, i8_bw_mat_1b, i8_bw_res_1, 32),
	mkI8("l64", append(i8_s1, i8_s0...), i8_bw_mat_1a, i8_bw_mat_1b, append(i8_bw_res_1, i8_bw_res_0...), 64),
	mkI8("l128", append(i8_s1, i8_s0...), i8_bw_mat_1a, i8_bw_mat_1b, append(i8_bw_res_1, i8_bw_res_0...), 128),
	mkI8("l256", append(i8_s1, i8_s0...), i8_bw_mat_1a, i8_bw_mat_1b, append(i8_bw_res_1, i8_bw_res_0...), 256),
	mkI8("l512", append(i8_s1, i8_s0...), i8_bw_mat_1a, i8_bw_mat_1b, append(i8_bw_res_1, i8_bw_res_0...), 512),
	mkI8("l1024", append(i8_s1, i8_s0...), i8_bw_mat_1a, i8_bw_mat_1b, append(i8_bw_res_1, i8_bw_res_0...), 1024),
	mkI8("l1023", append(i8_s1, i8_s0...), i8_bw_mat_1a, i8_bw_mat_1b, append(i8_bw_res_1, i8_bw_res_0...), 1023),
	mkI8("l511", append(i8_s1, i8_s0...), i8_bw_mat_1a, i8_bw_mat_1b, append(i8_bw_res_1, i8_bw_res_0...), 511),
	mkI8("l255", append(i8_s1, i8_s0...), i8_bw_mat_1a, i8_bw_mat_1b, append(i8_bw_res_1, i8_bw_res_0...), 255),
	mkI8("l127", i8_s1, i8_bw_mat_1a, i8_bw_mat_1b, i8_bw_res_1, 127),
	mkI8("l63", i8_s1, i8_bw_mat_1a, i8_bw_mat_1b, i8_bw_res_1, 63),
	mkI8("l31", i8_s1, i8_bw_mat_1a, i8_bw_mat_1b, i8_bw_res_1, 31),
	mkI8("l23", i8_s1, i8_bw_mat_1a, i8_bw_mat_1b, i8_bw_res_1, 23),
	mkI8("l15", i8_s1, i8_bw_mat_1a, i8_bw_mat_1b, i8_bw_res_1, 15),
	mkI8("l7", i8_s1, i8_bw_mat_1a, i8_bw_mat_1b, i8_bw_res_1, 7),
	mkI8("neg512", i8_s2, i8_bw_mat_2a, i8_bw_mat_2b, i8_bw_res_2, 512),
	mkI8("neg32", i8_s2, i8_bw_mat_2a, i8_bw_mat_2b, i8_bw_res_2, 32),
	mkI8("neg31", i8_s2, i8_bw_mat_2a, i8_bw_mat_2b, i8_bw_res_2, 31),
	mkI8("ext512", i8_s3, i8_bw_mat_3a, i8_bw_mat_3b, i8_bw_res_3, 512),
	mkI8("ext32", i8_s3, i8_bw_mat_3a, i8_bw_mat_3b, i8_bw_res_3, 32),
	mkI8("ext31", i8_s3, i8_bw_mat_3a, i8_bw_mat_3b, i8_bw_res_3, 31),
}
