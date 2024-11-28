// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"fmt"
	"math"
	"math/bits"

	"golang.org/x/exp/slices"
)

var (
	u8_s0 = []uint8{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	u8_eq_mat_0 uint8 = 5
	u8_eq_res_0       = []byte{0x6a, 0x1e, 0x48, 0x2c}

	u8_ne_mat_0 uint8 = 5
	u8_ne_res_0       = []byte{0x95, 0xe1, 0xb7, 0xd3}

	u8_lt_mat_0 uint8 = 5
	u8_lt_res_0       = []byte{0x05, 0x21, 0x27, 0x01}

	u8_le_mat_0 uint8 = 5
	u8_le_res_0       = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	u8_gt_mat_0 uint8 = 5
	u8_gt_res_0       = []byte{0x90, 0xc0, 0x90, 0xd2}

	u8_ge_mat_0 uint8 = 5
	u8_ge_res_0       = []byte{0xfa, 0xde, 0xd8, 0xfe}

	u8_bw_mat_0a uint8 = 5
	u8_bw_mat_0b uint8 = 10
	u8_bw_res_0        = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	u8_s1 = []uint8{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 50,
		100, 50, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}

	u8_eq_res_1       = []byte{0x41, 0x42, 0xc4, 0x0e}
	u8_eq_mat_1 uint8 = 5

	u8_ne_res_1       = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	u8_ne_mat_1 uint8 = 5

	u8_lt_res_1       = []byte{0x0e, 0x00, 0x00, 0x00}
	u8_lt_mat_1 uint8 = 5

	u8_le_res_1       = []byte{0x4f, 0x42, 0xc4, 0x0e}
	u8_le_mat_1 uint8 = 5

	u8_gt_res_1       = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	u8_gt_mat_1 uint8 = 5

	u8_ge_res_1       = []byte{0xf1, 0xff, 0xff, 0xff}
	u8_ge_mat_1 uint8 = 5

	u8_bw_res_1        = []byte{0xf1, 0x42, 0xc4, 0x0e}
	u8_bw_mat_1a uint8 = 5
	u8_bw_mat_1b uint8 = 10

	// extreme values
	u8_s2 = []uint8{
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
	}
	u8_eq_res_2       = []byte{0x88, 0x88, 0x88, 0x88}
	u8_eq_mat_2 uint8 = math.MaxUint8

	u8_ne_res_2       = []byte{0x77, 0x77, 0x77, 0x77}
	u8_ne_mat_2 uint8 = math.MaxUint8

	u8_lt_res_2       = []byte{0x77, 0x77, 0x77, 0x77}
	u8_lt_mat_2 uint8 = math.MaxUint8

	u8_le_res_2       = []byte{0xff, 0xff, 0xff, 0xff}
	u8_le_mat_2 uint8 = math.MaxUint8

	u8_gt_res_2       = []byte{0x00, 0x00, 0x00, 0x00}
	u8_gt_mat_2 uint8 = math.MaxUint8

	u8_ge_res_2       = []byte{0x88, 0x88, 0x88, 0x88}
	u8_ge_mat_2 uint8 = math.MaxUint8

	u8_bw_res_2        = []byte{0xcc, 0xcc, 0xcc, 0xcc}
	u8_bw_mat_2a uint8 = math.MaxInt8
	u8_bw_mat_2b uint8 = math.MaxUint8
)

// creates an uint8 test case from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mkU8(name string, src []uint8, match, match2 uint8, result []byte, length int) MatchTest[uint8] {
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
	return MatchTest[uint8]{
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
var Uint8EqualCases = []MatchTest[uint8]{
	{"l0", make([]uint8, 0), u8_eq_mat_1, 0, []byte{}, 0},
	{"nil", nil, u8_eq_mat_1, 0, []byte{}, 0},
	mkU8("vec1", u8_s0, u8_eq_mat_0, 0, u8_eq_res_0, 32),
	mkU8("vec2", u8_s0, u8_eq_mat_0, 0, u8_eq_res_0, 512),
	mkU8("l32", u8_s1, u8_eq_mat_1, 0, u8_eq_res_1, 32),
	mkU8("l64", append(u8_s1, u8_s0...), u8_eq_mat_1, 0, append(u8_eq_res_1, u8_eq_res_0...), 64),
	mkU8("l128", append(u8_s1, u8_s0...), u8_eq_mat_1, 0, append(u8_eq_res_1, u8_eq_res_0...), 128),
	mkU8("l256", append(u8_s1, u8_s0...), u8_eq_mat_1, 0, append(u8_eq_res_1, u8_eq_res_0...), 256),
	mkU8("l512", append(u8_s1, u8_s0...), u8_eq_mat_1, 0, append(u8_eq_res_1, u8_eq_res_0...), 512),
	mkU8("l1024", append(u8_s1, u8_s0...), u8_eq_mat_1, 0, append(u8_eq_res_1, u8_eq_res_0...), 1024),
	mkU8("l1023", append(u8_s1, u8_s0...), u8_eq_mat_1, 0, append(u8_eq_res_1, u8_eq_res_0...), 1023),
	mkU8("l511", append(u8_s1, u8_s0...), u8_eq_mat_1, 0, append(u8_eq_res_1, u8_eq_res_0...), 511),
	mkU8("l255", append(u8_s1, u8_s0...), u8_eq_mat_1, 0, append(u8_eq_res_1, u8_eq_res_0...), 255),
	mkU8("l127", u8_s1, u8_eq_mat_1, 0, u8_eq_res_1, 127),
	mkU8("l63", u8_s1, u8_eq_mat_1, 0, u8_eq_res_1, 63),
	mkU8("l31", u8_s1, u8_eq_mat_1, 0, u8_eq_res_1, 31),
	mkU8("l23", u8_s1, u8_eq_mat_1, 0, u8_eq_res_1, 23),
	mkU8("l15", u8_s1, u8_eq_mat_1, 0, u8_eq_res_1, 15),
	mkU8("l7", u8_s1, u8_eq_mat_1, 0, u8_eq_res_1, 7),
	mkU8("ext512", u8_s2, u8_eq_mat_2, 0, u8_eq_res_2, 512),
	mkU8("ext32", u8_s2, u8_eq_mat_2, 0, u8_eq_res_2, 32),
	mkU8("ext31", u8_s2, u8_eq_mat_2, 0, u8_eq_res_2, 31),
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
var Uint8NotEqualCases = []MatchTest[uint8]{
	{"l0", make([]uint8, 0), u8_ne_mat_1, 0, []byte{}, 0},
	{"nil", nil, u8_ne_mat_1, 0, []byte{}, 0},
	mkU8("vec1", u8_s0, u8_ne_mat_0, 0, u8_ne_res_0, 32),
	mkU8("vec2", u8_s0, u8_ne_mat_0, 0, u8_ne_res_0, 512),
	mkU8("l32", u8_s1, u8_ne_mat_1, 0, u8_ne_res_1, 32),
	mkU8("l64", append(u8_s1, u8_s0...), u8_ne_mat_1, 0, append(u8_ne_res_1, u8_ne_res_0...), 64),
	mkU8("l128", append(u8_s1, u8_s0...), u8_ne_mat_1, 0, append(u8_ne_res_1, u8_ne_res_0...), 128),
	mkU8("l256", append(u8_s1, u8_s0...), u8_ne_mat_1, 0, append(u8_ne_res_1, u8_ne_res_0...), 256),
	mkU8("l512", append(u8_s1, u8_s0...), u8_ne_mat_1, 0, append(u8_ne_res_1, u8_ne_res_0...), 512),
	mkU8("l1024", append(u8_s1, u8_s0...), u8_ne_mat_1, 0, append(u8_ne_res_1, u8_ne_res_0...), 1024),
	mkU8("l1023", append(u8_s1, u8_s0...), u8_ne_mat_1, 0, append(u8_ne_res_1, u8_ne_res_0...), 1023),
	mkU8("l511", append(u8_s1, u8_s0...), u8_ne_mat_1, 0, append(u8_ne_res_1, u8_ne_res_0...), 511),
	mkU8("l255", append(u8_s1, u8_s0...), u8_ne_mat_1, 0, append(u8_ne_res_1, u8_ne_res_0...), 255),
	mkU8("l127", u8_s1, u8_ne_mat_1, 0, u8_ne_res_1, 127),
	mkU8("l63", u8_s1, u8_ne_mat_1, 0, u8_ne_res_1, 63),
	mkU8("l31", u8_s1, u8_ne_mat_1, 0, u8_ne_res_1, 31),
	mkU8("l23", u8_s1, u8_ne_mat_1, 0, u8_ne_res_1, 23),
	mkU8("l15", u8_s1, u8_ne_mat_1, 0, u8_ne_res_1, 15),
	mkU8("l7", u8_s1, u8_ne_mat_1, 0, u8_ne_res_1, 7),
	mkU8("ext512", u8_s2, u8_ne_mat_2, 0, u8_ne_res_2, 512),
	mkU8("ext32", u8_s2, u8_ne_mat_2, 0, u8_ne_res_2, 32),
	mkU8("ext31", u8_s2, u8_ne_mat_2, 0, u8_ne_res_2, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var Uint8LessCases = []MatchTest[uint8]{
	{"l0", make([]uint8, 0), u8_lt_mat_1, 0, []byte{}, 0},
	{"nil", nil, u8_lt_mat_1, 0, []byte{}, 0},
	mkU8("vec1", u8_s0, u8_lt_mat_0, 0, u8_lt_res_0, 32),
	mkU8("vec2", u8_s0, u8_lt_mat_0, 0, u8_lt_res_0, 512),
	mkU8("l32", u8_s1, u8_lt_mat_1, 0, u8_lt_res_1, 32),
	mkU8("l64", append(u8_s1, u8_s0...), u8_lt_mat_1, 0, append(u8_lt_res_1, u8_lt_res_0...), 64),
	mkU8("l128", append(u8_s1, u8_s0...), u8_lt_mat_1, 0, append(u8_lt_res_1, u8_lt_res_0...), 128),
	mkU8("l256", append(u8_s1, u8_s0...), u8_lt_mat_1, 0, append(u8_lt_res_1, u8_lt_res_0...), 256),
	mkU8("l512", append(u8_s1, u8_s0...), u8_lt_mat_1, 0, append(u8_lt_res_1, u8_lt_res_0...), 512),
	mkU8("l1024", append(u8_s1, u8_s0...), u8_lt_mat_1, 0, append(u8_lt_res_1, u8_lt_res_0...), 1024),
	mkU8("l1023", append(u8_s1, u8_s0...), u8_lt_mat_1, 0, append(u8_lt_res_1, u8_lt_res_0...), 1023),
	mkU8("l511", append(u8_s1, u8_s0...), u8_lt_mat_1, 0, append(u8_lt_res_1, u8_lt_res_0...), 511),
	mkU8("l255", append(u8_s1, u8_s0...), u8_lt_mat_1, 0, append(u8_lt_res_1, u8_lt_res_0...), 255),
	mkU8("l127", u8_s1, u8_lt_mat_1, 0, u8_lt_res_1, 127),
	mkU8("l63", u8_s1, u8_lt_mat_1, 0, u8_lt_res_1, 63),
	mkU8("l31", u8_s1, u8_lt_mat_1, 0, u8_lt_res_1, 31),
	mkU8("l23", u8_s1, u8_lt_mat_1, 0, u8_lt_res_1, 23),
	mkU8("l15", u8_s1, u8_lt_mat_1, 0, u8_lt_res_1, 15),
	mkU8("l7", u8_s1, u8_lt_mat_1, 0, u8_lt_res_1, 7),
	mkU8("ext512", u8_s2, u8_lt_mat_2, 0, u8_lt_res_2, 512),
	mkU8("ext32", u8_s2, u8_lt_mat_2, 0, u8_lt_res_2, 32),
	mkU8("ext31", u8_s2, u8_lt_mat_2, 0, u8_lt_res_2, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var Uint8LessEqualCases = []MatchTest[uint8]{
	{"l0", make([]uint8, 0), u8_le_mat_1, 0, []byte{}, 0},
	{"nil", nil, u8_le_mat_1, 0, []byte{}, 0},
	mkU8("vec1", u8_s0, u8_le_mat_0, 0, u8_le_res_0, 32),
	mkU8("vec2", u8_s0, u8_le_mat_0, 0, u8_le_res_0, 512),
	mkU8("l32", u8_s1, u8_le_mat_1, 0, u8_le_res_1, 32),
	mkU8("l64", append(u8_s1, u8_s0...), u8_le_mat_1, 0, append(u8_le_res_1, u8_le_res_0...), 64),
	mkU8("l128", append(u8_s1, u8_s0...), u8_le_mat_1, 0, append(u8_le_res_1, u8_le_res_0...), 128),
	mkU8("l256", append(u8_s1, u8_s0...), u8_le_mat_1, 0, append(u8_le_res_1, u8_le_res_0...), 256),
	mkU8("l512", append(u8_s1, u8_s0...), u8_le_mat_1, 0, append(u8_le_res_1, u8_le_res_0...), 512),
	mkU8("l1024", append(u8_s1, u8_s0...), u8_le_mat_1, 0, append(u8_le_res_1, u8_le_res_0...), 1024),
	mkU8("l1023", append(u8_s1, u8_s0...), u8_le_mat_1, 0, append(u8_le_res_1, u8_le_res_0...), 1023),
	mkU8("l511", append(u8_s1, u8_s0...), u8_le_mat_1, 0, append(u8_le_res_1, u8_le_res_0...), 511),
	mkU8("l255", append(u8_s1, u8_s0...), u8_le_mat_1, 0, append(u8_le_res_1, u8_le_res_0...), 255),
	mkU8("l127", u8_s1, u8_le_mat_1, 0, u8_le_res_1, 127),
	mkU8("l63", u8_s1, u8_le_mat_1, 0, u8_le_res_1, 63),
	mkU8("l31", u8_s1, u8_le_mat_1, 0, u8_le_res_1, 31),
	mkU8("l23", u8_s1, u8_le_mat_1, 0, u8_le_res_1, 23),
	mkU8("l15", u8_s1, u8_le_mat_1, 0, u8_le_res_1, 15),
	mkU8("l7", u8_s1, u8_le_mat_1, 0, u8_le_res_1, 7),
	mkU8("ext512", u8_s2, u8_le_mat_2, 0, u8_le_res_2, 512),
	mkU8("ext32", u8_s2, u8_le_mat_2, 0, u8_le_res_2, 32),
	mkU8("ext31", u8_s2, u8_le_mat_2, 0, u8_le_res_2, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var Uint8GreaterCases = []MatchTest[uint8]{
	{"l0", make([]uint8, 0), u8_gt_mat_1, 0, []byte{}, 0},
	{"nil", nil, u8_gt_mat_1, 0, []byte{}, 0},
	mkU8("vec1", u8_s0, u8_gt_mat_0, 0, u8_gt_res_0, 32),
	mkU8("vec2", u8_s0, u8_gt_mat_0, 0, u8_gt_res_0, 512),
	mkU8("l32", u8_s1, u8_gt_mat_1, 0, u8_gt_res_1, 32),
	mkU8("l64", append(u8_s1, u8_s0...), u8_gt_mat_1, 0, append(u8_gt_res_1, u8_gt_res_0...), 64),
	mkU8("l128", append(u8_s1, u8_s0...), u8_gt_mat_1, 0, append(u8_gt_res_1, u8_gt_res_0...), 128),
	mkU8("l256", append(u8_s1, u8_s0...), u8_gt_mat_1, 0, append(u8_gt_res_1, u8_gt_res_0...), 256),
	mkU8("l512", append(u8_s1, u8_s0...), u8_gt_mat_1, 0, append(u8_gt_res_1, u8_gt_res_0...), 512),
	mkU8("l1024", append(u8_s1, u8_s0...), u8_gt_mat_1, 0, append(u8_gt_res_1, u8_gt_res_0...), 1024),
	mkU8("l1023", append(u8_s1, u8_s0...), u8_gt_mat_1, 0, append(u8_gt_res_1, u8_gt_res_0...), 1023),
	mkU8("l511", append(u8_s1, u8_s0...), u8_gt_mat_1, 0, append(u8_gt_res_1, u8_gt_res_0...), 511),
	mkU8("l255", append(u8_s1, u8_s0...), u8_gt_mat_1, 0, append(u8_gt_res_1, u8_gt_res_0...), 255),
	mkU8("l127", u8_s1, u8_gt_mat_1, 0, u8_gt_res_1, 127),
	mkU8("l63", u8_s1, u8_gt_mat_1, 0, u8_gt_res_1, 63),
	mkU8("l31", u8_s1, u8_gt_mat_1, 0, u8_gt_res_1, 31),
	mkU8("l23", u8_s1, u8_gt_mat_1, 0, u8_gt_res_1, 23),
	mkU8("l15", u8_s1, u8_gt_mat_1, 0, u8_gt_res_1, 15),
	mkU8("l7", u8_s1, u8_gt_mat_1, 0, u8_gt_res_1, 7),
	mkU8("ext512", u8_s2, u8_gt_mat_2, 0, u8_gt_res_2, 512),
	mkU8("ext32", u8_s2, u8_gt_mat_2, 0, u8_gt_res_2, 32),
	mkU8("ext31", u8_s2, u8_gt_mat_2, 0, u8_gt_res_2, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var Uint8GreaterEqualCases = []MatchTest[uint8]{
	{"l0", make([]uint8, 0), u8_ge_mat_1, 0, []byte{}, 0},
	{"nil", nil, u8_ge_mat_1, 0, []byte{}, 0},
	mkU8("vec1", u8_s0, u8_ge_mat_0, 0, u8_ge_res_0, 32),
	mkU8("vec2", u8_s0, u8_ge_mat_0, 0, u8_ge_res_0, 512),
	mkU8("l32", u8_s1, u8_ge_mat_1, 0, u8_ge_res_1, 32),
	mkU8("l64", append(u8_s1, u8_s0...), u8_ge_mat_1, 0, append(u8_ge_res_1, u8_ge_res_0...), 64),
	mkU8("l128", append(u8_s1, u8_s0...), u8_ge_mat_1, 0, append(u8_ge_res_1, u8_ge_res_0...), 128),
	mkU8("l256", append(u8_s1, u8_s0...), u8_ge_mat_1, 0, append(u8_ge_res_1, u8_ge_res_0...), 256),
	mkU8("l512", append(u8_s1, u8_s0...), u8_ge_mat_1, 0, append(u8_ge_res_1, u8_ge_res_0...), 512),
	mkU8("l1024", append(u8_s1, u8_s0...), u8_ge_mat_1, 0, append(u8_ge_res_1, u8_ge_res_0...), 1024),
	mkU8("l1023", append(u8_s1, u8_s0...), u8_ge_mat_1, 0, append(u8_ge_res_1, u8_ge_res_0...), 1023),
	mkU8("l511", append(u8_s1, u8_s0...), u8_ge_mat_1, 0, append(u8_ge_res_1, u8_ge_res_0...), 511),
	mkU8("l255", append(u8_s1, u8_s0...), u8_ge_mat_1, 0, append(u8_ge_res_1, u8_ge_res_0...), 255),
	mkU8("l127", u8_s1, u8_ge_mat_1, 0, u8_ge_res_1, 127),
	mkU8("l63", u8_s1, u8_ge_mat_1, 0, u8_ge_res_1, 63),
	mkU8("l31", u8_s1, u8_ge_mat_1, 0, u8_ge_res_1, 31),
	mkU8("l23", u8_s1, u8_ge_mat_1, 0, u8_ge_res_1, 23),
	mkU8("l15", u8_s1, u8_ge_mat_1, 0, u8_ge_res_1, 15),
	mkU8("l7", u8_s1, u8_ge_mat_1, 0, u8_ge_res_1, 7),
	mkU8("ext512", u8_s2, u8_ge_mat_2, 0, u8_ge_res_2, 512),
	mkU8("ext32", u8_s2, u8_ge_mat_2, 0, u8_ge_res_2, 32),
	mkU8("ext31", u8_s2, u8_ge_mat_2, 0, u8_ge_res_2, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var Uint8BetweenCases = []MatchTest[uint8]{
	{"l0", make([]uint8, 0), u8_bw_mat_1a, u8_bw_mat_1b, []byte{}, 0},
	{"nil", nil, u8_bw_mat_1a, u8_bw_mat_1b, []byte{}, 0},
	mkU8("vec1", u8_s0, u8_bw_mat_0a, u8_bw_mat_0b, u8_bw_res_0, 32),
	mkU8("vec2", u8_s0, u8_bw_mat_0a, u8_bw_mat_0b, u8_bw_res_0, 512),
	mkU8("l32", u8_s1, u8_bw_mat_1a, u8_bw_mat_1b, u8_bw_res_1, 32),
	mkU8("l64", append(u8_s1, u8_s0...), u8_bw_mat_1a, u8_bw_mat_1b, append(u8_bw_res_1, u8_bw_res_0...), 64),
	mkU8("l128", append(u8_s1, u8_s0...), u8_bw_mat_1a, u8_bw_mat_1b, append(u8_bw_res_1, u8_bw_res_0...), 128),
	mkU8("l256", append(u8_s1, u8_s0...), u8_bw_mat_1a, u8_bw_mat_1b, append(u8_bw_res_1, u8_bw_res_0...), 256),
	mkU8("l512", append(u8_s1, u8_s0...), u8_bw_mat_1a, u8_bw_mat_1b, append(u8_bw_res_1, u8_bw_res_0...), 512),
	mkU8("l1024", append(u8_s1, u8_s0...), u8_bw_mat_1a, u8_bw_mat_1b, append(u8_bw_res_1, u8_bw_res_0...), 1024),
	mkU8("l1023", append(u8_s1, u8_s0...), u8_bw_mat_1a, u8_bw_mat_1b, append(u8_bw_res_1, u8_bw_res_0...), 1023),
	mkU8("l511", append(u8_s1, u8_s0...), u8_bw_mat_1a, u8_bw_mat_1b, append(u8_bw_res_1, u8_bw_res_0...), 511),
	mkU8("l255", append(u8_s1, u8_s0...), u8_bw_mat_1a, u8_bw_mat_1b, append(u8_bw_res_1, u8_bw_res_0...), 255),
	mkU8("l127", u8_s1, u8_bw_mat_1a, u8_bw_mat_1b, u8_bw_res_1, 127),
	mkU8("l63", u8_s1, u8_bw_mat_1a, u8_bw_mat_1b, u8_bw_res_1, 63),
	mkU8("l31", u8_s1, u8_bw_mat_1a, u8_bw_mat_1b, u8_bw_res_1, 31),
	mkU8("l23", u8_s1, u8_bw_mat_1a, u8_bw_mat_1b, u8_bw_res_1, 23),
	mkU8("l15", u8_s1, u8_bw_mat_1a, u8_bw_mat_1b, u8_bw_res_1, 15),
	mkU8("l7", u8_s1, u8_bw_mat_1a, u8_bw_mat_1b, u8_bw_res_1, 7),
	mkU8("ext512", u8_s2, u8_bw_mat_2a, u8_bw_mat_2b, u8_bw_res_2, 512),
	mkU8("ext32", u8_s2, u8_bw_mat_2a, u8_bw_mat_2b, u8_bw_res_2, 32),
	mkU8("ext31", u8_s2, u8_bw_mat_2a, u8_bw_mat_2b, u8_bw_res_2, 31),
}
