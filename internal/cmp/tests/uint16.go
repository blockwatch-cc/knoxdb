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
	u16_s0 = []uint16{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}

	u16_eq_mat_0 uint16 = 5
	u16_eq_res_0        = []byte{0x6a, 0x1e, 0x48, 0x2c}

	u16_ne_mat_0 uint16 = 5
	u16_ne_res_0        = []byte{0x95, 0xe1, 0xb7, 0xd3}

	u16_lt_mat_0 uint16 = 5
	u16_lt_res_0        = []byte{0x05, 0x21, 0x27, 0x01}

	u16_le_mat_0 uint16 = 5
	u16_le_res_0        = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	u16_gt_mat_0 uint16 = 5
	u16_gt_res_0        = []byte{0x90, 0xc0, 0x90, 0xd2}

	u16_ge_mat_0 uint16 = 5
	u16_ge_res_0        = []byte{0xfa, 0xde, 0xd8, 0xfe}

	u16_bw_mat_0a uint16 = 5
	u16_bw_mat_0b uint16 = 10
	u16_bw_res_0         = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	u16_s1 = []uint16{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 50000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}

	u16_eq_res_1        = []byte{0x41, 0x42, 0xc4, 0x0e}
	u16_eq_mat_1 uint16 = 5

	u16_ne_res_1        = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	u16_ne_mat_1 uint16 = 5

	u16_lt_res_1        = []byte{0x0e, 0x00, 0x00, 0x00}
	u16_lt_mat_1 uint16 = 5

	u16_le_res_1        = []byte{0x4f, 0x42, 0xc4, 0x0e}
	u16_le_mat_1 uint16 = 5

	u16_gt_res_1        = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	u16_gt_mat_1 uint16 = 5

	u16_ge_res_1        = []byte{0xf1, 0xff, 0xff, 0xff}
	u16_ge_mat_1 uint16 = 5

	u16_bw_res_1         = []byte{0xf1, 0x42, 0xc4, 0x0e}
	u16_bw_mat_1a uint16 = 5
	u16_bw_mat_1b uint16 = 10

	// extreme values
	u16_s2 = []uint16{
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
	}
	u16_eq_res_2        = []byte{0x88, 0x88, 0x88, 0x88}
	u16_eq_mat_2 uint16 = math.MaxUint16

	u16_ne_res_2        = []byte{0x77, 0x77, 0x77, 0x77}
	u16_ne_mat_2 uint16 = math.MaxUint16

	u16_lt_res_2        = []byte{0x77, 0x77, 0x77, 0x77}
	u16_lt_mat_2 uint16 = math.MaxUint16

	u16_le_res_2        = []byte{0xff, 0xff, 0xff, 0xff}
	u16_le_mat_2 uint16 = math.MaxUint16

	u16_gt_res_2        = []byte{0x00, 0x00, 0x00, 0x00}
	u16_gt_mat_2 uint16 = math.MaxUint16

	u16_ge_res_2        = []byte{0x88, 0x88, 0x88, 0x88}
	u16_ge_mat_2 uint16 = math.MaxUint16

	u16_bw_res_2         = []byte{0xcc, 0xcc, 0xcc, 0xcc}
	u16_bw_mat_2a uint16 = math.MaxUint8
	u16_bw_mat_2b uint16 = math.MaxUint16

	u16_bw_res_3         = []byte{0xff, 0xff, 0xff, 0xff}
	u16_bw_mat_3a uint16 = 0
	u16_bw_mat_3b uint16 = math.MaxUint16
)

// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mku16(name string, src []uint16, match, match2 uint16, result []byte, length int) MatchTest[uint16] {
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
	return MatchTest[uint16]{
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
var Uint16EqualCases = []MatchTest[uint16]{
	{"l0", make([]uint16, 0), u16_eq_mat_1, 0, []byte{}, 0},
	{"nil", nil, u16_eq_mat_1, 0, []byte{}, 0},
	mku16("vec1", u16_s0, u16_eq_mat_0, 0, u16_eq_res_0, 32),
	mku16("vec2", u16_s0, u16_eq_mat_0, 0, u16_eq_res_0, 256),
	mku16("l32", u16_s1, u16_eq_mat_1, 0, u16_eq_res_1, 32),
	mku16("l64", append(u16_s1, u16_s0...), u16_eq_mat_1, 0, append(u16_eq_res_1, u16_eq_res_0...), 64),
	mku16("l128", append(u16_s1, u16_s0...), u16_eq_mat_1, 0, append(u16_eq_res_1, u16_eq_res_0...), 128),
	mku16("l256", append(u16_s1, u16_s0...), u16_eq_mat_1, 0, append(u16_eq_res_1, u16_eq_res_0...), 256),
	mku16("l512", append(u16_s1, u16_s0...), u16_eq_mat_1, 0, append(u16_eq_res_1, u16_eq_res_0...), 512),
	mku16("l511", append(u16_s1, u16_s0...), u16_eq_mat_1, 0, append(u16_eq_res_1, u16_eq_res_0...), 511),
	mku16("l255", append(u16_s1, u16_s0...), u16_eq_mat_1, 0, append(u16_eq_res_1, u16_eq_res_0...), 255),
	mku16("l127", u16_s1, u16_eq_mat_1, 0, u16_eq_res_1, 127),
	mku16("l63", u16_s1, u16_eq_mat_1, 0, u16_eq_res_1, 63),
	mku16("l31", u16_s1, u16_eq_mat_1, 0, u16_eq_res_1, 31),
	mku16("l23", u16_s1, u16_eq_mat_1, 0, u16_eq_res_1, 23),
	mku16("l15", u16_s1, u16_eq_mat_1, 0, u16_eq_res_1, 15),
	mku16("l7", u16_s1, u16_eq_mat_1, 0, u16_eq_res_1, 7),
	mku16("ext256", u16_s2, u16_eq_mat_2, 0, u16_eq_res_2, 256),
	mku16("ext32", u16_s2, u16_eq_mat_2, 0, u16_eq_res_2, 32),
	mku16("ext31", u16_s2, u16_eq_mat_2, 0, u16_eq_res_2, 31),
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
var Uint16NotEqualCases = []MatchTest[uint16]{
	{"l0", make([]uint16, 0), u16_ne_mat_1, 0, []byte{}, 0},
	{"nil", nil, u16_ne_mat_1, 0, []byte{}, 0},
	mku16("vec1", u16_s0, u16_ne_mat_0, 0, u16_ne_res_0, 32),
	mku16("vec2", u16_s0, u16_ne_mat_0, 0, u16_ne_res_0, 256),
	mku16("l32", u16_s1, u16_ne_mat_1, 0, u16_ne_res_1, 32),
	mku16("l64", append(u16_s1, u16_s0...), u16_ne_mat_1, 0, append(u16_ne_res_1, u16_ne_res_0...), 64),
	mku16("l128", append(u16_s1, u16_s0...), u16_ne_mat_1, 0, append(u16_ne_res_1, u16_ne_res_0...), 128),
	mku16("l256", append(u16_s1, u16_s0...), u16_ne_mat_1, 0, append(u16_ne_res_1, u16_ne_res_0...), 256),
	mku16("l512", append(u16_s1, u16_s0...), u16_ne_mat_1, 0, append(u16_ne_res_1, u16_ne_res_0...), 512),
	mku16("l511", append(u16_s1, u16_s0...), u16_ne_mat_1, 0, append(u16_ne_res_1, u16_ne_res_0...), 511),
	mku16("l255", append(u16_s1, u16_s0...), u16_ne_mat_1, 0, append(u16_ne_res_1, u16_ne_res_0...), 255),
	mku16("l127", u16_s1, u16_ne_mat_1, 0, u16_ne_res_1, 127),
	mku16("l63", u16_s1, u16_ne_mat_1, 0, u16_ne_res_1, 63),
	mku16("l31", u16_s1, u16_ne_mat_1, 0, u16_ne_res_1, 31),
	mku16("l23", u16_s1, u16_ne_mat_1, 0, u16_ne_res_1, 23),
	mku16("l15", u16_s1, u16_ne_mat_1, 0, u16_ne_res_1, 15),
	mku16("l7", u16_s1, u16_ne_mat_1, 0, u16_ne_res_1, 7),
	mku16("ext256", u16_s2, u16_ne_mat_2, 0, u16_ne_res_2, 256),
	mku16("ext32", u16_s2, u16_ne_mat_2, 0, u16_ne_res_2, 32),
	mku16("ext31", u16_s2, u16_ne_mat_2, 0, u16_ne_res_2, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var Uint16LessCases = []MatchTest[uint16]{
	{"l0", make([]uint16, 0), u16_lt_mat_1, 0, []byte{}, 0},
	{"nil", nil, u16_lt_mat_1, 0, []byte{}, 0},
	mku16("vec1", u16_s0, u16_lt_mat_0, 0, u16_lt_res_0, 32),
	mku16("vec2", u16_s0, u16_lt_mat_0, 0, u16_lt_res_0, 256),
	mku16("l32", u16_s1, u16_lt_mat_1, 0, u16_lt_res_1, 32),
	mku16("l64", append(u16_s1, u16_s0...), u16_lt_mat_1, 0, append(u16_lt_res_1, u16_lt_res_0...), 64),
	mku16("l128", append(u16_s1, u16_s0...), u16_lt_mat_1, 0, append(u16_lt_res_1, u16_lt_res_0...), 128),
	mku16("l256", append(u16_s1, u16_s0...), u16_lt_mat_1, 0, append(u16_lt_res_1, u16_lt_res_0...), 256),
	mku16("l512", append(u16_s1, u16_s0...), u16_lt_mat_1, 0, append(u16_lt_res_1, u16_lt_res_0...), 512),
	mku16("l511", append(u16_s1, u16_s0...), u16_lt_mat_1, 0, append(u16_lt_res_1, u16_lt_res_0...), 511),
	mku16("l255", append(u16_s1, u16_s0...), u16_lt_mat_1, 0, append(u16_lt_res_1, u16_lt_res_0...), 255),
	mku16("l127", u16_s1, u16_lt_mat_1, 0, u16_lt_res_1, 127),
	mku16("l63", u16_s1, u16_lt_mat_1, 0, u16_lt_res_1, 63),
	mku16("l31", u16_s1, u16_lt_mat_1, 0, u16_lt_res_1, 31),
	mku16("l23", u16_s1, u16_lt_mat_1, 0, u16_lt_res_1, 23),
	mku16("l15", u16_s1, u16_lt_mat_1, 0, u16_lt_res_1, 15),
	mku16("l7", u16_s1, u16_lt_mat_1, 0, u16_lt_res_1, 7),
	mku16("ext256", u16_s2, u16_lt_mat_2, 0, u16_lt_res_2, 256),
	mku16("ext32", u16_s2, u16_lt_mat_2, 0, u16_lt_res_2, 32),
	mku16("ext31", u16_s2, u16_lt_mat_2, 0, u16_lt_res_2, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var Uint16LessEqualCases = []MatchTest[uint16]{
	{"l0", make([]uint16, 0), u16_le_mat_1, 0, []byte{}, 0},
	{"nil", nil, u16_le_mat_1, 0, []byte{}, 0},
	mku16("vec1", u16_s0, u16_le_mat_0, 0, u16_le_res_0, 32),
	mku16("vec2", u16_s0, u16_le_mat_0, 0, u16_le_res_0, 256),
	mku16("l32", u16_s1, u16_le_mat_1, 0, u16_le_res_1, 32),
	mku16("l64", append(u16_s1, u16_s0...), u16_le_mat_1, 0, append(u16_le_res_1, u16_le_res_0...), 64),
	mku16("l128", append(u16_s1, u16_s0...), u16_le_mat_1, 0, append(u16_le_res_1, u16_le_res_0...), 128),
	mku16("l256", append(u16_s1, u16_s0...), u16_le_mat_1, 0, append(u16_le_res_1, u16_le_res_0...), 256),
	mku16("l512", append(u16_s1, u16_s0...), u16_le_mat_1, 0, append(u16_le_res_1, u16_le_res_0...), 512),
	mku16("l511", append(u16_s1, u16_s0...), u16_le_mat_1, 0, append(u16_le_res_1, u16_le_res_0...), 511),
	mku16("l255", append(u16_s1, u16_s0...), u16_le_mat_1, 0, append(u16_le_res_1, u16_le_res_0...), 255),
	mku16("l127", u16_s1, u16_le_mat_1, 0, u16_le_res_1, 127),
	mku16("l63", u16_s1, u16_le_mat_1, 0, u16_le_res_1, 63),
	mku16("l31", u16_s1, u16_le_mat_1, 0, u16_le_res_1, 31),
	mku16("l23", u16_s1, u16_le_mat_1, 0, u16_le_res_1, 23),
	mku16("l15", u16_s1, u16_le_mat_1, 0, u16_le_res_1, 15),
	mku16("l7", u16_s1, u16_le_mat_1, 0, u16_le_res_1, 7),
	mku16("ext256", u16_s2, u16_le_mat_2, 0, u16_le_res_2, 256),
	mku16("ext32", u16_s2, u16_le_mat_2, 0, u16_le_res_2, 32),
	mku16("ext31", u16_s2, u16_le_mat_2, 0, u16_le_res_2, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var Uint16GreaterCases = []MatchTest[uint16]{
	{"l0", make([]uint16, 0), u16_gt_mat_1, 0, []byte{}, 0},
	{"nil", nil, u16_gt_mat_1, 0, []byte{}, 0},
	mku16("vec1", u16_s0, u16_gt_mat_0, 0, u16_gt_res_0, 32),
	mku16("vec2", u16_s0, u16_gt_mat_0, 0, u16_gt_res_0, 256),
	mku16("l32", u16_s1, u16_gt_mat_1, 0, u16_gt_res_1, 32),
	mku16("l64", append(u16_s1, u16_s0...), u16_gt_mat_1, 0, append(u16_gt_res_1, u16_gt_res_0...), 64),
	mku16("l128", append(u16_s1, u16_s0...), u16_gt_mat_1, 0, append(u16_gt_res_1, u16_gt_res_0...), 128),
	mku16("l256", append(u16_s1, u16_s0...), u16_gt_mat_1, 0, append(u16_gt_res_1, u16_gt_res_0...), 256),
	mku16("l512", append(u16_s1, u16_s0...), u16_gt_mat_1, 0, append(u16_gt_res_1, u16_gt_res_0...), 512),
	mku16("l511", append(u16_s1, u16_s0...), u16_gt_mat_1, 0, append(u16_gt_res_1, u16_gt_res_0...), 511),
	mku16("l255", append(u16_s1, u16_s0...), u16_gt_mat_1, 0, append(u16_gt_res_1, u16_gt_res_0...), 255),
	mku16("l127", u16_s1, u16_gt_mat_1, 0, u16_gt_res_1, 127),
	mku16("l63", u16_s1, u16_gt_mat_1, 0, u16_gt_res_1, 63),
	mku16("l31", u16_s1, u16_gt_mat_1, 0, u16_gt_res_1, 31),
	mku16("l23", u16_s1, u16_gt_mat_1, 0, u16_gt_res_1, 23),
	mku16("l15", u16_s1, u16_gt_mat_1, 0, u16_gt_res_1, 15),
	mku16("l7", u16_s1, u16_gt_mat_1, 0, u16_gt_res_1, 7),
	mku16("ext256", u16_s2, u16_gt_mat_2, 0, u16_gt_res_2, 256),
	mku16("ext32", u16_s2, u16_gt_mat_2, 0, u16_gt_res_2, 32),
	mku16("ext31", u16_s2, u16_gt_mat_2, 0, u16_gt_res_2, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var Uint16GreaterEqualCases = []MatchTest[uint16]{
	{"l0", make([]uint16, 0), u16_ge_mat_1, 0, []byte{}, 0},
	{"nil", nil, u16_ge_mat_1, 0, []byte{}, 0},
	mku16("vec1", u16_s0, u16_ge_mat_0, 0, u16_ge_res_0, 32),
	mku16("vec2", u16_s0, u16_ge_mat_0, 0, u16_ge_res_0, 256),
	mku16("l32", u16_s1, u16_ge_mat_1, 0, u16_ge_res_1, 32),
	mku16("l64", append(u16_s1, u16_s0...), u16_ge_mat_1, 0, append(u16_ge_res_1, u16_ge_res_0...), 64),
	mku16("l128", append(u16_s1, u16_s0...), u16_ge_mat_1, 0, append(u16_ge_res_1, u16_ge_res_0...), 128),
	mku16("l256", append(u16_s1, u16_s0...), u16_ge_mat_1, 0, append(u16_ge_res_1, u16_ge_res_0...), 256),
	mku16("l512", append(u16_s1, u16_s0...), u16_ge_mat_1, 0, append(u16_ge_res_1, u16_ge_res_0...), 512),
	mku16("l511", append(u16_s1, u16_s0...), u16_ge_mat_1, 0, append(u16_ge_res_1, u16_ge_res_0...), 511),
	mku16("l255", append(u16_s1, u16_s0...), u16_ge_mat_1, 0, append(u16_ge_res_1, u16_ge_res_0...), 255),
	mku16("l127", u16_s1, u16_ge_mat_1, 0, u16_ge_res_1, 127),
	mku16("l63", u16_s1, u16_ge_mat_1, 0, u16_ge_res_1, 63),
	mku16("l31", u16_s1, u16_ge_mat_1, 0, u16_ge_res_1, 31),
	mku16("l23", u16_s1, u16_ge_mat_1, 0, u16_ge_res_1, 23),
	mku16("l15", u16_s1, u16_ge_mat_1, 0, u16_ge_res_1, 15),
	mku16("l7", u16_s1, u16_ge_mat_1, 0, u16_ge_res_1, 7),
	mku16("ext256", u16_s2, u16_ge_mat_2, 0, u16_ge_res_2, 256),
	mku16("ext32", u16_s2, u16_ge_mat_2, 0, u16_ge_res_2, 32),
	mku16("ext31", u16_s2, u16_ge_mat_2, 0, u16_ge_res_2, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var Uint16BetweenCases = []MatchTest[uint16]{
	{"l0", make([]uint16, 0), u16_bw_mat_1a, u16_bw_mat_1b, []byte{}, 0},
	{"nil", nil, u16_bw_mat_1a, u16_bw_mat_1b, []byte{}, 0},
	mku16("vec1", u16_s0, u16_bw_mat_0a, u16_bw_mat_0b, u16_bw_res_0, 32),
	mku16("vec2", u16_s0, u16_bw_mat_0a, u16_bw_mat_0b, u16_bw_res_0, 256),
	mku16("l32", u16_s1, u16_bw_mat_1a, u16_bw_mat_1b, u16_bw_res_1, 32),
	mku16("l64", append(u16_s1, u16_s0...), u16_bw_mat_1a, u16_bw_mat_1b, append(u16_bw_res_1, u16_bw_res_0...), 64),
	mku16("l128", append(u16_s1, u16_s0...), u16_bw_mat_1a, u16_bw_mat_1b, append(u16_bw_res_1, u16_bw_res_0...), 128),
	mku16("l256", append(u16_s1, u16_s0...), u16_bw_mat_1a, u16_bw_mat_1b, append(u16_bw_res_1, u16_bw_res_0...), 256),
	mku16("l512", append(u16_s1, u16_s0...), u16_bw_mat_1a, u16_bw_mat_1b, append(u16_bw_res_1, u16_bw_res_0...), 512),
	mku16("l511", append(u16_s1, u16_s0...), u16_bw_mat_1a, u16_bw_mat_1b, append(u16_bw_res_1, u16_bw_res_0...), 511),
	mku16("l255", append(u16_s1, u16_s0...), u16_bw_mat_1a, u16_bw_mat_1b, append(u16_bw_res_1, u16_bw_res_0...), 255),
	mku16("l127", u16_s1, u16_bw_mat_1a, u16_bw_mat_1b, u16_bw_res_1, 127),
	mku16("l63", u16_s1, u16_bw_mat_1a, u16_bw_mat_1b, u16_bw_res_1, 63),
	mku16("l31", u16_s1, u16_bw_mat_1a, u16_bw_mat_1b, u16_bw_res_1, 31),
	mku16("l23", u16_s1, u16_bw_mat_1a, u16_bw_mat_1b, u16_bw_res_1, 23),
	mku16("l15", u16_s1, u16_bw_mat_1a, u16_bw_mat_1b, u16_bw_res_1, 15),
	mku16("l7", u16_s1, u16_bw_mat_1a, u16_bw_mat_1b, u16_bw_res_1, 7),
	mku16("ext256", u16_s2, u16_bw_mat_2a, u16_bw_mat_2b, u16_bw_res_2, 256),
	mku16("ext32", u16_s2, u16_bw_mat_2a, u16_bw_mat_2b, u16_bw_res_2, 32),
	mku16("ext31", u16_s2, u16_bw_mat_2a, u16_bw_mat_2b, u16_bw_res_2, 31),
	mku16("full", u16_s2, u16_bw_mat_3a, u16_bw_mat_3b, u16_bw_res_3, 32),
}
