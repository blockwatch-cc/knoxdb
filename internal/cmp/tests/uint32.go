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
	u32_s0 = []uint32{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	u32_eq_mat_0 uint32 = 5
	u32_eq_res_0        = []byte{0x6a, 0x1e, 0x48, 0x2c}

	u32_ne_mat_0 uint32 = 5
	u32_ne_res_0        = []byte{0x95, 0xe1, 0xb7, 0xd3}

	u32_lt_mat_0 uint32 = 5
	u32_lt_res_0        = []byte{0x05, 0x21, 0x27, 0x01}

	u32_le_mat_0 uint32 = 5
	u32_le_res_0        = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	u32_gt_mat_0 uint32 = 5
	u32_gt_res_0        = []byte{0x90, 0xc0, 0x90, 0xd2}

	u32_ge_mat_0 uint32 = 5
	u32_ge_res_0        = []byte{0xfa, 0xde, 0xd8, 0xfe}

	u32_bw_mat_0a  uint32 = 5
	u32_bw_mat_0ab uint32 = 10
	u32_bw_res_0          = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	u32_s1 = []uint32{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 500000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}

	u32_eq_res_1        = []byte{0x41, 0x42, 0xc4, 0x0e}
	u32_eq_mat_1 uint32 = 5

	u32_ne_res_1        = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	u32_ne_mat_1 uint32 = 5

	u32_lt_res_1        = []byte{0x0e, 0x00, 0x00, 0x00}
	u32_lt_mat_1 uint32 = 5

	u32_le_res_1        = []byte{0x4f, 0x42, 0xc4, 0x0e}
	u32_le_mat_1 uint32 = 5

	u32_gt_res_1        = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	u32_gt_mat_1 uint32 = 5

	u32_ge_res_1        = []byte{0xf1, 0xff, 0xff, 0xff}
	u32_ge_mat_1 uint32 = 5

	u32_bw_res_1          = []byte{0xf1, 0x42, 0xc4, 0x0e}
	u32_bw_mat_1a  uint32 = 5
	u32_bw_mat_1ab uint32 = 10

	// extreme values
	u32_s2 = []uint32{
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
	}

	u32_eq_res_2        = []byte{0x88, 0x88, 0x88, 0x88}
	u32_eq_mat_2 uint32 = math.MaxUint32

	u32_ne_res_2        = []byte{0x77, 0x77, 0x77, 0x77}
	u32_ne_mat_2 uint32 = math.MaxUint32

	u32_lt_res_2        = []byte{0x77, 0x77, 0x77, 0x77}
	u32_lt_mat_2 uint32 = math.MaxUint32

	u32_le_res_2        = []byte{0xff, 0xff, 0xff, 0xff}
	u32_le_mat_2 uint32 = math.MaxUint32

	u32_gt_res_2        = []byte{0x00, 0x00, 0x00, 0x00}
	u32_gt_mat_2 uint32 = math.MaxUint32

	u32_ge_res_2        = []byte{0x88, 0x88, 0x88, 0x88}
	u32_ge_mat_2 uint32 = math.MaxUint32

	u32_bw_res_2          = []byte{0xcc, 0xcc, 0xcc, 0xcc}
	u32_bw_mat_2a  uint32 = math.MaxUint16
	u32_bw_mat_2ab uint32 = math.MaxUint32

	u32_bw_res_3         = []byte{0xff, 0xff, 0xff, 0xff}
	u32_bw_mat_3a uint32 = 0
	u32_bw_mat_3b uint32 = math.MaxUint32
)

// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mkU32(name string, src []uint32, match, match2 uint32, result []byte, length int) MatchTest[uint32] {
	if len(src)%8 != 0 {
		panic(fmt.Errorf("f64 %s: length of slice has to be a multiple of 8", name))
	}
	if len(result) != BitFieldLen(len(src)) {
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
	return MatchTest[uint32]{
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
var Uint32EqualCases = []MatchTest[uint32]{
	{"l0", make([]uint32, 0), u32_eq_mat_1, 0, []byte{}, 0},
	{"nil", nil, u32_eq_mat_1, 0, []byte{}, 0},
	mkU32("vec1", u32_s0, u32_eq_mat_0, 0, u32_eq_res_0, 32),
	mkU32("vec2", u32_s0, u32_eq_mat_0, 0, u32_eq_res_0, 128),
	mkU32("l32", u32_s1, u32_eq_mat_1, 0, u32_eq_res_1, 32),
	mkU32("l64", append(u32_s1, u32_s0...), u32_eq_mat_1, 0, append(u32_eq_res_1, u32_eq_res_0...), 64),
	mkU32("l128", append(u32_s1, u32_s0...), u32_eq_mat_1, 0, append(u32_eq_res_1, u32_eq_res_0...), 128),
	mkU32("l256", append(u32_s1, u32_s0...), u32_eq_mat_1, 0, append(u32_eq_res_1, u32_eq_res_0...), 256),
	mkU32("l255", u32_s1, u32_eq_mat_1, 0, u32_eq_res_1, 127),
	mkU32("l127", u32_s1, u32_eq_mat_1, 0, u32_eq_res_1, 127),
	mkU32("l63", u32_s1, u32_eq_mat_1, 0, u32_eq_res_1, 63),
	mkU32("l31", u32_s1, u32_eq_mat_1, 0, u32_eq_res_1, 31),
	mkU32("l23", u32_s1, u32_eq_mat_1, 0, u32_eq_res_1, 23),
	mkU32("l15", u32_s1, u32_eq_mat_1, 0, u32_eq_res_1, 15),
	mkU32("l7", u32_s1, u32_eq_mat_1, 0, u32_eq_res_1, 7),
	mkU32("ext128", u32_s2, u32_eq_mat_2, 0, u32_eq_res_2, 128),
	mkU32("ext32", u32_s2, u32_eq_mat_2, 0, u32_eq_res_2, 32),
	mkU32("ext31", u32_s2, u32_eq_mat_2, 0, u32_eq_res_2, 31),
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
var Uint32NotEqualCases = []MatchTest[uint32]{
	{"l0", make([]uint32, 0), u32_ne_mat_1, 0, []byte{}, 0},
	{"nil", nil, u32_ne_mat_1, 0, []byte{}, 0},
	mkU32("vec1", u32_s0, u32_ne_mat_0, 0, u32_ne_res_0, 32),
	mkU32("vec2", u32_s0, u32_ne_mat_0, 0, u32_ne_res_0, 128),
	mkU32("l32", u32_s1, u32_ne_mat_1, 0, u32_ne_res_1, 32),
	mkU32("l64", append(u32_s1, u32_s0...), u32_ne_mat_1, 0, append(u32_ne_res_1, u32_ne_res_0...), 64),
	mkU32("l128", append(u32_s1, u32_s0...), u32_ne_mat_1, 0, append(u32_ne_res_1, u32_ne_res_0...), 128),
	mkU32("l256", append(u32_s1, u32_s0...), u32_ne_mat_1, 0, append(u32_ne_res_1, u32_ne_res_0...), 256),
	mkU32("l255", u32_s1, u32_ne_mat_1, 0, u32_ne_res_1, 127),
	mkU32("l127", u32_s1, u32_ne_mat_1, 0, u32_ne_res_1, 127),
	mkU32("l63", u32_s1, u32_ne_mat_1, 0, u32_ne_res_1, 63),
	mkU32("l31", u32_s1, u32_ne_mat_1, 0, u32_ne_res_1, 31),
	mkU32("l23", u32_s1, u32_ne_mat_1, 0, u32_ne_res_1, 23),
	mkU32("l15", u32_s1, u32_ne_mat_1, 0, u32_ne_res_1, 15),
	mkU32("l7", u32_s1, u32_ne_mat_1, 0, u32_ne_res_1, 7),
	mkU32("ext128", u32_s2, u32_ne_mat_2, 0, u32_ne_res_2, 128),
	mkU32("ext32", u32_s2, u32_ne_mat_2, 0, u32_ne_res_2, 32),
	mkU32("ext31", u32_s2, u32_ne_mat_2, 0, u32_ne_res_2, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var Uint32LessCases = []MatchTest[uint32]{
	{"l0", make([]uint32, 0), u32_lt_mat_1, 0, []byte{}, 0},
	{"nil", nil, u32_lt_mat_1, 0, []byte{}, 0},
	mkU32("vec1", u32_s0, u32_lt_mat_0, 0, u32_lt_res_0, 32),
	mkU32("vec2", u32_s0, u32_lt_mat_0, 0, u32_lt_res_0, 128),
	mkU32("l32", u32_s1, u32_lt_mat_1, 0, u32_lt_res_1, 32),
	mkU32("l64", append(u32_s1, u32_s0...), u32_lt_mat_1, 0, append(u32_lt_res_1, u32_lt_res_0...), 64),
	mkU32("l128", append(u32_s1, u32_s0...), u32_lt_mat_1, 0, append(u32_lt_res_1, u32_lt_res_0...), 128),
	mkU32("l256", append(u32_s1, u32_s0...), u32_lt_mat_1, 0, append(u32_lt_res_1, u32_lt_res_0...), 256),
	mkU32("l255", u32_s1, u32_lt_mat_1, 0, u32_lt_res_1, 127),
	mkU32("l127", u32_s1, u32_lt_mat_1, 0, u32_lt_res_1, 127),
	mkU32("l63", u32_s1, u32_lt_mat_1, 0, u32_lt_res_1, 63),
	mkU32("l31", u32_s1, u32_lt_mat_1, 0, u32_lt_res_1, 31),
	mkU32("l23", u32_s1, u32_lt_mat_1, 0, u32_lt_res_1, 23),
	mkU32("l15", u32_s1, u32_lt_mat_1, 0, u32_lt_res_1, 15),
	mkU32("l7", u32_s1, u32_lt_mat_1, 0, u32_lt_res_1, 7),
	mkU32("ext128", u32_s2, u32_lt_mat_2, 0, u32_lt_res_2, 128),
	mkU32("ext32", u32_s2, u32_lt_mat_2, 0, u32_lt_res_2, 32),
	mkU32("ext31", u32_s2, u32_lt_mat_2, 0, u32_lt_res_2, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var Uint32LessEqualCases = []MatchTest[uint32]{
	{"l0", make([]uint32, 0), u32_le_mat_1, 0, []byte{}, 0},
	{"nil", nil, u32_le_mat_1, 0, []byte{}, 0},
	mkU32("vec1", u32_s0, u32_le_mat_0, 0, u32_le_res_0, 32),
	mkU32("vec2", u32_s0, u32_le_mat_0, 0, u32_le_res_0, 128),
	mkU32("l32", u32_s1, u32_le_mat_1, 0, u32_le_res_1, 32),
	mkU32("l64", append(u32_s1, u32_s0...), u32_le_mat_1, 0, append(u32_le_res_1, u32_le_res_0...), 64),
	mkU32("l128", append(u32_s1, u32_s0...), u32_le_mat_1, 0, append(u32_le_res_1, u32_le_res_0...), 128),
	mkU32("l256", append(u32_s1, u32_s0...), u32_le_mat_1, 0, append(u32_le_res_1, u32_le_res_0...), 256),
	mkU32("l255", u32_s1, u32_le_mat_1, 0, u32_le_res_1, 127),
	mkU32("l127", u32_s1, u32_le_mat_1, 0, u32_le_res_1, 127),
	mkU32("l63", u32_s1, u32_le_mat_1, 0, u32_le_res_1, 63),
	mkU32("l31", u32_s1, u32_le_mat_1, 0, u32_le_res_1, 31),
	mkU32("l23", u32_s1, u32_le_mat_1, 0, u32_le_res_1, 23),
	mkU32("l15", u32_s1, u32_le_mat_1, 0, u32_le_res_1, 15),
	mkU32("l7", u32_s1, u32_le_mat_1, 0, u32_le_res_1, 7),
	mkU32("ext128", u32_s2, u32_le_mat_2, 0, u32_le_res_2, 128),
	mkU32("ext32", u32_s2, u32_le_mat_2, 0, u32_le_res_2, 32),
	mkU32("ext31", u32_s2, u32_le_mat_2, 0, u32_le_res_2, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var Uint32GreaterCases = []MatchTest[uint32]{
	{"l0", make([]uint32, 0), u32_gt_mat_1, 0, []byte{}, 0},
	{"nil", nil, u32_gt_mat_1, 0, []byte{}, 0},
	mkU32("vec1", u32_s0, u32_gt_mat_0, 0, u32_gt_res_0, 32),
	mkU32("vec2", u32_s0, u32_gt_mat_0, 0, u32_gt_res_0, 128),
	mkU32("l32", u32_s1, u32_gt_mat_1, 0, u32_gt_res_1, 32),
	mkU32("l64", append(u32_s1, u32_s0...), u32_gt_mat_1, 0, append(u32_gt_res_1, u32_gt_res_0...), 64),
	mkU32("l128", append(u32_s1, u32_s0...), u32_gt_mat_1, 0, append(u32_gt_res_1, u32_gt_res_0...), 128),
	mkU32("l256", append(u32_s1, u32_s0...), u32_gt_mat_1, 0, append(u32_gt_res_1, u32_gt_res_0...), 256),
	mkU32("l255", u32_s1, u32_gt_mat_1, 0, u32_gt_res_1, 127),
	mkU32("l127", u32_s1, u32_gt_mat_1, 0, u32_gt_res_1, 127),
	mkU32("l63", u32_s1, u32_gt_mat_1, 0, u32_gt_res_1, 63),
	mkU32("l31", u32_s1, u32_gt_mat_1, 0, u32_gt_res_1, 31),
	mkU32("l23", u32_s1, u32_gt_mat_1, 0, u32_gt_res_1, 23),
	mkU32("l15", u32_s1, u32_gt_mat_1, 0, u32_gt_res_1, 15),
	mkU32("l7", u32_s1, u32_gt_mat_1, 0, u32_gt_res_1, 7),
	mkU32("ext128", u32_s2, u32_gt_mat_2, 0, u32_gt_res_2, 128),
	mkU32("ext32", u32_s2, u32_gt_mat_2, 0, u32_gt_res_2, 32),
	mkU32("ext31", u32_s2, u32_gt_mat_2, 0, u32_gt_res_2, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var Uint32GreaterEqualCases = []MatchTest[uint32]{
	{"l0", make([]uint32, 0), u32_ge_mat_1, 0, []byte{}, 0},
	{"nil", nil, u32_ge_mat_1, 0, []byte{}, 0},
	mkU32("vec1", u32_s0, u32_ge_mat_0, 0, u32_ge_res_0, 32),
	mkU32("vec2", u32_s0, u32_ge_mat_0, 0, u32_ge_res_0, 128),
	mkU32("l32", u32_s1, u32_ge_mat_1, 0, u32_ge_res_1, 32),
	mkU32("l64", append(u32_s1, u32_s0...), u32_ge_mat_1, 0, append(u32_ge_res_1, u32_ge_res_0...), 64),
	mkU32("l128", append(u32_s1, u32_s0...), u32_ge_mat_1, 0, append(u32_ge_res_1, u32_ge_res_0...), 128),
	mkU32("l256", append(u32_s1, u32_s0...), u32_ge_mat_1, 0, append(u32_ge_res_1, u32_ge_res_0...), 256),
	mkU32("l255", u32_s1, u32_ge_mat_1, 0, u32_ge_res_1, 127),
	mkU32("l127", u32_s1, u32_ge_mat_1, 0, u32_ge_res_1, 127),
	mkU32("l63", u32_s1, u32_ge_mat_1, 0, u32_ge_res_1, 63),
	mkU32("l31", u32_s1, u32_ge_mat_1, 0, u32_ge_res_1, 31),
	mkU32("l23", u32_s1, u32_ge_mat_1, 0, u32_ge_res_1, 23),
	mkU32("l15", u32_s1, u32_ge_mat_1, 0, u32_ge_res_1, 15),
	mkU32("l7", u32_s1, u32_ge_mat_1, 0, u32_ge_res_1, 7),
	mkU32("ext128", u32_s2, u32_ge_mat_2, 0, u32_ge_res_2, 128),
	mkU32("ext32", u32_s2, u32_ge_mat_2, 0, u32_ge_res_2, 32),
	mkU32("ext31", u32_s2, u32_ge_mat_2, 0, u32_ge_res_2, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var Uint32BetweenCases = []MatchTest[uint32]{
	{"l0", make([]uint32, 0), u32_bw_mat_1a, u32_bw_mat_1ab, []byte{}, 0},
	{"nil", nil, u32_bw_mat_1a, u32_bw_mat_1ab, []byte{}, 0},
	mkU32("vec1", u32_s0, u32_bw_mat_0a, u32_bw_mat_0ab, u32_bw_res_0, 32),
	mkU32("vec2", u32_s0, u32_bw_mat_0a, u32_bw_mat_0ab, u32_bw_res_0, 128),
	mkU32("l32", u32_s1, u32_bw_mat_1a, u32_bw_mat_1ab, u32_bw_res_1, 32),
	mkU32("l64", append(u32_s1, u32_s0...), u32_bw_mat_1a, u32_bw_mat_1ab, append(u32_bw_res_1, u32_bw_res_0...), 64),
	mkU32("l128", append(u32_s1, u32_s0...), u32_bw_mat_1a, u32_bw_mat_1ab, append(u32_bw_res_1, u32_bw_res_0...), 128),
	mkU32("l256", append(u32_s1, u32_s0...), u32_bw_mat_1a, u32_bw_mat_1ab, append(u32_bw_res_1, u32_bw_res_0...), 256),
	mkU32("l255", append(u32_s1, u32_s0...), u32_bw_mat_1a, u32_bw_mat_1ab, append(u32_bw_res_1, u32_bw_res_0...), 255),
	mkU32("l127", u32_s1, u32_bw_mat_1a, u32_bw_mat_1ab, u32_bw_res_1, 127),
	mkU32("l63", u32_s1, u32_bw_mat_1a, u32_bw_mat_1ab, u32_bw_res_1, 63),
	mkU32("l31", u32_s1, u32_bw_mat_1a, u32_bw_mat_1ab, u32_bw_res_1, 31),
	mkU32("l23", u32_s1, u32_bw_mat_1a, u32_bw_mat_1ab, u32_bw_res_1, 23),
	mkU32("l15", u32_s1, u32_bw_mat_1a, u32_bw_mat_1ab, u32_bw_res_1, 15),
	mkU32("l7", u32_s1, u32_bw_mat_1a, u32_bw_mat_1ab, u32_bw_res_1, 7),
	mkU32("ext64", u32_s2, u32_bw_mat_2a, u32_bw_mat_2ab, u32_bw_res_2, 128),
	mkU32("ext32", u32_s2, u32_bw_mat_2a, u32_bw_mat_2ab, u32_bw_res_2, 32),
	mkU32("ext31", u32_s2, u32_bw_mat_2a, u32_bw_mat_2ab, u32_bw_res_2, 31),
	mkU32("full", u32_s2, u32_bw_mat_3a, u32_bw_mat_3b, u32_bw_res_3, 32),
}
