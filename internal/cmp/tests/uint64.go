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
	u64_s0 = []uint64{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	u64_eq_mat_0 uint64 = 5
	u64_eq_res_0        = []byte{0x6a, 0x1e, 0x48, 0x2c}

	u64_ne_mat_0 uint64 = 5
	u64_ne_res_0        = []byte{0x95, 0xe1, 0xb7, 0xd3}

	u64_lt_mat_0 uint64 = 5
	u64_lt_res_0        = []byte{0x05, 0x21, 0x27, 0x01}

	u64_le_mat_0 uint64 = 5
	u64_le_res_0        = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	u64_gt_mat_0 uint64 = 5
	u64_gt_res_0        = []byte{0x90, 0xc0, 0x90, 0xd2}

	u64_ge_mat_0 uint64 = 5
	u64_ge_res_0        = []byte{0xfa, 0xde, 0xd8, 0xfe}

	u64_bw_mat_0a uint64 = 5
	u64_bw_mat_0b uint64 = 10
	u64_bw_res_0         = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	u64_s1 = []uint64{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 500000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}

	u64_eq_res_1        = []byte{0x41, 0x42, 0xc4, 0x0e}
	u64_eq_mat_1 uint64 = 5

	u64_ne_res_1        = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	u64_ne_mat_1 uint64 = 5

	u64_lt_res_1        = []byte{0x0e, 0x00, 0x00, 0x00}
	u64_lt_mat_1 uint64 = 5

	u64_le_res_1        = []byte{0x4f, 0x42, 0xc4, 0x0e}
	u64_le_mat_1 uint64 = 5

	u64_gt_res_1        = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	u64_gt_mat_1 uint64 = 5

	u64_ge_res_1        = []byte{0xf1, 0xff, 0xff, 0xff}
	u64_ge_mat_1 uint64 = 5

	u64_bw_res_1         = []byte{0xf1, 0x42, 0xc4, 0x0e}
	u64_bw_mat_1a uint64 = 5
	u64_bw_mat_1b uint64 = 10

	// extreme values
	u64_s2 = []uint64{
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
	}
	u64_eq_res_2        = []byte{0x88, 0x88, 0x88, 0x88}
	u64_eq_mat_2 uint64 = math.MaxUint64

	u64_ne_res_2        = []byte{0x77, 0x77, 0x77, 0x77}
	u64_ne_mat_2 uint64 = math.MaxUint64

	u64_lt_res_2        = []byte{0x77, 0x77, 0x77, 0x77}
	u64_lt_mat_2 uint64 = math.MaxUint64

	u64_le_res_2        = []byte{0xff, 0xff, 0xff, 0xff}
	u64_le_mat_2 uint64 = math.MaxUint64

	u64_gt_res_2        = []byte{0x00, 0x00, 0x00, 0x00}
	u64_gt_mat_2 uint64 = math.MaxUint64

	u64_ge_res_2        = []byte{0x88, 0x88, 0x88, 0x88}
	u64_ge_mat_2 uint64 = math.MaxUint64

	u64_bw_res_2         = []byte{0xcc, 0xcc, 0xcc, 0xcc}
	u64_bw_mat_2a uint64 = math.MaxUint32
	u64_bw_mat_2b uint64 = math.MaxUint64
)

// creates an uint64 test case from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mkU64(name string, src []uint64, match, match2 uint64, result []byte, length int) MatchTest[uint64] {
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
	return MatchTest[uint64]{
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
var Uint64EqualCases = []MatchTest[uint64]{
	{"l0", make([]uint64, 0), u64_eq_mat_1, 0, []byte{}, 0},
	{"nil", nil, u64_eq_mat_1, 0, []byte{}, 0},
	mkU64("vec1", u64_s0, u64_eq_mat_0, 0, u64_eq_res_0, 32),
	mkU64("vec2", u64_s0, u64_eq_mat_0, 0, u64_eq_res_0, 64),
	mkU64("l32", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 32),
	mkU64("l64", append(u64_s1, u64_s0...), u64_eq_mat_1, 0, append(u64_eq_res_1, u64_eq_res_0...), 64),
	mkU64("l128", append(u64_s1, u64_s0...), u64_eq_mat_1, 0, append(u64_eq_res_1, u64_eq_res_0...), 128),
	mkU64("l127", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 127),
	mkU64("l63", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 63),
	mkU64("l31", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 31),
	mkU64("l23", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 23),
	mkU64("l15", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 15),
	mkU64("l7", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 7),
	mkU64("ext64", u64_s2, u64_eq_mat_2, 0, u64_eq_res_2, 64),
	mkU64("ext32", u64_s2, u64_eq_mat_2, 0, u64_eq_res_2, 32),
	mkU64("ext31", u64_s2, u64_eq_mat_2, 0, u64_eq_res_2, 31),
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
var Uint64NotEqualCases = []MatchTest[uint64]{
	{"l0", make([]uint64, 0), u64_ne_mat_1, 0, []byte{}, 0},
	{"nil", nil, u64_ne_mat_1, 0, []byte{}, 0},
	mkU64("vec1", u64_s0, u64_ne_mat_0, 0, u64_ne_res_0, 32),
	mkU64("vec2", u64_s0, u64_ne_mat_0, 0, u64_ne_res_0, 64),
	mkU64("l32", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 32),
	mkU64("l64", append(u64_s1, u64_s0...), u64_ne_mat_1, 0, append(u64_ne_res_1, u64_ne_res_0...), 64),
	mkU64("l128", append(u64_s1, u64_s0...), u64_ne_mat_1, 0, append(u64_ne_res_1, u64_ne_res_0...), 128),
	mkU64("l127", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 127),
	mkU64("l63", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 63),
	mkU64("l31", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 31),
	mkU64("l23", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 23),
	mkU64("l15", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 15),
	mkU64("l7", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 7),
	mkU64("ext64", u64_s2, u64_ne_mat_2, 0, u64_ne_res_2, 64),
	mkU64("ext32", u64_s2, u64_ne_mat_2, 0, u64_ne_res_2, 32),
	mkU64("ext31", u64_s2, u64_ne_mat_2, 0, u64_ne_res_2, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var Uint64LessCases = []MatchTest[uint64]{
	{"l0", make([]uint64, 0), u64_lt_mat_1, 0, []byte{}, 0},
	{"nil", nil, u64_lt_mat_1, 0, []byte{}, 0},
	mkU64("vec1", u64_s0, u64_lt_mat_0, 0, u64_lt_res_0, 32),
	mkU64("vec2", u64_s0, u64_lt_mat_0, 0, u64_lt_res_0, 64),
	mkU64("l32", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 32),
	mkU64("l64", append(u64_s1, u64_s0...), u64_lt_mat_1, 0, append(u64_lt_res_1, u64_lt_res_0...), 64),
	mkU64("l128", append(u64_s1, u64_s0...), u64_lt_mat_1, 0, append(u64_lt_res_1, u64_lt_res_0...), 128),
	mkU64("l127", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 127),
	mkU64("l63", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 63),
	mkU64("l31", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 31),
	mkU64("l23", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 23),
	mkU64("l15", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 15),
	mkU64("l7", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 7),
	mkU64("ext64", u64_s2, u64_lt_mat_2, 0, u64_lt_res_2, 64),
	mkU64("ext32", u64_s2, u64_lt_mat_2, 0, u64_lt_res_2, 32),
	mkU64("ext31", u64_s2, u64_lt_mat_2, 0, u64_lt_res_2, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var Uint64LessEqualCases = []MatchTest[uint64]{
	{"l0", make([]uint64, 0), u64_le_mat_1, 0, []byte{}, 0},
	{"nil", nil, u64_le_mat_1, 0, []byte{}, 0},
	mkU64("vec1", u64_s0, u64_le_mat_0, 0, u64_le_res_0, 32),
	mkU64("vec2", u64_s0, u64_le_mat_0, 0, u64_le_res_0, 64),
	mkU64("l32", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 32),
	mkU64("l64", append(u64_s1, u64_s0...), u64_le_mat_1, 0, append(u64_le_res_1, u64_le_res_0...), 64),
	mkU64("l128", append(u64_s1, u64_s0...), u64_le_mat_1, 0, append(u64_le_res_1, u64_le_res_0...), 128),
	mkU64("l127", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 127),
	mkU64("l63", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 63),
	mkU64("l31", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 31),
	mkU64("l23", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 23),
	mkU64("l15", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 15),
	mkU64("l7", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 7),
	mkU64("ext64", u64_s2, u64_le_mat_2, 0, u64_le_res_2, 64),
	mkU64("ext32", u64_s2, u64_le_mat_2, 0, u64_le_res_2, 32),
	mkU64("ext31", u64_s2, u64_le_mat_2, 0, u64_le_res_2, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var Uint64GreaterCases = []MatchTest[uint64]{
	{"l0", make([]uint64, 0), u64_gt_mat_1, 0, []byte{}, 0},
	{"nil", nil, u64_gt_mat_1, 0, []byte{}, 0},
	mkU64("vec1", u64_s0, u64_gt_mat_0, 0, u64_gt_res_0, 32),
	mkU64("vec2", u64_s0, u64_gt_mat_0, 0, u64_gt_res_0, 64),
	mkU64("l32", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 32),
	mkU64("l64", append(u64_s1, u64_s0...), u64_gt_mat_1, 0, append(u64_gt_res_1, u64_gt_res_0...), 64),
	mkU64("l128", append(u64_s1, u64_s0...), u64_gt_mat_1, 0, append(u64_gt_res_1, u64_gt_res_0...), 128),
	mkU64("l127", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 127),
	mkU64("l63", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 63),
	mkU64("l31", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 31),
	mkU64("l23", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 23),
	mkU64("l15", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 15),
	mkU64("l7", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 7),
	mkU64("ext64", u64_s2, u64_gt_mat_2, 0, u64_gt_res_2, 64),
	mkU64("ext32", u64_s2, u64_gt_mat_2, 0, u64_gt_res_2, 32),
	mkU64("ext31", u64_s2, u64_gt_mat_2, 0, u64_gt_res_2, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var Uint64GreaterEqualCases = []MatchTest[uint64]{
	{"l0", make([]uint64, 0), u64_ge_mat_1, 0, []byte{}, 0},
	{"nil", nil, u64_ge_mat_1, 0, []byte{}, 0},
	mkU64("vec1", u64_s0, u64_ge_mat_0, 0, u64_ge_res_0, 32),
	mkU64("vec2", u64_s0, u64_ge_mat_0, 0, u64_ge_res_0, 64),
	mkU64("l32", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 32),
	mkU64("l64", append(u64_s1, u64_s0...), u64_ge_mat_1, 0, append(u64_ge_res_1, u64_ge_res_0...), 64),
	mkU64("l128", append(u64_s1, u64_s0...), u64_ge_mat_1, 0, append(u64_ge_res_1, u64_ge_res_0...), 128),
	mkU64("l127", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 127),
	mkU64("l63", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 63),
	mkU64("l31", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 31),
	mkU64("l23", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 23),
	mkU64("l15", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 15),
	mkU64("l7", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 7),
	mkU64("ext64", u64_s2, u64_ge_mat_2, 0, u64_ge_res_2, 64),
	mkU64("ext32", u64_s2, u64_ge_mat_2, 0, u64_ge_res_2, 32),
	mkU64("ext31", u64_s2, u64_ge_mat_2, 0, u64_ge_res_2, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var Uint64BetweenCases = []MatchTest[uint64]{
	{"l0", make([]uint64, 0), u64_bw_mat_1a, u64_bw_mat_1b, []byte{}, 0},
	{"nil", nil, u64_bw_mat_1a, u64_bw_mat_1b, []byte{}, 0},
	mkU64("vec1", u64_s0, u64_bw_mat_0a, u64_bw_mat_0b, u64_bw_res_0, 32),
	mkU64("vec2", u64_s0, u64_bw_mat_0a, u64_bw_mat_0b, u64_bw_res_0, 64),
	mkU64("l32", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 32),
	mkU64("l64", append(u64_s1, u64_s0...), u64_bw_mat_1a, u64_bw_mat_1b, append(u64_bw_res_1, u64_bw_res_0...), 64),
	mkU64("l128", append(u64_s1, u64_s0...), u64_bw_mat_1a, u64_bw_mat_1b, append(u64_bw_res_1, u64_bw_res_0...), 128),
	mkU64("l127", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 127),
	mkU64("l63", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 63),
	mkU64("l31", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 31),
	mkU64("l23", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 23),
	mkU64("l15", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 15),
	mkU64("l7", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 7),
	mkU64("ext64", u64_s2, u64_bw_mat_2a, u64_bw_mat_2b, u64_bw_res_2, 64),
	mkU64("ext32", u64_s2, u64_bw_mat_2a, u64_bw_mat_2b, u64_bw_res_2, 32),
	mkU64("ext31", u64_s2, u64_bw_mat_2a, u64_bw_mat_2b, u64_bw_res_2, 31),
}
