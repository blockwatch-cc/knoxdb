// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"fmt"
	"math"
	"math/bits"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
	"golang.org/x/exp/slices"
)

type (
	Int128      = num.Int128
	Int128Slice = []num.Int128
)

var (
	Int128From2Int64 = num.Int128From2Int64
	Int128FromInt64  = num.Int128FromInt64
	MinInt128        = num.MinInt128
	MaxInt128        = num.MaxInt128
	ZeroInt128       = num.ZeroInt128
)

func RandInt128Slice(n int) Int128Slice {
	s := make([]Int128, n)
	for i := 0; i < n; i++ {
		s[i][0] = util.RandUint64()
		s[i][1] = util.RandUint64()
	}
	return s
}

type Int128MatchTest struct {
	Name   string
	Slice  Int128Slice
	Match  Int128 // used for every test
	Match2 Int128 // used for between tests
	Result []byte
	Count  int64
}

var (
	// positive values only
	i128_s1 = []Int128{
		{2, 5}, {2, 2}, {2, 3}, {2, 4},
		{2, 7}, {2, 8}, {2, 5}, {2, 10},
		{1, 5}, {1, 2}, {1, 3}, {1, 4},
		{1, 7}, {1, 8}, {1, 5}, {1, 10},
		{3, 5}, {3, 2}, {3, 3}, {3, 4},
		{3, 7}, {3, 8}, {3, 5}, {3, 10},
		{2, 5}, {0, 2}, {10, 3}, {0, 40},
		{2, 0}, {2, 10}, {2, 10}, {2, 5},
	}
	i128_eq_res_1 = []byte{0x41, 0x00, 0x00, 0x81}
	i128_eq_mat_1 = Int128From2Int64(2, 5)

	i128_ne_res_1 = []byte{0xbe, 0xff, 0xff, 0x7e}
	i128_ne_mat_1 = Int128From2Int64(2, 5)

	i128_lt_res_1 = []byte{0x0e, 0xff, 0x00, 0x1a}
	i128_lt_mat_1 = Int128From2Int64(2, 5)

	i128_le_res_1 = []byte{0x4f, 0xff, 0x00, 0x9b}
	i128_le_mat_1 = Int128From2Int64(2, 5)

	i128_gt_res_1 = []byte{0xb0, 0x00, 0xff, 0x64}
	i128_gt_mat_1 = Int128From2Int64(2, 5)

	i128_ge_res_1 = []byte{0xf1, 0x00, 0xff, 0xe5}
	i128_ge_mat_1 = Int128From2Int64(2, 5)

	i128_bw_res_1  = []byte{0xf1, 0x00, 0x00, 0xe1}
	i128_bw_mat_1a = Int128From2Int64(2, 5)
	i128_bw_mat_1b = Int128From2Int64(2, 10)

	// negative and positive values mixed
	i128_s2 = []Int128{
		Int128From2Int64(-2, -5), Int128From2Int64(-2, -4), Int128From2Int64(-2, -3), Int128From2Int64(-2, -2),
		Int128From2Int64(-2, -7), Int128From2Int64(-2, -8), Int128From2Int64(-2, -5), Int128From2Int64(-2, -10),
		Int128From2Int64(-1, -5), Int128From2Int64(-1, -4), Int128From2Int64(-1, -3), Int128From2Int64(-1, -2),
		Int128From2Int64(-1, -7), Int128From2Int64(-1, -8), Int128From2Int64(-1, -5), Int128From2Int64(-1, -10),
		Int128From2Int64(-3, -5), Int128From2Int64(-3, -4), Int128From2Int64(-3, -3), Int128From2Int64(-3, -2),
		Int128From2Int64(-3, -7), Int128From2Int64(-3, -8), Int128From2Int64(-3, -5), Int128From2Int64(-3, -10),
		Int128From2Int64(2, -5), Int128From2Int64(2, -4), Int128From2Int64(2, -3), Int128From2Int64(2, -2),
		Int128From2Int64(2, -7), Int128From2Int64(2, -8), Int128From2Int64(2, -5), Int128From2Int64(2, 10),
	}
	i128_eq_res_2 = []byte{0x41, 0x0, 0x0, 0x0}
	i128_eq_mat_2 = Int128From2Int64(-2, -5)

	i128_ne_res_2 = []byte{0xbe, 0xff, 0xff, 0xff}
	i128_ne_mat_2 = Int128From2Int64(-2, -5)

	i128_lt_res_2 = []byte{0xb0, 0x00, 0xff, 0x00}
	i128_lt_mat_2 = Int128From2Int64(-2, -5)

	i128_le_res_2 = []byte{0xf1, 0x00, 0xff, 0x00}
	i128_le_mat_2 = Int128From2Int64(-2, -5)

	i128_gt_res_2 = []byte{0x0e, 0xff, 0x00, 0xff}
	i128_gt_mat_2 = Int128From2Int64(-2, -5)

	i128_ge_res_2 = []byte{0x4f, 0xff, 0x00, 0xff}
	i128_ge_mat_2 = Int128From2Int64(-2, -5)

	i128_bw_res_2  = []byte{0xf1, 0x00, 0x00, 0x00}
	i128_bw_mat_2a = Int128From2Int64(-2, -10)
	i128_bw_mat_2b = Int128From2Int64(-2, -5)

	// extreme values
	i128_s3 = []Int128{
		Int128FromInt64(math.MaxInt16), Int128FromInt64(math.MinInt16),
		Int128FromInt64(math.MaxInt32), Int128FromInt64(math.MinInt32),
		Int128FromInt64(math.MaxInt64), Int128FromInt64(math.MinInt64),
		MaxInt128, MinInt128,
		Int128FromInt64(math.MaxInt16), Int128FromInt64(math.MinInt16),
		Int128FromInt64(math.MaxInt32), Int128FromInt64(math.MinInt32),
		Int128FromInt64(math.MaxInt64), Int128FromInt64(math.MinInt64),
		MaxInt128, MinInt128,
		Int128FromInt64(math.MaxInt16), Int128FromInt64(math.MinInt16),
		Int128FromInt64(math.MaxInt32), Int128FromInt64(math.MinInt32),
		Int128FromInt64(math.MaxInt64), Int128FromInt64(math.MinInt64),
		MaxInt128, MinInt128,
		Int128FromInt64(math.MaxInt16), Int128FromInt64(math.MinInt16),
		Int128FromInt64(math.MaxInt32), Int128FromInt64(math.MinInt32),
		Int128FromInt64(math.MaxInt64), Int128FromInt64(math.MinInt64),
		MaxInt128, MinInt128,
	}
	i128_eq_res_3 = []byte{0x80, 0x80, 0x80, 0x80}
	i128_eq_mat_3 = MinInt128

	i128_ne_res_3 = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	i128_ne_mat_3 = MinInt128

	i128_lt_res_3 = []byte{0x0, 0x0, 0x0, 0x0}
	i128_lt_mat_3 = MinInt128

	i128_le_res_3 = []byte{0x80, 0x80, 0x80, 0x80}
	i128_le_mat_3 = MinInt128

	i128_gt_res_3 = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	i128_gt_mat_3 = MinInt128

	i128_ge_res_3 = []byte{0xff, 0xff, 0xff, 0xff}
	i128_ge_mat_3 = MinInt128

	i128_bw_res_3  = []byte{0x50, 0x50, 0x50, 0x50}
	i128_bw_mat_3a = Int128FromInt64(math.MaxInt64)
	i128_bw_mat_3b = MaxInt128
)

// creates an Int128 test case from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mkI128(name string, src []Int128, match, match2 Int128, result []byte, length int) Int128MatchTest {
	if len(src)%8 != 0 {
		panic(fmt.Errorf("i128 %s: length of slice has to be a multiple of 8", name))
	}
	if len(result) != bitFieldLen(len(src)) {
		panic(fmt.Errorf("i128 %s: length of slice and length of result does not match", name))
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
	return Int128MatchTest{
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
var Int128EqualCases = []Int128MatchTest{
	{"l0", make([]Int128, 0), i128_eq_mat_1, ZeroInt128, []byte{}, 0},
	{"nil", nil, i128_eq_mat_1, ZeroInt128, []byte{}, 0},
	mkI128("l32", i128_s1, i128_eq_mat_1, ZeroInt128, i128_eq_res_1, 32),
	mkI128("l64", i128_s1, i128_eq_mat_1, ZeroInt128, i128_eq_res_1, 64),
	mkI128("l128", i128_s1, i128_eq_mat_1, ZeroInt128, i128_eq_res_1, 128),
	mkI128("l127", i128_s1, i128_eq_mat_1, ZeroInt128, i128_eq_res_1, 127),
	mkI128("l63", i128_s1, i128_eq_mat_1, ZeroInt128, i128_eq_res_1, 63),
	mkI128("l31", i128_s1, i128_eq_mat_1, ZeroInt128, i128_eq_res_1, 31),
	mkI128("l23", i128_s1, i128_eq_mat_1, ZeroInt128, i128_eq_res_1, 23),
	mkI128("l15", i128_s1, i128_eq_mat_1, ZeroInt128, i128_eq_res_1, 15),
	mkI128("l7", i128_s1, i128_eq_mat_1, ZeroInt128, i128_eq_res_1, 7),
	mkI128("neg64", i128_s2, i128_eq_mat_2, ZeroInt128, i128_eq_res_2, 64),
	mkI128("neg32", i128_s2, i128_eq_mat_2, ZeroInt128, i128_eq_res_2, 32),
	mkI128("neg31", i128_s2, i128_eq_mat_2, ZeroInt128, i128_eq_res_2, 31),
	mkI128("ext64", i128_s3, i128_eq_mat_3, ZeroInt128, i128_eq_res_3, 64),
	mkI128("ext32", i128_s3, i128_eq_mat_3, ZeroInt128, i128_eq_res_3, 32),
	mkI128("ext31", i128_s3, i128_eq_mat_3, ZeroInt128, i128_eq_res_3, 31),
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//

var Int128NotEqualCases = []Int128MatchTest{
	{"l0", make([]Int128, 0), i128_ne_mat_1, ZeroInt128, []byte{}, 0},
	{"nil", nil, i128_ne_mat_1, ZeroInt128, []byte{}, 0},
	mkI128("l32", i128_s1, i128_ne_mat_1, ZeroInt128, i128_ne_res_1, 32),
	mkI128("l64", i128_s1, i128_ne_mat_1, ZeroInt128, i128_ne_res_1, 64),
	mkI128("l128", i128_s1, i128_ne_mat_1, ZeroInt128, i128_ne_res_1, 128),
	mkI128("l127", i128_s1, i128_ne_mat_1, ZeroInt128, i128_ne_res_1, 127),
	mkI128("l63", i128_s1, i128_ne_mat_1, ZeroInt128, i128_ne_res_1, 63),
	mkI128("l31", i128_s1, i128_ne_mat_1, ZeroInt128, i128_ne_res_1, 31),
	mkI128("l23", i128_s1, i128_ne_mat_1, ZeroInt128, i128_ne_res_1, 23),
	mkI128("l15", i128_s1, i128_ne_mat_1, ZeroInt128, i128_ne_res_1, 15),
	mkI128("l7", i128_s1, i128_ne_mat_1, ZeroInt128, i128_ne_res_1, 7),
	mkI128("neg64", i128_s2, i128_ne_mat_2, ZeroInt128, i128_ne_res_2, 64),
	mkI128("neg32", i128_s2, i128_ne_mat_2, ZeroInt128, i128_ne_res_2, 32),
	mkI128("neg31", i128_s2, i128_ne_mat_2, ZeroInt128, i128_ne_res_2, 31),
	mkI128("ext64", i128_s3, i128_ne_mat_3, ZeroInt128, i128_ne_res_3, 64),
	mkI128("ext32", i128_s3, i128_ne_mat_3, ZeroInt128, i128_ne_res_3, 32),
	mkI128("ext31", i128_s3, i128_ne_mat_3, ZeroInt128, i128_ne_res_3, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var Int128LessCases = []Int128MatchTest{
	{"l0", make([]Int128, 0), i128_lt_mat_1, ZeroInt128, []byte{}, 0},
	{"nil", nil, i128_lt_mat_1, ZeroInt128, []byte{}, 0},
	mkI128("l32", i128_s1, i128_lt_mat_1, ZeroInt128, i128_lt_res_1, 32),
	mkI128("l64", i128_s1, i128_lt_mat_1, ZeroInt128, i128_lt_res_1, 64),
	mkI128("l128", i128_s1, i128_lt_mat_1, ZeroInt128, i128_lt_res_1, 128),
	mkI128("l127", i128_s1, i128_lt_mat_1, ZeroInt128, i128_lt_res_1, 127),
	mkI128("l63", i128_s1, i128_lt_mat_1, ZeroInt128, i128_lt_res_1, 63),
	mkI128("l31", i128_s1, i128_lt_mat_1, ZeroInt128, i128_lt_res_1, 31),
	mkI128("l23", i128_s1, i128_lt_mat_1, ZeroInt128, i128_lt_res_1, 23),
	mkI128("l15", i128_s1, i128_lt_mat_1, ZeroInt128, i128_lt_res_1, 15),
	mkI128("l7", i128_s1, i128_lt_mat_1, ZeroInt128, i128_lt_res_1, 7),
	mkI128("neg64", i128_s2, i128_lt_mat_2, ZeroInt128, i128_lt_res_2, 64),
	mkI128("neg32", i128_s2, i128_lt_mat_2, ZeroInt128, i128_lt_res_2, 32),
	mkI128("neg31", i128_s2, i128_lt_mat_2, ZeroInt128, i128_lt_res_2, 31),
	mkI128("ext64", i128_s3, i128_lt_mat_3, ZeroInt128, i128_lt_res_3, 64),
	mkI128("ext32", i128_s3, i128_lt_mat_3, ZeroInt128, i128_lt_res_3, 32),
	mkI128("ext31", i128_s3, i128_lt_mat_3, ZeroInt128, i128_lt_res_3, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var Int128LessEqualCases = []Int128MatchTest{
	{"l0", make([]Int128, 0), i128_le_mat_1, ZeroInt128, []byte{}, 0},
	{"nil", nil, i128_le_mat_1, ZeroInt128, []byte{}, 0},
	mkI128("l32", i128_s1, i128_le_mat_1, ZeroInt128, i128_le_res_1, 32),
	mkI128("l64", i128_s1, i128_le_mat_1, ZeroInt128, i128_le_res_1, 64),
	mkI128("l128", i128_s1, i128_le_mat_1, ZeroInt128, i128_le_res_1, 128),
	mkI128("l127", i128_s1, i128_le_mat_1, ZeroInt128, i128_le_res_1, 127),
	mkI128("l63", i128_s1, i128_le_mat_1, ZeroInt128, i128_le_res_1, 63),
	mkI128("l31", i128_s1, i128_le_mat_1, ZeroInt128, i128_le_res_1, 31),
	mkI128("l23", i128_s1, i128_le_mat_1, ZeroInt128, i128_le_res_1, 23),
	mkI128("l15", i128_s1, i128_le_mat_1, ZeroInt128, i128_le_res_1, 15),
	mkI128("l7", i128_s1, i128_le_mat_1, ZeroInt128, i128_le_res_1, 7),
	mkI128("neg64", i128_s2, i128_le_mat_2, ZeroInt128, i128_le_res_2, 64),
	mkI128("neg32", i128_s2, i128_le_mat_2, ZeroInt128, i128_le_res_2, 32),
	mkI128("neg31", i128_s2, i128_le_mat_2, ZeroInt128, i128_le_res_2, 31),
	mkI128("ext64", i128_s3, i128_le_mat_3, ZeroInt128, i128_le_res_3, 64),
	mkI128("ext32", i128_s3, i128_le_mat_3, ZeroInt128, i128_le_res_3, 32),
	mkI128("ext31", i128_s3, i128_le_mat_3, ZeroInt128, i128_le_res_3, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var Int128GreaterCases = []Int128MatchTest{
	{"l0", make([]Int128, 0), i128_gt_mat_1, ZeroInt128, []byte{}, 0},
	{"nil", nil, i128_gt_mat_1, ZeroInt128, []byte{}, 0},
	mkI128("l32", i128_s1, i128_gt_mat_1, ZeroInt128, i128_gt_res_1, 32),
	mkI128("l64", i128_s1, i128_gt_mat_1, ZeroInt128, i128_gt_res_1, 64),
	mkI128("l128", i128_s1, i128_gt_mat_1, ZeroInt128, i128_gt_res_1, 128),
	mkI128("l127", i128_s1, i128_gt_mat_1, ZeroInt128, i128_gt_res_1, 127),
	mkI128("l63", i128_s1, i128_gt_mat_1, ZeroInt128, i128_gt_res_1, 63),
	mkI128("l31", i128_s1, i128_gt_mat_1, ZeroInt128, i128_gt_res_1, 31),
	mkI128("l23", i128_s1, i128_gt_mat_1, ZeroInt128, i128_gt_res_1, 23),
	mkI128("l15", i128_s1, i128_gt_mat_1, ZeroInt128, i128_gt_res_1, 15),
	mkI128("l7", i128_s1, i128_gt_mat_1, ZeroInt128, i128_gt_res_1, 7),
	mkI128("neg64", i128_s2, i128_gt_mat_2, ZeroInt128, i128_gt_res_2, 64),
	mkI128("neg32", i128_s2, i128_gt_mat_2, ZeroInt128, i128_gt_res_2, 32),
	mkI128("neg31", i128_s2, i128_gt_mat_2, ZeroInt128, i128_gt_res_2, 31),
	mkI128("ext64", i128_s3, i128_gt_mat_3, ZeroInt128, i128_gt_res_3, 64),
	mkI128("ext32", i128_s3, i128_gt_mat_3, ZeroInt128, i128_gt_res_3, 32),
	mkI128("ext31", i128_s3, i128_gt_mat_3, ZeroInt128, i128_gt_res_3, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var Int128GreaterEqualCases = []Int128MatchTest{
	{"l0", make([]Int128, 0), i128_ge_mat_1, ZeroInt128, []byte{}, 0},
	{"nil", nil, i128_ge_mat_1, ZeroInt128, []byte{}, 0},
	mkI128("l32", i128_s1, i128_ge_mat_1, ZeroInt128, i128_ge_res_1, 32),
	mkI128("l64", i128_s1, i128_ge_mat_1, ZeroInt128, i128_ge_res_1, 64),
	mkI128("l128", i128_s1, i128_ge_mat_1, ZeroInt128, i128_ge_res_1, 128),
	mkI128("l127", i128_s1, i128_ge_mat_1, ZeroInt128, i128_ge_res_1, 127),
	mkI128("l63", i128_s1, i128_ge_mat_1, ZeroInt128, i128_ge_res_1, 63),
	mkI128("l31", i128_s1, i128_ge_mat_1, ZeroInt128, i128_ge_res_1, 31),
	mkI128("l23", i128_s1, i128_ge_mat_1, ZeroInt128, i128_ge_res_1, 23),
	mkI128("l15", i128_s1, i128_ge_mat_1, ZeroInt128, i128_ge_res_1, 15),
	mkI128("l7", i128_s1, i128_ge_mat_1, ZeroInt128, i128_ge_res_1, 7),
	mkI128("neg64", i128_s2, i128_ge_mat_2, ZeroInt128, i128_ge_res_2, 64),
	mkI128("neg32", i128_s2, i128_ge_mat_2, ZeroInt128, i128_ge_res_2, 32),
	mkI128("neg31", i128_s2, i128_ge_mat_2, ZeroInt128, i128_ge_res_2, 31),
	mkI128("ext64", i128_s3, i128_ge_mat_3, ZeroInt128, i128_ge_res_3, 64),
	mkI128("ext32", i128_s3, i128_ge_mat_3, ZeroInt128, i128_ge_res_3, 32),
	mkI128("ext31", i128_s3, i128_ge_mat_3, ZeroInt128, i128_ge_res_3, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var Int128BetweenCases = []Int128MatchTest{
	{"l0", make([]Int128, 0), i128_bw_mat_1a, i128_bw_mat_1b, []byte{}, 0},
	{"nil", nil, i128_bw_mat_1a, i128_bw_mat_1b, []byte{}, 0},
	mkI128("l32", i128_s1, i128_bw_mat_1a, i128_bw_mat_1b, i128_bw_res_1, 32),
	mkI128("l64", i128_s1, i128_bw_mat_1a, i128_bw_mat_1b, i128_bw_res_1, 64),
	mkI128("l128", i128_s1, i128_bw_mat_1a, i128_bw_mat_1b, i128_bw_res_1, 128),
	mkI128("l127", i128_s1, i128_bw_mat_1a, i128_bw_mat_1b, i128_bw_res_1, 127),
	mkI128("l63", i128_s1, i128_bw_mat_1a, i128_bw_mat_1b, i128_bw_res_1, 63),
	mkI128("l31", i128_s1, i128_bw_mat_1a, i128_bw_mat_1b, i128_bw_res_1, 31),
	mkI128("l23", i128_s1, i128_bw_mat_1a, i128_bw_mat_1b, i128_bw_res_1, 23),
	mkI128("l15", i128_s1, i128_bw_mat_1a, i128_bw_mat_1b, i128_bw_res_1, 15),
	mkI128("l7", i128_s1, i128_bw_mat_1a, i128_bw_mat_1b, i128_bw_res_1, 7),
	mkI128("neg64", i128_s2, i128_bw_mat_2a, i128_bw_mat_2b, i128_bw_res_2, 64),
	mkI128("neg32", i128_s2, i128_bw_mat_2a, i128_bw_mat_2b, i128_bw_res_2, 32),
	mkI128("neg31", i128_s2, i128_bw_mat_2a, i128_bw_mat_2b, i128_bw_res_2, 31),
	mkI128("ext64", i128_s3, i128_bw_mat_3a, i128_bw_mat_3b, i128_bw_res_3, 64),
	mkI128("ext32", i128_s3, i128_bw_mat_3a, i128_bw_mat_3b, i128_bw_res_3, 32),
	mkI128("ext31", i128_s3, i128_bw_mat_3a, i128_bw_mat_3b, i128_bw_res_3, 31),
}
