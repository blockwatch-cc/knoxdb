// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"fmt"
	"math"
	"math/bits"
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
)

type (
	Int256      = num.Int256
	Int256Slice = []num.Int256
)

var (
	Int256From2Int64 = num.Int256From2Int64
	Int256FromInt64  = num.Int256FromInt64
	Int256FromInt128 = num.Int256FromInt128
	MinInt256        = num.MinInt256
	MaxInt256        = num.MaxInt256
	ZeroInt256       = num.ZeroInt256
)

func RandInt256Slice(n int) Int256Slice {
	s := make([]Int256, n)
	for i := 0; i < n; i++ {
		s[i][0] = util.RandUint64()
		s[i][1] = util.RandUint64()
		s[i][2] = util.RandUint64()
		s[i][3] = util.RandUint64()
	}
	return s
}

type Int256MatchTest struct {
	Name   string
	Slice  Int256Slice
	Match  Int256 // used for every test
	Match2 Int256 // used for between tests
	Result []byte
	Count  int64
}

var (
	// positive values only
	i256_s1 = []Int256{
		{0, 0, 2, 5}, {0, 0, 2, 2}, {0, 0, 2, 3}, {0, 0, 2, 4},
		{0, 0, 2, 7}, {0, 0, 2, 8}, {0, 0, 2, 5}, {0, 0, 2, 10},
		{0, 0, 1, 5}, {0, 0, 1, 2}, {0, 0, 1, 3}, {0, 0, 1, 4},
		{0, 0, 1, 7}, {0, 0, 1, 8}, {0, 0, 1, 5}, {0, 0, 1, 10},
		{0, 0, 3, 5}, {0, 0, 3, 2}, {0, 0, 3, 3}, {0, 0, 3, 4},
		{0, 0, 3, 7}, {0, 0, 3, 8}, {0, 0, 3, 5}, {0, 0, 3, 10},
		{0, 0, 2, 5}, {0, 0, 0, 2}, {0, 0, 10, 3}, {0, 0, 0, 40},
		{0, 0, 2, 0}, {0, 0, 2, 10}, {0, 0, 2, 10}, {0, 0, 2, 5},
	}
	i256_eq_res_1 = []byte{0x41, 0x00, 0x00, 0x81}
	i256_eq_mat_1 = Int256From2Int64(2, 5)

	i256_ne_res_1 = []byte{0xbe, 0xff, 0xff, 0x7e}
	i256_ne_mat_1 = Int256From2Int64(2, 5)

	i256_lt_res_1 = []byte{0x0e, 0xff, 0x00, 0x1a}
	i256_lt_mat_1 = Int256From2Int64(2, 5)

	i256_le_res_1 = []byte{0x4f, 0xff, 0x00, 0x9b}
	i256_le_mat_1 = Int256From2Int64(2, 5)

	i256_gt_res_1 = []byte{0xb0, 0x00, 0xff, 0x64}
	i256_gt_mat_1 = Int256From2Int64(2, 5)

	i256_ge_res_1 = []byte{0xf1, 0x00, 0xff, 0xe5}
	i256_ge_mat_1 = Int256From2Int64(2, 5)

	i256_bw_res_1  = []byte{0xf1, 0x00, 0x00, 0xe1}
	i256_bw_mat_1a = Int256From2Int64(2, 5)
	i256_bw_mat_1b = Int256From2Int64(2, 10)

	// negative and positive values mixed
	i256_s2 = []Int256{
		Int256From2Int64(-2, -5), Int256From2Int64(-2, -4), Int256From2Int64(-2, -3), Int256From2Int64(-2, -2),
		Int256From2Int64(-2, -7), Int256From2Int64(-2, -8), Int256From2Int64(-2, -5), Int256From2Int64(-2, -10),
		Int256From2Int64(-1, -5), Int256From2Int64(-1, -4), Int256From2Int64(-1, -3), Int256From2Int64(-1, -2),
		Int256From2Int64(-1, -7), Int256From2Int64(-1, -8), Int256From2Int64(-1, -5), Int256From2Int64(-1, -10),
		Int256From2Int64(-3, -5), Int256From2Int64(-3, -4), Int256From2Int64(-3, -3), Int256From2Int64(-3, -2),
		Int256From2Int64(-3, -7), Int256From2Int64(-3, -8), Int256From2Int64(-3, -5), Int256From2Int64(-3, -10),
		Int256From2Int64(2, -5), Int256From2Int64(2, -4), Int256From2Int64(2, -3), Int256From2Int64(2, -2),
		Int256From2Int64(2, -7), Int256From2Int64(2, -8), Int256From2Int64(2, -5), Int256From2Int64(2, 10),
	}
	i256_eq_res_2 = []byte{0x41, 0x0, 0x0, 0x0}
	i256_eq_mat_2 = Int256From2Int64(-2, -5)

	i256_ne_res_2 = []byte{0xbe, 0xff, 0xff, 0xff}
	i256_ne_mat_2 = Int256From2Int64(-2, -5)

	i256_lt_res_2 = []byte{0xb0, 0x00, 0xff, 0x00}
	i256_lt_mat_2 = Int256From2Int64(-2, -5)

	i256_le_res_2 = []byte{0xf1, 0x00, 0xff, 0x00}
	i256_le_mat_2 = Int256From2Int64(-2, -5)

	i256_gt_res_2 = []byte{0x0e, 0xff, 0x00, 0xff}
	i256_gt_mat_2 = Int256From2Int64(-2, -5)

	i256_ge_res_2 = []byte{0x4f, 0xff, 0x00, 0xff}
	i256_ge_mat_2 = Int256From2Int64(-2, -5)

	i256_bw_res_2  = []byte{0xf1, 0x00, 0x00, 0x00}
	i256_bw_mat_2a = Int256From2Int64(-2, -10)
	i256_bw_mat_2b = Int256From2Int64(-2, -5)

	// extreme values
	i256_s3 = []Int256{
		Int256FromInt64(math.MaxInt32), Int256FromInt64(math.MinInt32),
		Int256FromInt64(math.MaxInt64), Int256FromInt64(math.MinInt64),
		Int256FromInt128(MaxInt128), Int256FromInt128(MinInt128),
		MaxInt256, MinInt256,
		Int256FromInt64(math.MaxInt32), Int256FromInt64(math.MinInt32),
		Int256FromInt64(math.MaxInt64), Int256FromInt64(math.MinInt64),
		Int256FromInt128(MaxInt128), Int256FromInt128(MinInt128),
		MaxInt256, MinInt256,
		Int256FromInt64(math.MaxInt32), Int256FromInt64(math.MinInt32),
		Int256FromInt64(math.MaxInt64), Int256FromInt64(math.MinInt64),
		Int256FromInt128(MaxInt128), Int256FromInt128(MinInt128),
		MaxInt256, MinInt256,
		Int256FromInt64(math.MaxInt32), Int256FromInt64(math.MinInt32),
		Int256FromInt64(math.MaxInt64), Int256FromInt64(math.MinInt64),
		Int256FromInt128(MaxInt128), Int256FromInt128(MinInt128),
		MaxInt256, MinInt256,
	}
	i256_eq_res_3 = []byte{0x80, 0x80, 0x80, 0x80}
	i256_eq_mat_3 = MinInt256

	i256_ne_res_3 = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	i256_ne_mat_3 = MinInt256

	i256_lt_res_3 = []byte{0x0, 0x0, 0x0, 0x0}
	i256_lt_mat_3 = MinInt256

	i256_le_res_3 = []byte{0x80, 0x80, 0x80, 0x80}
	i256_le_mat_3 = MinInt256

	i256_gt_res_3 = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	i256_gt_mat_3 = MinInt256

	i256_ge_res_3 = []byte{0xff, 0xff, 0xff, 0xff}
	i256_ge_mat_3 = MinInt256

	i256_bw_res_3  = []byte{0x50, 0x50, 0x50, 0x50}
	i256_bw_mat_3a = Int256FromInt128(MaxInt128)
	i256_bw_mat_3b = MaxInt256
)

// creates an Int256 test case from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mkI256(name string, src []Int256, match, match2 Int256, result []byte, length int) Int256MatchTest {
	if len(src)%8 != 0 {
		panic(fmt.Errorf("i256 %s: length of slice has to be a multiple of 8", name))
	}
	if len(result) != BitFieldLen(len(src)) {
		panic(fmt.Errorf("i256 %s: length of slice and length of result does not match", name))
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
	return Int256MatchTest{
		Name:   name,
		Slice:  src,
		Match:  match,
		Match2: match2,
		Result: result,
		Count:  int64(cnt),
	}
}

// Test Drivers
type (
	Int256MatchFunc  = func(*num.Int256Stride, num.Int256, []byte, []byte) int64
	Int256MatchFunc2 = func(*num.Int256Stride, num.Int256, num.Int256, []byte, []byte) int64
)

func TestInt256Cases(t *testing.T, cases []Int256MatchTest, fn Int256MatchFunc) {
	t.Helper()
	for _, c := range cases {
		bits, mask := MakeBitsAndMaskPoisonTail(len(c.Slice), 32, nil)
		cnt := fn(num.Int256Optimize(c.Slice), c.Match, bits, mask)
		assert.Len(t, bits, len(c.Result), c.Name)
		assert.Equal(t, c.Count, cnt, "%s: unexpected result bit count", c.Name)
		assert.Equal(t, c.Result, bits, "%s:unexpected result", c.Name)
		assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "%s: boundary violation", c.Name)
	}
}

func TestInt256Cases2(t *testing.T, cases []Int256MatchTest, fn Int256MatchFunc2) {
	t.Helper()
	for _, c := range cases {
		bits, mask := MakeBitsAndMaskPoisonTail(len(c.Slice), 32, nil)
		cnt := fn(num.Int256Optimize(c.Slice), c.Match, c.Match2, bits, mask)
		assert.Len(t, bits, len(c.Result), c.Name)
		assert.Equal(t, c.Count, cnt, "%s: unexpected result bit count", c.Name)
		assert.Equal(t, c.Result, bits, "%s: unexpected result", c.Name)
		assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "%s: boundary violation", c.Name)
	}
}

func BenchInt256Cases(b *testing.B, fn Int256MatchFunc) {
	b.Helper()
	for _, c := range tests.BenchmarkSizes {
		for _, m := range BenchmarkMasks {
			a := num.Int256Optimize(RandInt256Slice(c.N))
			bits, mask := MakeBitsAndMaskPoison(a.Len(), m.Pattern)
			b.Run(c.Name+"/mask_"+m.Name, func(b *testing.B) {
				b.SetBytes(int64(c.N * 32))
				for range b.N {
					fn(a, MaxInt256.Rsh(1), bits, mask)
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchInt256Cases2(b *testing.B, fn Int256MatchFunc2) {
	b.Helper()
	for _, c := range tests.BenchmarkSizes {
		for _, m := range BenchmarkMasks {
			a := num.Int256Optimize(RandInt256Slice(c.N))
			bits, mask := MakeBitsAndMaskPoison(a.Len(), m.Pattern)
			b.Run(c.Name+"/mask_"+m.Name, func(b *testing.B) {
				b.SetBytes(int64(c.N * 32))
				for i := 0; i < b.N; i++ {
					fn(a, MaxInt256.Rsh(2), MaxInt256.Rsh(1), bits, mask)
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

// -----------------------------------------------------------------------------
// Equal Testcases
var Int256EqualCases = []Int256MatchTest{
	{"l0", make([]Int256, 0), i256_eq_mat_1, ZeroInt256, []byte{}, 0},
	{"nil", nil, i256_eq_mat_1, ZeroInt256, []byte{}, 0},
	mkI256("l32", i256_s1, i256_eq_mat_1, ZeroInt256, i256_eq_res_1, 32),
	mkI256("l64", i256_s1, i256_eq_mat_1, ZeroInt256, i256_eq_res_1, 64),
	mkI256("l128", i256_s1, i256_eq_mat_1, ZeroInt256, i256_eq_res_1, 128),
	mkI256("l127", i256_s1, i256_eq_mat_1, ZeroInt256, i256_eq_res_1, 127),
	mkI256("l63", i256_s1, i256_eq_mat_1, ZeroInt256, i256_eq_res_1, 63),
	mkI256("l31", i256_s1, i256_eq_mat_1, ZeroInt256, i256_eq_res_1, 31),
	mkI256("l23", i256_s1, i256_eq_mat_1, ZeroInt256, i256_eq_res_1, 23),
	mkI256("l15", i256_s1, i256_eq_mat_1, ZeroInt256, i256_eq_res_1, 15),
	mkI256("l7", i256_s1, i256_eq_mat_1, ZeroInt256, i256_eq_res_1, 7),
	mkI256("neg64", i256_s2, i256_eq_mat_2, ZeroInt256, i256_eq_res_2, 64),
	mkI256("neg32", i256_s2, i256_eq_mat_2, ZeroInt256, i256_eq_res_2, 32),
	mkI256("neg31", i256_s2, i256_eq_mat_2, ZeroInt256, i256_eq_res_2, 31),
	mkI256("ext64", i256_s3, i256_eq_mat_3, ZeroInt256, i256_eq_res_3, 64),
	mkI256("ext32", i256_s3, i256_eq_mat_3, ZeroInt256, i256_eq_res_3, 32),
	mkI256("ext31", i256_s3, i256_eq_mat_3, ZeroInt256, i256_eq_res_3, 31),
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
var Int256NotEqualCases = []Int256MatchTest{
	{"l0", make([]Int256, 0), i256_ne_mat_1, ZeroInt256, []byte{}, 0},
	{"nil", nil, i256_ne_mat_1, ZeroInt256, []byte{}, 0},
	mkI256("l32", i256_s1, i256_ne_mat_1, ZeroInt256, i256_ne_res_1, 32),
	mkI256("l64", i256_s1, i256_ne_mat_1, ZeroInt256, i256_ne_res_1, 64),
	mkI256("l128", i256_s1, i256_ne_mat_1, ZeroInt256, i256_ne_res_1, 128),
	mkI256("l127", i256_s1, i256_ne_mat_1, ZeroInt256, i256_ne_res_1, 127),
	mkI256("l63", i256_s1, i256_ne_mat_1, ZeroInt256, i256_ne_res_1, 63),
	mkI256("l31", i256_s1, i256_ne_mat_1, ZeroInt256, i256_ne_res_1, 31),
	mkI256("l23", i256_s1, i256_ne_mat_1, ZeroInt256, i256_ne_res_1, 23),
	mkI256("l15", i256_s1, i256_ne_mat_1, ZeroInt256, i256_ne_res_1, 15),
	mkI256("l7", i256_s1, i256_ne_mat_1, ZeroInt256, i256_ne_res_1, 7),
	mkI256("neg64", i256_s2, i256_ne_mat_2, ZeroInt256, i256_ne_res_2, 64),
	mkI256("neg32", i256_s2, i256_ne_mat_2, ZeroInt256, i256_ne_res_2, 32),
	mkI256("neg31", i256_s2, i256_ne_mat_2, ZeroInt256, i256_ne_res_2, 31),
	mkI256("ext64", i256_s3, i256_ne_mat_3, ZeroInt256, i256_ne_res_3, 64),
	mkI256("ext32", i256_s3, i256_ne_mat_3, ZeroInt256, i256_ne_res_3, 32),
	mkI256("ext31", i256_s3, i256_ne_mat_3, ZeroInt256, i256_ne_res_3, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var Int256LessCases = []Int256MatchTest{
	{"l0", make([]Int256, 0), i256_lt_mat_1, ZeroInt256, []byte{}, 0},
	{"nil", nil, i256_lt_mat_1, ZeroInt256, []byte{}, 0},
	mkI256("l32", i256_s1, i256_lt_mat_1, ZeroInt256, i256_lt_res_1, 32),
	mkI256("l64", i256_s1, i256_lt_mat_1, ZeroInt256, i256_lt_res_1, 64),
	mkI256("l128", i256_s1, i256_lt_mat_1, ZeroInt256, i256_lt_res_1, 128),
	mkI256("l127", i256_s1, i256_lt_mat_1, ZeroInt256, i256_lt_res_1, 127),
	mkI256("l63", i256_s1, i256_lt_mat_1, ZeroInt256, i256_lt_res_1, 63),
	mkI256("l31", i256_s1, i256_lt_mat_1, ZeroInt256, i256_lt_res_1, 31),
	mkI256("l23", i256_s1, i256_lt_mat_1, ZeroInt256, i256_lt_res_1, 23),
	mkI256("l15", i256_s1, i256_lt_mat_1, ZeroInt256, i256_lt_res_1, 15),
	mkI256("l7", i256_s1, i256_lt_mat_1, ZeroInt256, i256_lt_res_1, 7),
	mkI256("neg64", i256_s2, i256_lt_mat_2, ZeroInt256, i256_lt_res_2, 64),
	mkI256("neg32", i256_s2, i256_lt_mat_2, ZeroInt256, i256_lt_res_2, 32),
	mkI256("neg31", i256_s2, i256_lt_mat_2, ZeroInt256, i256_lt_res_2, 31),
	mkI256("ext64", i256_s3, i256_lt_mat_3, ZeroInt256, i256_lt_res_3, 64),
	mkI256("ext32", i256_s3, i256_lt_mat_3, ZeroInt256, i256_lt_res_3, 32),
	mkI256("ext31", i256_s3, i256_lt_mat_3, ZeroInt256, i256_lt_res_3, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var Int256LessEqualCases = []Int256MatchTest{
	{"l0", make([]Int256, 0), i256_le_mat_1, ZeroInt256, []byte{}, 0},
	{"nil", nil, i256_le_mat_1, ZeroInt256, []byte{}, 0},
	mkI256("l32", i256_s1, i256_le_mat_1, ZeroInt256, i256_le_res_1, 32),
	mkI256("l64", i256_s1, i256_le_mat_1, ZeroInt256, i256_le_res_1, 64),
	mkI256("l128", i256_s1, i256_le_mat_1, ZeroInt256, i256_le_res_1, 128),
	mkI256("l127", i256_s1, i256_le_mat_1, ZeroInt256, i256_le_res_1, 127),
	mkI256("l63", i256_s1, i256_le_mat_1, ZeroInt256, i256_le_res_1, 63),
	mkI256("l31", i256_s1, i256_le_mat_1, ZeroInt256, i256_le_res_1, 31),
	mkI256("l23", i256_s1, i256_le_mat_1, ZeroInt256, i256_le_res_1, 23),
	mkI256("l15", i256_s1, i256_le_mat_1, ZeroInt256, i256_le_res_1, 15),
	mkI256("l7", i256_s1, i256_le_mat_1, ZeroInt256, i256_le_res_1, 7),
	mkI256("neg64", i256_s2, i256_le_mat_2, ZeroInt256, i256_le_res_2, 64),
	mkI256("neg32", i256_s2, i256_le_mat_2, ZeroInt256, i256_le_res_2, 32),
	mkI256("neg31", i256_s2, i256_le_mat_2, ZeroInt256, i256_le_res_2, 31),
	mkI256("ext64", i256_s3, i256_le_mat_3, ZeroInt256, i256_le_res_3, 64),
	mkI256("ext32", i256_s3, i256_le_mat_3, ZeroInt256, i256_le_res_3, 32),
	mkI256("ext31", i256_s3, i256_le_mat_3, ZeroInt256, i256_le_res_3, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var Int256GreaterCases = []Int256MatchTest{
	{"l0", make([]Int256, 0), i256_gt_mat_1, ZeroInt256, []byte{}, 0},
	{"nil", nil, i256_gt_mat_1, ZeroInt256, []byte{}, 0},
	mkI256("l32", i256_s1, i256_gt_mat_1, ZeroInt256, i256_gt_res_1, 32),
	mkI256("l64", i256_s1, i256_gt_mat_1, ZeroInt256, i256_gt_res_1, 64),
	mkI256("l128", i256_s1, i256_gt_mat_1, ZeroInt256, i256_gt_res_1, 128),
	mkI256("l127", i256_s1, i256_gt_mat_1, ZeroInt256, i256_gt_res_1, 127),
	mkI256("l63", i256_s1, i256_gt_mat_1, ZeroInt256, i256_gt_res_1, 63),
	mkI256("l31", i256_s1, i256_gt_mat_1, ZeroInt256, i256_gt_res_1, 31),
	mkI256("l23", i256_s1, i256_gt_mat_1, ZeroInt256, i256_gt_res_1, 23),
	mkI256("l15", i256_s1, i256_gt_mat_1, ZeroInt256, i256_gt_res_1, 15),
	mkI256("l7", i256_s1, i256_gt_mat_1, ZeroInt256, i256_gt_res_1, 7),
	mkI256("neg64", i256_s2, i256_gt_mat_2, ZeroInt256, i256_gt_res_2, 64),
	mkI256("neg32", i256_s2, i256_gt_mat_2, ZeroInt256, i256_gt_res_2, 32),
	mkI256("neg31", i256_s2, i256_gt_mat_2, ZeroInt256, i256_gt_res_2, 31),
	mkI256("ext64", i256_s3, i256_gt_mat_3, ZeroInt256, i256_gt_res_3, 64),
	mkI256("ext32", i256_s3, i256_gt_mat_3, ZeroInt256, i256_gt_res_3, 32),
	mkI256("ext31", i256_s3, i256_gt_mat_3, ZeroInt256, i256_gt_res_3, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var Int256GreaterEqualCases = []Int256MatchTest{
	{"l0", make([]Int256, 0), i256_ge_mat_1, ZeroInt256, []byte{}, 0},
	{"nil", nil, i256_ge_mat_1, ZeroInt256, []byte{}, 0},
	mkI256("l32", i256_s1, i256_ge_mat_1, ZeroInt256, i256_ge_res_1, 32),
	mkI256("l64", i256_s1, i256_ge_mat_1, ZeroInt256, i256_ge_res_1, 64),
	mkI256("l128", i256_s1, i256_ge_mat_1, ZeroInt256, i256_ge_res_1, 128),
	mkI256("l127", i256_s1, i256_ge_mat_1, ZeroInt256, i256_ge_res_1, 127),
	mkI256("l63", i256_s1, i256_ge_mat_1, ZeroInt256, i256_ge_res_1, 63),
	mkI256("l31", i256_s1, i256_ge_mat_1, ZeroInt256, i256_ge_res_1, 31),
	mkI256("l23", i256_s1, i256_ge_mat_1, ZeroInt256, i256_ge_res_1, 23),
	mkI256("l15", i256_s1, i256_ge_mat_1, ZeroInt256, i256_ge_res_1, 15),
	mkI256("l7", i256_s1, i256_ge_mat_1, ZeroInt256, i256_ge_res_1, 7),
	mkI256("neg64", i256_s2, i256_ge_mat_2, ZeroInt256, i256_ge_res_2, 64),
	mkI256("neg32", i256_s2, i256_ge_mat_2, ZeroInt256, i256_ge_res_2, 32),
	mkI256("neg31", i256_s2, i256_ge_mat_2, ZeroInt256, i256_ge_res_2, 31),
	mkI256("ext64", i256_s3, i256_ge_mat_3, ZeroInt256, i256_ge_res_3, 64),
	mkI256("ext32", i256_s3, i256_ge_mat_3, ZeroInt256, i256_ge_res_3, 32),
	mkI256("ext31", i256_s3, i256_ge_mat_3, ZeroInt256, i256_ge_res_3, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var Int256BetweenCases = []Int256MatchTest{
	{"l0", make([]Int256, 0), i256_bw_mat_1a, i256_bw_mat_1b, []byte{}, 0},
	{"nil", nil, i256_bw_mat_1a, i256_bw_mat_1b, []byte{}, 0},
	mkI256("l32", i256_s1, i256_bw_mat_1a, i256_bw_mat_1b, i256_bw_res_1, 32),
	mkI256("l64", i256_s1, i256_bw_mat_1a, i256_bw_mat_1b, i256_bw_res_1, 64),
	mkI256("l128", i256_s1, i256_bw_mat_1a, i256_bw_mat_1b, i256_bw_res_1, 128),
	mkI256("l127", i256_s1, i256_bw_mat_1a, i256_bw_mat_1b, i256_bw_res_1, 127),
	mkI256("l63", i256_s1, i256_bw_mat_1a, i256_bw_mat_1b, i256_bw_res_1, 63),
	mkI256("l31", i256_s1, i256_bw_mat_1a, i256_bw_mat_1b, i256_bw_res_1, 31),
	mkI256("l23", i256_s1, i256_bw_mat_1a, i256_bw_mat_1b, i256_bw_res_1, 23),
	mkI256("l15", i256_s1, i256_bw_mat_1a, i256_bw_mat_1b, i256_bw_res_1, 15),
	mkI256("l7", i256_s1, i256_bw_mat_1a, i256_bw_mat_1b, i256_bw_res_1, 7),
	mkI256("neg64", i256_s2, i256_bw_mat_2a, i256_bw_mat_2b, i256_bw_res_2, 64),
	mkI256("neg32", i256_s2, i256_bw_mat_2a, i256_bw_mat_2b, i256_bw_res_2, 32),
	mkI256("neg31", i256_s2, i256_bw_mat_2a, i256_bw_mat_2b, i256_bw_res_2, 31),
	mkI256("ext64", i256_s3, i256_bw_mat_3a, i256_bw_mat_3b, i256_bw_res_3, 64),
	mkI256("ext32", i256_s3, i256_bw_mat_3a, i256_bw_mat_3b, i256_bw_res_3, 32),
	mkI256("ext31", i256_s3, i256_bw_mat_3a, i256_bw_mat_3b, i256_bw_res_3, 31),
}
