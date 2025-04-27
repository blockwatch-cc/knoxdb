// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

type BytesMatchTest struct {
	Name   string
	Slice  [][]byte
	Match  []byte // used for every test
	Match2 []byte // used for between tests
	Result []byte
	Count  int64
}

func RandBytes(n int) [][]byte {
	return util.RandByteSlices(n, 8)
}

func Uint64Bytes(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}

// creates a Bytes test case from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mkBytes(name string, src []uint64, match, match2 uint64, result []byte, length int) BytesMatchTest {
	if len(src)%8 != 0 {
		panic(fmt.Errorf("bytes %s: length of slice has to be a multiple of 8", name))
	}
	if len(result) != bitFieldLen(len(src)) {
		panic(fmt.Errorf("bytes %s: length of slice and length of result does not match", name))
	}

	// create new src at requested length
	bsrc := make([][]byte, len(src))
	for i, v := range src {
		bsrc[i] = Uint64Bytes(v)
	}
	l := length
	for l > len(bsrc) {
		bsrc = append(bsrc, bsrc...)
	}
	bsrc = bsrc[:l]

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
	return BytesMatchTest{
		Name:   name,
		Slice:  bsrc,
		Match:  Uint64Bytes(match),
		Match2: Uint64Bytes(match2),
		Result: result,
		Count:  int64(cnt),
	}
}

// Test Drivers
type (
	BytesMatchFunc  = func([][]byte, []byte, []byte, []byte) int64
	BytesMatchFunc2 = func([][]byte, []byte, []byte, []byte, []byte) int64
)

func TestBytesCases(t *testing.T, cases []BytesMatchTest, fn BytesMatchFunc) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			bits, mask := MakeBitsAndMaskPoisonTail(len(c.Slice), 32, maskAll)
			cnt := fn(c.Slice, c.Match, bits, mask)
			assert.Len(t, bits, len(c.Result))
			assert.Equal(t, c.Count, cnt, "unexpected result bit count")
			assert.Equal(t, c.Result, bits, "unexpected result")
			assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "boundary violation")
		})
	}
}

func TestBytesCases2(t *testing.T, cases []BytesMatchTest, fn BytesMatchFunc2) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			bits, mask := MakeBitsAndMaskPoisonTail(len(c.Slice), 32, maskAll)
			cnt := fn(c.Slice, c.Match, c.Match2, bits, mask)
			assert.Len(t, bits, len(c.Result), c.Name)
			assert.Equal(t, c.Count, cnt, "unexpected result bit count")
			assert.Equal(t, c.Result, bits, "unexpected result")
			assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "boundary violation")
		})
	}
}

func BenchBytesCases(b *testing.B, fn BytesMatchFunc) {
	b.Helper()
	for _, c := range tests.BenchmarkSizes {
		for _, m := range BenchmarkMasks {
			a := RandBytes(c.N)
			bits, mask := MakeBitsAndMaskPoison(len(a), m.Pattern)
			b.Run(c.Name+"/mask_"+m.Name, func(b *testing.B) {
				b.SetBytes(int64(c.N * 8))
				for range b.N {
					fn(a, Uint64Bytes(math.MaxUint64>>1), bits, mask)
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchBytesCases2(b *testing.B, fn BytesMatchFunc2) {
	b.Helper()
	for _, c := range tests.BenchmarkSizes {
		for _, m := range BenchmarkMasks {
			a := RandBytes(c.N)
			bits, mask := MakeBitsAndMaskPoison(len(a), m.Pattern)
			b.Run(c.Name+"/mask_"+m.Name, func(b *testing.B) {
				b.SetBytes(int64(c.N * 16))
				for range b.N {
					fn(a, Uint64Bytes(math.MaxUint64>>2), Uint64Bytes(math.MaxUint64>>1), bits, mask)
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

// -----------------------------------------------------------------------------
// Equal Testcases
var BytesEqualCases = []BytesMatchTest{
	{"l0", make([][]byte, 0), []byte{5}, []byte{}, []byte{}, 0},
	{"nil", nil, []byte{5}, []byte{}, []byte{}, 0},
	mkBytes("vec1", u64_s0, u64_eq_mat_0, 0, u64_eq_res_0, 32),
	mkBytes("vec2", u64_s0, u64_eq_mat_0, 0, u64_eq_res_0, 64),
	mkBytes("l32", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 32),
	mkBytes("l64", append(u64_s1, u64_s0...), u64_eq_mat_1, 0, append(u64_eq_res_1, u64_eq_res_0...), 64),
	mkBytes("l128", append(u64_s1, u64_s0...), u64_eq_mat_1, 0, append(u64_eq_res_1, u64_eq_res_0...), 128),
	mkBytes("l127", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 127),
	mkBytes("l63", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 63),
	mkBytes("l31", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 31),
	mkBytes("l23", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 23),
	mkBytes("l15", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 15),
	mkBytes("l7", u64_s1, u64_eq_mat_1, 0, u64_eq_res_1, 7),
	mkBytes("ext64", u64_s2, u64_eq_mat_2, 0, u64_eq_res_2, 64),
	mkBytes("ext32", u64_s2, u64_eq_mat_2, 0, u64_eq_res_2, 32),
	mkBytes("ext31", u64_s2, u64_eq_mat_2, 0, u64_eq_res_2, 31),
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
var BytesNotEqualCases = []BytesMatchTest{
	{"l0", make([][]byte, 0), []byte{5}, []byte{}, []byte{}, 0},
	{"nil", nil, []byte{5}, []byte{}, []byte{}, 0},
	mkBytes("vec1", u64_s0, u64_ne_mat_0, 0, u64_ne_res_0, 32),
	mkBytes("vec2", u64_s0, u64_ne_mat_0, 0, u64_ne_res_0, 64),
	mkBytes("l32", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 32),
	mkBytes("l64", append(u64_s1, u64_s0...), u64_ne_mat_1, 0, append(u64_ne_res_1, u64_ne_res_0...), 64),
	mkBytes("l128", append(u64_s1, u64_s0...), u64_ne_mat_1, 0, append(u64_ne_res_1, u64_ne_res_0...), 128),
	mkBytes("l127", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 127),
	mkBytes("l63", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 63),
	mkBytes("l31", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 31),
	mkBytes("l23", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 23),
	mkBytes("l15", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 15),
	mkBytes("l7", u64_s1, u64_ne_mat_1, 0, u64_ne_res_1, 7),
	mkBytes("ext64", u64_s2, u64_ne_mat_2, 0, u64_ne_res_2, 64),
	mkBytes("ext32", u64_s2, u64_ne_mat_2, 0, u64_ne_res_2, 32),
	mkBytes("ext31", u64_s2, u64_ne_mat_2, 0, u64_ne_res_2, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var BytesLessCases = []BytesMatchTest{
	{"l0", make([][]byte, 0), []byte{5}, []byte{}, []byte{}, 0},
	{"nil", nil, []byte{5}, []byte{}, []byte{}, 0},
	mkBytes("vec1", u64_s0, u64_lt_mat_0, 0, u64_lt_res_0, 32),
	mkBytes("vec2", u64_s0, u64_lt_mat_0, 0, u64_lt_res_0, 64),
	mkBytes("l32", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 32),
	mkBytes("l64", append(u64_s1, u64_s0...), u64_lt_mat_1, 0, append(u64_lt_res_1, u64_lt_res_0...), 64),
	mkBytes("l128", append(u64_s1, u64_s0...), u64_lt_mat_1, 0, append(u64_lt_res_1, u64_lt_res_0...), 128),
	mkBytes("l127", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 127),
	mkBytes("l63", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 63),
	mkBytes("l31", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 31),
	mkBytes("l23", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 23),
	mkBytes("l15", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 15),
	mkBytes("l7", u64_s1, u64_lt_mat_1, 0, u64_lt_res_1, 7),
	mkBytes("ext64", u64_s2, u64_lt_mat_2, 0, u64_lt_res_2, 64),
	mkBytes("ext32", u64_s2, u64_lt_mat_2, 0, u64_lt_res_2, 32),
	mkBytes("ext31", u64_s2, u64_lt_mat_2, 0, u64_lt_res_2, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var BytesLessEqualCases = []BytesMatchTest{
	{"l0", make([][]byte, 0), []byte{5}, []byte{}, []byte{}, 0},
	{"nil", nil, []byte{5}, []byte{}, []byte{}, 0},
	mkBytes("vec1", u64_s0, u64_le_mat_0, 0, u64_le_res_0, 32),
	mkBytes("vec2", u64_s0, u64_le_mat_0, 0, u64_le_res_0, 64),
	mkBytes("l32", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 32),
	mkBytes("l64", append(u64_s1, u64_s0...), u64_le_mat_1, 0, append(u64_le_res_1, u64_le_res_0...), 64),
	mkBytes("l128", append(u64_s1, u64_s0...), u64_le_mat_1, 0, append(u64_le_res_1, u64_le_res_0...), 128),
	mkBytes("l127", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 127),
	mkBytes("l63", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 63),
	mkBytes("l31", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 31),
	mkBytes("l23", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 23),
	mkBytes("l15", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 15),
	mkBytes("l7", u64_s1, u64_le_mat_1, 0, u64_le_res_1, 7),
	mkBytes("ext64", u64_s2, u64_le_mat_2, 0, u64_le_res_2, 64),
	mkBytes("ext32", u64_s2, u64_le_mat_2, 0, u64_le_res_2, 32),
	mkBytes("ext31", u64_s2, u64_le_mat_2, 0, u64_le_res_2, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var BytesGreaterCases = []BytesMatchTest{
	{"l0", make([][]byte, 0), []byte{5}, []byte{}, []byte{}, 0},
	{"nil", nil, []byte{5}, []byte{}, []byte{}, 0},
	mkBytes("vec1", u64_s0, u64_gt_mat_0, 0, u64_gt_res_0, 32),
	mkBytes("vec2", u64_s0, u64_gt_mat_0, 0, u64_gt_res_0, 64),
	mkBytes("l32", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 32),
	mkBytes("l64", append(u64_s1, u64_s0...), u64_gt_mat_1, 0, append(u64_gt_res_1, u64_gt_res_0...), 64),
	mkBytes("l128", append(u64_s1, u64_s0...), u64_gt_mat_1, 0, append(u64_gt_res_1, u64_gt_res_0...), 128),
	mkBytes("l127", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 127),
	mkBytes("l63", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 63),
	mkBytes("l31", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 31),
	mkBytes("l23", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 23),
	mkBytes("l15", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 15),
	mkBytes("l7", u64_s1, u64_gt_mat_1, 0, u64_gt_res_1, 7),
	mkBytes("ext64", u64_s2, u64_gt_mat_2, 0, u64_gt_res_2, 64),
	mkBytes("ext32", u64_s2, u64_gt_mat_2, 0, u64_gt_res_2, 32),
	mkBytes("ext31", u64_s2, u64_gt_mat_2, 0, u64_gt_res_2, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var BytesGreaterEqualCases = []BytesMatchTest{
	{"l0", make([][]byte, 0), []byte{5}, []byte{}, []byte{}, 0},
	{"nil", nil, []byte{5}, []byte{}, []byte{}, 0},
	mkBytes("vec1", u64_s0, u64_ge_mat_0, 0, u64_ge_res_0, 32),
	mkBytes("vec2", u64_s0, u64_ge_mat_0, 0, u64_ge_res_0, 64),
	mkBytes("l32", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 32),
	mkBytes("l64", append(u64_s1, u64_s0...), u64_ge_mat_1, 0, append(u64_ge_res_1, u64_ge_res_0...), 64),
	mkBytes("l128", append(u64_s1, u64_s0...), u64_ge_mat_1, 0, append(u64_ge_res_1, u64_ge_res_0...), 128),
	mkBytes("l127", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 127),
	mkBytes("l63", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 63),
	mkBytes("l31", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 31),
	mkBytes("l23", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 23),
	mkBytes("l15", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 15),
	mkBytes("l7", u64_s1, u64_ge_mat_1, 0, u64_ge_res_1, 7),
	mkBytes("ext64", u64_s2, u64_ge_mat_2, 0, u64_ge_res_2, 64),
	mkBytes("ext32", u64_s2, u64_ge_mat_2, 0, u64_ge_res_2, 32),
	mkBytes("ext31", u64_s2, u64_ge_mat_2, 0, u64_ge_res_2, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var BytesBetweenCases = []BytesMatchTest{
	{"l0", make([][]byte, 0), []byte{5}, []byte{10}, []byte{}, 0},
	{"nil", nil, []byte{5}, []byte{10}, []byte{}, 0},
	mkBytes("vec1", u64_s0, u64_bw_mat_0a, u64_bw_mat_0b, u64_bw_res_0, 32),
	mkBytes("vec2", u64_s0, u64_bw_mat_0a, u64_bw_mat_0b, u64_bw_res_0, 64),
	mkBytes("l32", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 32),
	mkBytes("l64", append(u64_s1, u64_s0...), u64_bw_mat_1a, u64_bw_mat_1b, append(u64_bw_res_1, u64_bw_res_0...), 64),
	mkBytes("l128", append(u64_s1, u64_s0...), u64_bw_mat_1a, u64_bw_mat_1b, append(u64_bw_res_1, u64_bw_res_0...), 128),
	mkBytes("l127", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 127),
	mkBytes("l63", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 63),
	mkBytes("l31", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 31),
	mkBytes("l23", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 23),
	mkBytes("l15", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 15),
	mkBytes("l7", u64_s1, u64_bw_mat_1a, u64_bw_mat_1b, u64_bw_res_1, 7),
	mkBytes("ext64", u64_s2, u64_bw_mat_2a, u64_bw_mat_2b, u64_bw_res_2, 64),
	mkBytes("ext32", u64_s2, u64_bw_mat_2a, u64_bw_mat_2b, u64_bw_res_2, 32),
	mkBytes("ext31", u64_s2, u64_bw_mat_2a, u64_bw_mat_2b, u64_bw_res_2, 31),
}
