// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stringx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/bitset/generic"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
)

var (
	BitFieldLen = generic.BitFieldLen
)

func TestCompare(t *testing.T) {
	testCompareCases(t, BytesEqualCases, types.FilterModeEqual)
	testCompareCases(t, BytesNotEqualCases, types.FilterModeNotEqual)
	testCompareCases(t, BytesLessCases, types.FilterModeLt)
	testCompareCases(t, BytesLessEqualCases, types.FilterModeLe)
	testCompareCases(t, BytesGreaterCases, types.FilterModeGt)
	testCompareCases(t, BytesGreaterEqualCases, types.FilterModeGe)
	testCompareCases(t, BytesBetweenCases, types.FilterModeRange)
}

func BenchmarkCompare(b *testing.B) {
	benchCompareCases(b, types.FilterModeEqual)
	benchCompareCases(b, types.FilterModeNotEqual)
	benchCompareCases(b, types.FilterModeLt)
	benchCompareCases(b, types.FilterModeLe)
	benchCompareCases(b, types.FilterModeGt)
	benchCompareCases(b, types.FilterModeGe)
	benchCompareCases(b, types.FilterModeRange)
}

// -----------------------------------------------------------------------------
// Test Drivers
//

func testCompareCases(t *testing.T, cases []BytesMatchTest, mode types.FilterMode) {
	t.Helper()
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s/%s", mode, c.Name), func(t *testing.T) {
			src := c.Data
			bits, _ := MakeBitsAndMaskPoison(c.N, nil)
			set := bitset.NewFromBytes(bits, c.N)
			switch mode {
			case types.FilterModeEqual:
				src.MatchEqual(c.Match, set, nil)
			case types.FilterModeNotEqual:
				src.MatchNotEqual(c.Match, set, nil)
			case types.FilterModeGt:
				src.MatchGreater(c.Match, set, nil)
			case types.FilterModeGe:
				src.MatchGreaterEqual(c.Match, set, nil)
			case types.FilterModeLt:
				src.MatchLess(c.Match, set, nil)
			case types.FilterModeLe:
				src.MatchLessEqual(c.Match, set, nil)
			case types.FilterModeRange:
				src.MatchBetween(c.Match, c.Match2, set, nil)
			}
			assert.Len(t, bits, len(c.Result))
			assert.Equal(t, c.Count, set.Count(), "unexpected result bit count")
			assert.Equal(t, c.Result, bits, "unexpected result")
			assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "boundary violation")
		})
	}
}

func benchCompareCases(b *testing.B, mode types.FilterMode) {
	b.Helper()
	for _, c := range BenchmarkSizes {
		for _, m := range BenchmarkMasks {
			src := NewStringPool(c.N)
			src.AppendMany(RandBytes(c.N)...)
			bits, mask := MakeBitsAndMaskPoison(c.N, m.Pattern)
			set := bitset.NewFromBytes(bits, c.N)
			var msk *bitset.Bitset
			if mask != nil {
				msk = bitset.NewFromBytes(mask, c.N)
			}
			match := Uint64Bytes(math.MaxUint64 >> 2)
			match2 := Uint64Bytes(math.MaxUint64 >> 1)
			b.Run(fmt.Sprintf("%s/%s/mask_%s", mode, c.Name, m.Name), func(b *testing.B) {
				b.SetBytes(int64(c.N * BYTE_LEN))
				for b.Loop() {
					switch mode {
					case types.FilterModeEqual:
						src.MatchEqual(match, set, msk)
					case types.FilterModeNotEqual:
						src.MatchNotEqual(match, set, msk)
					case types.FilterModeGt:
						src.MatchGreater(match, set, msk)
					case types.FilterModeGe:
						src.MatchGreaterEqual(match, set, msk)
					case types.FilterModeLt:
						src.MatchLess(match, set, msk)
					case types.FilterModeLe:
						src.MatchLessEqual(match, set, msk)
					case types.FilterModeRange:
						src.MatchBetween(match, match2, set, nil)
					}
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

// -----------------------------------------------------------------------------
// Testcases
//

type BytesMatchTest struct {
	Name   string
	N      int
	Data   *StringPool
	Match  []byte // used for every test
	Match2 []byte // used for between tests
	Result []byte
	Count  int
}

const BYTE_LEN = 32

var poison = []byte{0xfa}

func RandBytes(n int) [][]byte {
	return util.RandByteSlices(n, BYTE_LEN)
}

func Uint64Bytes(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}

func MakePoison(sz int) []byte {
	return bytes.Repeat(poison, sz)
}

// allocate the result bitset and fill padding with poison
func MakeBitsAndMaskPoison(sz int, maskBits []byte) ([]byte, []byte) {
	l := BitFieldLen(sz)
	bits := make([]byte, l+32)
	var mask []byte
	if len(maskBits) > 0 && maskBits[0] != 0xff && maskBits[0] != 0 {
		mask = bytes.Repeat(maskBits, l/len(maskBits))
	}
	for i := range 32 {
		bits[l+i] = 0xfa
	}
	bits = bits[:l]
	return bits, mask
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
	if len(result) != BitFieldLen(len(src)) {
		panic(fmt.Errorf("bytes %s: length of slice and length of result does not match", name))
	}

	// create new src at requested length
	pool := NewStringPool(length)
	for _, v := range src[:min(length, len(src))] {
		pool.Append(Uint64Bytes(v))
	}
	// fmt.Printf("Fill src=%d pool=%d\n", len(src), pool.Len())

	// duplicate strings until we reach the requested length
	n := length - pool.Len()
	for n > 0 {
		pool.Range(0, min(n, pool.Len())).AppendTo(pool, nil)
		// fmt.Printf("Clone pool=[%d:%d] n=%d => %d\n", 0, min(n, pool.Len()), n, pool.Len())
		n = length - pool.Len()
	}

	// if pool.Len() != length {
	//  panic(fmt.Errorf("Invalid len=%d, want=%d", pool.Len(), length))
	// }

	// create new result at requested length
	result = bytes.Clone(result)
	n = BitFieldLen(length)
	for n > len(result) {
		result = append(result, result...)
	}
	result = result[:n]

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
		N:      length,
		Data:   pool,
		Match:  Uint64Bytes(match),
		Match2: Uint64Bytes(match2),
		Result: result,
		Count:  cnt,
	}
}

// -----------------------------------------------------------------------------
// Equal Testcases
var BytesEqualCases = []BytesMatchTest{
	{"l0", 0, NewStringPool(0), []byte{5}, []byte{}, []byte{}, 0},
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
	{"l0", 0, NewStringPool(0), []byte{5}, []byte{}, []byte{}, 0},
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
	{"l0", 0, NewStringPool(0), []byte{5}, []byte{}, []byte{}, 0},
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
	{"l0", 0, NewStringPool(0), []byte{5}, []byte{}, []byte{}, 0},
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
	{"l0", 0, NewStringPool(0), []byte{5}, []byte{}, []byte{}, 0},
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
	{"l0", 0, NewStringPool(0), []byte{5}, []byte{}, []byte{}, 0},
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
	{"l0", 0, NewStringPool(0), []byte{5}, []byte{10}, []byte{}, 0},
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

	u64_bw_res_3         = []byte{0xff, 0xff, 0xff, 0xff}
	u64_bw_mat_3a uint64 = 0
	u64_bw_mat_3b uint64 = math.MaxUint64
)
