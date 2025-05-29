// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

import (
	"fmt"
	"math"
	"math/bits"
	"slices"
)

var (
	f32_s0 = []float32{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	f32_eq_mat_0 float32 = 5
	f32_eq_res_0         = []byte{0x6a, 0x1e, 0x48, 0x2c}

	f32_ne_mat_0 float32 = 5
	f32_ne_res_0         = []byte{0x95, 0xe1, 0xb7, 0xd3}

	f32_lt_mat_0 float32 = 5
	f32_lt_res_0         = []byte{0x05, 0x21, 0x27, 0x01}

	f32_le_mat_0 float32 = 5
	f32_le_res_0         = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	f32_gt_mat_0 float32 = 5
	f32_gt_res_0         = []byte{0x90, 0xc0, 0x90, 0xd2}

	f32_ge_mat_0 float32 = 5
	f32_ge_res_0         = []byte{0xfa, 0xde, 0xd8, 0xfe}

	f32_bw_mat_0a float32 = 5
	f32_bw_mat_0b float32 = 10
	f32_bw_res_0          = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	// positive int values only
	f32_s1 = []float32{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 500000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	f32_eq_res_1         = []byte{0x41, 0x42, 0xc4, 0x0e}
	f32_eq_mat_1 float32 = 5

	f32_ne_res_1         = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	f32_ne_mat_1 float32 = 5

	f32_lt_res_1         = []byte{0x0e, 0x00, 0x00, 0x00}
	f32_lt_mat_1 float32 = 5

	f32_le_res_1         = []byte{0x4f, 0x42, 0xc4, 0x0e}
	f32_le_mat_1 float32 = 5

	f32_gt_res_1         = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	f32_gt_mat_1 float32 = 5

	f32_ge_res_1         = []byte{0xf1, 0xff, 0xff, 0xff}
	f32_ge_mat_1 float32 = 5

	f32_bw_res_1          = []byte{0xf1, 0x42, 0xc4, 0x0e}
	f32_bw_mat_1a float32 = 5
	f32_bw_mat_1b float32 = 10

	// negative and positive values mixed
	f32_s2 = []float32{
		-5.12, 2.5, -3.1, 5.45,
		7.125, 8.2, 9.4, -10.25,
		15.25, 50.25, 55.25, 500.25,
		1000.25, -500000.25, 113.25, 12.25,
		31.25, 32.25, 33.25, 34.25,
		35, -36, 37.25, 38.25,
		39.25, 40.25, -41.25, 42.25,
		43.25, 44.25, 45.25, -46.25,
	}
	f32_eq_res_2         = []byte{0x01, 0x0, 0x0, 0x0}
	f32_eq_mat_2 float32 = -5.12

	f32_ne_res_2         = []byte{0xfe, 0xff, 0xff, 0xff}
	f32_ne_mat_2 float32 = -5.12

	f32_lt_res_2         = []byte{0x80, 0x20, 0x20, 0x84}
	f32_lt_mat_2 float32 = -5.12

	f32_le_res_2         = []byte{0x81, 0x20, 0x20, 0x84}
	f32_le_mat_2 float32 = -5.12

	f32_gt_res_2         = []byte{0x7e, 0xdf, 0xdf, 0x7b}
	f32_gt_mat_2 float32 = -5.12

	f32_ge_res_2         = []byte{0x7f, 0xdf, 0xdf, 0x7b}
	f32_ge_mat_2 float32 = -5.12

	f32_bw_res_2          = []byte{0x7f, 0x00, 0x00, 0x00}
	f32_bw_mat_2a float32 = -5.12
	f32_bw_mat_2b float32 = 10

	// extreme values
	f32_s3 = []float32{
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
	}
	f32_eq_res_3         = []byte{0x44, 0x44, 0x44, 0x44}
	f32_eq_mat_3 float32 = math.MaxFloat32

	f32_ne_res_3         = []byte{0xbb, 0xbb, 0xbb, 0xbb}
	f32_ne_mat_3 float32 = math.MaxFloat32

	f32_lt_res_3         = []byte{0xbb, 0xbb, 0xbb, 0xbb}
	f32_lt_mat_3 float32 = math.MaxFloat32

	f32_le_res_3         = []byte{0xff, 0xff, 0xff, 0xff}
	f32_le_mat_3 float32 = math.MaxFloat32

	f32_gt_res_3         = []byte{0x0, 0x0, 0x0, 0x0}
	f32_gt_mat_3 float32 = math.MaxFloat32

	f32_ge_res_3         = []byte{0x44, 0x44, 0x44, 0x44}
	f32_ge_mat_3 float32 = math.MaxFloat32

	f32_bw_res_3          = []byte{0x55, 0x55, 0x55, 0x55}
	f32_bw_mat_3a float32 = math.MaxFloat32 / 2
	f32_bw_mat_3b float32 = math.MaxFloat32

	// NaN/Inf values
	f32_s4 = []float32{
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
	}
	f32_eq_res_4         = []byte{0x0, 0x0, 0x0, 0x0}
	f32_eq_mat_4 float32 = float32(math.NaN())

	f32_ne_res_4         = []byte{0xff, 0xff, 0xff, 0xff}
	f32_ne_mat_4 float32 = float32(math.NaN())

	f32_lt_res_4         = []byte{0x0, 0x0, 0x0, 0x0}
	f32_lt_mat_4 float32 = float32(math.NaN())

	f32_le_res_4         = []byte{0x0, 0x0, 0x0, 0x0}
	f32_le_mat_4 float32 = float32(math.NaN())

	f32_gt_res_4         = []byte{0x0, 0x0, 0x0, 0x0}
	f32_gt_mat_4 float32 = float32(math.NaN())

	f32_ge_res_4         = []byte{0x0, 0x0, 0x0, 0x0}
	f32_ge_mat_4 float32 = float32(math.NaN())

	f32_bw_res_4          = []byte{0x0, 0x0, 0x0, 0x0}
	f32_bw_mat_4a float32 = float32(math.NaN())
	f32_bw_mat_4b float32 = float32(math.NaN())
)

// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - match, match2: are only copied to the resulting test case
//   - result: result for the given slice
//   - len: desired length of the test case
func mkF32(name string, src []float32, match, match2 float32, result []byte, length int) MatchTest[float32] {
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
	return MatchTest[float32]{
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
var Float32EqualCases = []MatchTest[float32]{
	{"l0", make([]float32, 0), f32_eq_mat_1, 0, []byte{}, 0},
	{"nil", nil, f32_eq_mat_1, 0, []byte{}, 0},
	mkF32("vec1", f32_s0, f32_eq_mat_0, 0, f32_eq_res_0, 32),
	mkF32("vec2", f32_s0, f32_eq_mat_0, 0, f32_eq_res_0, 64),
	mkF32("l32", f32_s1, f32_eq_mat_1, 0, f32_eq_res_1, 32),
	mkF32("l64", append(f32_s1, f32_s0...), f32_eq_mat_1, 0, append(f32_eq_res_1, f32_eq_res_0...), 64),
	mkF32("l128", append(f32_s1, f32_s0...), f32_eq_mat_1, 0, append(f32_eq_res_1, f32_eq_res_0...), 128),
	mkF32("l256", append(f32_s1, f32_s0...), f32_eq_mat_1, 0, append(f32_eq_res_1, f32_eq_res_0...), 256),
	mkF32("l255", append(f32_s1, f32_s0...), f32_eq_mat_1, 0, append(f32_eq_res_1, f32_eq_res_0...), 255),
	mkF32("l127", append(f32_s1, f32_s0...), f32_eq_mat_1, 0, append(f32_eq_res_1, f32_eq_res_0...), 127),
	mkF32("l63", f32_s1, f32_eq_mat_1, 0, f32_eq_res_1, 63),
	mkF32("l31", f32_s1, f32_eq_mat_1, 0, f32_eq_res_1, 31),
	mkF32("l23", f32_s1, f32_eq_mat_1, 0, f32_eq_res_1, 23),
	mkF32("l15", f32_s1, f32_eq_mat_1, 0, f32_eq_res_1, 15),
	mkF32("l7", f32_s1, f32_eq_mat_1, 0, f32_eq_res_1, 7),
	mkF32("neg128", f32_s2, f32_eq_mat_2, 0, f32_eq_res_2, 128),
	mkF32("neg32", f32_s2, f32_eq_mat_2, 0, f32_eq_res_2, 32),
	mkF32("neg31", f32_s2, f32_eq_mat_2, 0, f32_eq_res_2, 31),
	mkF32("ext128", f32_s3, f32_eq_mat_3, 0, f32_eq_res_3, 128),
	mkF32("ext32", f32_s3, f32_eq_mat_3, 0, f32_eq_res_3, 32),
	mkF32("ext31", f32_s3, f32_eq_mat_3, 0, f32_eq_res_3, 31),
	mkF32("nan128", f32_s4, f32_eq_mat_4, 0, f32_eq_res_4, 128),
	mkF32("nan32", f32_s4, f32_eq_mat_4, 0, f32_eq_res_4, 32),
	mkF32("nan31", f32_s4, f32_eq_mat_4, 0, f32_eq_res_4, 31),
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
var Float32NotEqualCases = []MatchTest[float32]{
	{"l0", make([]float32, 0), f32_ne_mat_1, 0, []byte{}, 0},
	{"nil", nil, f32_ne_mat_1, 0, []byte{}, 0},
	mkF32("vec1", f32_s0, f32_ne_mat_0, 0, f32_ne_res_0, 32),
	mkF32("vec2", f32_s0, f32_ne_mat_0, 0, f32_ne_res_0, 128),
	mkF32("l32", f32_s1, f32_ne_mat_1, 0, f32_ne_res_1, 32),
	mkF32("l64", append(f32_s1, f32_s0...), f32_ne_mat_1, 0, append(f32_ne_res_1, f32_ne_res_0...), 64),
	mkF32("l128", append(f32_s1, f32_s0...), f32_ne_mat_1, 0, append(f32_ne_res_1, f32_ne_res_0...), 128),
	mkF32("l256", append(f32_s1, f32_s0...), f32_ne_mat_1, 0, append(f32_ne_res_1, f32_ne_res_0...), 256),
	mkF32("l255", append(f32_s1, f32_s0...), f32_ne_mat_1, 0, append(f32_ne_res_1, f32_ne_res_0...), 255),
	mkF32("l127", append(f32_s1, f32_s0...), f32_ne_mat_1, 0, append(f32_ne_res_1, f32_ne_res_0...), 127),
	mkF32("l63", f32_s1, f32_ne_mat_1, 0, f32_ne_res_1, 63),
	mkF32("l31", f32_s1, f32_ne_mat_1, 0, f32_ne_res_1, 31),
	mkF32("l23", f32_s1, f32_ne_mat_1, 0, f32_ne_res_1, 23),
	mkF32("l15", f32_s1, f32_ne_mat_1, 0, f32_ne_res_1, 15),
	mkF32("l7", f32_s1, f32_ne_mat_1, 0, f32_ne_res_1, 7),
	mkF32("neg128", f32_s2, f32_ne_mat_2, 0, f32_ne_res_2, 128),
	mkF32("neg32", f32_s2, f32_ne_mat_2, 0, f32_ne_res_2, 32),
	mkF32("neg31", f32_s2, f32_ne_mat_2, 0, f32_ne_res_2, 31),
	mkF32("ext128", f32_s3, f32_ne_mat_3, 0, f32_ne_res_3, 128),
	mkF32("ext32", f32_s3, f32_ne_mat_3, 0, f32_ne_res_3, 32),
	mkF32("ext31", f32_s3, f32_ne_mat_3, 0, f32_ne_res_3, 31),
	mkF32("nan128", f32_s4, f32_ne_mat_4, 0, f32_ne_res_4, 128),
	mkF32("nan32", f32_s4, f32_ne_mat_4, 0, f32_ne_res_4, 32),
	mkF32("nan31", f32_s4, f32_ne_mat_4, 0, f32_ne_res_4, 31),
}

// -----------------------------------------------------------------------------
// Less Testcases
var Float32LessCases = []MatchTest[float32]{
	{"l0", make([]float32, 0), f32_lt_mat_1, 0, []byte{}, 0},
	{"nil", nil, f32_lt_mat_1, 0, []byte{}, 0},
	mkF32("vec1", f32_s0, f32_lt_mat_0, 0, f32_lt_res_0, 32),
	mkF32("vec2", f32_s0, f32_lt_mat_0, 0, f32_lt_res_0, 128),
	mkF32("l32", f32_s1, f32_lt_mat_1, 0, f32_lt_res_1, 32),
	mkF32("l64", append(f32_s1, f32_s0...), f32_lt_mat_1, 0, append(f32_lt_res_1, f32_lt_res_0...), 64),
	mkF32("l128", append(f32_s1, f32_s0...), f32_lt_mat_1, 0, append(f32_lt_res_1, f32_lt_res_0...), 128),
	mkF32("l256", append(f32_s1, f32_s0...), f32_lt_mat_1, 0, append(f32_lt_res_1, f32_lt_res_0...), 256),
	mkF32("l255", append(f32_s1, f32_s0...), f32_lt_mat_1, 0, append(f32_lt_res_1, f32_lt_res_0...), 255),
	mkF32("l127", append(f32_s1, f32_s0...), f32_lt_mat_1, 0, append(f32_lt_res_1, f32_lt_res_0...), 127),
	mkF32("l63", f32_s1, f32_lt_mat_1, 0, f32_lt_res_1, 63),
	mkF32("l31", f32_s1, f32_lt_mat_1, 0, f32_lt_res_1, 31),
	mkF32("l23", f32_s1, f32_lt_mat_1, 0, f32_lt_res_1, 23),
	mkF32("l15", f32_s1, f32_lt_mat_1, 0, f32_lt_res_1, 15),
	mkF32("l7", f32_s1, f32_lt_mat_1, 0, f32_lt_res_1, 7),
	mkF32("neg128", f32_s2, f32_lt_mat_2, 0, f32_lt_res_2, 128),
	mkF32("neg32", f32_s2, f32_lt_mat_2, 0, f32_lt_res_2, 32),
	mkF32("neg31", f32_s2, f32_lt_mat_2, 0, f32_lt_res_2, 31),
	mkF32("ext128", f32_s3, f32_lt_mat_3, 0, f32_lt_res_3, 128),
	mkF32("ext32", f32_s3, f32_lt_mat_3, 0, f32_lt_res_3, 32),
	mkF32("ext31", f32_s3, f32_lt_mat_3, 0, f32_lt_res_3, 31),
	mkF32("nan128", f32_s4, f32_lt_mat_4, 0, f32_lt_res_4, 128),
	mkF32("nan32", f32_s4, f32_lt_mat_4, 0, f32_lt_res_4, 32),
	mkF32("nan31", f32_s4, f32_lt_mat_4, 0, f32_lt_res_4, 31),
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
var Float32LessEqualCases = []MatchTest[float32]{
	{"l0", make([]float32, 0), f32_le_mat_1, 0, []byte{}, 0},
	{"nil", nil, f32_le_mat_1, 0, []byte{}, 0},
	mkF32("vec1", f32_s0, f32_le_mat_0, 0, f32_le_res_0, 32),
	mkF32("vec2", f32_s0, f32_le_mat_0, 0, f32_le_res_0, 128),
	mkF32("l32", f32_s1, f32_le_mat_1, 0, f32_le_res_1, 32),
	mkF32("l64", append(f32_s1, f32_s0...), f32_le_mat_1, 0, append(f32_le_res_1, f32_le_res_0...), 64),
	mkF32("l128", append(f32_s1, f32_s0...), f32_le_mat_1, 0, append(f32_le_res_1, f32_le_res_0...), 128),
	mkF32("l256", append(f32_s1, f32_s0...), f32_le_mat_1, 0, append(f32_le_res_1, f32_le_res_0...), 256),
	mkF32("l255", append(f32_s1, f32_s0...), f32_le_mat_1, 0, append(f32_le_res_1, f32_le_res_0...), 255),
	mkF32("l127", append(f32_s1, f32_s0...), f32_le_mat_1, 0, append(f32_le_res_1, f32_le_res_0...), 127),
	mkF32("l63", f32_s1, f32_le_mat_1, 0, f32_le_res_1, 63),
	mkF32("l31", f32_s1, f32_le_mat_1, 0, f32_le_res_1, 31),
	mkF32("l23", f32_s1, f32_le_mat_1, 0, f32_le_res_1, 23),
	mkF32("l15", f32_s1, f32_le_mat_1, 0, f32_le_res_1, 15),
	mkF32("l7", f32_s1, f32_le_mat_1, 0, f32_le_res_1, 7),
	mkF32("neg128", f32_s2, f32_le_mat_2, 0, f32_le_res_2, 128),
	mkF32("neg32", f32_s2, f32_le_mat_2, 0, f32_le_res_2, 32),
	mkF32("neg31", f32_s2, f32_le_mat_2, 0, f32_le_res_2, 31),
	mkF32("ext128", f32_s3, f32_le_mat_3, 0, f32_le_res_3, 128),
	mkF32("ext32", f32_s3, f32_le_mat_3, 0, f32_le_res_3, 32),
	mkF32("ext31", f32_s3, f32_le_mat_3, 0, f32_le_res_3, 31),
	mkF32("nan128", f32_s4, f32_le_mat_4, 0, f32_le_res_4, 128),
	mkF32("nan32", f32_s4, f32_le_mat_4, 0, f32_le_res_4, 32),
	mkF32("nan31", f32_s4, f32_le_mat_4, 0, f32_le_res_4, 31),
}

// -----------------------------------------------------------------------------
// Greater Testcases
var Float32GreaterCases = []MatchTest[float32]{
	{"l0", make([]float32, 0), f32_gt_mat_1, 0, []byte{}, 0},
	{"nil", nil, f32_gt_mat_1, 0, []byte{}, 0},
	mkF32("vec1", f32_s0, f32_gt_mat_0, 0, f32_gt_res_0, 32),
	mkF32("vec2", f32_s0, f32_gt_mat_0, 0, f32_gt_res_0, 128),
	mkF32("l32", f32_s1, f32_gt_mat_1, 0, f32_gt_res_1, 32),
	mkF32("l64", append(f32_s1, f32_s0...), f32_gt_mat_1, 0, append(f32_gt_res_1, f32_gt_res_0...), 64),
	mkF32("l128", append(f32_s1, f32_s0...), f32_gt_mat_1, 0, append(f32_gt_res_1, f32_gt_res_0...), 128),
	mkF32("l256", append(f32_s1, f32_s0...), f32_gt_mat_1, 0, append(f32_gt_res_1, f32_gt_res_0...), 256),
	mkF32("l255", append(f32_s1, f32_s0...), f32_gt_mat_1, 0, append(f32_gt_res_1, f32_gt_res_0...), 255),
	mkF32("l127", append(f32_s1, f32_s0...), f32_gt_mat_1, 0, append(f32_gt_res_1, f32_gt_res_0...), 127),
	mkF32("l63", f32_s1, f32_gt_mat_1, 0, f32_gt_res_1, 63),
	mkF32("l31", f32_s1, f32_gt_mat_1, 0, f32_gt_res_1, 31),
	mkF32("l23", f32_s1, f32_gt_mat_1, 0, f32_gt_res_1, 23),
	mkF32("l15", f32_s1, f32_gt_mat_1, 0, f32_gt_res_1, 15),
	mkF32("l7", f32_s1, f32_gt_mat_1, 0, f32_gt_res_1, 7),
	mkF32("neg128", f32_s2, f32_gt_mat_2, 0, f32_gt_res_2, 128),
	mkF32("neg32", f32_s2, f32_gt_mat_2, 0, f32_gt_res_2, 32),
	mkF32("neg31", f32_s2, f32_gt_mat_2, 0, f32_gt_res_2, 31),
	mkF32("ext128", f32_s3, f32_gt_mat_3, 0, f32_gt_res_3, 128),
	mkF32("ext32", f32_s3, f32_gt_mat_3, 0, f32_gt_res_3, 32),
	mkF32("ext31", f32_s3, f32_gt_mat_3, 0, f32_gt_res_3, 31),
	mkF32("nan128", f32_s4, f32_gt_mat_4, 0, f32_gt_res_4, 128),
	mkF32("nan32", f32_s4, f32_gt_mat_4, 0, f32_gt_res_4, 32),
	mkF32("nan31", f32_s4, f32_gt_mat_4, 0, f32_gt_res_4, 31),
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
var Float32GreaterEqualCases = []MatchTest[float32]{
	{"l0", make([]float32, 0), f32_ge_mat_1, 0, []byte{}, 0},
	{"nil", nil, f32_ge_mat_1, 0, []byte{}, 0},
	mkF32("vec1", f32_s0, f32_ge_mat_0, 0, f32_ge_res_0, 32),
	mkF32("vec2", f32_s0, f32_ge_mat_0, 0, f32_ge_res_0, 64),
	mkF32("l32", f32_s1, f32_ge_mat_1, 0, f32_ge_res_1, 32),
	mkF32("l64", append(f32_s1, f32_s0...), f32_ge_mat_1, 0, append(f32_ge_res_1, f32_ge_res_0...), 64),
	mkF32("l128", append(f32_s1, f32_s0...), f32_ge_mat_1, 0, append(f32_ge_res_1, f32_ge_res_0...), 128),
	mkF32("l256", append(f32_s1, f32_s0...), f32_ge_mat_1, 0, append(f32_ge_res_1, f32_ge_res_0...), 256),
	mkF32("l255", append(f32_s1, f32_s0...), f32_ge_mat_1, 0, append(f32_ge_res_1, f32_ge_res_0...), 255),
	mkF32("l127", append(f32_s1, f32_s0...), f32_ge_mat_1, 0, append(f32_ge_res_1, f32_ge_res_0...), 127),
	mkF32("l63", f32_s1, f32_ge_mat_1, 0, f32_ge_res_1, 63),
	mkF32("l31", f32_s1, f32_ge_mat_1, 0, f32_ge_res_1, 31),
	mkF32("l23", f32_s1, f32_ge_mat_1, 0, f32_ge_res_1, 23),
	mkF32("l15", f32_s1, f32_ge_mat_1, 0, f32_ge_res_1, 15),
	mkF32("l7", f32_s1, f32_ge_mat_1, 0, f32_ge_res_1, 7),
	mkF32("neg128", f32_s2, f32_ge_mat_2, 0, f32_ge_res_2, 128),
	mkF32("neg32", f32_s2, f32_ge_mat_2, 0, f32_ge_res_2, 32),
	mkF32("neg31", f32_s2, f32_ge_mat_2, 0, f32_ge_res_2, 31),
	mkF32("ext128", f32_s3, f32_ge_mat_3, 0, f32_ge_res_3, 128),
	mkF32("ext32", f32_s3, f32_ge_mat_3, 0, f32_ge_res_3, 32),
	mkF32("ext31", f32_s3, f32_ge_mat_3, 0, f32_ge_res_3, 31),
	mkF32("nan128", f32_s4, f32_ge_mat_4, 0, f32_ge_res_4, 128),
	mkF32("nan32", f32_s4, f32_ge_mat_4, 0, f32_ge_res_4, 32),
	mkF32("nan31", f32_s4, f32_ge_mat_4, 0, f32_ge_res_4, 31),
}

// -----------------------------------------------------------------------------
// Between Testcases
var Float32BetweenCases = []MatchTest[float32]{
	{"l0", make([]float32, 0), f32_bw_mat_1a, f32_bw_mat_1b, []byte{}, 0},
	{"nil", nil, f32_bw_mat_1a, f32_bw_mat_1b, []byte{}, 0},
	mkF32("vec1", f32_s0, f32_bw_mat_0a, f32_bw_mat_0b, f32_bw_res_0, 32),
	mkF32("vec2", f32_s0, f32_bw_mat_0a, f32_bw_mat_0b, f32_bw_res_0, 64),
	mkF32("l32", f32_s1, f32_bw_mat_1a, f32_bw_mat_1b, f32_bw_res_1, 32),
	mkF32("l64", append(f32_s1, f32_s0...), f32_bw_mat_1a, f32_bw_mat_1b, append(f32_bw_res_1, f32_bw_res_0...), 64),
	mkF32("l128", append(f32_s1, f32_s0...), f32_bw_mat_1a, f32_bw_mat_1b, append(f32_bw_res_1, f32_bw_res_0...), 128),
	mkF32("l256", append(f32_s1, f32_s0...), f32_bw_mat_1a, f32_bw_mat_1b, append(f32_bw_res_1, f32_bw_res_0...), 256),
	mkF32("l255", append(f32_s1, f32_s0...), f32_bw_mat_1a, f32_bw_mat_1b, append(f32_bw_res_1, f32_bw_res_0...), 255),
	mkF32("l127", append(f32_s1, f32_s0...), f32_bw_mat_1a, f32_bw_mat_1b, append(f32_bw_res_1, f32_bw_res_0...), 127),
	mkF32("l63", f32_s1, f32_bw_mat_1a, f32_bw_mat_1b, f32_bw_res_1, 63),
	mkF32("l31", f32_s1, f32_bw_mat_1a, f32_bw_mat_1b, f32_bw_res_1, 31),
	mkF32("l23", f32_s1, f32_bw_mat_1a, f32_bw_mat_1b, f32_bw_res_1, 23),
	mkF32("l15", f32_s1, f32_bw_mat_1a, f32_bw_mat_1b, f32_bw_res_1, 15),
	mkF32("l7", f32_s1, f32_bw_mat_1a, f32_bw_mat_1b, f32_bw_res_1, 7),
	mkF32("neg128", f32_s2, f32_bw_mat_2a, f32_bw_mat_2b, f32_bw_res_2, 128),
	mkF32("neg32", f32_s2, f32_bw_mat_2a, f32_bw_mat_2b, f32_bw_res_2, 32),
	mkF32("neg31", f32_s2, f32_bw_mat_2a, f32_bw_mat_2b, f32_bw_res_2, 31),
	mkF32("ext128", f32_s3, f32_bw_mat_3a, f32_bw_mat_3b, f32_bw_res_3, 128),
	mkF32("ext32", f32_s3, f32_bw_mat_3a, f32_bw_mat_3b, f32_bw_res_3, 32),
	mkF32("ext31", f32_s3, f32_bw_mat_3a, f32_bw_mat_3b, f32_bw_res_3, 31),
	mkF32("nan128", f32_s4, f32_bw_mat_4a, f32_bw_mat_4b, f32_bw_res_4, 128),
	mkF32("nan32", f32_s4, f32_bw_mat_4a, f32_bw_mat_4b, f32_bw_res_4, 32),
	mkF32("nan31", f32_s4, f32_bw_mat_4a, f32_bw_mat_4b, f32_bw_res_4, 31),
}
