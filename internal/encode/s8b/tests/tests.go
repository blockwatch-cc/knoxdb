// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"fmt"
	"slices"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

type EncodeFunc[T types.Integer] func([]byte, []T, T, T) ([]byte, error)
type DecodeFunc[T types.Integer] func([]T, []byte, T) (int, error)
type CompareFunc func([]byte, uint64, *bitset.Bitset)
type CompareFunc2 func([]byte, uint64, uint64, *bitset.Bitset)

type TestCase[T types.Integer] struct {
	Name string
	Data []T
	Gen  func() []T
	Err  bool
}

func MakeTests[T types.Integer]() []TestCase[T] {
	width := unsafe.Sizeof(T(0))
	tests := []TestCase[T]{
		{Name: "nil", Data: nil},
		{Name: "empty", Data: []T{}},
		{Name: "mixed sizes", Data: []T{7, 6, 127, 4, 3, 2, 1}},
		{Name: "240 ones", Gen: ones[T](240)},
		{Name: "240 ones plus 5", Gen: func() []T {
			in := ones[T](240)()
			in[127] = 5
			return in
		}},
		{Name: "127 ones plus 5", Gen: func() []T {
			in := ones[T](128)()
			in[127] = 5
			return in
		}},
		{Name: "256 ones plus 5", Gen: func() []T {
			in := ones[T](256)()
			in[255] = 5
			return in
		}},
		{Name: "1 bit", Gen: bits[T](128, 1)},
		{Name: "2 bits", Gen: bits[T](128, 2)},
		{Name: "3 bits", Gen: bits[T](128, 3)},
		{Name: "4 bits", Gen: bits[T](128, 4)},
		{Name: "5 bits", Gen: bits[T](128, 5)},
		{Name: "6 bits", Gen: bits[T](128, 6)},
		{Name: "7 bits", Gen: bits[T](128, 7)},
		{Name: "8 bits", Gen: bits[T](128, 8)},
		{Name: "67", Data: slices.Repeat([]T{67}, 640)},
	}
	combi := TestCase[T]{
		Name: "combination",
		Gen: combine[T](
			bits[T](128, 1),
			bits[T](128, 2),
			bits[T](128, 3),
			bits[T](128, 4),
			bits[T](128, 5),
			bits[T](128, 6),
			bits[T](128, 7),
			bits[T](128, 8),
		)}

	if width > 1 {
		tests = append(tests, []TestCase[T]{
			{Name: "10 bits", Gen: bits[T](128, 10)},
			{Name: "12 bits", Gen: bits[T](128, 12)},
			{Name: "15 bits", Gen: bits[T](128, 15)},
		}...)
		combi.Gen = combine[T](
			bits[T](128, 1),
			bits[T](128, 2),
			bits[T](128, 3),
			bits[T](128, 4),
			bits[T](128, 5),
			bits[T](128, 6),
			bits[T](128, 7),
			bits[T](128, 8),
			bits[T](128, 10),
			bits[T](128, 12),
			bits[T](128, 15),
			bits[T](128, 16),
		)
	}

	if width > 2 {
		tests = append(tests, []TestCase[T]{
			{Name: "20 bits", Gen: bits[T](128, 20)},
			{Name: "30 bits", Gen: bits[T](128, 30)},
			{Name: "32 bits", Gen: bits[T](128, 32)},
		}...)
		combi.Gen = combine[T](
			bits[T](128, 1),
			bits[T](128, 2),
			bits[T](128, 3),
			bits[T](128, 4),
			bits[T](128, 5),
			bits[T](128, 6),
			bits[T](128, 7),
			bits[T](128, 8),
			bits[T](128, 10),
			bits[T](128, 12),
			bits[T](128, 15),
			bits[T](128, 20),
			bits[T](128, 30),
			bits[T](128, 32),
		)
	}
	if width > 4 {
		tests = append(tests, []TestCase[T]{
			{Name: "60 bits", Gen: bits[T](120, 60)},
			{
				Name: "too big",
				Data: util.ReinterpretSlice[uint64, T]([]uint64{7, 6, 2<<61 - 1, 4, 3, 2, 1}),
				Err:  true,
			},
		}...)

		combi.Gen = combine[T](
			bits[T](128, 1),
			bits[T](128, 2),
			bits[T](128, 3),
			bits[T](128, 4),
			bits[T](128, 5),
			bits[T](128, 6),
			bits[T](128, 7),
			bits[T](128, 8),
			bits[T](128, 10),
			bits[T](128, 12),
			bits[T](128, 15),
			bits[T](128, 20),
			bits[T](128, 30),
			bits[T](128, 60),
		)
	}

	return append(tests, combi)
}

func EncodeTest[T types.Integer](t *testing.T, enc EncodeFunc[T], dec DecodeFunc[T]) {
	for _, c := range MakeTests[T]() {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			in := c.Data
			if c.Gen != nil {
				in = c.Gen()
			}
			var minv, maxv T
			if len(in) > 0 {
				minv, maxv = slices.Min(in), slices.Max(in)
			}
			buf := make([]byte, len(in)*8)

			// encode unsigned tests without min-FOR to be compatible with
			// testcase data for testing all selectors
			if !types.IsSigned[T]() {
				minv = 0
			}
			buf, err := enc(buf, slices.Clone(in), minv, maxv)
			if c.Err {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}

			dst := make([]T, len(in))
			n, err := dec(dst, buf, minv)
			require.NoError(t, err)

			if len(in) > 0 {
				require.Equal(t, in, dst[:n])
			}
		})
	}
}

type CompareCase[T types.Integer] struct {
	Name       string
	Gen        func(int) []T
	SignedOnly bool
}

func convertSlice[T types.Number](src []int) []T {
	dst := make([]T, len(src))
	for i, v := range src {
		dst[i] = T(v)
	}
	return dst
}

func MakeCompareCases[T types.Integer]() []CompareCase[T] {
	return []CompareCase[T]{
		// test each selector
		{"zero", func(n int) []T { return tests.GenConst[T](n, 0) }, false},
		{"one", func(n int) []T { return tests.GenConst[T](n, 1) }, false},
		{"s2", func(n int) []T { return tests.GenRndBits[T](n, 1) }, false},
		{"s3", func(n int) []T { return tests.GenRndBits[T](n, 2) }, false},
		{"s4", func(n int) []T { return tests.GenRndBits[T](n, 3) }, false},
		{"s5", func(n int) []T { return tests.GenRndBits[T](n, 4) }, false},
		{"s6", func(n int) []T { return tests.GenRndBits[T](n, 5) }, false},
		{"s7", func(n int) []T { return tests.GenRndBits[T](n, 6) }, false},
		{"s8", func(n int) []T { return tests.GenRndBits[T](n, 7) }, false},
		{"s9", func(n int) []T { return tests.GenRndBits[T](n, 8) }, false},
		{"s10", func(n int) []T { return tests.GenRndBits[T](n, 10) }, false},
		{"s11", func(n int) []T { return tests.GenRndBits[T](n, 12) }, false},
		{"s12", func(n int) []T { return tests.GenRndBits[T](n, 15) }, false},
		{"s13", func(n int) []T { return tests.GenRndBits[T](n, 20) }, false},
		{"s14", func(n int) []T { return tests.GenRndBits[T](n, 30) }, false},
		{"s15", func(n int) []T { return tests.GenRndBits[T](n, 60) }, false},
		// random tests
		{"rnd", tests.GenRnd[T], false},
		// custom tests
		{"seq", func(n int) []T { return tests.GenSeq[T](n, 0) }, false},
		{"256", func(n int) []T { return tests.GenRndBits[T](n, 8) }, false},
		{"adj-bug", func(n int) []T {
			vals := convertSlice[T]([]int{
				22, 17, 201, 169, 41, 85, 178, 137, 222, 109, 73, 37, 3, 121,
				183, 7, 16, 181, 138, 191, 187, 113, 94, 124, 142, 127, 132,
				195, 81, 228, 247, 251, 58, 10, 32, 63, 67, 164, 96, 253, 173,
				172, 54, 53, 224, 107, 100, 133, 45, 251, 7, 213, 120, 184, 212,
				177, 38, 24, 1, 118, 81, 81, 234, 70, 213, 146, 173, 248, 96,
				185, 176, 4, 33, 192, 50, 176, 91, 180, 29, 114, 100, 178, 238,
				182, 218, 11, 33, 194, 192, 113, 52, 10, 56, 237, 236, 97, 79,
				203, 128, 87, 47, 162, 248, 239, 101, 138, 68, 65, 194, 96, 118,
				66, 141, 196, 76, 169, 185, 97, 157, 3, 124, 207, 133, 193, 233,
				221, 232, 235, 252, 185, 205, 217, 108, 87, 241, 190, 202, 93,
				57, 111, 123, 38, 69, 193, 18, 216, 121, 20, 5, 160, 191, 188,
				149, 4, 58, 218, 136, 155, 58, 134, 111, 14, 19, 105, 74, 89,
				112, 235, 13, 1, 214, 31, 216, 144, 110, 57, 111, 215, 249, 23,
				138, 103, 47, 66, 73, 141, 147, 174, 143, 172, 235, 85, 93, 114,
				161, 188, 133, 191, 186, 13, 219, 62, 28, 130, 89, 107, 144,
				22, 21, 240, 63, 40, 1, 203, 121, 39, 69, 250, 125, 43, 203,
				153, 135, 138, 146, 111, 152, 156, 245, 205, 177, 185, 158, 235,
				46, 122, 130, 73, 171, 99, 38, 148, 214, 121, 35, 25, 100, 181,
				155, 209, 172, 243, 107, 121, 201, 135, 55, 133, 215, 200, 157,
				126, 185, 236, 63, 197, 124, 159, 33, 182, 60, 35, 90, 66, 217,
				137, 130, 117, 6, 88, 166, 243, 109, 195, 152, 104, 101, 157, 25,
				208, 41, 46, 236, 0, 160, 234, 253, 30, 249, 133, 3, 6, 1, 2, 31,
				124, 245, 59, 202, 36, 59, 221, 18, 29, 163, 121, 222, 218, 9, 16,
				254, 1, 161, 155, 160, 71, 130, 85, 22, 181, 201, 225, 5, 191, 122,
				13, 37, 46, 85, 58, 114, 89, 134, 137, 233, 20, 132, 66, 77, 69,
				185, 133, 208, 212, 201, 27, 49, 90, 63, 24, 218, 137, 127, 44, 96,
				238, 13, 82, 79, 23, 210, 112, 176, 229, 38, 47, 226, 52, 166, 73,
				234, 17, 241, 38, 97, 44, 175, 240, 54, 181, 237, 138, 52, 246,
				137, 117, 50, 37, 140, 141, 129, 127, 9, 76, 116, 158, 185, 249,
				252, 34, 228, 7, 168, 242, 82, 175, 115, 233, 16, 244, 54, 224, 54,
				221, 113, 94, 220, 248, 130, 153, 133, 187, 35, 10, 174, 37, 188,
				191, 163, 56, 183, 70, 107, 249, 122, 191, 184, 89, 240, 9, 137,
				164, 62, 192, 58, 225, 5, 34, 249, 19, 230, 91, 180, 43, 13, 159,
				104, 172, 95, 24, 21, 1, 212, 12, 186, 169, 205, 123, 199, 253,
				114, 81, 44, 146, 158, 171, 244, 54, 84, 79, 151, 40, 191, 122, 116,
				36, 81, 79, 105, 244, 136, 125, 75, 243, 180, 36, 46, 201, 231, 158,
				64, 41, 209, 138, 30, 175, 84, 162, 55, 170, 32, 220, 76, 123, 182,
				200, 44, 10, 97, 89, 32, 72, 172, 247, 251, 1, 1, 44, 183, 127, 0,
				251, 25, 73, 109, 180, 245, 232, 23, 79, 15, 240, 151, 121, 106, 170,
				97, 142, 177, 94, 63, 21, 6, 39, 135, 21, 250, 27, 96, 65, 196, 44,
				200, 59, 89, 114, 166, 219, 171, 204, 163, 128, 248, 193, 141, 146,
				179, 174, 96, 103, 73, 40, 95, 66, 22, 44, 134, 24, 70, 235, 31, 236,
				230, 209, 127, 135, 231, 71, 147, 32, 122, 31, 76, 32, 63, 19, 73, 1,
				123, 186, 159, 227, 214, 54, 241, 60, 61, 172, 37, 254, 151, 114, 74,
				72, 52, 166, 246, 85, 106, 139, 151, 95, 107, 38, 237, 136, 67, 214,
				174, 16, 96, 95, 14, 84, 60, 159, 88, 93, 102, 43, 136, 135, 110, 117,
				58, 88, 14, 12, 235, 107, 33, 210, 229, 72, 233, 109, 37, 32, 29, 119,
				158, 170, 25, 204, 223, 9, 79, 69, 45, 230, 13, 206, 212, 18, 91, 99,
				196, 63, 131, 90, 131, 21, 48, 179, 245, 198, 128, 182, 90, 49, 119,
				196, 132, 6, 40, 198, 168, 251, 173, 230, 80, 95, 217, 53, 4, 150, 165,
				195, 154, 194, 48, 42, 201, 84, 166, 67, 186, 232, 97, 74, 37, 139,
				216, 235, 203, 136, 144, 206, 60, 107, 198, 104, 203, 140, 178, 3, 40,
				106, 184, 110, 83, 62, 31, 49, 219, 77, 217, 227, 216, 173, 109, 169,
				207, 216, 201, 90, 204, 246, 125, 43, 164, 142, 184, 65, 202, 71, 126,
				184, 66, 20, 0, 159, 17, 44, 112, 14, 79, 195, 247, 40, 222, 226, 15,
				177, 3, 149, 81, 14, 112, 210, 241, 119, 180, 3, 179, 11, 98, 220, 29,
				172, 51, 184, 46, 79, 173, 5, 240, 51, 98, 64, 115, 104, 31, 1, 63,
				235, 3, 31, 152, 171, 196, 236, 216, 203, 80, 58, 166, 82, 19, 69, 104,
				80, 102, 101, 48, 215, 18, 6, 99, 205, 152, 145, 13, 132, 238, 2, 129,
				230, 238, 179, 36, 6, 155, 157, 228, 234, 54, 36, 217, 163, 210, 84, 77,
				21, 2, 116, 82, 105, 203, 227, 59, 118, 105, 89, 251, 136, 210, 163, 207,
				168, 27, 196, 34, 164, 164, 222, 121, 200, 239, 26, 62, 36, 15, 185, 12,
				73, 205, 159, 21, 238, 161, 136, 146, 241, 150, 132, 213, 36, 97, 44,
				88, 78, 200, 213, 81, 195, 99, 200, 73, 249, 69, 77, 146, 219, 193, 67,
				24, 209, 87, 117, 232, 140, 244, 239, 105, 83, 105, 210, 25, 171, 28, 66,
				106, 61, 53, 145, 156, 69, 237, 110, 70, 14, 179, 203, 44, 210, 73, 189,
				236, 120, 239, 98, 134, 74, 72, 105, 80, 5, 192, 116, 31, 192, 83, 137,
				157, 127, 113, 201, 109, 20, 11, 208, 170, 49, 187, 163, 247, 240, 101,
				227, 80, 24, 111, 250, 61, 126, 251, 49, 79, 22, 4, 250, 134, 9,
			})
			return vals[:n]
		}, false},
		{"sign-bug", func(n int) []T {
			vals := convertSlice[T]([]int{
				0, -3, -126, -89, 113, -5, 22, 69, -127, 123, -106, -64, -65, 83, -84, 107, -11, 118, 91, 29, -58, 69, 123, -8, -46, -59, 120, -91, -93, -7, 37, 41, -108, 102, 95, 126, 24, -107, 31, 95, -90, -56, 30, 106, 104, 31, 60, -114, 53, 33, 29, -8, -59, 67, -114, 87, 90, 63, 9, 24, 80, 2, -32, 7, 46, -96, -106, 34, -2, -98, 40, -72, -5, -96, 45, -28, -107, -100, 46, -98, -64, -88, -64, -121, -106, -28, 3, 42, 116, 90, -18, -53, -110, 30, 89, -29, -79, 53, -69, 2, 107, 114, 79, -47, 98, -89, -128, -24, 71, -16, -94, 33, 9, -97, -91, -79, -116, -45, -40, -128,
			})
			for len(vals) < n {
				vals = append(vals, vals...)
			}
			return vals[:n]
		}, true},
	}
}

var CompareSizes = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 30, 60, 120, 240, 1024}

func CompareTest[T types.Integer](t *testing.T, enc EncodeFunc[T], dec DecodeFunc[uint64], cmp CompareFunc, mode types.FilterMode) {
	for _, sz := range CompareSizes {
		for _, c := range MakeCompareCases[T]() {
			t.Run(fmt.Sprintf("%T/%s/sz_%d", T(0), c.Name, sz), func(t *testing.T) {
				if c.SignedOnly && !types.IsSigned[T]() {
					t.Skip()
				}
				vals := c.Gen(sz)
				minv, maxv := slices.Min(vals), slices.Max(vals)
				buf, err := enc(make([]byte, sz*8), vals, minv, maxv) // with MinFOR
				require.NoError(t, err)
				bits := bitset.New(sz)
				dst := make([]uint64, sz)
				dec(dst, buf, 0) // sic! we manually add minv below

				// value exists
				val := vals[len(vals)/2]
				cmp(buf, uint64(val)-uint64(minv), bits)
				// t.Logf("Exist Min=%d Max=%d", minv, maxv)
				// t.Log(hex.Dump(bits.Bytes()))
				// t.Logf("Orig: %v", vals)
				// t.Logf("Conv: %v", dst)
				ensureBits(t, vals, val, val, bits, mode)
				bits.Zero()
				require.Equal(t, 0, bits.Count(), "cleared")

				// skip test if value would wrap around
				if maxv < types.MaxVal[T]() {
					// value over bounds
					over := maxv + 1
					cmp(buf, uint64(over)-uint64(minv), bits)
					// t.Logf("Over Min=%d Max=%d Cmp=%d", minv, maxv, over)
					// t.Log(hex.Dump(bits.Bytes()))
					// t.Logf("Orig: %v", vals)
					// t.Logf("Conv: %v", dst)
					ensureBits(t, vals, over, over, bits, mode)
					bits.Zero()
					require.Equal(t, 0, bits.Count(), "cleared")
				}

				// Testcase disabled as it is expected to always fail due to min-FOR
				// Note: when cmp value is < minv then minFOR wraps around, this
				// case must be checked by callers to compare funcs
				// if minv > types.MinVal[T]() {
				// 	// value under bounds
				// 	under := minv - 1
				// 	cmp(buf, uint64(under)-uint64(minv), bits)
				// 	t.Logf("Under Min=%d Max=%d Cmp=%d", minv, maxv, under)
				// 	t.Log(hex.Dump(bits.Bytes()))
				// 	t.Logf("Orig: %v", vals)
				// 	t.Logf("Conv: %v", dst)
				// 	ensureBits(t, vals, under, under, bits, mode)
				// 	bits.Zero()
				// 	require.Equal(t, 0, bits.Count(), "cleared")
				// }
			})
		}
	}
}

// range mode specific test with 2 values
func CompareTest2[T types.Integer](t *testing.T, enc EncodeFunc[T], dec DecodeFunc[uint64], cmp CompareFunc2, mode types.FilterMode) {
	for _, sz := range CompareSizes {
		for _, c := range MakeCompareCases[T]() {
			t.Run(fmt.Sprintf("%T/%s/sz_%d", T(0), c.Name, sz), func(t *testing.T) {
				if c.SignedOnly && !types.IsSigned[T]() {
					t.Skip()
				}
				vals := c.Gen(sz)
				minv, maxv := slices.Min(vals), slices.Max(vals)
				buf, err := enc(make([]byte, sz*8), vals, minv, maxv) // with MinFOR
				require.NoError(t, err)
				bits := bitset.New(sz)
				dst := make([]uint64, sz)
				dec(dst, buf, 0) // sic! we manually add minv below

				// single value
				val := vals[len(vals)/2]
				cmp(buf, uint64(val)-uint64(minv), uint64(val)-uint64(minv), bits)
				ensureBits(t, vals, val, val, bits, mode)
				bits.Zero()
				require.Equal(t, 0, bits.Count(), "cleared")

				// full range
				cmp(buf, uint64(minv)-uint64(minv), uint64(maxv)-uint64(minv), bits)
				// t.Logf("Full Min=%d Max=%d", minv, maxv)
				// t.Log(hex.Dump(bits.Bytes()))
				// t.Logf("Orig: %v", vals)
				// t.Logf("Conv: %v", dst)
				ensureBits(t, vals, minv, maxv, bits, mode)
				bits.Zero()
				require.Equal(t, 0, bits.Count(), "cleared")

				// partial range
				from, to := max(val/2, minv+1), min(val*2, maxv-1)
				if from > to {
					from, to = to, from
				}
				// skip test if values would wrap around
				if from > minv && to < maxv {
					cmp(buf, uint64(from)-uint64(minv), uint64(to)-uint64(minv), bits)
					// t.Logf("Partial Min=%d Max=%d From=%d To=%d", minv, maxv, from, to)
					// t.Log(hex.Dump(bits.Bytes()))
					// t.Logf("Orig: %v", vals)
					// t.Logf("Conv: %v", dst)
					ensureBits(t, vals, from, to, bits, mode)
					bits.Zero()
					require.Equal(t, 0, bits.Count(), "cleared")
				}

				// skip test if value would wrap around
				if maxv < types.MaxVal[T]()-1 {
					// out of bounds (over)
					cmp(buf, uint64(maxv+1)-uint64(minv), uint64(maxv+2)-uint64(minv), bits)
					// t.Logf("Over Min=%d Max=%d From=%d To=%d", minv, maxv, maxv+1, maxv+2)
					// t.Log(hex.Dump(bits.Bytes()))
					// t.Logf("Orig: %v", vals)
					// t.Logf("Conv: %v", dst)
					ensureBits(t, vals, maxv+1, maxv+2, bits, mode)
					bits.Zero()
					require.Equal(t, 0, bits.Count(), "cleared")
				}

				// skip test if value would wrap around
				if minv > types.MinVal[T]()+2 {
					// out of bounds (under)
					cmp(buf, uint64(minv-2)-uint64(minv), uint64(minv-1)-uint64(minv), bits)
					// t.Logf("Under Min=%d Max=%d From=%d To=%d", minv, maxv, minv+2, minv+1)
					// t.Log(hex.Dump(bits.Bytes()))
					// t.Logf("Orig: %v", vals)
					// t.Logf("Conv: %v", dst)
					ensureBits(t, vals, minv-2, minv-1, bits, mode)
					bits.Zero()
					require.Equal(t, 0, bits.Count(), "cleared")
				}
			})
		}
	}
}

func ensureBits[T types.Integer](t *testing.T, vals []T, val, val2 T, bits *bitset.Bitset, mode types.FilterMode) {
	// if !testing.Short() {
	// 	for i, v := range vals {
	// 		t.Logf("Val %d: %d", i, v)
	// 	}
	// 	t.Logf("Bitset %x", bits.Bytes())
	// }
	minv, maxv := slices.Min(vals), slices.Max(vals)
	switch mode {
	case types.FilterModeEqual:
		for i, v := range vals {
			require.Equal(t, v == val, bits.Contains(i), "bit=%d val=%d %s %d min=%d max=%d",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeNotEqual:
		for i, v := range vals {
			require.Equal(t, v != val, bits.Contains(i), "bit=%d val=%d %s %d min=%d max=%d",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeLt:
		for i, v := range vals {
			require.Equal(t, v < val, bits.Contains(i), "bit=%d val=%d %s %d min=%d max=%d",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeLe:
		for i, v := range vals {
			require.Equal(t, v <= val, bits.Contains(i), "bit=%d val=%d %s %d min=%d max=%d",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeGt:
		for i, v := range vals {
			require.Equal(t, v > val, bits.Contains(i), "bit=%d val=%d %s %d min=%d max=%d",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeGe:
		for i, v := range vals {
			require.Equal(t, v >= val, bits.Contains(i), "bit=%d val=%d %s %d min=%d max=%d",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeRange:
		for i, v := range vals {
			require.Equal(t, v >= val && v <= val2, bits.Contains(i), "bit=%d val=%d %s [%d,%d] min=%d max=%d",
				i, v, mode, val, val2, minv, maxv)
		}
	}
}
