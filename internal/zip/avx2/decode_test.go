// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package avx2

import (
	"math/rand"
	"reflect"
	"testing"

	"golang.org/x/exp/slices"

	"blockwatch.cc/knoxdb/internal/zip/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	zzDeltaEncodeUint64Cases = tests.ZzDeltaEncodeUint64Cases
	zzDeltaEncodeUint32Cases = tests.ZzDeltaEncodeUint32Cases
	zzDeltaEncodeUint16Cases = tests.ZzDeltaEncodeUint16Cases
	zzDeltaEncodeUint8Cases  = tests.ZzDeltaEncodeUint8Cases

	benchmarkSizes = tests.BenchmarkSizes
	randInt64Slice = tests.RandInt64Slice
	randInt32Slice = tests.RandInt32Slice
	randInt16Slice = tests.RandInt16Slice
	randInt8Slice  = tests.RandInt8Slice
	Int64Size      = tests.Int64Size
	Int32Size      = tests.Int32Size
	Int16Size      = tests.Int16Size
	Int8Size       = tests.Int8Size
)

// ---------------- zzDeltaDecodeInt64 -------------------------------------------------------------

func TestZzDeltaDecodeInt64AVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range zzDeltaEncodeUint64Cases {
		slice := slices.Clone(c.Result)
		zzDeltaDecodeInt64AVX2(slice)
		if got, want := len(slice), len(c.Slice); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(slice, c.Slice) {
			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
		}
	}
}

func BenchmarkZzDeltaDecodeInt64AVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.L, 1)
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int64Size))
			for i := 0; i < B.N; i++ {
				zzDeltaDecodeInt64AVX2(a)
			}
		})
	}
}

// ---------------- zzDeltaDecodeInt32 -------------------------------------------------------------

func TestZzDeltaDecodeInt32AVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range zzDeltaEncodeUint32Cases {
		slice := slices.Clone(c.Result)
		zzDeltaDecodeInt32AVX2(slice)
		if got, want := len(slice), len(c.Slice); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(slice, c.Slice) {
			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
		}
	}
}

func BenchmarkZzDeltaDecodeInt32AVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randInt32Slice(n.L, 1)
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int32Size))
			for i := 0; i < B.N; i++ {
				zzDeltaDecodeInt32AVX2(a)
			}
		})
	}
}

// ---------------- zzDeltaDecodeInt16 -------------------------------------------------------------

func TestZzDeltaDecodeInt16AVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range zzDeltaEncodeUint16Cases {
		slice := slices.Clone(c.Result)
		zzDeltaDecodeInt16AVX2(slice)
		if got, want := len(slice), len(c.Slice); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(slice, c.Slice) {
			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
		}
	}
}

func BenchmarkZzDeltaDecodeInt16AVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randInt16Slice(n.L, 1)
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int16Size))
			for i := 0; i < B.N; i++ {
				zzDeltaDecodeInt16AVX2(a)
			}
		})
	}
}

// ---------------- zzDeltaDecodeInt8 -------------------------------------------------------------

func TestZzDeltaDecodeInt8AVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range zzDeltaEncodeUint8Cases {
		slice := slices.Clone(c.Result)
		zzDeltaDecodeInt8AVX2(slice)
		if got, want := len(slice), len(c.Slice); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(slice, c.Slice) {
			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
		}
	}
}

func BenchmarkZzDeltaDecodeInt8AVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randInt8Slice(n.L, 1)
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int8Size))
			for i := 0; i < B.N; i++ {
				zzDeltaDecodeInt8AVX2(a)
			}
		})
	}
}

// ------------ deltaDecodeTime -----------------------------------------------------------------

func BenchmarkDeltaDecodeTimeAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	mod := uint64(1000000000)
	for _, n := range benchmarkSizes {
		a := make([]uint64, n.L)
		for i := 0; i < n.L; i++ {
			a[i] = uint64(rand.Intn(10000))
		}
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int64Size))
			for i := 0; i < B.N; i++ {
				deltaDecodeTimeAVX2(a, mod)
			}
		})
	}
}

// ------------ zzDeltaDecodeTime -----------------------------------------------------------------

func BenchmarkZzDeltaDecodeTimeAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	mod := uint64(1000000000)
	for _, n := range benchmarkSizes {
		a := make([]uint64, n.L)
		for i := 0; i < n.L; i++ {
			a[i] = uint64(rand.Intn(10000))
		}
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int64Size))
			for i := 0; i < B.N; i++ {
				zzDeltaDecodeTimeAVX2(a, mod)
			}
		})
	}
}

// ------------ zzDecodeInt64 -----------------------------------------------------------------

// func TestZzDecodeInt64AVX2(T *testing.T) {
// 	if !util.UseAVX2 {
// 		T.SkipNow()
// 	}
// 	for _, c := range zzDeltaEncodeUint64Cases {
// 		slice := slices.Clone(c.Result)
// 		zzDecodeInt64AVX2(slice)
// 		if got, want := len(slice), len(c.Slice); got != want {
// 			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
// 		}
// 		if !reflect.DeepEqual(slice, c.Slice) {
// 			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
// 		}
// 	}
// }

// func BenchmarkZzDecodeInt64AVX2(B *testing.B) {
// 	if !util.UseAVX2 {
// 		B.SkipNow()
// 	}
// 	for _, n := range benchmarkSizes {
// 		a := randInt64Slice(n.L, 1)
// 		B.Run(n.Name, func(B *testing.B) {
// 			B.SetBytes(int64(n.L * Int64Size))
// 			for i := 0; i < B.N; i++ {
// 				zzDecodeInt64AVX2(a)
// 			}
// 		})
// 	}
// }

// -------------- deltaDecodeInt64 ---------------------------------------------------------------

// func TestDeltaDecodeInt64AVX2(T *testing.T) {
// 	if !util.UseAVX2 {
// 		T.SkipNow()
// 	}
// 	for _, c := range zzDeltaEncodeUint64Cases {
// 		slice := slices.Clone(c.Result)
// 		deltaDecodeInt64AVX2(slice)
// 		if got, want := len(slice), len(c.Slice); got != want {
// 			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
// 		}
// 		if !reflect.DeepEqual(slice, c.Slice) {
// 			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
// 		}
// 	}
// }

// func BenchmarkDeltaDecodeInt64AVX2(B *testing.B) {
// 	if !util.UseAVX2 {
// 		B.SkipNow()
// 	}
// 	for _, n := range benchmarkSizes {
// 		a := randInt64Slice(n.L, 1)
// 		B.Run(n.Name, func(B *testing.B) {
// 			B.SetBytes(int64(n.L * Int64Size))
// 			for i := 0; i < B.N; i++ {
// 				deltaDecodeInt64AVX2(a)
// 			}
// 		})
// 	}
// }

// -------------- deltaDecodeInt64 ---------------------------------------------------------------

// func TestDeltaDecodeInt32AVX2(T *testing.T) {
// 	if !util.UseAVX2 {
// 		T.SkipNow()
// 	}
// 	for _, c := range zzDeltaEncodeUint32Cases {
// 		slice := slices.Clone(c.Result)
// 		deltaDecodeInt32AVX2(slice)
// 		if got, want := len(slice), len(c.Slicd); got != want {
// 			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
// 		}
// 		if !reflect.DeepEqual(slice, c.Slice) {
// 			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
// 		}
// 	}
// }

// func BenchmarkDeltaDecodeInt32AVX2(B *testing.B) {
// 	if !util.UseAVX2 {
// 		B.SkipNow()
// 	}
// 	for _, n := range benchmarkSizes {
// 		a := randInt32Slice(n.L, 1)
// 		B.Run(n.Name, func(B *testing.B) {
// 			B.SetBytes(int64(n.L * Int32Size))
// 			for i := 0; i < B.N; i++ {
// 				deltaDecodeInt32AVX2(a)
// 			}
// 		})
// 	}
// }
