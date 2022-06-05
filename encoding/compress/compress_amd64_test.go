// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package compress

import (
	"reflect"
	"testing"

	"blockwatch.cc/knoxdb/util"
)

// ------------ zzDecodeInt64 -----------------------------------------------------------------

func TestZzDecodeInt64AVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range zzDecodeInt64Cases {
		slice := make([]int64, len(c.slice))
		copy(slice, c.slice)
		zzDecodeInt64AVX2(slice)
		if got, want := len(slice), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(slice, c.result) {
			T.Errorf("%s: unexpected result %v, expected %v", c.name, slice, c.result)
		}
	}
}

func BenchmarkZzDecodeInt64AVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.l, 1)
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				zzDecodeInt64AVX2(a)
			}
		})
	}
}

// -------------- deltaDecodeInt64 ---------------------------------------------------------------

func TestDeltaDecodeInt64AVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range deltaDecodeInt64Cases {
		slice := make([]int64, len(c.slice))
		copy(slice, c.slice)
		deltaDecodeInt64AVX2(slice)
		if got, want := len(slice), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(slice, c.result) {
			T.Errorf("%s: unexpected result %v, expected %v", c.name, slice, c.result)
		}
	}
}

func BenchmarkDeltaDecodeInt64AVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.l, 1)
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				deltaDecodeInt64AVX2(a)
			}
		})
	}
}

// ---------------- zzDeltaDecodeInt64 -------------------------------------------------------------

func TestZzDeltaDecodeInt64AVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range zzDeltaDecodeInt64Cases {
		slice := make([]int64, len(c.slice))
		copy(slice, c.slice)
		zzDeltaDecodeInt64AVX2(slice)
		if got, want := len(slice), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(slice, c.result) {
			T.Errorf("%s: unexpected result %v, expected %v", c.name, slice, c.result)
		}
	}
}

func BenchmarkZzDeltaDecodeInt64AVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.l, 1)
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				zzDeltaDecodeInt64AVX2(a)
			}
		})
	}
}

func BenchmarkZzDeltaDecodeInt64AVX2Combine(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.l, 1)
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				zzDeltaDecodeInt64AVX2X(a)
			}
		})
	}
}
