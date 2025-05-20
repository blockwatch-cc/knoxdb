// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx2

import (
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/internal/cpu"
	"blockwatch.cc/knoxdb/internal/tests"
	ztests "blockwatch.cc/knoxdb/internal/zip/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

var (
	zzDeltaEncodeUint64Cases = ztests.ZzDeltaEncodeUint64Cases
	zzDeltaEncodeUint32Cases = ztests.ZzDeltaEncodeUint32Cases
	zzDeltaEncodeUint16Cases = ztests.ZzDeltaEncodeUint16Cases
	zzDeltaEncodeUint8Cases  = ztests.ZzDeltaEncodeUint8Cases
)

// ---------------- zzDeltaDecodeInt64 -------------------------------------------------------------

func TestZzDeltaDecodeInt64AVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range zzDeltaEncodeUint64Cases {
		slice := slices.Clone(c.Result)
		zzDeltaDecodeInt64AVX2(slice)
		require.Len(t, slice, len(c.Slice), "len")
		require.Equal(t, slice, c.Slice)
	}
}

func BenchmarkZzDeltaDecodeInt64AVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	for _, c := range tests.BenchmarkSizes {
		a := util.RandInts[int64](c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				zzDeltaDecodeInt64AVX2(a)
			}
		})
	}
}

// ---------------- zzDeltaDecodeInt32 -------------------------------------------------------------

func TestZzDeltaDecodeInt32AVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range zzDeltaEncodeUint32Cases {
		slice := slices.Clone(c.Result)
		zzDeltaDecodeInt32AVX2(slice)
		require.Len(t, slice, len(c.Slice), "len")
		require.Equal(t, slice, c.Slice)
	}
}

func BenchmarkZzDeltaDecodeInt32AVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	for _, c := range tests.BenchmarkSizes {
		a := util.RandInts[int32](c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				zzDeltaDecodeInt32AVX2(a)
			}
		})
	}
}

// ---------------- zzDeltaDecodeInt16 -------------------------------------------------------------

func TestZzDeltaDecodeInt16AVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range zzDeltaEncodeUint16Cases {
		slice := slices.Clone(c.Result)
		zzDeltaDecodeInt16AVX2(slice)
		require.Len(t, slice, len(c.Slice), "len")
		require.Equal(t, slice, c.Slice)
	}
}

func BenchmarkZzDeltaDecodeInt16AVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	for _, c := range tests.BenchmarkSizes {
		a := util.RandInts[int16](c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				zzDeltaDecodeInt16AVX2(a)
			}
		})
	}
}

// ---------------- zzDeltaDecodeInt8 -------------------------------------------------------------

func TestZzDeltaDecodeInt8AVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range zzDeltaEncodeUint8Cases {
		slice := slices.Clone(c.Result)
		zzDeltaDecodeInt8AVX2(slice)
		require.Len(t, slice, len(c.Slice), "len")
		require.Equal(t, slice, c.Slice)
	}
}

func BenchmarkZzDeltaDecodeInt8AVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	for _, c := range tests.BenchmarkSizes {
		a := util.RandInts[int8](c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 1))
			for range b.N {
				zzDeltaDecodeInt8AVX2(a)
			}
		})
	}
}

// ------------ deltaDecodeTime -----------------------------------------------------------------

func BenchmarkDeltaDecodeTimeAVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	mod := uint64(1000000000)
	for _, c := range tests.BenchmarkSizes {
		a := util.RandUintsn[uint64](c.N, 10000)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				deltaDecodeTimeAVX2(a, mod)
			}
		})
	}
}

// ------------ zzDeltaDecodeTime -----------------------------------------------------------------

func BenchmarkZzDeltaDecodeTimeAVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	mod := uint64(1000000000)
	for _, c := range tests.BenchmarkSizes {
		a := util.RandUintsn[uint64](c.N, 10000)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				zzDeltaDecodeTimeAVX2(a, mod)
			}
		})
	}
}

// ------------ zzDecodeInt64 -----------------------------------------------------------------

// func TestZzDecodeInt64AVX2(t *testing.T) {
// 	if !cpu.UseAVX2 {
// 		t.SkipNow()
// 	}
// 	for _, c := range zzDeltaEncodeUint64Cases {
// 		slice := slices.Clone(c.Result)
// 		zzDecodeInt64AVX2(slice)
// 		if got, want := len(slice), len(c.Slice); got != want {
// 			t.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
// 		}
// 		if !reflect.DeepEqual(slice, c.Slice) {
// 			t.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
// 		}
// 	}
// }

// func BenchmarkZzDecodeInt64AVX2(b *testing.B) {
// 	if !cpu.UseAVX2 {
// 		b.SkipNow()
// 	}
// 	for _, n := range benchmarkSizes {
// 		a := randInt64Slice(n.L, 1)
// 		b.Run(n.Name, func(b *testing.B) {
// 			b.SetBytes(int64(n.L * Int64Size))
// 			for i := 0; i < b.N; i++ {
// 				zzDecodeInt64AVX2(a)
// 			}
// 		})
// 	}
// }

// -------------- deltaDecodeInt64 ---------------------------------------------------------------

// func TestDeltaDecodeInt64AVX2(t *testing.T) {
// 	if !cpu.UseAVX2 {
// 		t.SkipNow()
// 	}
// 	for _, c := range zzDeltaEncodeUint64Cases {
// 		slice := slices.Clone(c.Result)
// 		deltaDecodeInt64AVX2(slice)
// 		if got, want := len(slice), len(c.Slice); got != want {
// 			t.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
// 		}
// 		if !reflect.DeepEqual(slice, c.Slice) {
// 			t.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
// 		}
// 	}
// }

// func BenchmarkDeltaDecodeInt64AVX2(b *testing.B) {
// 	if !cpu.UseAVX2 {
// 		b.SkipNow()
// 	}
// 	for _, n := range benchmarkSizes {
// 		a := randInt64Slice(n.L, 1)
// 		b.Run(n.Name, func(b *testing.B) {
// 			b.SetBytes(int64(n.L * Int64Size))
// 			for i := 0; i < b.N; i++ {
// 				deltaDecodeInt64AVX2(a)
// 			}
// 		})
// 	}
// }

// -------------- deltaDecodeInt64 ---------------------------------------------------------------

// func TestDeltaDecodeInt32AVX2(t *testing.T) {
// 	if !cpu.UseAVX2 {
// 		t.SkipNow()
// 	}
// 	for _, c := range zzDeltaEncodeUint32Cases {
// 		slice := slices.Clone(c.Result)
// 		deltaDecodeInt32AVX2(slice)
// 		if got, want := len(slice), len(c.Slicd); got != want {
// 			t.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
// 		}
// 		if !reflect.DeepEqual(slice, c.Slice) {
// 			t.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
// 		}
// 	}
// }

// func BenchmarkDeltaDecodeInt32AVX2(b *testing.B) {
// 	if !cpu.UseAVX2 {
// 		b.SkipNow()
// 	}
// 	for _, n := range benchmarkSizes {
// 		a := randInt32Slice(n.L, 1)
// 		b.Run(n.Name, func(b *testing.B) {
// 			b.SetBytes(int64(n.L * Int32Size))
// 			for i := 0; i < b.N; i++ {
// 				deltaDecodeInt32AVX2(a)
// 			}
// 		})
// 	}
// }
