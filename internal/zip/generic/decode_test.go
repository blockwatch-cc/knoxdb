// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package generic

import (
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

// --------------- zzDeltaDecodeInt64 --------------------------------------------------------------

func TestZzDeltaDecodeInt64Generic(t *testing.T) {
	for _, c := range zzDeltaEncodeUint64Cases {
		slice := slices.Clone(c.Result)
		ZzDeltaDecodeInt64(slice)
		require.Len(t, slice, len(c.Slice), "len")
		require.Equal(t, slice, c.Slice)
	}
}

func BenchmarkZzDeltaDecodeInt64Generic(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		a := util.RandInts[int64](c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				ZzDeltaDecodeInt64(a)
			}
		})
	}
}

// --------------- zzDeltaDecodeInt32 --------------------------------------------------------------

func TestZzDeltaDecodeInt32Generic(t *testing.T) {
	for _, c := range zzDeltaEncodeUint32Cases {
		slice := slices.Clone(c.Result)
		ZzDeltaDecodeInt32(slice)
		require.Len(t, slice, len(c.Slice), "len")
		require.Equal(t, slice, c.Slice)
	}
}

func BenchmarkZzDeltaDecodeInt32Generic(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		a := util.RandInts[int32](c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				ZzDeltaDecodeInt32(a)
			}
		})
	}
}

// --------------- zzDeltaDecodeInt16 --------------------------------------------------------------

func TestZzDeltaDecodeInt16Generic(t *testing.T) {
	for _, c := range zzDeltaEncodeUint16Cases {
		slice := slices.Clone(c.Result)
		ZzDeltaDecodeInt16(slice)
		require.Len(t, slice, len(c.Slice), "len")
		require.Equal(t, slice, c.Slice)
	}
}

func BenchmarkZzDeltaDecodeInt16Generic(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		a := util.RandInts[int16](c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				ZzDeltaDecodeInt16(a)
			}
		})
	}
}

// --------------- zzDeltaDecodeInt8 --------------------------------------------------------------

func TestZzDeltaDecodeInt8Generic(t *testing.T) {
	for _, c := range zzDeltaEncodeUint8Cases {
		slice := slices.Clone(c.Result)
		ZzDeltaDecodeInt8(slice)
		require.Len(t, slice, len(c.Slice), "len")
		require.Equal(t, slice, c.Slice)
	}
}

func BenchmarkZzDeltaDecodeInt8Generic(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		a := util.RandInts[int8](c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 1))
			for range b.N {
				ZzDeltaDecodeInt8(a)
			}
		})
	}
}

// Disabled tests for unused functions

// --------------- zzDecodeInt64 --------------------------------------------------------------

// func TestZzDecodeInt64Generic(t *testing.T) {
// 	for _, c := range []Int64Test{
// 		{
// 			name:   "l0",
// 			slice:  make([]int64, 0),
// 			result: []int64{},
// 		},
// 		CreateInt64TestCase("l3", int64ZzDeltaEncoded, int64DeltaEncoded, 3),
// 		CreateInt64TestCase("l4", int64ZzDeltaEncoded, int64DeltaEncoded, 4),
// 		CreateInt64TestCase("l7", int64ZzDeltaEncoded, int64DeltaEncoded, 7),
// 		CreateInt64TestCase("l8", int64ZzDeltaEncoded, int64DeltaEncoded, 8),
// 	} {
// 		slice := make([]int64, len(c.Slice))
// 		copy(slice, c.Slice)
// 		ZzDecodeInt64(slice)
// 		if got, want := len(slice), len(c.Result); got != want {
// 			t.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
// 		}
// 		if !reflect.DeepEqual(slice, c.Result) {
// 			t.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Result)
// 		}
// 	}
// }

// func BenchmarkZzDecodeInt64Generic(b *testing.B) {
// 	for _, n := range benchmarkSizes {
// 		a := randInt64Slice(n.L, 1)
// 		b.Run(n.Name, func(b *testing.B) {
// 			b.SetBytes(int64(n.L * Int64Size))
// 			for i := 0; i < b.N; i++ {
// 				ZzDecodeInt64(a)
// 			}
// 		})
// 	}
// }

// ----------------- deltaDecodeInt64 ------------------------------------------------------------

// func TestDeltaDecodeInt64Generic(t *testing.T) {
// 	for _, c := range []Int64Test{
// 		{
// 			name:   "l0",
// 			slice:  make([]int64, 0),
// 			result: []int64{},
// 		},
// 		CreateInt64TestCase("l3", int64DeltaEncoded, int64DecodedSlice, 3),
// 		CreateInt64TestCase("l4", int64DeltaEncoded, int64DecodedSlice, 4),
// 		CreateInt64TestCase("l7", int64DeltaEncoded, int64DecodedSlice, 7),
// 		CreateInt64TestCase("l8", int64DeltaEncoded, int64DecodedSlice, 8),
// 	} {
// 		slice := make([]int64, len(c.Slice))
// 		copy(slice, c.Slice)
// 		DeltaDecodeInt64(slice)
// 		if got, want := len(slice), len(c.Result); got != want {
// 			t.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
// 		}
// 		if !reflect.DeepEqual(slice, c.Result) {
// 			t.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Result)
// 		}
// 	}
// }

// func BenchmarkDeltaDecodeInt64Generic(b *testing.B) {
// 	for _, n := range benchmarkSizes {
// 		a := randInt64Slice(n.L, 1)
// 		b.Run(n.Name, func(b *testing.B) {
// 			b.SetBytes(int64(n.L * Int64Size))
// 			for i := 0; i < b.N; i++ {
// 				DeltaDecodeInt64(a)
// 			}
// 		})
// 	}
// }

// ----------------- deltaDecodeInt32 ------------------------------------------------------------

// func TestDeltaDecodeInt32Generic(t *testing.T) {
// 	for _, c := range []Int32Test{
// 		{
// 			name:   "l0",
// 			slice:  make([]int32, 0),
// 			result: []int32{},
// 		},
// 		CreateInt32TestCase("l3", int64DeltaEncoded, int64DecodedSlice, 3),
// 		CreateInt32TestCase("l4", int64DeltaEncoded, int64DecodedSlice, 4),
// 		CreateInt32TestCase("l7", int64DeltaEncoded, int64DecodedSlice, 7),
// 		CreateInt32TestCase("l8", int64DeltaEncoded, int64DecodedSlice, 8),
// 	} {
// 		slice := make([]int32, len(c.Slice))
// 		copy(slice, c.Slice)
// 		DeltaDecodeInt32(slice)
// 		if got, want := len(slice), len(c.Result); got != want {
// 			t.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
// 		}
// 		if !reflect.DeepEqual(slice, c.Result) {
// 			t.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Result)
// 		}
// 	}
// }

// func BenchmarkDeltaDecodeInt32Generic(b *testing.B) {
// 	for _, n := range benchmarkSizes {
// 		a := randInt32Slice(n.L, 1)
// 		b.Run(n.Name, func(b *testing.B) {
// 			b.SetBytes(int64(n.L * Int32Size))
// 			for i := 0; i < b.N; i++ {
// 				DeltaDecodeInt32(a)
// 			}
// 		})
// 	}
// }
