// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package generic

import (
	"reflect"
	"testing"

	"golang.org/x/exp/slices"
)

// --------------- zzDeltaDecodeInt64 --------------------------------------------------------------

func TestZzDeltaDecodeInt64Generic(T *testing.T) {
	for _, c := range zzDeltaEncodeUint64Cases {
		slice := slices.Clone(c.Result)
		ZzDeltaDecodeInt64(slice)
		if got, want := len(slice), len(c.Slice); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(slice, c.Slice) {
			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
		}
	}
}

func BenchmarkZzDeltaDecodeInt64Generic(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.L, 1)
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int64Size))
			for i := 0; i < B.N; i++ {
				ZzDeltaDecodeInt64(a)
			}
		})
	}
}

// --------------- zzDeltaDecodeInt32 --------------------------------------------------------------

func TestZzDeltaDecodeInt32Generic(T *testing.T) {
	for _, c := range zzDeltaEncodeUint32Cases {
		slice := slices.Clone(c.Result)
		ZzDeltaDecodeInt32(slice)
		if got, want := len(slice), len(c.Slice); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(slice, c.Slice) {
			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
		}
	}
}

func BenchmarkZzDeltaDecodeInt32Generic(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt32Slice(n.L, 1)
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int32Size))
			for i := 0; i < B.N; i++ {
				ZzDeltaDecodeInt32(a)
			}
		})
	}
}

// --------------- zzDeltaDecodeInt16 --------------------------------------------------------------

func TestZzDeltaDecodeInt16Generic(T *testing.T) {
	for _, c := range zzDeltaEncodeUint16Cases {
		slice := slices.Clone(c.Result)
		ZzDeltaDecodeInt16(slice)
		if got, want := len(slice), len(c.Slice); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(slice, c.Slice) {
			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
		}
	}
}

func BenchmarkZzDeltaDecodeInt16Generic(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt16Slice(n.L, 1)
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int16Size))
			for i := 0; i < B.N; i++ {
				ZzDeltaDecodeInt16(a)
			}
		})
	}
}

// --------------- zzDeltaDecodeInt8 --------------------------------------------------------------

func TestZzDeltaDecodeInt8Generic(T *testing.T) {
	for _, c := range zzDeltaEncodeUint8Cases {
		slice := slices.Clone(c.Result)
		ZzDeltaDecodeInt8(slice)
		if got, want := len(slice), len(c.Slice); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(slice, c.Slice) {
			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Slice)
		}
	}
}

func BenchmarkZzDeltaDecodeInt8Generic(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt8Slice(n.L, 1)
		B.Run(n.Name, func(B *testing.B) {
			B.SetBytes(int64(n.L * Int8Size))
			for i := 0; i < B.N; i++ {
				ZzDeltaDecodeInt8(a)
			}
		})
	}
}

// --------------- zzDecodeInt64 --------------------------------------------------------------

// func TestZzDecodeInt64Generic(T *testing.T) {
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
// 			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
// 		}
// 		if !reflect.DeepEqual(slice, c.Result) {
// 			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Result)
// 		}
// 	}
// }

// func BenchmarkZzDecodeInt64Generic(B *testing.B) {
// 	for _, n := range benchmarkSizes {
// 		a := randInt64Slice(n.L, 1)
// 		B.Run(n.Name, func(B *testing.B) {
// 			B.SetBytes(int64(n.L * Int64Size))
// 			for i := 0; i < B.N; i++ {
// 				ZzDecodeInt64(a)
// 			}
// 		})
// 	}
// }

// ----------------- deltaDecodeInt64 ------------------------------------------------------------

// func TestDeltaDecodeInt64Generic(T *testing.T) {
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
// 			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
// 		}
// 		if !reflect.DeepEqual(slice, c.Result) {
// 			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Result)
// 		}
// 	}
// }

// func BenchmarkDeltaDecodeInt64Generic(B *testing.B) {
// 	for _, n := range benchmarkSizes {
// 		a := randInt64Slice(n.L, 1)
// 		B.Run(n.Name, func(B *testing.B) {
// 			B.SetBytes(int64(n.L * Int64Size))
// 			for i := 0; i < B.N; i++ {
// 				DeltaDecodeInt64(a)
// 			}
// 		})
// 	}
// }

// ----------------- deltaDecodeInt32 ------------------------------------------------------------

// func TestDeltaDecodeInt32Generic(T *testing.T) {
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
// 			T.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
// 		}
// 		if !reflect.DeepEqual(slice, c.Result) {
// 			T.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Result)
// 		}
// 	}
// }

// func BenchmarkDeltaDecodeInt32Generic(B *testing.B) {
// 	for _, n := range benchmarkSizes {
// 		a := randInt32Slice(n.L, 1)
// 		B.Run(n.Name, func(B *testing.B) {
// 			B.SetBytes(int64(n.L * Int32Size))
// 			for i := 0; i < B.N; i++ {
// 				DeltaDecodeInt32(a)
// 			}
// 		})
// 	}
// }
