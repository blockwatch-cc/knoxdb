// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhashVec

import (
	"reflect"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
)

func TestXXhash32Uint32SliceAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxhash32Uint32Cases {
		// pre-allocate the result slice
		res := make([]uint32, len(c.slice))
		xxhash32Uint32SliceAVX2(c.slice, res, 0)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func TestXXhash32Uint32SliceAVX512(t *testing.T) {
	if !util.UseAVX512_F {
		t.SkipNow()
	}
	for _, c := range xxhash32Uint32Cases {
		// pre-allocate the result slice
		res := make([]uint32, len(c.slice))
		xxhash32Uint32SliceAVX512(c.slice, res, 0)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXHash32Uint32SliceAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(n.l * 4))
			for i := 0; i < b.N; i++ {
				xxhash32Uint32SliceAVX2(a, res, 0)
			}
		})
	}
}

func BenchmarkXXHash32Uint32SliceAVX512(b *testing.B) {
	if !util.UseAVX512_F {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(n.l * 4))
			for i := 0; i < b.N; i++ {
				xxhash32Uint32SliceAVX512(a, res, 0)
			}
		})
	}
}

/*************** xxhash32Int32 *******************************************************/

func TestXXhash32Int32SliceAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxhash32Int32Cases {
		// pre-allocate the result slice
		res := make([]uint32, len(c.slice))
		xxhash32Int32SliceAVX2(c.slice, res, 0)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func TestXXhash32Int32SliceAVX512(t *testing.T) {
	if !util.UseAVX512_F {
		t.SkipNow()
	}
	for _, c := range xxhash32Int32Cases {
		// pre-allocate the result slice
		res := make([]uint32, len(c.slice))
		xxhash32Int32SliceAVX512(c.slice, res, 0)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXHash32Int32SliceAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandInts[int32](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(n.l * 4))
			for i := 0; i < b.N; i++ {
				xxhash32Int32SliceAVX2(a, res, 0)
			}
		})
	}
}

func BenchmarkXXHash32Int32SliceAVX512(b *testing.B) {
	if !util.UseAVX512_F {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandInts[int32](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(n.l * 4))
			for i := 0; i < b.N; i++ {
				xxhash32Int32SliceAVX512(a, res, 0)
			}
		})
	}
}

/*************** xxhash32Uint64 *******************************************************/

func TestXXHash32Uint64SliceAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxhash32Uint64Cases {
		// pre-allocate the result slice
		res := make([]uint32, len(c.slice))
		xxhash32Uint64SliceAVX2(c.slice, res, 0)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func TestXXHash32Uint64SliceAVX512(t *testing.T) {
	if !util.UseAVX512_F {
		t.SkipNow()
	}
	for _, c := range xxhash32Uint64Cases {
		// pre-allocate the result slice
		res := make([]uint32, len(c.slice))
		xxhash32Uint64SliceAVX512(c.slice, res, 0)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXHash32Uint64SliceAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxhash32Uint64SliceAVX2(a, res, 0)
			}
		})
	}
}

func BenchmarkXXHash32Uint64SliceAVX512(b *testing.B) {
	if !util.UseAVX512_F {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxhash32Uint64SliceAVX512(a, res, 0)
			}
		})
	}
}

/*************** xxhash32Int64 *******************************************************/

func TestXXhash32Int64SliceAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxhash32Int64Cases {
		// pre-allocate the result slice
		res := make([]uint32, len(c.slice))
		xxhash32Int64SliceAVX2(c.slice, res, 0)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func TestXXhash32Int64SliceAVX512(t *testing.T) {
	if !util.UseAVX512_F {
		t.SkipNow()
	}
	for _, c := range xxhash32Int64Cases {
		// pre-allocate the result slice
		res := make([]uint32, len(c.slice))
		xxhash32Int64SliceAVX512(c.slice, res, 0)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXHash32Int64SliceAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandInts[int64](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(n.l * 4))
			for i := 0; i < b.N; i++ {
				xxhash32Int64SliceAVX2(a, res, 0)
			}
		})
	}
}

func BenchmarkXXHash32Int64SliceAVX512(b *testing.B) {
	if !util.UseAVX512_F {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandInts[int64](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(n.l * 4))
			for i := 0; i < b.N; i++ {
				xxhash32Int64SliceAVX512(a, res, 0)
			}
		})
	}
}

/*************** xxhash64Uint32 *******************************************************/

func TestXXHash64Uint32SliceAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxhash64Uint32Cases {
		// pre-allocate the result slice
		res := make([]uint64, len(c.slice))
		xxhash64Uint32SliceAVX2(c.slice, res)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func TestXXHash64Uint32SliceAVX512(t *testing.T) {
	if !util.UseAVX512_DQ {
		t.SkipNow()
	}
	for _, c := range xxhash64Uint32Cases {
		// pre-allocate the result slice
		res := make([]uint64, len(c.slice))
		xxhash64Uint32SliceAVX512(c.slice, res)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXHash64Uint32SliceAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxhash64Uint32SliceAVX2(a, res)
			}
		})
	}
}

func BenchmarkXXHash64Uint32SliceAVX512(b *testing.B) {
	if !util.UseAVX512_DQ {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxhash64Uint32SliceAVX512(a, res)
			}
		})
	}
}

/*************** xxhash64Uint64 *******************************************************/

func TestXXHash64Uint64SliceAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxhash64Uint64Cases {
		// pre-allocate the result slice
		res := make([]uint64, len(c.slice))
		xxhash64Uint64SliceAVX2(c.slice, res)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func TestXXHash64Uint64SliceAVX512(t *testing.T) {
	if !util.UseAVX512_DQ {
		t.SkipNow()
	}
	for _, c := range xxhash64Uint64Cases {
		// pre-allocate the result slice
		res := make([]uint64, len(c.slice))
		xxhash64Uint64SliceAVX512(c.slice, res)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXHash64Uint64SliceAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxhash64Uint64SliceAVX2(a, res)
			}
		})
	}
}

func BenchmarkXXHash64Uint64SliceAVX512(b *testing.B) {
	if !util.UseAVX512_DQ {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxhash64Uint64SliceAVX512(a, res)
			}
		})
	}
}

/*************** xxh3Uint32 *******************************************************/

func TestXXH3Uint32SliceAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxh3Uint32Cases {
		// pre-allocate the result slice
		res := make([]uint64, len(c.slice))
		xxh3Uint32SliceAVX2(c.slice, res)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func TestXXH3Uint32SliceAVX512(t *testing.T) {
	if !util.UseAVX512_DQ {
		t.SkipNow()
	}
	for _, c := range xxh3Uint32Cases {
		// pre-allocate the result slice
		res := make([]uint64, len(c.slice))
		xxh3Uint32SliceAVX512(c.slice, res)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXH3Uint32SliceAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxh3Uint32SliceAVX2(a, res)
			}
		})
	}
}

func BenchmarkXXH3Uint32SliceAVX512(b *testing.B) {
	if !util.UseAVX512_DQ {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxh3Uint32SliceAVX512(a, res)
			}
		})
	}
}

/*************** xxh3Uint64 *******************************************************/

func TestXXH3Uint64SliceAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxh3Uint64Cases {
		// pre-allocate the result slice
		res := make([]uint64, len(c.slice))
		xxh3Uint64SliceAVX2(c.slice, res)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func TestXXH3Uint64SliceAVX512(t *testing.T) {
	if !util.UseAVX512_DQ {
		t.SkipNow()
	}
	for _, c := range xxh3Uint64Cases {
		// pre-allocate the result slice
		res := make([]uint64, len(c.slice))
		xxh3Uint64SliceAVX512(c.slice, res)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXH3Uint64SliceAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxh3Uint64SliceAVX2(a, res)
			}
		})
	}
}

func BenchmarkXXH3Uint64SliceAVX512(b *testing.B) {
	if !util.UseAVX512_DQ {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxh3Uint64SliceAVX512(a, res)
			}
		})
	}
}
