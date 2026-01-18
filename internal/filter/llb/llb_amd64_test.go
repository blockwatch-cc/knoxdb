// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package llb

import (
	"fmt"
	"math"
	"testing"

	"blockwatch.cc/knoxdb/internal/cpu"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

func TestCardinalityUint32AVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.SkipNow()
	}
	llb := NewFilter()
	step := 10000
	unique := map[uint32]bool{}
	slice := make([]uint32, step)
	var j int
	for i := 0; i < 100000; i++ {
		val := uint32(util.RandIntn(i + step))
		unique[val] = true
		slice[j] = val
		j++

		if j%step == 0 {
			exact := uint64(len(unique))
			llb_add_u32_avx2(llb, slice, 0)
			j = 0
			res := llb_cardinality_avx2(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > 2 {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestCardinalityUint32AVX512(t *testing.T) {
	if !cpu.UseAVX512_CD {
		t.SkipNow()
	}
	llb := NewFilter()
	step := 10000
	unique := map[uint32]bool{}
	slice := make([]uint32, step)
	var j int
	for i := 0; i < 100000; i++ {
		val := uint32(util.RandIntn(i + step))
		unique[val] = true
		slice[j] = val
		j++

		if j%step == 0 {
			exact := uint64(len(unique))
			llb_add_u32_avx512(llb, slice, 0)
			j = 0
			res := llb_cardinality_avx512(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > 2 {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestCardinalityUint64AVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.SkipNow()
	}
	llb := NewFilter()
	step := 10000
	unique := map[uint64]bool{}
	slice := make([]uint64, step)
	var j int
	for i := 0; i < 100000; i++ {
		val := uint64(util.RandIntn(i + step))
		unique[val] = true
		slice[j] = val
		j++

		if j%step == 0 {
			exact := uint64(len(unique))
			llb_add_u64_avx2(llb, slice, 0)
			j = 0
			res := llb_cardinality_avx2(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > 2 {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestCardinalityUint64AVX512(t *testing.T) {
	if !cpu.UseAVX512_CD {
		t.SkipNow()
	}
	llb := NewFilter()
	step := 10000
	unique := map[uint64]bool{}
	slice := make([]uint64, step)
	var j int
	for i := 0; i < 100000; i++ {
		val := uint64(util.RandIntn(i + step))
		unique[val] = true
		slice[j] = val
		j++

		if j%step == 0 {
			exact := uint64(len(unique))
			llb_add_u64_avx512(llb, slice, 0)
			j = 0
			res := llb_cardinality_avx512(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > 2 {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestMergeAVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.SkipNow()
	}

	llb1 := NewFilter()
	llb2 := NewFilter()
	unique := map[uint64]bool{}

	for i := 1; i <= 300000; i++ {
		val := util.RandUint64()
		llb1.AddUint64(val)
		unique[val] = true

		val = util.RandUint64()
		llb2.AddUint64(val)
		unique[val] = true
	}

	llb_merge_core_avx2(llb1.buf, llb2.buf)
	exact := len(unique)
	res := int(llb1.Cardinality())

	ratio := 100 * math.Abs(float64(res-exact)) / float64(exact)
	expectedError := 1.04 / math.Sqrt(float64(llb1.P()))

	if float64(res) < float64(exact)-(float64(exact)*expectedError) || float64(res) > float64(exact)+(float64(exact)*expectedError) {
		t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
	}

	llb_merge_core_avx2(llb1.buf, llb2.buf)
	exact = res
	res = int(llb1.Cardinality())

	if float64(res) < float64(exact)-(float64(exact)*expectedError) || float64(res) > float64(exact)+(float64(exact)*expectedError) {
		t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
	}
}

func BenchmarkAddUint32AVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	for _, c := range tests.BenchmarkSizes {
		for _, p := range benchPrecisons {
			data := tests.GenRnd[uint32](c.N)
			b.Run(fmt.Sprintf("%s/p=%d", c.Name, p), func(b *testing.B) {
				b.SetBytes(int64(4 * c.N))
				for b.Loop() {
					f := NewFilterWithPrecision(p)
					llb_add_u32_avx2(f, data, 0)
				}
			})
		}
	}
}

func BenchmarkAddUint32AVX512(b *testing.B) {
	if !cpu.UseAVX512_CD {
		b.SkipNow()
	}
	for _, c := range tests.BenchmarkSizes {
		for _, p := range benchPrecisons {
			data := tests.GenRnd[uint32](c.N)
			b.Run(fmt.Sprintf("%s/p=%d", c.Name, p), func(b *testing.B) {
				b.SetBytes(int64(4 * c.N))
				for b.Loop() {
					f := NewFilterWithPrecision(p)
					llb_add_u32_avx512(f, data, 0)
				}
			})
		}
	}
}

func BenchmarkAddUint64AVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	for _, c := range tests.BenchmarkSizes {
		for _, p := range benchPrecisons {
			data := tests.GenRnd[uint64](c.N)
			b.Run(fmt.Sprintf("%s/p=%d", c.Name, p), func(b *testing.B) {
				b.SetBytes(int64(8 * c.N))
				for b.Loop() {
					f := NewFilterWithPrecision(p)
					llb_add_u64_avx2(f, data, 0)
				}
			})
		}
	}
}

func BenchmarkAddUint64AVX512(b *testing.B) {
	if !cpu.UseAVX512_CD {
		b.SkipNow()
	}
	for _, c := range tests.BenchmarkSizes {
		for _, p := range benchPrecisons {
			data := tests.GenRnd[uint64](c.N)
			b.Run(fmt.Sprintf("%s/p=%d", c.Name, p), func(b *testing.B) {
				b.SetBytes(int64(8 * c.N))
				for b.Loop() {
					f := NewFilterWithPrecision(p)
					llb_add_u64_avx512(f, data, 0)
				}
			})
		}
	}
}

func BenchmarkCardinalityAVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	for _, c := range tests.BenchmarkSizes {
		for _, p := range benchPrecisons {
			data := tests.GenRnd[uint32](c.N)
			f := NewFilterWithPrecision(p)
			f.AddMultiUint32(data)
			b.Run(fmt.Sprintf("%s/p=%d", c.Name, p), func(b *testing.B) {
				b.SetBytes(int64(len(f.buf)))
				for b.Loop() {
					llb_cardinality_avx2(f)
				}
			})
		}
	}
}

func BenchmarkCardinalityAVX512(b *testing.B) {
	if !cpu.UseAVX512_F {
		b.SkipNow()
	}
	for _, c := range tests.BenchmarkSizes {
		for _, p := range benchPrecisons {
			data := tests.GenRnd[uint32](c.N)
			f := NewFilterWithPrecision(p)
			f.AddMultiUint32(data)
			b.Run(fmt.Sprintf("%s/p=%d", c.Name, p), func(b *testing.B) {
				b.SetBytes(int64(len(f.buf)))
				for b.Loop() {
					llb_cardinality_avx512(f)
				}
			})
		}
	}
}

func BenchmarkMergeAVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	for _, c := range tests.BenchmarkSizes {
		for _, p := range benchPrecisons {
			data1 := tests.GenRnd[uint32](c.N)
			data2 := tests.GenRnd[uint32](c.N)

			f1 := NewFilterWithPrecision(p)
			f2 := NewFilterWithPrecision(p)
			f1.AddMultiUint32(data1)
			f2.AddMultiUint32(data2)

			b.Run(fmt.Sprintf("%s/p=%d", c.Name, p), func(b *testing.B) {
				b.SetBytes(int64(len(f1.buf)))
				for b.Loop() {
					o, err := NewFilterBuffer(f1.Bytes(), p)
					if err != nil {
						b.Fatal(err)
					}
					llb_merge_core_avx2(o.buf, f2.buf)
				}
			})
		}
	}
}
