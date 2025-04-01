// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package loglogbeta

import (
	"fmt"
	"math"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
)

func TestCardinalityManyUint32AVX2(t *testing.T) {
	if !util.UseAVX2 {
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
			filterAddManyUint32AVX2(llb, slice, 0)
			j = 0
			res := filterCardinalityAVX2(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > 2 {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestCardinalityManyUint32AVX512(t *testing.T) {
	if !util.UseAVX512_CD {
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
			filterAddManyUint32AVX512(llb, slice, 0)
			j = 0
			res := filterCardinalityAVX512(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > 2 {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestCardinalityManyInt32AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	llb := NewFilter()
	step := 10000
	unique := map[int32]bool{}
	slice := make([]int32, step)
	var j int
	for i := 0; i < 100000; i++ {
		val := int32(util.RandIntn(i + step))
		unique[val] = true
		slice[j] = val
		j++

		if j%step == 0 {
			exact := uint64(len(unique))
			filterAddManyInt32AVX2(llb, slice, 0)
			j = 0
			res := filterCardinalityAVX2(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > 2 {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestCardinalityManyInt32AVX512(t *testing.T) {
	if !util.UseAVX512_CD {
		t.SkipNow()
	}
	llb := NewFilter()
	step := 10000
	unique := map[int32]bool{}
	slice := make([]int32, step)
	var j int
	for i := 0; i < 100000; i++ {
		val := int32(util.RandIntn(i + step))
		unique[val] = true
		slice[j] = val
		j++

		if j%step == 0 {
			exact := uint64(len(unique))
			filterAddManyInt32AVX512(llb, slice, 0)
			j = 0
			res := filterCardinalityAVX512(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > 2 {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestCardinalityManyUint64AVX2(t *testing.T) {
	if !util.UseAVX2 {
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
			filterAddManyUint64AVX2(llb, slice, 0)
			j = 0
			res := filterCardinalityAVX2(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > 2 {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestCardinalityManyUint64AVX512(t *testing.T) {
	if !util.UseAVX512_CD {
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
			filterAddManyUint64AVX512(llb, slice, 0)
			j = 0
			res := filterCardinalityAVX512(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > 2 {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestCardinalityManyInt64AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	llb := NewFilter()
	step := 10000
	unique := map[int64]bool{}
	slice := make([]int64, step)
	var j int
	for i := 0; i < 100000; i++ {
		val := int64(util.RandIntn(i + step))
		unique[val] = true
		slice[j] = val
		j++

		if j%step == 0 {
			exact := uint64(len(unique))
			filterAddManyInt64AVX2(llb, slice, 0)
			j = 0
			res := filterCardinalityAVX2(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > 2 {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestCardinalityManyInt64AVX512(t *testing.T) {
	if !util.UseAVX512_CD {
		t.SkipNow()
	}
	llb := NewFilter()
	step := 10000
	unique := map[int64]bool{}
	slice := make([]int64, step)
	var j int
	for i := 0; i < 100000; i++ {
		val := int64(util.RandIntn(i + step))
		unique[val] = true
		slice[j] = val
		j++

		if j%step == 0 {
			exact := uint64(len(unique))
			filterAddManyInt64AVX512(llb, slice, 0)
			j = 0
			res := filterCardinalityAVX512(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > 2 {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestMergeAVX2(t *testing.T) {
	if !util.UseAVX2 {
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

	filterMergeAVX2(llb1.buf, llb2.buf)
	exact := len(unique)
	res := int(llb1.Cardinality())

	ratio := 100 * math.Abs(float64(res-exact)) / float64(exact)
	expectedError := 1.04 / math.Sqrt(float64(llb1.P()))

	if float64(res) < float64(exact)-(float64(exact)*expectedError) || float64(res) > float64(exact)+(float64(exact)*expectedError) {
		t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
	}

	filterMergeAVX2(llb1.buf, llb2.buf)
	exact = res
	res = int(llb1.Cardinality())

	if float64(res) < float64(exact)-(float64(exact)*expectedError) || float64(res) > float64(exact)+(float64(exact)*expectedError) {
		t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
	}
}

func BenchmarkFilterAddManyUint32AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, c := range benchCases {
		data := RandUint32(c.n)
		b.Run(fmt.Sprintf("n=%d_p=%d", c.n, c.p), func(b *testing.B) {
			b.SetBytes(int64(4 * c.n))
			for i := 0; i < b.N; i++ {
				filter := NewFilterWithPrecision(c.p)
				filterAddManyUint32AVX2(filter, data, 0)
			}
		})
	}
}

func BenchmarkFilterAddManyUint32AVX512(b *testing.B) {
	if !util.UseAVX512_CD {
		b.SkipNow()
	}
	for _, c := range benchCases {
		data := RandUint32(c.n)
		b.Run(fmt.Sprintf("n=%d_p=%d", c.n, c.p), func(b *testing.B) {
			b.SetBytes(int64(4 * c.n))
			for i := 0; i < b.N; i++ {
				filter := NewFilterWithPrecision(c.p)
				filterAddManyUint32AVX512(filter, data, 0)
			}
		})
	}
}

func BenchmarkFilterAddManyUint64AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, c := range benchCases {
		data := RandUint64(c.n)
		b.Run(fmt.Sprintf("n=%d_p=%d", c.n, c.p), func(b *testing.B) {
			b.SetBytes(int64(8 * c.n))
			for i := 0; i < b.N; i++ {
				filter := NewFilterWithPrecision(c.p)
				filterAddManyUint64AVX2(filter, data, 0)
			}
		})
	}
}

func BenchmarkFilterAddManyUint64AVX512(b *testing.B) {
	if !util.UseAVX512_CD {
		b.SkipNow()
	}
	for _, c := range benchCases {
		data := RandUint64(c.n)
		b.Run(fmt.Sprintf("n=%d_p=%d", c.n, c.p), func(b *testing.B) {
			b.SetBytes(int64(8 * c.n))
			for i := 0; i < b.N; i++ {
				filter := NewFilterWithPrecision(c.p)
				filterAddManyUint64AVX512(filter, data, 0)
			}
		})
	}
}

func BenchmarkFilterCardinalityAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, c := range benchCases {
		data := RandUint32(c.n)

		filter := NewFilterWithPrecision(c.p)
		for _, v := range data {
			filter.AddHash(v)
		}

		b.Run(fmt.Sprintf("n=%d_p=%d", c.n, c.p), func(b *testing.B) {
			b.SetBytes(int64(len(filter.buf)))
			for i := 0; i < b.N; i++ {
				_ = filterCardinalityAVX2(filter)
			}
		})
	}
}

func BenchmarkFilterCardinalityAVX512(b *testing.B) {
	if !util.UseAVX512_F {
		b.SkipNow()
	}
	for _, c := range benchCases {
		data := RandUint32(c.n)

		filter := NewFilterWithPrecision(c.p)
		for _, v := range data {
			filter.AddHash(v)
		}

		b.Run(fmt.Sprintf("n=%d_p=%d", c.n, c.p), func(b *testing.B) {
			b.SetBytes(int64(len(filter.buf)))
			for i := 0; i < b.N; i++ {
				_ = filterCardinalityAVX512(filter)
			}
		})
	}
}

func BenchmarkFilterMergeAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, c := range benchCases {
		data1 := RandUint32(c.n)
		data2 := RandUint32(c.n)

		filter1 := NewFilterWithPrecision(c.p)
		filter2 := NewFilterWithPrecision(c.p)
		for i := 0; i < c.n; i++ {
			filter1.AddHash(data1[i])
			filter2.AddHash(data2[i])
		}

		b.Run(fmt.Sprintf("n=%d_p=%d", c.n, c.p), func(b *testing.B) {
			b.SetBytes(int64(len(filter1.buf)))
			for i := 0; i < b.N; i++ {
				other, err := NewFilterBuffer(filter1.Bytes(), filter1.P())
				if err != nil {
					b.Fatal(err)
				}
				filterMergeAVX2(other.buf, filter2.buf)
			}
		})
	}
}
