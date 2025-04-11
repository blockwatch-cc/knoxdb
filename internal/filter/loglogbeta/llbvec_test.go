// Copyright (c) 2021-2024 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package loglogbeta

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
)

const MaxAllowedLlbError = 5.0

func estimateError(got, exp uint64) float64 {
	var delta uint64
	if got > exp {
		delta = got - exp
	} else {
		delta = exp - got
	}
	return float64(delta) / float64(exp)
}

func TestZeros(t *testing.T) {
	registers := [m]uint8{}
	exp := 0.0
	for i := range registers {
		val := uint8(util.RandIntn(32))
		if val == 0 {
			exp++
		}
		registers[i] = val
	}
	_, got := regSumAndZeros(registers[:])
	if got != exp {
		t.Errorf("expected %.2f, got %.2f", exp, got)
	}
}

func RandUint64(n int) []uint64 {
	b := make([]uint64, n)
	for i := 0; i < n; i++ {
		b[i] = uint64(util.RandInt32())
	}
	return b
}

func RandUint32(n int) []uint32 {
	b := make([]uint32, n)
	for i := 0; i < n; i++ {
		b[i] = uint32(util.RandInt32())
	}
	return b
}

func TestCardinality(t *testing.T) {
	llb := NewFilter()
	step := 10000
	unique := map[uint64]bool{}

	for i := 1; len(unique) <= 100000; i++ {
		val := util.RandUint64()
		llb.AddUint64(val)
		unique[val] = true

		if len(unique)%step == 0 {
			exact := uint64(len(unique))
			res := llb.Cardinality()
			step *= 10

			ratio := 100 * estimateError(res, exact)
			if ratio > MaxAllowedLlbError {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestPrecision(t *testing.T) {
	for _, sz := range []int{1024, 2048, 8192, 16 * 1024, 32 * 1024, 64 * 1024} {
		for _, f := range []int{8, 9, 10, 11, 12, 13, 14, 15, 16} {
			now := time.Now()
			flt := NewFilterWithPrecision(uint32(f))
			flt.AddManyInt64(util.RandInts[int64](sz))
			c := flt.Cardinality()
			t.Logf("F=%d SZ=%d C=%d ERR=%f RT=%s", f, sz, c, float64(sz-int(c))/float64(sz), time.Since(now))
		}
	}
}

func TestCardinalityManyUint32Generic(t *testing.T) {
	llb := NewFilter()
	step := 10000
	unique := map[uint32]bool{}
	slice := make([]uint32, step)
	var j int
	for i := 0; i < 100000; i++ {
		// val := uint32(util.RandIntn(i + step))
		val := uint32(util.RandIntn(i + step))
		unique[val] = true
		slice[j] = val
		j++

		if j%step == 0 {
			exact := uint64(len(unique))
			filterAddManyUint32Generic(llb, slice, 0)
			j = 0
			res := filterCardinalityGeneric(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > MaxAllowedLlbError {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestCardinalityManyUint64Generic(t *testing.T) {
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
			filterAddManyUint64Generic(llb, slice, 0)
			j = 0
			res := filterCardinalityGeneric(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > MaxAllowedLlbError {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestMergeGeneric(t *testing.T) {
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

	filterMergeGeneric(llb1.buf, llb2.buf)
	exact := len(unique)
	res := int(llb1.Cardinality())

	ratio := 100 * math.Abs(float64(res-exact)) / float64(exact)
	expectedError := 1.04 / math.Sqrt(float64(llb1.P()))

	if float64(res) < float64(exact)-(float64(exact)*expectedError) || float64(res) > float64(exact)+(float64(exact)*expectedError) {
		t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
	}

	filterMergeGeneric(llb1.buf, llb2.buf)
	exact = res
	res = int(llb1.Cardinality())

	if float64(res) < float64(exact)-(float64(exact)*expectedError) || float64(res) > float64(exact)+(float64(exact)*expectedError) {
		t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
	}
}

func TestMarshal(t *testing.T) {
	llb := NewFilter()
	unique := map[uint64]bool{}

	for i := 1; len(unique) <= 100000; i++ {
		val := util.RandUint64()
		llb.AddUint64(val)
		unique[val] = true
	}

	buf := llb.Bytes()
	ullb, err := NewFilterBuffer(buf, llb.P())
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !bytes.Equal(ullb.buf, llb.buf) {
		t.Errorf("Expected\n%s,\n\n got\n%s", hex.Dump(llb.buf), hex.Dump(ullb.buf))
	}
}

var benchCases = []struct {
	n int    // num values
	p uint32 // precision (size = 1<<p bytes)
}{
	{n: 32768, p: 10},
	{n: 32768, p: 11},
	{n: 32768, p: 12},
	{n: 32768, p: 13},
	{n: 32768, p: 14},
	{n: 32768, p: 15},
	{n: 32768, p: 16},
	{n: 65536, p: 10},
	{n: 65536, p: 11},
	{n: 65536, p: 12},
	{n: 65536, p: 13},
	{n: 65536, p: 14},
	{n: 65536, p: 15},
	{n: 65536, p: 16},
}

func BenchmarkFilterAddManyUint32Generic(b *testing.B) {
	for _, c := range benchCases {
		data := RandUint32(c.n)
		b.Run(fmt.Sprintf("n=%d_p=%d", c.n, c.p), func(b *testing.B) {
			b.SetBytes(int64(4 * c.n))
			for i := 0; i < b.N; i++ {
				filter := NewFilterWithPrecision(c.p)
				filterAddManyUint32Generic(filter, data, 0)
			}
		})
	}
}

func BenchmarkFilterAddManyUint64Generic(b *testing.B) {
	for _, c := range benchCases {
		data := RandUint64(c.n)
		b.Run(fmt.Sprintf("n=%d_p=%d", c.n, c.p), func(b *testing.B) {
			b.SetBytes(int64(8 * c.n))
			for i := 0; i < b.N; i++ {
				filter := NewFilterWithPrecision(c.p)
				filterAddManyUint64Generic(filter, data, 0)
			}
		})
	}
}

func BenchmarkFilterCardinalityGeneric(b *testing.B) {
	for _, c := range benchCases {
		data := RandUint32(c.n)

		filter := NewFilterWithPrecision(c.p)
		for _, v := range data {
			filter.AddHash(v)
		}

		b.Run(fmt.Sprintf("n=%d_p=%d", c.n, c.p), func(b *testing.B) {
			b.SetBytes(int64(len(filter.buf)))
			for i := 0; i < b.N; i++ {
				_ = filterCardinalityGeneric(filter)
			}
		})
	}
}

func BenchmarkFilterMergeGeneric(b *testing.B) {
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
				filterMergeGeneric(other.buf, filter2.buf)
			}
		})
	}
}

func BenchmarkFilterAddExact(b *testing.B) {
	lastn := 0
	for _, c := range benchCases {
		if c.n == lastn {
			continue
		}
		lastn = c.n
		data := RandUint64(c.n)
		b.Run(fmt.Sprintf("n=%d", c.n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filter := make(map[uint64]struct{}, len(data))
				for _, v := range data {
					filter[v] = struct{}{}
				}
			}
		})
	}
}

func BenchmarkFilterCardinalityExact(b *testing.B) {
	lastn := 0
	for _, c := range benchCases {
		if c.n == lastn {
			continue
		}
		lastn = c.n
		data := RandUint64(c.n)
		filter := make(map[uint64]struct{}, len(data))
		for _, v := range data {
			filter[v] = struct{}{}
		}

		b.Run(fmt.Sprintf("n=%d", c.n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = len(filter)
			}
		})
	}
}

func BenchmarkFilterAddExactHashed(b *testing.B) {
	lastn := 0
	for _, c := range benchCases {
		if c.n == lastn {
			continue
		}
		lastn = c.n
		u64 := xxhash.Vec64u64(RandUint64(c.n), nil)
		b.Run(fmt.Sprintf("n=%d", c.n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				flt := NewFilterWithPrecision(16)
				flt.AddManyUint64(u64)
			}
		})
	}
}

func BenchmarkFilterCardinalityExactHashed(b *testing.B) {
	lastn := 0
	for _, c := range benchCases {
		if c.n == lastn {
			continue
		}
		lastn = c.n
		u64 := xxhash.Vec64u64(RandUint64(c.n), nil)
		b.Run(fmt.Sprintf("n=%d", c.n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				res := slicex.NewOrderedNumbers(u64).SetUnique().Values
				_ = len(res)
			}
		})
	}
}
