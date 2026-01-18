// Copyright (c) 2021-2024 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package llb

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/tests"
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
			// now := time.Now()
			flt := NewFilterWithPrecision(uint32(f))
			flt.AddMultiInt64(util.RandInts[int64](sz))
			c := flt.Cardinality()
			_ = c
			// t.Logf("F=%d SZ=%d C=%d ERR=%f RT=%s", f, sz, c, float64(sz-int(c))/float64(sz), time.Since(now))
		}
	}
}

func TestCardinalityUint32Go(t *testing.T) {
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
			llb_add_u32_purego(llb, slice, 0)
			j = 0
			res := llb_cardinality_purego(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > MaxAllowedLlbError {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestCardinalityMultiUint64Go(t *testing.T) {
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
			llb_add_u64_purego(llb, slice, 0)
			j = 0
			res := llb_cardinality_purego(llb)

			ratio := 100 * estimateError(res, exact)
			if ratio > MaxAllowedLlbError {
				t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
			}
		}
	}
}

func TestMergeGo(t *testing.T) {
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

	llb_merge_purego(llb1.buf, llb2.buf)
	exact := len(unique)
	res := int(llb1.Cardinality())

	ratio := 100 * math.Abs(float64(res-exact)) / float64(exact)
	expectedError := 1.04 / math.Sqrt(float64(llb1.P()))

	if float64(res) < float64(exact)-(float64(exact)*expectedError) || float64(res) > float64(exact)+(float64(exact)*expectedError) {
		t.Errorf("Exact %d, got %d which is %.2f%% error", exact, res, ratio)
	}

	llb_merge_purego(llb1.buf, llb2.buf)
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

var benchPrecisons = []uint32{8, 12, 14, 16}

func BenchmarkAddUint32Go(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, p := range benchPrecisons {
			data := tests.GenRnd[uint32](c.N)
			b.Run(fmt.Sprintf("%s/p=%d", c.Name, p), func(b *testing.B) {
				b.SetBytes(int64(4 * c.N))
				for b.Loop() {
					f := NewFilterWithPrecision(p)
					llb_add_u32_purego(f, data, 0)
				}
			})
		}
	}
}

func BenchmarkAddUint64Go(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, p := range benchPrecisons {
			data := tests.GenRnd[uint64](c.N)
			b.Run(fmt.Sprintf("%s/p=%d", c.Name, p), func(b *testing.B) {
				b.SetBytes(int64(8 * c.N))
				for b.Loop() {
					f := NewFilterWithPrecision(p)
					llb_add_u64_purego(f, data, 0)
				}
			})
		}
	}
}

func BenchmarkAddHashes(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, p := range benchPrecisons {
			data := tests.GenRnd[uint64](c.N)
			hashes := make([]uint64, c.N)
			b.Run(fmt.Sprintf("%s/p=%d", c.Name, p), func(b *testing.B) {
				b.SetBytes(int64(8 * c.N))
				for b.Loop() {
					u64 := xxhash.Vec64u64(data, hashes)
					f := NewFilterWithPrecision(p)
					f.AddHashes(u64)
				}
			})
		}
	}
}

func BenchmarkCardinalityGo(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, p := range benchPrecisons {
			data := tests.GenRnd[uint32](c.N)
			f := NewFilterWithPrecision(p)
			f.AddMultiUint32(data)
			b.Run(fmt.Sprintf("%s/p=%d", c.Name, p), func(b *testing.B) {
				b.SetBytes(int64(len(f.buf)))
				for b.Loop() {
					_ = llb_cardinality_purego(f)
				}
			})
		}
	}
}

func BenchmarkMergeGo(b *testing.B) {
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
					other, err := NewFilterBuffer(f1.Bytes(), f1.P())
					if err != nil {
						b.Fatal(err)
					}
					llb_merge_purego(other.buf, f2.buf)
				}
			})
		}
	}
}

func BenchmarkUniqueMap(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[uint64](c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(8 * c.N))
			for b.Loop() {
				f := make(map[uint64]struct{}, len(data))
				for _, v := range data {
					f[v] = struct{}{}
				}
				_ = len(f)
			}
		})
	}
}

func BenchmarkUniqueSort(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[uint64](c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(8 * c.N))
			for b.Loop() {
				res := slicex.Unique(slices.Clone(data))
				_ = len(res)
			}
		})
	}
}
