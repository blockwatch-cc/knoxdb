package main

import (
	"math"
	"math/rand/v2"
	"testing"

	"golang.org/x/exp/constraints"
)

func TestAnalyzeInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    []int64
		expMin   int64
		expMax   int64
		expDelta int64
		expRuns  int64
	}{
		{"Empty", []int64{}, 0, 0, 0, 0},
		{"Single", []int64{42}, 42, 42, 0, 1},
		{"DeltaNoDups", []int64{-1, 0, 1, 2}, -1, 2, 1, 4},
		{"Runs", []int64{-1, -1, 5, 5, 1, 1}, -1, 5, 0, 3},
		{"DictFriendly", []int64{-1, 1, 5, 1, -1, 1}, -1, 5, 0, 6},
		{"AllSame", []int64{5, 5, 5, 5, 5}, 5, 5, 0, 1},
		{"Alternating", []int64{1, 0, 1, 0, 1}, 0, 1, 0, 5},
		{"LargeDelta", []int64{10, 20, 30, 40, 50}, 10, 50, 10, 5},
		{"NegDelta", []int64{-10, -7, -4, -1, 2}, -10, 2, 3, 5},
		{"Bounds", []int64{math.MinInt64, 0, math.MaxInt64}, math.MinInt64, math.MaxInt64, 0, 3},
		{"Short", []int64{1, 2, 3}, 1, 3, 1, 3},
		{"MixedRuns", []int64{1, 1, 2, 2, 5, 8, 8}, 1, 8, 0, 4},
		{"Unaligned", []int64{1, 2, 3, 4, 5, 6, 7}, 1, 7, 1, 7},
		{"Random", []int64{3, 1, 4, 1, 5, 9, 2, 6, 5, 3}, 1, 9, 0, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minv, maxv, delta, numRuns := AnalyzeInt64(tt.input)
			if minv != tt.expMin {
				t.Errorf("min: got %d, want %d", minv, tt.expMin)
			}
			if maxv != tt.expMax {
				t.Errorf("max: got %d, want %d", maxv, tt.expMax)
			}
			if delta != tt.expDelta {
				t.Errorf("delta: got %d, want %d", delta, tt.expDelta)
			}
			if numRuns != tt.expRuns {
				t.Errorf("numRuns: got %d, want %d", numRuns, tt.expRuns)
			}
		})
	}
}

func BenchmarkAnalyzeInt64(b *testing.B) {
	for _, c := range BenchmarksInt64 {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for i := 0; i < b.N; i++ {
				AnalyzeInt64(c.Data)
			}
		})
	}
}

type Benchmark[T Integer] struct {
	Name string
	Data []T
}

var BenchmarksInt64 = []Benchmark[int64]{
	{"dups_1K", GenDups[int64](1024, 10)}, // 10% unique
	{"dups_16K", GenDups[int64](16*1024, 10)},
	{"dups_64K", GenDups[int64](64*1024, 10)},

	{"runs_1K", GenRuns[int64](1024, 10)}, // run length 10
	{"runs_16K", GenRuns[int64](16*1024, 10)},
	{"runs_64K", GenRuns[int64](64*1024, 10)},

	{"seq_1K", GenSequence[int64](1024)},
	{"seq_16K", GenSequence[int64](16 * 1024)},
	{"seq_64K", GenSequence[int64](64 * 1024)},
}

type Integer interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8
}

func GenSequence[T Integer](n int) []T {
	res := make([]T, n)
	for i := range res {
		res[i] = T(i)
	}
	return res
}

const BENCH_WIDTH = 60

var (
	RandIntn    = rand.IntN
	RandInt64   = rand.Int64
	RandInt64n  = rand.Int64N
	RandUint64n = rand.Uint64N
	RandUint64  = rand.Uint64
)

func RandIntsn[T constraints.Signed](sz int, max T) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandInt64n(int64(max)))
	}
	return s
}

func RandInts[T constraints.Signed](sz int) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandInt64())
	}
	return s
}

func RandUintsn[T constraints.Unsigned](sz int, max T) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandUint64n(uint64(max)))
	}
	return s
}

func RandUints[T constraints.Unsigned](sz int) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandUint64())
	}
	return s
}

func GenDups[T Integer](n, u int) []T {
	c := n / u
	res := make([]T, n)
	var t T
	switch any(t).(type) {
	case int64:
		unique := RandIntsn[int64](c, 1<<BENCH_WIDTH-1)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case int32:
		unique := RandInts[int32](c)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case int16:
		unique := RandInts[int16](c)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case int8:
		unique := RandInts[int8](c)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case uint64:
		unique := RandUintsn[uint64](c, 1<<BENCH_WIDTH-1)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case uint32:
		unique := RandUints[uint32](c)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case uint16:
		unique := RandUints[uint16](c)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case uint8:
		unique := RandUints[uint8](c)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	}
	return res
}
func GenRuns[T Integer](n, r int) []T {
	res := make([]T, 0, n)
	sz := (n + r - 1) / r
	var t T
	switch any(t).(type) {
	case int64:
		for _, v := range RandIntsn[int64](sz, 1<<BENCH_WIDTH-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case int32:
		for _, v := range RandInts[int32](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case int16:
		for _, v := range RandInts[int16](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case int8:
		for _, v := range RandInts[int8](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint64:
		for _, v := range RandUintsn[uint64](sz, 1<<BENCH_WIDTH-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint32:
		for _, v := range RandUints[uint32](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint16:
		for _, v := range RandUints[uint16](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint8:
		for _, v := range RandUints[uint8](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	}
	return res
}
