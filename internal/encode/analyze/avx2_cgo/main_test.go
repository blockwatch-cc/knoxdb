package main

import (
	"math"
	"math/rand/v2"
	"testing"

	"golang.org/x/exp/constraints"
)

type Signed interface {
	int64 | int32 | int16 | int8
}

type Unsigned interface {
	uint64 | uint32 | uint16 | uint8
}

type TestCase[T Integer] struct {
	name     string
	input    []T
	expMin   T
	expMax   T
	expDelta T
	expRuns  T
}

func makeSignedTests[T Signed]() []TestCase[T] {
	return []TestCase[T]{
		{"Empty", []T{}, 0, 0, 0, 0},
		{"Single", []T{42}, 42, 42, 0, 1},
		{"DeltaNoDups", []T{-1, 0, 1, 2}, -1, 2, 1, 4},
		{"Runs", []T{-1, -1, 5, 5, 1, 1}, -1, 5, 0, 3},
		{"DictFriendly", []T{-1, 1, 5, 1, -1, 1}, -1, 5, 0, 6},
		{"AllSame", []T{5, 5, 5, 5, 5}, 5, 5, 0, 1},
		{"Alternating", []T{1, 0, 1, 0, 1}, 0, 1, 0, 5},
		{"LargeDelta", []T{10, 20, 30, 40, 50}, 10, 50, 10, 5},
		{"Bounds", []T{MinVal[T]().(T), 0, MaxVal[T]().(T)}, MinVal[T]().(T), MaxVal[T]().(T), 0, 3},
		{"Short", []T{1, 2, 3}, 1, 3, 1, 3},
		{"MixedRuns", []T{1, 1, 2, 2, 5, 8, 8}, 1, 8, 0, 4},
		{"Unaligned", []T{1, 2, 3, 4, 5, 6, 7}, 1, 7, 1, 7},
		{"Random", []T{3, 1, 4, 1, 5, 9, 2, 6, 5, 3}, 1, 9, 0, 10},
		{"NegDelta", []T{
			32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17,
			16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0,
		}, 0, 32, -1, 33},
		{"LongDelta", []T{
			-32, -31, -30, -29, -28, -27, -26, -25, -24, -23, -22, -21, -20, -19, -18, -17,
			-16, -15, -14, -13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1,
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
			16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
		}, -32, 32, 1, 65},
		{"LongRuns", []T{
			1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
			4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
		}, 1, 4, 0, 4},
	}
}

func makeUnsignedTests[T Unsigned]() []TestCase[T] {
	return []TestCase[T]{
		{"Empty", []T{}, 0, 0, 0, 0},
		{"Single", []T{42}, 42, 42, 0, 1},
		{"DeltaNoDups", []T{0, 1, 2, 3}, 0, 3, 1, 4},
		{"Runs", []T{0, 0, 5, 5, 1, 1}, 0, 5, 0, 3},
		{"DictFriendly", []T{0, 1, 5, 1, 0, 1}, 0, 5, 0, 6},
		{"AllSame", []T{5, 5, 5, 5, 5}, 5, 5, 0, 1},
		{"Alternating", []T{1, 0, 1, 0, 1}, 0, 1, 0, 5},
		{"LargeDelta", []T{10, 20, 30, 40, 50}, 10, 50, 10, 5},
		{"Bounds", []T{MinVal[T]().(T), 0, MaxVal[T]().(T)}, MinVal[T]().(T), MaxVal[T]().(T), 0, 2},
		{"Short", []T{1, 2, 3}, 1, 3, 1, 3},
		{"MixedRuns", []T{1, 1, 2, 2, 5, 8, 8}, 1, 8, 0, 4},
		{"Unaligned", []T{1, 2, 3, 4, 5, 6, 7}, 1, 7, 1, 7},
		{"Random", []T{3, 1, 4, 1, 5, 9, 2, 6, 5, 3}, 1, 9, 0, 10},
		{"NegDelta", []T{
			32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17,
			16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0,
		}, 0, 32, MaxVal[T]().(T), 33},
		{"LongDelta", []T{
			32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17,
			16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0,
		}, 0, 32, MaxVal[T]().(T), 33},
		{"LongRuns", []T{
			1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
			4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
		}, 1, 4, 0, 4},
	}
}

type AnalyzeFunc[T Integer] func([]T) (T, T, T, T)

func analyzeTest[T Integer](t *testing.T, cases []TestCase[T], fn AnalyzeFunc[T]) {
	t.Helper()
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			minv, maxv, delta, numRuns := fn(tt.input)
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

func TestAnalyzeInt64(t *testing.T) {
	analyzeTest[int64](t, makeSignedTests[int64](), AnalyzeInt64)
}

func TestAnalyzeUint64(t *testing.T) {
	analyzeTest[uint64](t, makeUnsignedTests[uint64](), AnalyzeUint64)
}

func TestAnalyzeInt32(t *testing.T) {
	analyzeTest[int32](t, makeSignedTests[int32](), AnalyzeInt32)
}

func TestAnalyzeUint32(t *testing.T) {
	analyzeTest[uint32](t, makeUnsignedTests[uint32](), AnalyzeUint32)
}

func TestAnalyzeInt16(t *testing.T) {
	analyzeTest[int16](t, makeSignedTests[int16](), AnalyzeInt16)
}

func TestAnalyzeUint16(t *testing.T) {
	analyzeTest[uint16](t, makeUnsignedTests[uint16](), AnalyzeUint16)
}

func TestAnalyzeInt8(t *testing.T) {
	analyzeTest[int8](t, makeSignedTests[int8](), AnalyzeInt8)
}

func TestAnalyzeUint8(t *testing.T) {
	analyzeTest[uint8](t, makeUnsignedTests[uint8](), AnalyzeUint8)
}

func BenchmarkAnalyzeInt64(b *testing.B) {
	for _, c := range makeBenchmarks[int64]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for i := 0; i < b.N; i++ {
				AnalyzeInt64(c.Data)
			}
		})
	}
}

func BenchmarkAnalyzeUint64(b *testing.B) {
	for _, c := range makeBenchmarks[uint64]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for i := 0; i < b.N; i++ {
				AnalyzeUint64(c.Data)
			}
		})
	}
}

func BenchmarkAnalyzeInt32(b *testing.B) {
	for _, c := range makeBenchmarks[int32]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 4))
			for i := 0; i < b.N; i++ {
				AnalyzeInt32(c.Data)
			}
		})
	}
}

func BenchmarkAnalyzeUint32(b *testing.B) {
	for _, c := range makeBenchmarks[uint32]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 4))
			for i := 0; i < b.N; i++ {
				AnalyzeUint32(c.Data)
			}
		})
	}
}

func BenchmarkAnalyzeInt16(b *testing.B) {
	for _, c := range makeBenchmarks[int16]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 2))
			for i := 0; i < b.N; i++ {
				AnalyzeInt16(c.Data)
			}
		})
	}
}

func BenchmarkAnalyzeUint16(b *testing.B) {
	for _, c := range makeBenchmarks[uint16]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 2))
			for i := 0; i < b.N; i++ {
				AnalyzeUint16(c.Data)
			}
		})
	}
}

func BenchmarkAnalyzeInt8(b *testing.B) {
	for _, c := range makeBenchmarks[int8]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data)))
			for i := 0; i < b.N; i++ {
				AnalyzeInt8(c.Data)
			}
		})
	}
}

func BenchmarkAnalyzeUint8(b *testing.B) {
	for _, c := range makeBenchmarks[uint8]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data)))
			for i := 0; i < b.N; i++ {
				AnalyzeUint8(c.Data)
			}
		})
	}
}

type Benchmark[T Integer] struct {
	Name string
	Data []T
}

func makeBenchmarks[T Integer]() []Benchmark[T] {
	return []Benchmark[T]{
		{"dups_1K", GenDups[T](1024, 10)}, // 10% unique
		{"dups_16K", GenDups[T](16*1024, 10)},
		{"dups_64K", GenDups[T](64*1024, 10)},

		{"runs_1K", GenRuns[T](1024, 10)}, // run length 10
		{"runs_16K", GenRuns[T](16*1024, 10)},
		{"runs_64K", GenRuns[T](64*1024, 10)},

		{"seq_1K", GenSequence[T](1024)},
		{"seq_16K", GenSequence[T](16 * 1024)},
		{"seq_64K", GenSequence[T](64 * 1024)},
	}
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

func MinVal[T Integer]() any {
	switch any(T(0)).(type) {
	case int64:
		return int64(math.MinInt64)
	case int32:
		return int32(math.MinInt32)
	case int16:
		return int16(math.MinInt16)
	case int8:
		return int8(math.MinInt8)
	case uint64:
		return uint64(0)
	case uint32:
		return uint32(0)
	case uint16:
		return uint16(0)
	case uint8:
		return uint8(0)
	default:
		return nil
	}
}

func MaxVal[T Integer]() any {
	switch any(T(0)).(type) {
	case int64:
		return int64(math.MaxInt64)
	case int32:
		return int32(math.MaxInt32)
	case int16:
		return int16(math.MaxInt16)
	case int8:
		return int8(math.MaxInt8)
	case uint64:
		return uint64(math.MaxUint64)
	case uint32:
		return uint32(math.MaxUint32)
	case uint16:
		return uint16(math.MaxUint16)
	case uint8:
		return uint8(math.MaxUint8)
	default:
		return nil
	}
}
