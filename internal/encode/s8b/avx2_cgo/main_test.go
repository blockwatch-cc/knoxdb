package main

import (
	"math/rand/v2"
	"slices"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

type Signed interface {
	int64 | int32 | int16 | int8
}

type Unsigned interface {
	uint64 | uint32 | uint16 | uint8
}

type Integer interface {
	Signed | Unsigned
}

func TestEncodeUint64(t *testing.T) {
	EncodeTest[uint64](t, EncodeUint64, DecodeLegacyWrapper[uint64])
}

func BenchmarkEncodeUint64(b *testing.B) {
	EncodeBenchmark[uint64](b, EncodeUint64)
}

func TestEncodeUint32(t *testing.T) {
	EncodeTest[uint32](t, EncodeUint32, DecodeLegacyWrapper[uint32])
}

func BenchmarkEncodeUint32(b *testing.B) {
	EncodeBenchmark[uint32](b, EncodeUint32)
}

func TestEncodeUint16(t *testing.T) {
	EncodeTest[uint16](t, EncodeUint16, DecodeLegacyWrapper[uint16])
}

func BenchmarkEncodeUint16(b *testing.B) {
	EncodeBenchmark[uint16](b, EncodeUint16)
}

func TestEncodeUint8(t *testing.T) {
	EncodeTest[uint8](t, EncodeUint8, DecodeLegacyWrapper[uint8])
}

func BenchmarkEncodeUint8(b *testing.B) {
	EncodeBenchmark[uint8](b, EncodeUint8)
}

func DecodeLegacyWrapper[T Unsigned](dst []T, buf []byte) (int, error) {
	src := FromByteSlice[uint64](buf)
	switch any(T(0)).(type) {
	case uint64:
		return DecodeLegacy(ReinterpretSlice[T, uint64](dst), src)
	default:
		u64 := make([]uint64, len(dst))
		n, err := DecodeLegacy(u64, src)
		if err != nil {
			return 0, err
		}
		for i := range n {
			dst[i] = T(u64[i])
		}
		return n, nil
	}
}

func EncodeBenchmark[T Unsigned](b *testing.B, fn EncodeFunc[T]) {
	for _, c := range MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf := make([]byte, 8*len(c.Data))
		var sz, n int
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * int(unsafe.Sizeof(T(0)))))
			for i := 0; i < b.N; i++ {
				buf, _ := fn(buf, c.Data, minv, maxv)
				sz += len(buf)
				n++
			}
			b.ReportMetric(float64(sz)/float64(n), "mean_bytes")
			// b.ReportMetric(float64(minv), "min_val")
			// b.ReportMetric(float64(maxv), "max_val")
		})
	}
}

func FromByteSlice[T Integer](s []byte) []T {
	return unsafe.Slice(
		(*T)(unsafe.Pointer(unsafe.SliceData(s))),
		len(s)/int(unsafe.Sizeof(T(0))),
	)
}

func ReinterpretSlice[T, S Integer](t []T) []S {
	if unsafe.Sizeof(T(0)) == unsafe.Sizeof(S(0)) {
		return *(*[]S)(unsafe.Pointer(&t))
	}
	return nil
}

type S8bTests[T Unsigned] struct {
	Name string
	In   []T
	Fn   func() []T
	Err  bool
}

func MakeTests[T Unsigned]() []S8bTests[T] {
	width := unsafe.Sizeof(T(0))
	tests := []S8bTests[T]{
		{Name: "nil", In: nil},
		{Name: "empty", In: []T{}},
		{Name: "mixed sizes", In: []T{7, 6, 255, 4, 3, 2, 1}},
		{Name: "240 ones", Fn: ones[T](240)},
		{Name: "120 ones plus 5", Fn: func() []T {
			in := ones[T](240)()
			in[120] = 5
			return in
		}},
		{Name: "119 ones plus 5", Fn: func() []T {
			in := ones[T](240)()
			in[119] = 5
			return in
		}},
		{Name: "239 ones plus 5", Fn: func() []T {
			in := ones[T](241)()
			in[239] = 5
			return in
		}},
		{Name: "1 bit", Fn: bits[T](120, 1)},
		{Name: "2 bits", Fn: bits[T](120, 2)},
		{Name: "3 bits", Fn: bits[T](120, 3)},
		{Name: "4 bits", Fn: bits[T](120, 4)},
		{Name: "5 bits", Fn: bits[T](120, 5)},
		{Name: "6 bits", Fn: bits[T](120, 6)},
		{Name: "7 bits", Fn: bits[T](120, 7)},
		{Name: "8 bits", Fn: bits[T](120, 8)},
	}
	combi := S8bTests[T]{
		Name: "combination",
		Fn: combine[T](
			bits[T](120, 1),
			bits[T](120, 2),
			bits[T](120, 3),
			bits[T](120, 4),
			bits[T](120, 5),
			bits[T](120, 6),
			bits[T](120, 7),
			bits[T](120, 8),
		)}

	if width > 1 {
		tests = append(tests, []S8bTests[T]{
			{Name: "10 bits", Fn: bits[T](120, 10)},
			{Name: "12 bits", Fn: bits[T](120, 12)},
			{Name: "15 bits", Fn: bits[T](120, 15)},
		}...)
		combi.Fn = combine[T](
			bits[T](120, 1),
			bits[T](120, 2),
			bits[T](120, 3),
			bits[T](120, 4),
			bits[T](120, 5),
			bits[T](120, 6),
			bits[T](120, 7),
			bits[T](120, 8),
			bits[T](120, 10),
			bits[T](120, 12),
			bits[T](120, 15),
			bits[T](120, 16),
		)
	}

	if width > 2 {
		tests = append(tests, []S8bTests[T]{
			{Name: "20 bits", Fn: bits[T](120, 20)},
			{Name: "30 bits", Fn: bits[T](120, 30)},
			{Name: "32 bits", Fn: bits[T](120, 32)},
			{Name: "Last big", In: ReinterpretSlice[uint32, T]([]uint32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0x3FFFFFFF})},
		}...)
		combi.Fn = combine[T](
			bits[T](120, 1),
			bits[T](120, 2),
			bits[T](120, 3),
			bits[T](120, 4),
			bits[T](120, 5),
			bits[T](120, 6),
			bits[T](120, 7),
			bits[T](120, 8),
			bits[T](120, 10),
			bits[T](120, 12),
			bits[T](120, 15),
			bits[T](120, 20),
			bits[T](120, 30),
			bits[T](120, 32),
		)
	}
	if width > 4 {
		tests = append(tests, []S8bTests[T]{
			{Name: "60 bits", Fn: bits[T](120, 60)},
			{
				Name: "too big",
				In:   ReinterpretSlice[uint64, T]([]uint64{7, 6, 2<<61 - 1, 4, 3, 2, 1}),
				Err:  true,
			},
		}...)

		combi.Fn = combine[T](
			bits[T](120, 1),
			bits[T](120, 2),
			bits[T](120, 3),
			bits[T](120, 4),
			bits[T](120, 5),
			bits[T](120, 6),
			bits[T](120, 7),
			bits[T](120, 8),
			bits[T](120, 10),
			bits[T](120, 12),
			bits[T](120, 15),
			bits[T](120, 20),
			bits[T](120, 30),
			bits[T](120, 60),
		)
	}

	return append(tests, combi)
}

type EncodeFunc[T Integer] func([]byte, []T, T, T) ([]byte, error)
type DecodeFunc[T Unsigned] func([]T, []byte) (int, error)

func EncodeTest[T Unsigned](t *testing.T, enc EncodeFunc[T], dec DecodeFunc[T]) {
	for _, test := range MakeTests[T]() {
		t.Run(test.Name, func(t *testing.T) {
			in := test.In
			if test.Fn != nil {
				in = test.Fn()
			}
			var _, maxv T
			if len(in) > 0 {
				_, maxv = slices.Min(in), slices.Max(in)
			}
			buf := make([]byte, len(in)*8)

			// encode without min-FOR to be compatible with testcase data
			// testing all selectors
			buf, err := enc(buf, slices.Clone(in), 0, maxv)
			if test.Err {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err, "vals %x", in)
			}

			dst := make([]T, len(in))
			n, err := dec(dst, buf)
			require.NoError(t, err)

			if len(in) > 0 {
				require.Equal(t, in, dst[:n])
			}
		})
	}
}

var (
	RandIntn    = rand.IntN
	RandInt64   = rand.Int64
	RandInt64n  = rand.Int64N
	RandUint64n = rand.Uint64N
	RandUint64  = rand.Uint64
)

const BENCH_WIDTH = 60

func ones[T Unsigned](n int) func() []T {
	return func() []T {
		in := make([]T, n)
		for i := 0; i < n; i++ {
			in[i] = 1
		}
		return in
	}
}

// bits generates sequence of n numbers with max bits,
// ensuring max bit is set for 50% of the values.
func bits[T Unsigned](n, bits int) func() []T {
	return func() []T {
		out := make([]T, n)
		maxVal := T(1<<uint8(bits) - 1)
		for i := range out {
			topBit := T((i & 1) << uint8(bits-1))
			out[i] = T(RandInt64n(int64(maxVal))) | topBit
			if out[i] > maxVal {
				panic("max")
			}
		}
		return out
	}
}

func combine[T Unsigned](fns ...func() []T) func() []T {
	return func() []T {
		var out []T
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}

type BenchmarkSize struct {
	Name string
	N    int
}

var BenchmarkSizes = []BenchmarkSize{
	{"1k", 1024},
	{"16k", 16 * 1024},
	{"64k", 64 * 1024},
}

type Benchmark[T Integer] struct {
	Name string
	Data []T
}

func MakeBenchmarks[T Integer]() []Benchmark[T] {
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

func RandIntsn[T Signed](sz int, max T) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandInt64n(int64(max)))
	}
	return s
}

func RandInts[T Signed](sz int) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandInt64())
	}
	return s
}

func RandUintsn[T Unsigned](sz int, max T) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandUint64n(uint64(max)))
	}
	return s
}

func RandUints[T Unsigned](sz int) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandUint64())
	}
	return s
}

func GenSequence[T Integer](n int) []T {
	res := make([]T, n)
	for i := range res {
		res[i] = T(i)
	}
	return res
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
		unique := RandIntsn[int32](c, 1<<(BENCH_WIDTH/2-1))
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
		unique := RandUintsn[uint32](c, 1<<(BENCH_WIDTH/2-1))
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
		for _, v := range RandIntsn[int32](sz, 1<<(BENCH_WIDTH/2-1)) {
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
		for _, v := range RandUintsn[uint32](sz, 1<<(BENCH_WIDTH/2-1)) {
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
