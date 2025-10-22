// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stringx

import (
	"bytes"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestStringPoolAppend(t *testing.T) {
	pool := NewStringPool(64)
	data := util.RandByteSlices(64, 8) // 64x length 8
	for i, v := range data {
		n := pool.Append(v)
		require.Equal(t, i, n, "append index")
		require.Equal(t, i+1, pool.Len(), "len")
		require.Equal(t, v, pool.Get(n), "get content")
		require.Equal(t, string(v), pool.GetString(n), "get content as string")
	}
	// test random ranges
	for range 16 {
		a, b := util.RandIntRange(0, len(data))
		rg := pool.Range(a, b)
		require.Equal(t, b-a, rg.Len(), "subrange length")
		for i := range b - a {
			require.Equal(t, data[a+i], rg.Get(i), "range pos", a+i, i)
		}
	}
}

func TestStringPoolSet(t *testing.T) {
	pool := NewStringPool(64)
	truth := make(map[int][]byte)
	// fill with random length strings
	for i := range 64 {
		l := util.RandIntn(64)
		v := util.RandBytes(l)
		truth[i] = v
		n := pool.Append(v)
		require.Equal(t, i, n, "append index")
		require.Equal(t, i+1, pool.Len(), "len")
		require.Equal(t, v, pool.Get(n), "get content")
		require.Equal(t, string(v), pool.GetString(n), "get content as string")
	}
	// set some random values
	for i := range util.RandIntsn(16, 64) {
		l := util.RandIntn(64)
		v := util.RandBytes(l)
		pool.Set(i, v)
		require.Equal(t, len(truth), pool.Len(), "len")
		require.Equal(t, v, pool.Get(i), "get content")
	}
}

func TestStringPoolDelete(t *testing.T) {
	for range 16 {
		pool := NewStringPool(64)
		pool.AppendMany(util.RandByteSlices(64, 8)...) // 64x length 8
		require.Equal(t, 64, pool.Len(), "len")
		a, b := util.RandIntRange(0, 64)
		pool.Delete(a, b)
		require.Equal(t, 64-(b-a), pool.Len(), "len")
	}
}

func TestStringPoolCmp(t *testing.T) {
	for range 16 {
		pool := NewStringPool(64)
		pool.AppendMany(util.RandByteSlices(64, 8)...) // 64x length 8
		a, b := util.RandIntn(64), util.RandIntn(64)
		require.Equal(t, bytes.Compare(pool.Get(a), pool.Get(b)), pool.Cmp(a, b), "cmp")
	}
}

func TestStringPoolExtremes(t *testing.T) {
	// min max
	pool := NewStringPool(64)
	require.Equal(t, []byte(nil), pool.Min(), "empty min")
	require.Equal(t, []byte(nil), pool.Max(), "empty max")
	la, lb := pool.MinMaxLen()
	require.Equal(t, 0, la, "empty min len")
	require.Equal(t, 0, lb, "empty max len")

	// add a couple strings
	pool.AppendString("hello")
	pool.AppendString("world")
	pool.AppendString("how")
	pool.AppendString("are") // min
	pool.AppendString("you") // max

	// check we get the correct min/max out
	require.Equal(t, "are", string(pool.Min()), "min")
	require.Equal(t, "you", string(pool.Max()), "max")
	la, lb = pool.MinMaxLen()
	require.Equal(t, 3, la, "min len")
	require.Equal(t, 5, lb, "max len")
}

func TestStringPoolIterators(t *testing.T) {
	pool := NewStringPool(64)
	data := util.RandByteSlices(64, 8) // 64x length 8
	pool.AppendMany(data...)

	// values
	var i int
	for v := range pool.Values() {
		require.Equal(t, data[i], v, "value", i)
		i++
	}

	// iterator
	for i, v := range pool.Iterator() {
		require.Equal(t, data[i], v, "it", i)
	}

	// StringIterator
	it := pool.Chunks()
	require.Equal(t, len(data), it.Len(), "it len")
	for i, v := range data {
		require.Equal(t, v, it.Get(i))
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

type BenchmarkMask struct {
	Name    string
	Pattern []byte
}

var BenchmarkMasks = []BenchmarkMask{
	{"0xFA", []byte{0xfa}},
	{"0x10", []byte{0x10}},
	{"0xFF", []byte{0xff}}, // translates to no mask
}

func BenchmarkStringPoolAppend(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		src := util.RandByteSlices(sz.N, 32)
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				pool := NewStringPoolSize(sz.N, 32)
				for _, v := range src {
					pool.Append(v)
				}
				pool.Close()
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkStringPoolGet(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		src := util.RandByteSlices(sz.N, 32)
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			pool := NewStringPoolSize(sz.N, 32)
			pool.AppendMany(src...)
			for b.Loop() {
				for i := range src {
					pool.Get(i)
				}
			}
			pool.Close()
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkStringPoolIterator(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		pool := NewStringPoolSize(sz.N, 32)
		for range sz.N {
			pool.Append(util.RandBytes(32))
		}
		var x int
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				for _, v := range pool.Iterator() {
					x += len(v)
				}
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
		_ = x
		pool.Close()
	}
}

func BenchmarkStringPoolMinMax(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		pool := NewStringPoolSize(sz.N, 32)
		for range sz.N {
			pool.Append(util.RandBytes(32))
		}
		var minv, maxv []byte
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				minv, maxv = pool.MinMax()
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
		_ = minv
		_ = maxv
		pool.Close()
	}
}

func BenchmarkStringPoolAppendTo(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		pool := NewStringPoolSize(sz.N, 32)
		for range sz.N {
			pool.Append(util.RandBytes(32))
		}
		var x int
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				p2 := NewStringPoolSize(sz.N, 32)
				pool.AppendTo(p2, nil)
				p2.Close()
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
		_ = x
		pool.Close()
	}
}

func BenchmarkByteSliceAppend(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		src := util.RandByteSlices(sz.N, 32)
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				dst := make([][]byte, 0, len(src))
				for _, v := range src {
					dst = append(dst, bytes.Clone(v))
				}
				_ = dst
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkByteSliceGet(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		src := util.RandByteSlices(sz.N, 32)
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				var dst []byte
				for i := range src {
					dst = src[i]
				}
				_ = dst
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkByteSliceIterator(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		src := util.RandByteSlices(sz.N, 32)
		var x int
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				for _, v := range src {
					x += len(v)
				}
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
		_ = x
	}
}
