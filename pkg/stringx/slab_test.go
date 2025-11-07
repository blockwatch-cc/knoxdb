// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stringx

import (
	"bytes"
	"sync"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSlabPoolAppend(t *testing.T) {
	pool := NewSlabPool(64)
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

func TestSlabPoolAllocAppend(t *testing.T) {
	// default page size: 64k
	// num strings: 4k
	// string len: 2k strings
	// expected allocs: 8
	// expected grow page slice: 1 (from cap 8 to 16)
	pool := NewSlabPool(4096)
	data := util.RandByteSlices(4096, 2048)
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
			require.Equal(t, data[a+i], rg.Get(i), "range pos real=%d effective=%d", a+i, i)
		}
	}
}

func TestSlabPoolSet(t *testing.T) {
	pool := NewSlabPool(64)
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

func TestSlabPoolDelete(t *testing.T) {
	for range 16 {
		pool := NewSlabPool(64)
		pool.AppendMany(util.RandByteSlices(64, 8)...) // 64x length 8
		require.Equal(t, 64, pool.Len(), "len")
		a, b := util.RandIntRange(0, 64)
		pool.Delete(a, b)
		require.Equal(t, 64-(b-a), pool.Len(), "len")
	}
}

func TestSlabPoolCmp(t *testing.T) {
	for range 16 {
		pool := NewSlabPool(64)
		pool.AppendMany(util.RandByteSlices(64, 8)...) // 64x length 8
		a, b := util.RandIntn(64), util.RandIntn(64)
		require.Equal(t, bytes.Compare(pool.Get(a), pool.Get(b)), pool.Cmp(a, b), "cmp")
	}
}

func TestSlabPoolExtremes(t *testing.T) {
	// min max
	pool := NewSlabPool(64)
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

func TestSlabPoolIterators(t *testing.T) {
	pool := NewSlabPool(64)
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

func TestSlabPoolParallel(t *testing.T) {
	pool := NewSlabPool(1 << 16)
	data := util.RandByteSlices(64, 8) // 64x length 8
	pool.AppendMany(data...)

	var (
		errg errgroup.Group
		mu   sync.Mutex
	)
	errg.SetLimit(32)

	// run one less than 1024 to not overflow pool
	for range 1023 {
		// writer (synced)
		errg.Go(func() error {
			mu.Lock()
			defer mu.Unlock()
			for _, v := range data {
				pool.Append(v)
			}
			return nil
		})

		// readers (concurrent)
		errg.Go(func() error {
			for range 1024 {
				n := util.RandIntn(pool.Len())
				buf := pool.Get(n)
				if len(buf) > 0 {
					require.Equal(t, buf, data[n%64], "string mismatch")
				}
			}
			return nil
		})
	}

	require.NoError(t, errg.Wait())
}

func BenchmarkSlabPoolAppend(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		src := util.RandByteSlices(sz.N, 32)
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				pool := NewSlabPoolSize(sz.N, 33*sz.N+1)
				for _, v := range src {
					pool.Append(v)
				}
				pool.Close()
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkSlabPoolGet(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		src := util.RandByteSlices(sz.N, 32)
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			pool := NewSlabPoolSize(sz.N, 33*sz.N+1)
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

func BenchmarkSlabPoolCmp(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		src := util.RandByteSlices(sz.N, 32)
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			pool := NewSlabPoolSize(sz.N, 32)
			pool.AppendMany(src...)
			for b.Loop() {
				for i := range src {
					pool.Cmp(i, (i+1)%sz.N)
				}
			}
			pool.Close()
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkSlabPoolIterator(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		pool := NewSlabPoolSize(sz.N, 32)
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

func BenchmarkSlabPoolChunk(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		pool := NewSlabPoolSize(sz.N, 32)
		for range sz.N {
			pool.Append(util.RandBytes(32))
		}
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				it := pool.Chunks()
				for {
					c, n := it.NextChunk()
					if n == 0 {
						break
					}
					_ = c
				}
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
		pool.Close()
	}
}

func BenchmarkSlabPoolMinMax(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		pool := NewSlabPoolSize(sz.N, 32)
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

func BenchmarkSlabPoolAppendTo(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		pool := NewSlabPoolSize(sz.N, 32)
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
