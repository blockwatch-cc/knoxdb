// Copyright (c) 2020-2025 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

package bloom

import (
	"encoding/binary"
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/hash"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

// Ensure filter can insert values and verify they exist.
func TestBytes(t *testing.T) {
	var num, fsize int
	if testing.Short() {
		num = 100000
		fsize = 1437758
	} else {
		num = 10000000
		fsize = 143775876
	}

	// These parameters will result, for 10M entries, with a bloom filter
	// with 0.001 false positive rate (1 in 1000 values will be incorrectly
	// identified as being present in the set).
	f := NewFilter(fsize)
	var v [4]byte
	for i := range num {
		binary.BigEndian.PutUint32(v[:], uint32(i))
		f.Add(hash.Hash(v[:]))
	}

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := range num {
		binary.BigEndian.PutUint32(v[:], uint32(i))
		require.True(t, f.Contains(hash.Hash(v[:])), "got false for value", v)
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		binary.BigEndian.PutUint32(v[:], uint32(i))
		if f.Contains(hash.Hash(v[:])) {
			fp++
		}
	}

	if fp > num/10 {
		// If we're an order of magnitude off, then it's arguable that there
		// is a bug in the bloom filter.
		t.Fatalf("got %d false positives which is an error rate of %f, expected error rate <=0.001", fp, float64(fp)/100000000)
	}
	t.Logf("Bloom false positive error rate was %f", float64(fp)/float64(num)/10)
}

// Ensure filter can insert values and verify they exist.
func TestUint32(t *testing.T) {
	var num, fsize int
	if testing.Short() {
		num = 100000
		fsize = 1437758
	} else {
		num = 10000000
		fsize = 143775876
	}

	// These parameters will result, for 10M entries, with a bloom filter
	// with 0.001 false positive rate (1 in 1000 values will be incorrectly
	// identified as being present in the set).
	f := NewFilter(fsize)
	slice := make([]uint32, num)
	for i := range num {
		slice[i] = uint32(i)
	}
	f.Add(hash.Vec32(slice, nil)...)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := range num {
		require.True(t, f.Contains(hash.Uint32(uint32(i))), "got false for value %d", i)
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if f.Contains(hash.Uint32(uint32(i))) {
			fp++
		}
	}

	if fp > num/10 {
		// If we're an order of magnitude off, then it's arguable that there
		// is a bug in the bloom filter.
		t.Fatalf("got %d false positives which is an error rate of %f, expected error rate <=0.001", fp, float64(fp)/100000000)
	}
	t.Logf("Bloom false positive error rate was %f", float64(fp)/float64(num)/10)
}

// Ensure filter can insert values and verify they exist.
func TestUint64(t *testing.T) {
	var num, fsize int
	if testing.Short() {
		num = 100000
		fsize = 1437758
	} else {
		num = 10000000
		fsize = 143775876
	}

	// These parameters will result, for 10M entries, with a bloom filter
	// with 0.001 false positive rate (1 in 1000 values will be incorrectly
	// identified as being present in the set).
	f := NewFilter(fsize)
	slice := make([]uint64, num)
	for i := range num {
		slice[i] = uint64(i)
	}
	f.Add(hash.Vec64(slice, nil)...)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := range num {
		require.True(t, f.Contains(hash.Uint64(uint64(i))), "got false for value %d", i)
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if f.Contains(hash.Uint64(uint64(i))) {
			fp++
		}
	}

	if fp > num/10 {
		// If we're an order of magnitude off, then it's arguable that there
		// is a bug in the bloom filter.
		t.Fatalf("got %d false positives which is an error rate of %f, expected error rate <=0.001", fp, float64(fp)/100000000)
	}
	t.Logf("Bloom false positive error rate was %f", float64(fp)/float64(num)/10)
}

func TestMerge(t *testing.T) {
	var num, fsize int
	if testing.Short() {
		num = 100000
		fsize = 1437758
	} else {
		num = 10000000
		fsize = 143775876
	}

	// These parameters will result, for 10M entries, with a bloom filter
	// with 0.001 false positive rate (1 in 1000 values will be incorrectly
	// identified as being present in the set).
	f := NewFilter(fsize)
	slice := make([]uint32, num/2)
	for i := 0; i < num/2; i++ {
		slice[i] = uint32(i)
	}
	f.Add(hash.Vec32(slice, nil)...)

	filter2 := NewFilter(fsize)
	for i := num / 2; i < num; i++ {
		slice[i-num/2] = uint32(i)
	}
	filter2.Add(hash.Vec32(slice, nil)...)

	f.Merge(filter2)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := range num {
		require.True(t, f.Contains(hash.Uint32(uint32(i))), "got false for value %d", i)
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if f.Contains(hash.Uint32(uint32(i))) {
			fp++
		}
	}

	if fp > num/10 {
		// If we're an order of magnitude off, then it's arguable that there
		// is a bug in the bloom filter.
		t.Fatalf("got %d false positives which is an error rate of %f, expected error rate <=0.001", fp, float64(fp)/100000000)
	}
	t.Logf("Bloom false positive error rate was %f", float64(fp)/float64(num)/10)
}

var bloomSizes = []struct {
	Name string
	M    int
	V    int
}{
	{"32kB", 32768, 1},
	{"128kB", 131072, 1},
	{"512kB", 524288, 1},
}

func BenchmarkAddManyBytesGo(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, s := range bloomSizes {
			data := make([][]byte, 0, c.N)
			buf := make([]byte, 4)
			for i := range c.N {
				binary.LittleEndian.PutUint32(buf, uint32(i))
				data = append(data, buf)
			}

			f := NewFilter(s.M)
			b.Run(fmt.Sprintf("%s/%s", c.Name, s.Name), func(b *testing.B) {
				b.SetBytes(4 * int64(c.N))
				for b.Loop() {
					f.Add(hash.Vec(data, nil)...)
				}
			})
		}
	}
}

func BenchmarkAddManyUint32Go(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, s := range bloomSizes {
			data := tests.GenSeq[uint32](c.N, 1)
			f := NewFilter(s.M)
			b.Run(fmt.Sprintf("%s/%s", c.Name, s.Name), func(b *testing.B) {
				b.SetBytes(4 * int64(c.N))
				for b.Loop() {
					f.Add(hash.Vec32(data, nil)...)
				}
			})
		}
	}
}

func BenchmarkAddManyUint64Go(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, s := range bloomSizes {
			data := tests.GenSeq[uint64](c.N, 1)
			f := NewFilter(s.M)
			b.Run(fmt.Sprintf("%s/%s", c.Name, s.Name), func(b *testing.B) {
				b.SetBytes(8 * int64(c.N))
				for b.Loop() {
					f.Add(hash.Vec64(data, nil)...)
				}
			})
		}
	}
}

func BenchmarkContainsGo(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, s := range bloomSizes {
			data := make([][]byte, 0, c.N)
			notData := make([][]byte, 0, c.N)
			for i := range c.N {
				data = append(data, fmt.Appendf(nil, "%d", i))
				notData = append(notData, fmt.Appendf(nil, "%d", c.N+i))
			}

			f := NewFilter(s.M)
			f.Add(hash.Vec(data, nil)...)

			b.Run(fmt.Sprintf("%s/%s/IN", c.Name, s.Name), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = f.Contains(hash.Hash(data[util.RandIntn(c.N)]))
				}
			})

			// not in
			b.Run(fmt.Sprintf("%s/%s/NI", c.Name, s.Name), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = f.Contains(hash.Hash(notData[util.RandIntn(c.N)]))
				}
			})
		}
	}
}

func BenchmarkMergeGo(b *testing.B) {
	for _, s := range bloomSizes {
		filter1 := NewFilter(s.M)
		filter2 := NewFilter(s.M)
		b.Run(s.Name, func(b *testing.B) {
			b.SetBytes(int64(s.M >> 3))
			for range b.N {
				filter1.Merge(filter2)
			}
		})
	}
}
