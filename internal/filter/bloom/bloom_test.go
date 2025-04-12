// Copyright (c) 2020-2025 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

package bloom

import (
	"encoding/binary"
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

// Ensure filter can insert values and verify they exist.
func TestBytesGo(t *testing.T) {
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
	v := make([]byte, 4)
	for i := 0; i < num; i++ {
		binary.BigEndian.PutUint32(v, uint32(i))
		f.Add(v)
	}

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		binary.BigEndian.PutUint32(v, uint32(i))
		if !f.ContainsHash(filter.Hash(v)) {
			t.Fatalf("got false for value %q, expected true", v)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		binary.BigEndian.PutUint32(v, uint32(i))
		if f.ContainsHash(filter.Hash(v)) {
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
func TestUint32Go(t *testing.T) {
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
	for i := 0; i < num; i++ {
		slice[i] = uint32(i)
	}
	add_u32_purego(f, slice)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !f.ContainsHash(filter.HashUint32(uint32(i))) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if f.ContainsHash(filter.HashUint32(uint32(i))) {
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
func TestUint64Go(t *testing.T) {
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
	for i := 0; i < num; i++ {
		slice[i] = uint64(i)
	}
	add_u64_purego(f, slice)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !f.ContainsHash(filter.HashUint64(uint64(i))) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if f.ContainsHash(filter.HashUint64(uint64(i))) {
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

func TestMergeGo(t *testing.T) {
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
	add_u32_purego(f, slice)

	filter2 := NewFilter(fsize)
	for i := num / 2; i < num; i++ {
		slice[i-num/2] = uint32(i)
	}
	add_u32_purego(filter2, slice)

	merge_purego(f.bits, filter2.bits)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !f.ContainsHash(filter.HashUint32(uint32(i))) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if f.ContainsHash(filter.HashUint32(uint32(i))) {
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
			for i := 0; i < c.N; i++ {
				binary.LittleEndian.PutUint32(buf, uint32(i))
				data = append(data, buf)
			}

			f := NewFilter(s.M)
			b.Run(fmt.Sprintf("%s/%s", c.Name, s.Name), func(b *testing.B) {
				b.SetBytes(4 * int64(c.N))
				for range b.N {
					f.AddMany(data)
				}
			})
		}
	}
}

func BenchmarkAddManyUint32Go(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, s := range bloomSizes {
			data := tests.GenSeq[uint32](c.N)
			f := NewFilter(s.M)
			b.Run(fmt.Sprintf("%s/%s", c.Name, s.Name), func(b *testing.B) {
				b.SetBytes(4 * int64(c.N))
				for range b.N {
					add_u32_purego(f, data)
				}
			})
		}
	}
}

func BenchmarkAddManyUint64Go(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, s := range bloomSizes {
			data := tests.GenSeq[uint64](c.N)
			f := NewFilter(s.M)
			b.Run(fmt.Sprintf("%s/%s", c.Name, s.Name), func(b *testing.B) {
				b.SetBytes(8 * int64(c.N))
				for range b.N {
					add_u64_purego(f, data)
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
				data = append(data, []byte(fmt.Sprintf("%d", i)))
				notData = append(notData, []byte(fmt.Sprintf("%d", c.N+i)))
			}

			f := NewFilter(s.M)
			add_strings_purego(f, data)

			b.Run(fmt.Sprintf("%s/%s/IN", c.Name, s.Name), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					_ = f.ContainsHash(filter.Hash(data[util.RandIntn(c.N)]))
				}
			})

			// not in
			b.Run(fmt.Sprintf("%s/%s/NI", c.Name, s.Name), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					_ = f.ContainsHash(filter.Hash(notData[util.RandIntn(c.N)]))
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
				merge_purego(filter1.bits, filter2.bits)
			}
		})
	}
}
