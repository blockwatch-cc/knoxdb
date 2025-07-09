// Copyright (c) 2020-2025 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

package bloom

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/cpu"
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/internal/tests"
)

func TestUint32AVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.Skip()
	}

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
	add_u32_avx2(f, slice)

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

func TestUint64AVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.Skip()
	}

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
	add_u64_avx2(f, slice)

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

func TestMergeAVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.Skip()
	}

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
	add_u32_avx2(f, slice)

	filter2 := NewFilter(fsize)
	for i := num / 2; i < num; i++ {
		slice[i-num/2] = uint32(i)
	}
	add_u32_avx2(filter2, slice)

	merge_core_avx2(f.bits, filter2.bits)

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

func BenchmarkAddManyUint32AVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.Skip()
	}
	for _, c := range tests.BenchmarkSizes {
		for _, s := range bloomSizes {
			data := tests.GenSeq[uint32](c.N, 1)
			filter := NewFilter(s.M)
			b.Run(fmt.Sprintf("%s/%s", c.Name, s.Name), func(b *testing.B) {
				b.SetBytes(4 * int64(c.N))
				for range b.N {
					add_u32_avx2(filter, data)
				}
			})
		}
	}
}

func BenchmarkAddManyUint64AVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.Skip()
	}
	for _, c := range tests.BenchmarkSizes {
		for _, s := range bloomSizes {
			data := tests.GenSeq[uint64](c.N, 1)
			filter := NewFilter(s.M)
			b.Run(fmt.Sprintf("%s/%s", c.Name, s.Name), func(b *testing.B) {
				b.SetBytes(8 * int64(c.N))
				for range b.N {
					add_u64_avx2(filter, data)
				}
			})
		}
	}
}

func BenchmarkMergeAVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.Skip()
	}
	for _, s := range bloomSizes {
		filter1 := NewFilter(s.M)
		filter2 := NewFilter(s.M)

		b.Run(s.Name, func(b *testing.B) {
			b.SetBytes(int64(s.M >> 3))
			for range b.N {
				merge_core_avx2(filter1.bits, filter2.bits)
			}
		})
	}
}
