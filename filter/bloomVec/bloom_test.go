package bloomVec

import (
	"encoding/binary"
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/util"
)

// Ensure filter can insert values and verify they exist.
func TestFilterAddContains(t *testing.T) {
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
	filter := NewFilter(fsize)
	v := make([]byte, 4)
	for i := 0; i < num; i++ {
		binary.BigEndian.PutUint32(v, uint32(i))
		filter.Add(v)
	}

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		binary.BigEndian.PutUint32(v, uint32(i))
		if !filter.Contains(v) {
			t.Fatalf("got false for value %q, expected true", v)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		binary.BigEndian.PutUint32(v, uint32(i))
		if filter.Contains(v) {
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
func TestFilterAddContainsUint32Generic(t *testing.T) {
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
	filter := NewFilter(fsize)
	slice := make([]uint32, num)
	for i := 0; i < num; i++ {
		slice[i] = uint32(i)
	}
	filterAddManyUint32Generic(*filter, slice, xxHash32Seed)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsUint32(uint32(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsUint32(uint32(i)) {
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

func TestFilterAddContainsUint32AVX2(t *testing.T) {
	if !util.UseAVX2 {
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
	filter := NewFilter(fsize)
	slice := make([]uint32, num)
	for i := 0; i < num; i++ {
		slice[i] = uint32(i)
	}
	filterAddManyUint32AVX2(*filter, slice, xxHash32Seed)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsUint32(uint32(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsUint32(uint32(i)) {
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
func TestFilterAddContainsInt32Generic(t *testing.T) {
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
	filter := NewFilter(fsize)
	slice := make([]int32, num)
	for i := 0; i < num; i++ {
		slice[i] = int32(i)
	}
	filterAddManyInt32Generic(*filter, slice, xxHash32Seed)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsInt32(int32(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsInt32(int32(i)) {
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

func TestFilterAddContainsInt32AVX2(t *testing.T) {
	if !util.UseAVX2 {
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
	filter := NewFilter(fsize)
	slice := make([]int32, num)
	for i := 0; i < num; i++ {
		slice[i] = int32(i)
	}
	filterAddManyInt32AVX2(*filter, slice, xxHash32Seed)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsInt32(int32(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsInt32(int32(i)) {
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
func TestFilterAddContainsUint64Generic(t *testing.T) {
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
	filter := NewFilter(fsize)
	slice := make([]uint64, num)
	for i := 0; i < num; i++ {
		slice[i] = uint64(i)
	}
	filterAddManyUint64Generic(*filter, slice, xxHash32Seed)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsUint64(uint64(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsUint64(uint64(i)) {
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

func TestFilterAddContainsUint64AVX2(t *testing.T) {
	if !util.UseAVX2 {
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
	filter := NewFilter(fsize)
	slice := make([]uint64, num)
	for i := 0; i < num; i++ {
		slice[i] = uint64(i)
	}
	filterAddManyUint64AVX2(*filter, slice, xxHash32Seed)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsUint64(uint64(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsUint64(uint64(i)) {
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
func TestFilterAddContainsInt64Generic(t *testing.T) {
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
	filter := NewFilter(fsize)
	slice := make([]int64, num)
	for i := 0; i < num; i++ {
		slice[i] = int64(i)
	}
	filterAddManyInt64Generic(*filter, slice, xxHash32Seed)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsInt64(int64(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsInt64(int64(i)) {
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

func TestFilterAddContainsInt64AVX2(t *testing.T) {
	if !util.UseAVX2 {
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
	filter := NewFilter(fsize)
	slice := make([]int64, num)
	for i := 0; i < num; i++ {
		slice[i] = int64(i)
	}
	filterAddManyInt64AVX2(*filter, slice, xxHash32Seed)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsInt64(int64(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsInt64(int64(i)) {
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

func TestFilterMergeGeneric(t *testing.T) {
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
	filter := NewFilter(fsize)
	slice := make([]uint32, num/2)
	for i := 0; i < num/2; i++ {
		slice[i] = uint32(i)
	}
	filterAddManyUint32Generic(*filter, slice, xxHash32Seed)

	filter2 := NewFilter(fsize)
	for i := num / 2; i < num; i++ {
		slice[i-num/2] = uint32(i)
	}
	filterAddManyUint32Generic(*filter2, slice, xxHash32Seed)

	filterMergeGeneric(filter.b, filter2.b)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsUint32(uint32(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsUint32(uint32(i)) {
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

func TestFilterMergeAVX2(t *testing.T) {
	if !util.UseAVX2 {
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
	filter := NewFilter(fsize)
	slice := make([]uint32, num/2)
	for i := 0; i < num/2; i++ {
		slice[i] = uint32(i)
	}
	filterAddManyUint32AVX2(*filter, slice, xxHash32Seed)

	filter2 := NewFilter(fsize)
	for i := num / 2; i < num; i++ {
		slice[i-num/2] = uint32(i)
	}
	filterAddManyUint32AVX2(*filter2, slice, xxHash32Seed)

	filterMergeAVX2(filter.b, filter2.b)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsUint32(uint32(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsUint32(uint32(i)) {
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

var benchCases = []struct {
	m int
	n int
	v int
}{
	// 32k packs
	{m: 32768, n: 32768, v: 1},
	{m: 65536, n: 32768, v: 1},
	{m: 131072, n: 32768, v: 1},
	{m: 262144, n: 32768, v: 1},
	{m: 524288, n: 32768, v: 1},
	{m: 1048576, n: 32768, v: 1},

	// 64k packs
	{m: 32768, n: 65536, v: 1},
	{m: 65536, n: 65536, v: 1},
	{m: 131072, n: 65536, v: 1},
	{m: 262144, n: 65536, v: 1},
	{m: 524288, n: 65536, v: 1},
	{m: 1048576, n: 65536, v: 1},
}

func BenchmarkFilterAdd(b *testing.B) {
	for _, c := range benchCases {
		data := make([][]byte, 0, c.n)
		buf := make([]byte, 4)
		for i := 0; i < c.n; i++ {
			binary.LittleEndian.PutUint32(buf, uint32(i))
			data = append(data, buf)
		}

		filter := NewFilter(c.m)
		b.Run(fmt.Sprintf("m=%d_n=%d", c.m, c.n), func(b *testing.B) {
			b.SetBytes(4 * int64(c.n))
			for i := 0; i < b.N; i++ {
				for _, v := range data {
					filter.Add(v)
				}
			}
		})

	}
}

func BenchmarkFilterAddManyUint32Generic(b *testing.B) {
	for _, c := range benchCases {
		data := make([]uint32, c.n)
		for i := 0; i < c.n; i++ {
			data[i] = uint32(i)
		}

		filter := NewFilter(c.m)
		b.Run(fmt.Sprintf("m=%d_n=%d", c.m, c.n), func(b *testing.B) {
			b.SetBytes(4 * int64(c.n))
			for i := 0; i < b.N; i++ {
				filterAddManyUint32Generic(*filter, data, xxHash32Seed)
			}
		})

	}
}

func BenchmarkFilterAddManyUint32AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}
	for _, c := range benchCases {
		data := make([]uint32, c.n)
		for i := 0; i < c.n; i++ {
			data[i] = uint32(i)
		}

		filter := NewFilter(c.m)
		b.Run(fmt.Sprintf("m=%d_n=%d", c.m, c.n), func(b *testing.B) {
			b.SetBytes(4 * int64(c.n))
			for i := 0; i < b.N; i++ {
				filterAddManyUint32AVX2(*filter, data, xxHash32Seed)
			}
		})

	}
}

func BenchmarkFilterAddManyUint64Generic(b *testing.B) {
	for _, c := range benchCases {
		data := make([]uint64, c.n)
		for i := 0; i < c.n; i++ {
			data[i] = uint64(i)
		}

		filter := NewFilter(c.m)
		b.Run(fmt.Sprintf("m=%d_n=%d", c.m, c.n), func(b *testing.B) {
			b.SetBytes(8 * int64(c.n))
			for i := 0; i < b.N; i++ {
				filterAddManyUint64Generic(*filter, data, xxHash32Seed)
			}
		})

	}
}

func BenchmarkFilterAddManyUint64AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}
	for _, c := range benchCases {
		data := make([]uint64, c.n)
		for i := 0; i < c.n; i++ {
			data[i] = uint64(i)
		}

		filter := NewFilter(c.m)
		b.Run(fmt.Sprintf("m=%d_n=%d", c.m, c.n), func(b *testing.B) {
			b.SetBytes(8 * int64(c.n))
			for i := 0; i < b.N; i++ {
				filterAddManyUint64AVX2(*filter, data, xxHash32Seed)
			}
		})

	}
}

func BenchmarkFilter_Contains(b *testing.B) {
	for _, c := range benchCases {
		data := make([][]byte, 0, c.n)
		notData := make([][]byte, 0, c.n)
		for i := 0; i < c.n; i++ {
			data = append(data, []byte(fmt.Sprintf("%d", i)))
			notData = append(notData, []byte(fmt.Sprintf("%d", c.n+i)))
		}

		filter := NewFilter(c.m)
		for _, v := range data {
			filter.Add(v)
		}

		b.Run(fmt.Sprintf("IN m=%d_n=%d", c.m, c.n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, v := range data[:c.v] {
					_ = filter.Contains(v)
				}
			}
		})

		// not in
		b.Run(fmt.Sprintf("NI m=%d_n=%d", c.m, c.n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, v := range notData[:c.v] {
					_ = filter.Contains(v)
				}
			}
		})
	}
}

func BenchmarkFilterMergeGeneric(b *testing.B) {
	for _, c := range benchCases {
		data1 := make([]uint32, c.n)
		data2 := make([]uint32, c.n)
		for i := 0; i < c.n; i++ {
			data1[i] = uint32(i)
			data2[i] = uint32(c.n + i)
		}

		filter1 := NewFilter(c.m)
		filter2 := NewFilter(c.m)
		filter1.AddManyUint32(data1)
		filter2.AddManyUint32(data2)

		b.Run(fmt.Sprintf("m=%d_n=%d", c.m, c.n), func(b *testing.B) {
			b.SetBytes(int64(c.m >> 3))
			for i := 0; i < b.N; i++ {
				filterMergeGeneric(filter1.b, filter2.b)
			}
		})
	}
}

func BenchmarkFilterMergeAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}
	for _, c := range benchCases {
		data1 := make([]uint32, c.n)
		data2 := make([]uint32, c.n)
		for i := 0; i < c.n; i++ {
			data1[i] = uint32(i)
			data2[i] = uint32(c.n + i)
		}

		filter1 := NewFilter(c.m)
		filter2 := NewFilter(c.m)
		filter1.AddManyUint32(data1)
		filter2.AddManyUint32(data2)

		b.Run(fmt.Sprintf("m=%d_n=%d", c.m, c.n), func(b *testing.B) {
			b.SetBytes(int64(c.m >> 3))
			for i := 0; i < b.N; i++ {
				filterMergeAVX2(filter1.b, filter2.b)
			}
		})
	}
}
