package pack

import (
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/encoding/block"
)

func BenchmarkBlock2Q_Rand(b *testing.B) {
	l, err := NewBlock2Q(8192 * block.BlockSz)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	trace := make([]uint64, b.N*2)
	for i := 0; i < b.N*2; i++ {
		trace[i] = rand.Uint64() % 32768
	}

	b.ResetTimer()

	var hit, miss int
	for i := 0; i < 2*b.N; i++ {
		if i%2 == 0 {
			b := block.NewBlock(block.BlockUint8, 0, 0)
			l.Add(trace[i], b)
		} else {
			_, ok := l.Get(trace[i])
			if ok {
				hit++
			} else {
				miss++
			}
		}
	}
	b.Logf("hit: %d miss: %d ratio: %f", hit, miss, float64(hit)/float64(miss))
}

func BenchmarkBlock2Q_Freq(b *testing.B) {
	l, err := NewBlock2Q(8192 * block.BlockSz)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	trace := make([]uint64, b.N*2)
	for i := 0; i < b.N*2; i++ {
		if i%2 == 0 {
			trace[i] = rand.Uint64() % 16384
		} else {
			trace[i] = rand.Uint64() % 32768
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b := block.NewBlock(block.BlockUint8, 0, 0)
		l.Add(uint64(trace[i]), b)
	}
	var hit, miss int
	for i := 0; i < b.N; i++ {
		_, ok := l.Get(trace[i])
		if ok {
			hit++
		} else {
			miss++
		}
	}
	b.Logf("hit: %d miss: %d ratio: %f", hit, miss, float64(hit)/float64(miss))
}

func TestBlock2Q_RandomOps(t *testing.T) {
	size := 128 * (block.BlockSz + 16384)
	l, err := NewBlock2Q(size)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	n := 200000
	for i := 0; i < n; i++ {
		key := rand.Uint64() % 512
		r := rand.Int63()
		switch r % 3 {
		case 0:
			size := rand.Intn(32768)
			b := block.NewBlock(block.BlockUint8, 0, size)
			b.Uint8 = b.Uint8[:size]
			l.Add(key, b)
			if int(b.RefCount) != 1 {
				t.Fatalf("bad: RefCount == %d after Add", b.RefCount)
			}

		case 1:
			if b, ok := l.Get(key); ok {
				if int(b.RefCount) != 2 {
					t.Fatalf("bad: RefCount == %d after Get", b.RefCount)
				}
				b.RefCount--
			}
		case 2:
			l.Remove(key)
		}

		if l.byteSize > size {
			t.Fatalf("bad: byteSize: %d size: %d recent: %d freq: %d",
				l.byteSize, size, l.recent.Len(), l.frequent.Len())
		}
	}
}

func TestBlock2Q_Get_RecentToFrequent(t *testing.T) {
	l, err := NewBlock2Q(128 * block.BlockSz)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Touch all the entries, should be in t1
	for i := 0; i < 128; i++ {
		b := block.NewBlock(block.BlockUint8, 0, 0)
		l.Add(uint64(i), b)
	}
	if n := l.recent.Len(); n != 128 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	// Get should upgrade to t2
	for i := 0; i < 128; i++ {
		_, ok := l.Get(uint64(i))
		if !ok {
			t.Fatalf("missing: %d", i)
		}
	}
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 128 {
		t.Fatalf("bad: %d", n)
	}

	// Get be from t2
	for i := 0; i < 128; i++ {
		_, ok := l.Get(uint64(i))
		if !ok {
			t.Fatalf("missing: %d", i)
		}
	}
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 128 {
		t.Fatalf("bad: %d", n)
	}
}

func TestBlock2Q_Add_RecentToFrequent(t *testing.T) {
	l, err := NewBlock2Q(128 * block.BlockSz)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add initially to recent
	b := block.NewBlock(block.BlockUint8, 0, 0)
	l.Add(1, b)
	if n := l.recent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	// Add should upgrade to frequent
	b = block.NewBlock(block.BlockUint8, 0, 0)
	l.Add(1, b)
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}

	// Add should remain in frequent
	b = block.NewBlock(block.BlockUint8, 0, 0)
	l.Add(1, b)
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
}

func TestBlock2Q_Add_RecentEvict(t *testing.T) {
	l, err := NewBlock2Q(4 * block.BlockSz)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add 1,2,3,4,5 -> Evict 1
	for i := 1; i < 6; i++ {
		b := block.NewBlock(block.BlockUint8, 0, 0)
		l.Add(uint64(i), b)
	}
	if n := l.recent.Len(); n != 4 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.recentEvict.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	// Pull in the recently evicted
	b := block.NewBlock(block.BlockUint8, 0, 0)
	l.Add(1, b)
	if n := l.recent.Len(); n != 3 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.recentEvict.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}

	// Add 6, should cause another recent evict
	b = block.NewBlock(block.BlockUint8, 0, 0)
	l.Add(6, b)
	if n := l.recent.Len(); n != 3 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.recentEvict.Len(); n != 2 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
}

func TestBlock2Q(t *testing.T) {
	l, err := NewBlock2Q(128 * block.BlockSz)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	b := make([]*block.Block, 256)
	for i := 0; i < 256; i++ {
		b[i] = block.NewBlock(block.BlockUint8, block.Compression(i), 0)
		l.Add(uint64(i), b[i])
	}
	if l.Len() != 128 {
		t.Fatalf("bad len: %v", l.Len())
	}

	for i, k := range l.Keys() {
		if v, ok := l.Get(k); !ok || uint64(v.Compression()) != k || int(v.Compression()) != i+128 {
			t.Fatalf("bad key: %v", k)
		} else {
			if v.RefCount != 2 {
				t.Errorf("RefCount of %d should be 2: %v", v.Compression(), v)
			}
			v.RefCount--
		}
	}
	for i := 0; i < 128; i++ {
		_, ok := l.Get(uint64(i))
		if ok {
			t.Fatalf("should be evicted")
		}
	}
	for i := 128; i < 256; i++ {
		v, ok := l.Get(uint64(i))
		if ok {
			if v.RefCount != 2 {
				t.Errorf("RefCount of %d should be 2: %v", v.Compression(), v)
			}
			v.RefCount--
		} else {
			t.Fatalf("should not be evicted")
		}
	}
	for i := 128; i < 192; i++ {
		l.Remove(uint64(i))
		_, ok := l.Get(uint64(i))
		if ok {
			t.Fatalf("should be deleted")
		}
	}

	l.Purge()
	if l.Len() != 0 {
		t.Fatalf("bad len: %v", l.Len())
	}
	if _, ok := l.Get(200); ok {
		t.Fatalf("should contain nothing")
	}
}

// Test that Contains doesn't update recent-ness
func TestBlock2Q_Contains(t *testing.T) {
	l, err := NewBlock2Q(2 * block.BlockSz)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 1; i < 3; i++ {
		b := block.NewBlock(block.BlockUint8, 0, 0)
		l.Add(uint64(i), b)
	}
	if !l.Contains(1) {
		t.Errorf("1 should be contained")
	}

	b := block.NewBlock(block.BlockUint8, 0, 0)
	l.Add(3, b)
	if l.Contains(1) {
		t.Errorf("Contains should not have updated recent-ness of 1")
	}
}

// Test that Peek doesn't update recent-ness
func TestBlock2Q_Peek(t *testing.T) {
	l, err := NewBlock2Q(2 * block.BlockSz)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 1; i < 3; i++ {
		b := block.NewBlock(block.BlockUint8, block.Compression(i), 0)
		l.Add(uint64(i), b)
	}
	if v, ok := l.Peek(1); !ok || v.Compression() != 1 {
		t.Errorf("1 should be set to 1: %v, %v", v, ok)
	} else {
		if v.RefCount != 2 {
			t.Errorf("RefCount of 1 should be 2: %v", v)
		}
	}

	b := block.NewBlock(block.BlockUint8, 0, 0)
	l.Add(3, b)
	if l.Contains(1) {
		t.Errorf("should not have updated recent-ness of 1")
	}
}
