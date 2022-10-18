package pack

import (
	"math/rand"
	"strconv"
	"testing"
)

func encKey(key uint32) string {
	return strconv.FormatUint(uint64(key), 10)
}

func Benchmark2Q_Rand(b *testing.B) {
	l, err := New2Q(8192 * szPackage)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	trace := make([]uint32, b.N*2)
	for i := 0; i < b.N*2; i++ {
		trace[i] = rand.Uint32() % 32768
	}

	b.ResetTimer()

	var hit, miss int
	for i := 0; i < 2*b.N; i++ {
		if i%2 == 0 {
			pkg := NewPackage(0)
			pkg.key = trace[i]
			l.Add(encKey(trace[i]), pkg)
		} else {
			_, ok := l.Get(encKey(trace[i]))
			if ok {
				hit++
			} else {
				miss++
			}
		}
	}
	b.Logf("hit: %d miss: %d ratio: %f", hit, miss, float64(hit)/float64(miss))
}

func Benchmark2Q_Freq(b *testing.B) {
	l, err := New2Q(8192 * szPackage)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	trace := make([]uint32, b.N*2)
	for i := 0; i < b.N*2; i++ {
		if i%2 == 0 {
			trace[i] = rand.Uint32() % 16384
		} else {
			trace[i] = rand.Uint32() % 32768
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pkg := NewPackage(0)
		pkg.key = trace[i]
		l.Add(encKey(trace[i]), pkg)
	}
	var hit, miss int
	for i := 0; i < b.N; i++ {
		_, ok := l.Get(encKey(trace[i]))
		if ok {
			hit++
		} else {
			miss++
		}
	}
	b.Logf("hit: %d miss: %d ratio: %f", hit, miss, float64(hit)/float64(miss))
}

func Test2Q_RandomOps(t *testing.T) {
	size := 128 * szPackage
	l, err := New2Q(size)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	n := 200000
	for i := 0; i < n; i++ {
		key := uint32(rand.Int63() % 512)
		r := rand.Int63()
		switch r % 3 {
		case 0:
			pkg := NewPackage(0)
			pkg.key = key
			l.Add(encKey(key), pkg)
		case 1:
			l.Get(encKey(key))
		case 2:
			l.Remove(encKey(key))
		}

		if l.byteSize > size {
			t.Fatalf("bad: byteSize: %d size: %d recent: %d freq: %d",
				l.byteSize, size, l.recent.Len(), l.frequent.Len())
		}
	}
}

func Test2Q_Get_RecentToFrequent(t *testing.T) {
	l, err := New2Q(128 * szPackage)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Touch all the entries, should be in t1
	for i := uint32(0); i < 128; i++ {
		pkg := NewPackage(0)
		pkg.key = i
		l.Add(encKey(i), pkg)
	}
	if n := l.recent.Len(); n != 128 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	// Get should upgrade to t2
	for i := uint32(0); i < 128; i++ {
		_, ok := l.Get(encKey(i))
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
	for i := uint32(0); i < 128; i++ {
		_, ok := l.Get(encKey(i))
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

func Test2Q_Add_RecentToFrequent(t *testing.T) {
	l, err := New2Q(128 * szPackage)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add initially to recent
	pkg := NewPackage(0)
	pkg.key = 1
	l.Add(encKey(1), pkg)
	if n := l.recent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	// Add should upgrade to frequent
	pkg = NewPackage(0)
	pkg.key = 1
	l.Add(encKey(1), pkg)
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}

	// Add should remain in frequent
	pkg = NewPackage(0)
	pkg.key = 1
	l.Add(encKey(1), pkg)
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
}

func Test2Q_Add_RecentEvict(t *testing.T) {
	l, err := New2Q(4 * szPackage)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add 1,2,3,4,5 -> Evict 1
	for i := uint32(1); i < 6; i++ {
		pkg := NewPackage(0)
		pkg.key = i
		l.Add(encKey(i), pkg)
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
	pkg := NewPackage(0)
	pkg.key = 1
	l.Add(encKey(1), pkg)
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
	pkg = NewPackage(0)
	pkg.key = 6
	l.Add(encKey(6), pkg)
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

func Test2Q(t *testing.T) {
	l, err := New2Q(128 * szPackage)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := uint32(0); i < 256; i++ {
		pkg := NewPackage(0)
		pkg.key = i
		l.Add(encKey(i), pkg)
	}
	if l.Len() != 128 {
		t.Fatalf("bad len: %v", l.Len())
	}

	for i, k := range l.Keys() {
		if v, ok := l.Get(k); !ok || encKey(v.key) != k || int(v.key) != i+128 {
			t.Fatalf("bad key: %v", k)
		}
	}
	for i := uint32(0); i < 128; i++ {
		_, ok := l.Get(encKey(i))
		if ok {
			t.Fatalf("should be evicted")
		}
	}
	for i := uint32(128); i < 256; i++ {
		_, ok := l.Get(encKey(i))
		if !ok {
			t.Fatalf("should not be evicted")
		}
	}
	for i := uint32(128); i < 192; i++ {
		l.Remove(encKey(i))
		_, ok := l.Get(encKey(i))
		if ok {
			t.Fatalf("should be deleted")
		}
	}

	l.Purge()
	if l.Len() != 0 {
		t.Fatalf("bad len: %v", l.Len())
	}
	if _, ok := l.Get(encKey(200)); ok {
		t.Fatalf("should contain nothing")
	}
}

// Test that Contains doesn't update recent-ness
func Test2Q_Contains(t *testing.T) {
	l, err := New2Q(2 * szPackage)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := uint32(1); i < 3; i++ {
		pkg := NewPackage(0)
		pkg.key = i
		l.Add(encKey(i), pkg)
	}
	if !l.Contains(encKey(1)) {
		t.Errorf("1 should be contained")
	}

	pkg := NewPackage(0)
	pkg.key = 3
	l.Add(encKey(3), pkg)
	if l.Contains(encKey(1)) {
		t.Errorf("Contains should not have updated recent-ness of 1")
	}
}

// Test that Peek doesn't update recent-ness
func Test2Q_Peek(t *testing.T) {
	l, err := New2Q(2 * szPackage)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := uint32(1); i < 3; i++ {
		pkg := NewPackage(0)
		pkg.key = i
		l.Add(encKey(i), pkg)
	}
	if v, ok := l.Peek("1"); !ok || v.key != 1 {
		t.Errorf("1 should be set to 1: %v, %v", v, ok)
	}

	pkg := NewPackage(0)
	pkg.key = 3
	l.Add(encKey(3), pkg)
	if l.Contains(encKey(1)) {
		t.Errorf("should not have updated recent-ness of 1")
	}
}
