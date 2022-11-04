// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc, stefan@blockwatch.cc

package rclru

import (
	"math/rand"
	"testing"
)

func NewTest2Q(sz int) (*TwoQueueCache[int, *TestPackage], error) {
	return New2Q[int, *TestPackage](sz)
}

func Benchmark2Q_Rand(b *testing.B) {
	l, err := NewTest2Q(8192 * szPackage)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	trace := make([]int, b.N*2)
	for i := 0; i < b.N*2; i++ {
		trace[i] = rand.Int() % 32768
	}

	b.ResetTimer()

	var hit, miss int
	for i := 0; i < 2*b.N; i++ {
		if i%2 == 0 {
			l.Add(trace[i], NewTestPackage(0, 0))
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

func Benchmark2Q_Freq(b *testing.B) {
	l, err := NewTest2Q(8192 * szPackage)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	trace := make([]int, b.N*2)
	for i := 0; i < b.N*2; i++ {
		if i%2 == 0 {
			trace[i] = rand.Int() % 16384
		} else {
			trace[i] = rand.Int() % 32768
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		l.Add(trace[i], NewTestPackage(0, 0))
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

func Test2Q_RandomOps(t *testing.T) {
	size := 128 * (szPackage + 16384)
	l, err := NewTest2Q(size)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	n := 200000
	for i := 0; i < n; i++ {
		key := rand.Int() % 512
		r := rand.Int()
		switch r % 3 {
		case 0:
			size := rand.Intn(32768)
			b := NewTestPackage(0, size)
			l.Add(key, b)
			if int(b.refCount) != 1 {
				t.Fatalf("bad: RefCount == %d after Add", b.refCount)
			}

		case 1:
			if b, ok := l.Get(key); ok {
				if int(b.refCount) != 2 {
					t.Fatalf("bad: RefCount == %d after Get", b.refCount)
				}
				b.DecRef()
			}
		case 2:
			l.Remove(key)
		}

		if l.stats.Size > int64(size) {
			t.Fatalf("bad: byteSize: %d size: %d recent: %d freq: %d",
				l.stats.Size, size, l.recent.Len(), l.frequent.Len())
		}
	}
}

func Test2Q_Get_RecentToFrequent(t *testing.T) {
	l, err := NewTest2Q(128 * szPackage)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Touch all the entries, should be in t1
	for i := 0; i < 128; i++ {
		l.Add(i, NewTestPackage(0, 0))
	}
	if n := l.recent.Len(); n != 128 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	// Get should upgrade to t2
	for i := 0; i < 128; i++ {
		_, ok := l.Get(i)
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
		_, ok := l.Get(i)
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
	l, err := NewTest2Q(3 * szPackage)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// same key, different value
	// Add initially to recent
	b1 := NewTestPackage(1, 0)
	l.Add(1, b1)
	if n := l.recent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	// Add should upgrade to frequent
	b2 := NewTestPackage(2, 0)
	l.Add(1, b2)
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}

	// Add should remain in frequent
	b3 := NewTestPackage(3, 0)
	l.Add(1, b3)
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}

	if b1.refCount != 0 {
		t.Fatalf("bad: refCount: %d", b1.refCount)
	}
	if b2.refCount != 0 {
		t.Fatalf("bad: refCount: %d", b2.refCount)
	}
	if b3.refCount != 1 {
		t.Fatalf("bad: refCount: %d", b3.refCount)
	}

	l.Purge()

	// same key, same value
	// Add initially to recent
	b := NewTestPackage(0, 0)
	l.Add(1, b)
	if n := l.recent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if b.refCount != 1 {
		t.Fatalf("bad: refCount: %d", b.refCount)
	}

	// Add should upgrade to frequent
	l.Add(1, b)
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if b.refCount != 1 {
		t.Fatalf("bad: refCount: %d", b.refCount)
	}

	// Add should remain in frequent
	l.Add(1, b)
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if b.refCount != 1 {
		t.Fatalf("bad: refCount: %d", b.refCount)
	}

	l.Purge()

	// Fill cache
	for i := 1; i < 4; i++ {
		l.Add(i, NewTestPackage(0, 0))
	}
	// update 3 with a bigger one -> should evict
	l.Add(3, NewTestPackage(0, 1))
	if l := l.Len(); l != 2 {
		t.Fatalf("bad: %d", l)
	}
}

func Test2Q_Add_RecentEvict(t *testing.T) {
	l, err := NewTest2Q(4 * szPackage)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add 1,2,3,4,5 -> Evict 1
	for i := 1; i < 6; i++ {
		l.Add(i, NewTestPackage(0, 0))
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
	l.Add(1, NewTestPackage(0, 0))
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
	l.Add(6, NewTestPackage(0, 0))
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
	l, err := NewTest2Q(128 * szPackage)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	b := make([]*TestPackage, 257)
	for i := 0; i < 256; i++ {
		b[i] = NewTestPackage(i, 0)
		l.Add(i, b[i])
	}
	if l.Len() != 128 {
		t.Fatalf("bad len: %v", l.Len())
	}

	for i, k := range l.Keys() {
		if v, ok := l.Get(k); !ok || v.key != k || v.key != i+128 {
			t.Fatalf("bad key: %v", k)
		} else {
			if v.refCount != 2 {
				t.Errorf("RefCount of %d should be 2: %v", v.key, v)
			}
			v.DecRef()
		}
	}
	for i := 0; i < 128; i++ {
		_, ok := l.Get(i)
		if ok {
			t.Fatalf("should be evicted")
		}
		if r := b[i].refCount; r != 0 {
			t.Fatalf("refCount: %d should 0 after evict", r)
		}
	}
	for i := 128; i < 256; i++ {
		v, ok := l.Get(i)
		if ok {
			if v.refCount != 2 {
				t.Errorf("RefCount of %d should be 2: %v", v.key, v)
			}
			v.DecRef()
		} else {
			t.Fatalf("should not be evicted")
		}
	}
	// add a bigger one -> should evict two
	b[256] = NewTestPackage(256, 1)
	l.Add(256, b[256])
	if l.Len() != 127 {
		t.Fatalf("bad len: %v", l.Len())
	}
	for i := 128; i < 130; i++ {
		_, ok := l.Get(i)
		if ok {
			t.Fatalf("should be evicted")
		}
	}

	for i := 130; i < 192; i++ {
		l.Remove(i)
		_, ok := l.Get(i)
		if ok {
			t.Fatalf("should be deleted")
		}
		if r := b[i].refCount; r != 0 {
			t.Fatalf("refCount: %d should 0 after Remove", r)
		}
	}

	l.Purge()
	if l.Len() != 0 {
		t.Fatalf("bad len: %v", l.Len())
	}
	for i := 0; i < 257; i++ {
		l.Remove(i)
		_, ok := l.Get(i)
		if ok {
			t.Fatalf("should be deleted")
		}
		if r := b[i].refCount; r != 0 {
			t.Fatalf("refCount: %d should 0 after Purge", r)
		}
	}
}

// Test that Contains doesn't update recent-ness
func Test2Q_Contains(t *testing.T) {
	l, err := NewTest2Q(2 * szPackage)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 1; i < 3; i++ {
		l.Add(i, NewTestPackage(0, 0))
	}
	if !l.Contains(1) {
		t.Errorf("1 should be contained")
	}

	l.Add(3, NewTestPackage(0, 0))
	if l.Contains(1) {
		t.Errorf("Contains should not have updated recent-ness of 1")
	}
}

// Test that Peek doesn't update recent-ness
func Test2Q_Peek(t *testing.T) {
	l, err := NewTest2Q(2 * szPackage)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 1; i < 3; i++ {
		l.Add(i, NewTestPackage(i, 0))
	}
	if v, ok := l.Peek(1); !ok || v.key != 1 {
		t.Errorf("1 should be set to 1: %v, %v", v, ok)
	} else {
		if v.refCount != 2 {
			t.Errorf("RefCount of 1 should be 2: %v", v)
		}
	}

	l.Add(3, NewTestPackage(0, 0))
	if l.Contains(1) {
		t.Errorf("should not have updated recent-ness of 1")
	}
}
