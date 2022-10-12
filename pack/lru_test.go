package pack

import (
	"strconv"
	"testing"
)

func TestLRU(t *testing.T) {
	evictCounter := 0
	onEvicted := func(k string, v *Package) {
		if k != strconv.FormatUint(uint64(v.key), 10) {
			t.Fatalf("Evict values not equal (%v!=%v)", k, v)
		}
		evictCounter++
	}
	l, err := NewLRU(onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 0; i < 256; i++ {
		pkg := NewPackage(0)
		pkg.key = uint32(i)
		l.Add(strconv.FormatUint(uint64(i), 10), pkg)
	}
	for i := 0; i < 128; i++ {
		l.RemoveOldest()
	}
	if l.Len() != 128 {
		t.Fatalf("bad len: %v", l.Len())
	}

	if evictCounter != 128 {
		t.Fatalf("bad evict count: %v", evictCounter)
	}

	for i, k := range l.Keys() {
		if v, ok := l.Get(k); !ok || strconv.FormatUint(uint64(v.key), 10) != k || int(v.key) != i+128 {
			t.Fatalf("bad key: %v", k)
		}
	}
	for i := 0; i < 128; i++ {
		_, ok := l.Get(strconv.FormatUint(uint64(i), 10))
		if ok {
			t.Fatalf("should be evicted")
		}
	}
	for i := 128; i < 256; i++ {
		_, ok := l.Get(strconv.FormatUint(uint64(i), 10))
		if !ok {
			t.Fatalf("should not be evicted")
		}
	}
	for i := 128; i < 192; i++ {
		ok := l.Remove(strconv.FormatUint(uint64(i), 10))
		if !ok {
			t.Fatalf("should be contained")
		}
		ok = l.Remove(strconv.FormatUint(uint64(i), 10))
		if ok {
			t.Fatalf("should not be contained")
		}
		_, ok = l.Get(strconv.FormatUint(uint64(i), 10))
		if ok {
			t.Fatalf("should be deleted")
		}
	}

	l.Get("192") // expect 192 to be last key in l.Keys()

	for i, k := range l.Keys() {
		if (i < 63 && k != strconv.FormatUint(uint64(i+193), 10)) || (i == 63 && k != "192") {
			t.Fatalf("out of order key: %v", k)
		}
	}

	l.Purge()
	if l.Len() != 0 {
		t.Fatalf("bad len: %v", l.Len())
	}
	if _, ok := l.Get("200"); ok {
		t.Fatalf("should contain nothing")
	}
}

func TestLRU_GetOldest_RemoveOldest(t *testing.T) {
	l, err := NewLRU(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for i := 0; i < 256; i++ {
		l.Add(strconv.FormatUint(uint64(i), 10), nil)
	}
	for i := 0; i < 128; i++ {
		l.RemoveOldest()
	}

	k, _, ok := l.GetOldest()
	if !ok {
		t.Fatalf("missing")
	}
	if k != "128" {
		t.Fatalf("bad: %v", k)
	}

	k, _, ok = l.RemoveOldest()
	if !ok {
		t.Fatalf("missing")
	}
	if k != "128" {
		t.Fatalf("bad: %v", k)
	}

	k, _, ok = l.RemoveOldest()
	if !ok {
		t.Fatalf("missing")
	}
	if k != "129" {
		t.Fatalf("bad: %v", k)
	}
}

// Test that Contains doesn't update recent-ness
func TestLRU_Contains(t *testing.T) {
	l, err := NewLRU(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	l.Add("1", nil)
	l.Add("2", nil)
	if !l.Contains("1") {
		t.Errorf("1 should be contained")
	}

	l.Add("3", nil)
	l.removeOldest()
	if l.Contains("1") {
		t.Errorf("Contains should not have updated recent-ness of 1")
	}
}

// Test that Peek doesn't update recent-ness
func TestLRU_Peek(t *testing.T) {
	l, err := NewLRU(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 1; i < 3; i++ {
		pkg := NewPackage(0)
		pkg.key = uint32(i)
		l.Add(strconv.FormatUint(uint64(i), 10), pkg)
	}
	if v, ok := l.Peek("1"); !ok || v.key != 1 {
		t.Errorf("1 should be set to 1: %v, %v", v, ok)
	}

	pkg := NewPackage(0)
	pkg.key = uint32(3)
	l.Add("3", pkg)
	l.removeOldest()
	if l.Contains("1") {
		t.Errorf("should not have updated recent-ness of 1")
	}
}
