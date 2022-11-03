package rclru

import (
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"

	"blockwatch.cc/knoxdb/encoding/block"
)

var szPackage = int(reflect.TypeOf(TestPackage{}).Size())

func NewTestLRU(onEvict EvictCallback[string, *TestPackage]) (*LRU[string, *TestPackage], error) {
	return NewLRU[string, *TestPackage](onEvict)
}

type TestPackage struct {
	refCount int64
	key      uint32
	blocks   []*block.Block
}

func NewTestPackage(key uint32, sz int) *TestPackage {
	return &TestPackage{
		key: key,
	}
}

func encodeKey(i int) string {
	return strconv.FormatUint(uint64(i), 10)
}

func encodeKeyU32(i uint32) string {
	return strconv.FormatUint(uint64(i), 10)
}

func (p *TestPackage) Key() string {
	return encodeKeyU32(p.key)
}

func (p *TestPackage) IncRef() int64 {
	return atomic.AddInt64(&p.refCount, 1)
}

func (p *TestPackage) DecRef() int64 {
	return atomic.AddInt64(&p.refCount, -1)
}

func (p *TestPackage) HeapSize() int {
	return szPackage
}

func TestLRU(t *testing.T) {
	evictCounter := 0
	onEvicted := func(k string, v *TestPackage) {
		if k != strconv.FormatUint(uint64(v.key), 10) {
			t.Fatalf("Evict values not equal (%v!=%v)", k, v.key)
		}
		evictCounter++
	}
	l, err := NewTestLRU(onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 0; i < 256; i++ {
		pkg := NewTestPackage(uint32(i), 0)
		l.Add(pkg.Key(), pkg)
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
		if v, ok := l.Get(k); !ok || v.Key() != k || int(v.key) != i+128 {
			t.Fatalf("bad key: %v", k)
		}
	}
	for i := 0; i < 128; i++ {
		_, ok := l.Get(encodeKey(i))
		if ok {
			t.Fatalf("should be evicted")
		}
	}
	for i := 128; i < 256; i++ {
		_, ok := l.Get(encodeKey(i))
		if !ok {
			t.Fatalf("should not be evicted")
		}
	}
	for i := 128; i < 192; i++ {
		ok := l.Remove(encodeKey(i))
		if !ok {
			t.Fatalf("should be contained")
		}
		ok = l.Remove(encodeKey(i))
		if ok {
			t.Fatalf("should not be contained")
		}
		_, ok = l.Get(encodeKey(i))
		if ok {
			t.Fatalf("should be deleted")
		}
	}

	l.Get("192") // expect 192 to be last key in l.Keys()

	for i, k := range l.Keys() {
		if (i < 63 && k != encodeKey(i+193)) || (i == 63 && k != "192") {
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
	l, err := NewTestLRU(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for i := 0; i < 256; i++ {
		l.Add(encodeKey(i), nil)
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
	l, err := NewTestLRU(nil)
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
	l, err := NewTestLRU(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 1; i < 3; i++ {
		pkg := NewTestPackage(uint32(i), 0)
		l.Add(pkg.Key(), pkg)
	}
	if v, ok := l.Peek("1"); !ok || v.key != 1 {
		t.Errorf("1 should be set to 1: %v, %v", v, ok)
	}

	pkg := NewTestPackage(3, 0)
	l.Add("3", pkg)
	l.removeOldest()
	if l.Contains("1") {
		t.Errorf("should not have updated recent-ness of 1")
	}
}
