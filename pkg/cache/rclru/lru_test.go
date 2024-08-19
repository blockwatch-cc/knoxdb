// Copyright (c) 2022-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc, stefan@blockwatch.cc

package rclru

import (
	"reflect"
	"sync/atomic"
	"testing"
)

var szPackage = int(reflect.TypeOf(TestPackage{}).Size())

func NewTestLRU() (*LRU[int, *TestPackage], error) {
	return NewLRU[int, *TestPackage]()
}

type TestPackage struct {
	refCount int64
	key      int
	data     []byte
}

func NewTestPackage(key int, sz int) *TestPackage {
	var data []byte
	if sz > 0 {
		data = make([]byte, sz)
	}
	return &TestPackage{
		key:  key,
		data: data,
	}
}

func (p *TestPackage) IncRef() int64 {
	return atomic.AddInt64(&p.refCount, 1)
}

func (p *TestPackage) DecRef() int64 {
	return atomic.AddInt64(&p.refCount, -1)
}

func (p *TestPackage) HeapSize() int {
	return szPackage + len(p.data)
}

func TestLRU(t *testing.T) {
	l, err := NewTestLRU()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 0; i < 256; i++ {
		l.Add(i, NewTestPackage(i, 0))
	}
	for i := 0; i < 128; i++ {
		l.RemoveOldest()
	}
	if l.Len() != 128 {
		t.Fatalf("bad len: %v", l.Len())
	}

	if l.evictCounter != 128 {
		t.Fatalf("bad evict count: %v", l.evictCounter)
	}

	for i, k := range l.Keys() {
		if v, ok := l.Get(k); !ok || v.key != k || v.key != i+128 {
			t.Fatalf("bad key: %v", k)
		}
	}
	for i := 0; i < 128; i++ {
		_, ok := l.Get(i)
		if ok {
			t.Fatalf("should be evicted")
		}
	}
	for i := 128; i < 256; i++ {
		_, ok := l.Get(i)
		if !ok {
			t.Fatalf("should not be evicted")
		}
	}
	for i := 128; i < 192; i++ {
		ok := l.Remove(i)
		if !ok {
			t.Fatalf("should be contained")
		}
		ok = l.Remove(i)
		if ok {
			t.Fatalf("should not be contained")
		}
		_, ok = l.Get(i)
		if ok {
			t.Fatalf("should be deleted")
		}
	}

	l.Get(192) // expect 192 to be last key in l.Keys()

	for i, k := range l.Keys() {
		if (i < 63 && k != i+193) || (i == 63 && k != 192) {
			t.Fatalf("out of order key: %v", k)
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
func TestLRU_Contains(t *testing.T) {
	l, err := NewTestLRU()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	l.Add(1, nil)
	l.Add(2, nil)
	if !l.Contains(1) {
		t.Errorf("1 should be contained")
	}

	l.Add(3, nil)
	l.removeOldest()
	if l.Contains(1) {
		t.Errorf("Contains should not have updated recent-ness of 1")
	}
}

// Test that Peek doesn't update recent-ness
func TestLRU_Peek(t *testing.T) {
	l, err := NewTestLRU()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 1; i < 3; i++ {
		l.Add(i, NewTestPackage(i, 0))
	}
	if v, ok := l.Peek(1); !ok || v.key != 1 {
		t.Errorf("1 should be set to 1: %v, %v", v, ok)
	}

	l.removeOldest()
	if l.Contains(1) {
		t.Errorf("should not have updated recent-ness of 1")
	}
}

func TestLRU_GetOldest_RemoveOldest(t *testing.T) {
	l, err := NewTestLRU()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for i := 0; i < 256; i++ {
		l.Add(i, nil)
	}
	for i := 0; i < 128; i++ {
		l.RemoveOldest()
	}

	k, _, ok := l.GetOldest()
	if !ok {
		t.Fatalf("missing")
	}
	if k != 128 {
		t.Fatalf("bad: %v", k)
	}

	k, _, ok = l.RemoveOldest()
	if !ok {
		t.Fatalf("missing")
	}
	if k != 128 {
		t.Fatalf("bad: %v", k)
	}

	k, _, ok = l.RemoveOldest()
	if !ok {
		t.Fatalf("missing")
	}
	if k != 129 {
		t.Fatalf("bad: %v", k)
	}
}
