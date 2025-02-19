// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import "sync/atomic"

// LockFreeMap is a concurrent Go map implementation that uses atomic
// operations instead of a mutex to manage concurrent access. It has
// almost no cost in the optimistic read case. Writes, however, copy
// the internal Go map on change (add or delete) which becomes
// expensive for frequent updates and for large maps.
//
// LockFreeMap implements the mostly read cache mentioned in the Go
// docu at https://pkg.go.dev/sync/atomic#example-Value-ReadMostly
type LockFreeMap[K comparable, V any] struct {
	val atomic.Value
}

func NewLockFreeMap[K comparable, V any]() *LockFreeMap[K, V] {
	m := &LockFreeMap[K, V]{}
	mp := make(map[K]V)
	m.val.Store(&mp)
	return m
}

func (m *LockFreeMap[K, V]) Get(k K) (V, bool) {
	v, ok := (*m.val.Load().(*map[K]V))[k]
	return v, ok
}

func (m *LockFreeMap[K, V]) Put(k K, v V) {
	for {
		p1 := m.val.Load().(*map[K]V)
		m1 := *p1
		m2 := make(map[K]V)
		for n, v := range m1 {
			m2[n] = v
		}
		m2[k] = v
		if m.val.CompareAndSwap(p1, &m2) {
			return
		}
	}
}

func (m *LockFreeMap[K, V]) Del(k K) {
	for {
		p1 := m.val.Load().(*map[K]V)
		m1 := *p1
		m2 := make(map[K]V)
		for n, v := range m1 {
			if n == k {
				continue
			}
			m2[n] = v
		}
		if m.val.CompareAndSwap(p1, &m2) {
			return
		}
	}
}

func (m *LockFreeMap[K, V]) Map() map[K]V {
	return *m.val.Load().(*map[K]V)
}

func (m *LockFreeMap[K, V]) Clear() {
	mp := make(map[K]V)
	m.val.Store(&mp)
}
