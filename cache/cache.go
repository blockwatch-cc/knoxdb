// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

type Cache[K comparable, V any] interface {
	Purge()
	Add(key K, value V) (updated, evicted bool)
	Get(key K) (value V, ok bool)
	Contains(key K) bool
	Peek(key K) (value V, ok bool)
	ContainsOrAdd(key K, value V) (ok, evicted bool)
	Remove(key K)
	RemoveOldest()
	Keys() []K
	Len() int
}

func NewNoCache[K comparable, V any]() Cache[K, V] {
	return &NoCache[K, V]{}
}

type NoCache[K comparable, V any] struct{}

func (n *NoCache[K, V]) Purge() {}

func (n *NoCache[K, V]) Add(_ K, _ V) (updated, evicted bool) {
	return
}

func (n *NoCache[K, V]) Get(_ K) (value V, ok bool) {
	return
}

func (n *NoCache[K, V]) Contains(_ K) bool {
	return false
}

func (n *NoCache[K, V]) Peek(_ K) (value V, ok bool) {
	return
}

func (n *NoCache[K, V]) ContainsOrAdd(key K, value V) (ok, evicted bool) {
	return
}

func (n *NoCache[K, V]) Remove(key K) {}

func (n *NoCache[K, V]) RemoveOldest() {}

func (n *NoCache[K, V]) Keys() []K {
	return nil
}

func (n *NoCache[K, V]) Len() int {
	return 0
}
