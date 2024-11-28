// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc, stefan@blockwatch.cc

package rclru

type RefCountedElem interface {
	IncRef() int64
	DecRef() int64
	HeapSize() int
}

type Cache[KeyType comparable, ValType RefCountedElem] interface {
	Purge()
	Add(KeyType, ValType) (updated, evicted bool)
	Get(KeyType) (ValType, bool)
	Contains(KeyType) bool
	Peek(KeyType) (ValType, bool)
	ContainsOrAdd(KeyType, ValType) (ok, evicted bool)
	Remove(KeyType)
	RemoveOldest()
	Keys() []KeyType
	Len() int
	GetQueueLen() (int, int, int)
	Params() CacheParams
	Stats() CacheStats
	ResetStats()
}

type NoCache[K comparable, V RefCountedElem] struct{}

func NewNoCache[K comparable, V RefCountedElem]() *NoCache[K, V] {
	return &NoCache[K, V]{}
}

func (n *NoCache[K, V]) Purge() {}

func (n *NoCache[K, V]) Add(_ K, _ V) (updated, evicted bool) {
	return
}

func (n *NoCache[K, V]) Get(_ K) (val V, ok bool) {
	return
}

func (n *NoCache[K, V]) Contains(_ K) bool {
	return false
}

func (n *NoCache[K, V]) Peek(_ K) (val V, ok bool) {
	return
}

func (n *NoCache[K, V]) ContainsOrAdd(_ K, val V) (ok, evicted bool) {
	return
}

func (n *NoCache[K, V]) Remove(_ K) {}

func (n *NoCache[K, V]) RemoveOldest() {}

func (n *NoCache[K, V]) Keys() []K {
	return nil
}

func (n *NoCache[K, V]) Len() int {
	return 0
}

func (n *NoCache[K, V]) GetQueueLen() (int, int, int) {
	return 0, 0, 0
}

func (n *NoCache[K, V]) Params() CacheParams {
	return CacheParams{}
}

func (n *NoCache[K, V]) Stats() CacheStats {
	return CacheStats{}
}

func (n *NoCache[K, V]) ResetStats() {}
