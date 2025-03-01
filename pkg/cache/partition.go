// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
	"blockwatch.cc/knoxdb/pkg/cache/rclru"
	"blockwatch.cc/knoxdb/pkg/util"
)

type CacheKey [2]uint64

func NewCacheKey(x, y uint64) CacheKey {
	return CacheKey{x, y}
}

type Partition[V any] interface {
	Add(key uint64, value V) (updated, evicted bool)
	Get(key uint64) (value V, ok bool)
	Contains(key uint64) bool
	ContainsOrAdd(key uint64, value V) (ok, evicted bool)
	Remove(key uint64)
	Keys() []uint64
	Len() int
	Purge()

	// Peek(key uint64) (value V, ok bool)
	// RemoveOldest()

	// external locked versions
	Lock()
	Unlock()
	GetLocked(key uint64) (value V, ok bool)
	AddLocked(key uint64, value V) (updated, evicted bool)
	ContainsOrAddLocked(key uint64, value V) (ok, evicted bool)
	RemoveLocked(key uint64)
}

type PartitionedCache[V any] struct {
	Cache[CacheKey, V]
}

func NewPartitionedCache[V rclru.RefCountedElem](sz int) *PartitionedCache[V] {
	var cache Cache[CacheKey, V]
	if sz == 0 {
		cache = NewNoCache[CacheKey, V]()
	} else {
		cache = rclru.New2Q[CacheKey, V](sz)
	}
	return &PartitionedCache[V]{
		Cache: cache,
	}
}

func (c *PartitionedCache[V]) Partition(k uint64) *CachePartition[V] {
	return &CachePartition[V]{
		Cache: c,
		Key:   k,
	}
}

var _ Partition[any] = (*CachePartition[any])(nil)

type CachePartition[V any] struct {
	Cache *PartitionedCache[V]
	Key   uint64
}

func (p *CachePartition[V]) makeKey(sub uint64) CacheKey {
	return CacheKey{p.Key, sub}
}

func (p *CachePartition[V]) Add(key uint64, value V) (updated, evicted bool) {
	return p.Cache.Add(p.makeKey(key), value)
}

func (p *CachePartition[V]) Get(key uint64) (value V, ok bool) {
	return p.Cache.Get(p.makeKey(key))
}

func (p *CachePartition[V]) Contains(key uint64) bool {
	return p.Cache.Contains(p.makeKey(key))
}

func (p *CachePartition[V]) ContainsOrAdd(key uint64, value V) (ok, evicted bool) {
	return p.Cache.ContainsOrAdd(p.makeKey(key), value)
}

func (p *CachePartition[V]) Remove(key uint64) {
	p.Cache.Remove(p.makeKey(key))
}

func (p *CachePartition[V]) Keys() []uint64 {
	keys := make([]uint64, 0)
	for _, k := range p.Cache.Keys() {
		if k[0] != p.Key {
			continue
		}
		keys = append(keys, k[1])
	}
	return keys
}

func (p *CachePartition[V]) Len() (n int) {
	for _, k := range p.Cache.Keys() {
		n += util.Bool2int(k[0] == p.Key)
	}
	return
}

func (p *CachePartition[V]) Purge() {
	keys := p.Cache.Keys()
	p.Cache.Lock()
	for _, k := range keys {
		if k[0] == p.Key {
			p.Cache.RemoveLocked(k)
		}
	}
	p.Cache.Unlock()
}

// func (p *CachePartition[V]) Peek(key uint64) (value V, ok bool) {
// 	return p.Cache.Peek(p.makeKey(key))
// }

// func (p *CachePartition[V]) RemoveOldest() {}

func (p *CachePartition[V]) Lock() {
	p.Cache.Lock()
}

func (p *CachePartition[V]) Unlock() {
	p.Cache.Unlock()
}

func (p *CachePartition[V]) AddLocked(key uint64, value V) (updated, evicted bool) {
	return p.Cache.AddLocked(p.makeKey(key), value)
}

func (p *CachePartition[V]) GetLocked(key uint64) (value V, ok bool) {
	return p.Cache.GetLocked(p.makeKey(key))
}

func (p *CachePartition[V]) ContainsOrAddLocked(key uint64, value V) (ok, evicted bool) {
	return p.Cache.ContainsOrAddLocked(p.makeKey(key), value)
}

func (p *CachePartition[V]) RemoveLocked(key uint64) {
	p.Cache.RemoveLocked(p.makeKey(key))
}
