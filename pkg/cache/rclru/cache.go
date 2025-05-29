// Copyright (c) 2022-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc, stefan@blockwatch.cc

package rclru

type RefCountedElem interface {
	Ref() int64
	Deref() int64
	Size() int
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

func (_ *NoCache[K, V]) Purge()                                    {}
func (_ *NoCache[K, V]) Add(_ K, _ V) (updated, evicted bool)      { return }
func (_ *NoCache[K, V]) Get(_ K) (val V, ok bool)                  { return }
func (_ *NoCache[K, V]) Contains(_ K) bool                         { return false }
func (_ *NoCache[K, V]) Peek(_ K) (val V, ok bool)                 { return }
func (_ *NoCache[K, V]) ContainsOrAdd(_ K, _ V) (ok, evicted bool) { return }
func (_ *NoCache[K, V]) Remove(_ K)                                {}
func (_ *NoCache[K, V]) RemoveOldest()                             {}
func (_ *NoCache[K, V]) Keys() []K                                 { return nil }
func (_ *NoCache[K, V]) Len() int                                  { return 0 }
func (_ *NoCache[K, V]) GetQueueLen() (int, int, int)              { return 0, 0, 0 }
func (_ *NoCache[K, V]) Params() (p CacheParams)                   { return }
func (_ *NoCache[K, V]) Stats() (s CacheStats)                     { return }
func (_ *NoCache[K, V]) ResetStats()                               {}
