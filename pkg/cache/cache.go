// Copyright (c) 2018-2024 Blockwatch Data Inc.
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

func (_ *NoCache[K, V]) Purge()                                    {}
func (_ *NoCache[K, V]) Add(_ K, _ V) (updated, evicted bool)      { return }
func (_ *NoCache[K, V]) Get(_ K) (value V, ok bool)                { return }
func (_ *NoCache[K, V]) Contains(_ K) bool                         { return false }
func (_ *NoCache[K, V]) Peek(_ K) (value V, ok bool)               { return }
func (_ *NoCache[K, V]) ContainsOrAdd(_ K, _ V) (ok, evicted bool) { return }
func (_ *NoCache[K, V]) Remove(_ K)                                {}
func (_ *NoCache[K, V]) RemoveOldest()                             {}
func (_ *NoCache[K, V]) Keys() []K                                 { return nil }
func (_ *NoCache[K, V]) Len() int                                  { return 0 }
