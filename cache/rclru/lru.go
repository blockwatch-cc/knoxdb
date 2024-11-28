// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc, stefan@blockwatch.cc

package rclru

import (
	"container/list"
)

// LRU implements a non-thread safe fixed size LRU cache
type LRU[K comparable, V RefCountedElem] struct {
	evictList    *list.List
	items        map[K]*list.Element
	evictCounter int
}

// entry is used to hold a value in the evictList
type entry[K comparable, V RefCountedElem] struct {
	key   K
	value V
}

// NewLRU constructs an LRU of the given size
func NewLRU[K comparable, V RefCountedElem]() (*LRU[K, V], error) {
	c := &LRU[K, V]{
		evictList:    list.New(),
		items:        make(map[K]*list.Element),
		evictCounter: 0,
	}
	return c, nil
}

// Purge is used to completely clear the cache.
func (c *LRU[K, V]) Purge() {
	for k := range c.items {
		delete(c.items, k)
	}
	c.evictList.Init()
}

// Add adds a value to the cache.  Returns true if an update occurred.
func (c *LRU[K, V]) Add(key K, value V) (updated bool) {
	// Check for existing item
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*entry[K, V]).value = value
		return true
	}

	// Add new item
	ent := &entry[K, V]{key, value}
	entry := c.evictList.PushFront(ent)
	c.items[key] = entry

	return false
}

// Get looks up a key's value from the cache.
func (c *LRU[K, V]) Get(key K) (value V, ok bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		return ent.Value.(*entry[K, V]).value, true
	}
	return
}

// Contains checks if a key is in the cache, without updating the recent-ness
// or deleting it for being stale.
func (c *LRU[K, V]) Contains(key K) (ok bool) {
	_, ok = c.items[key]
	return
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *LRU[K, V]) Peek(key K) (value V, ok bool) {
	if ent, ok := c.items[key]; ok {
		return ent.Value.(*entry[K, V]).value, true
	}
	return
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *LRU[K, V]) Remove(key K) (present bool) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *LRU[K, V]) RemoveOldest() (key K, value V, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
		kv := ent.Value.(*entry[K, V])
		return kv.key, kv.value, true
	}
	return
}

// GetOldest returns the oldest entry
func (c *LRU[K, V]) GetOldest() (key K, value V, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		kv := ent.Value.(*entry[K, V])
		return kv.key, kv.value, true
	}
	return
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *LRU[K, V]) Keys() []K {
	keys := make([]K, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = ent.Value.(*entry[K, V]).key
		i++
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *LRU[K, V]) Len() int {
	return c.evictList.Len()
}

// removeOldest removes the oldest item from the cache.
func (c *LRU[K, V]) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *LRU[K, V]) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*entry[K, V])
	delete(c.items, kv.key)
	c.evictCounter++
}
