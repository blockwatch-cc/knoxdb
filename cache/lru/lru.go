package lru

import (
	"sync"

	"blockwatch.cc/knoxdb/cache"
	"blockwatch.cc/knoxdb/cache/lru/internal"
)

// Cache is a thread-safe fixed size LRU cache.
type Cache[K comparable, V any] struct {
	lru  internal.LRUCache[K, V]
	lock sync.RWMutex
}

// New creates an LRU of the given size.
func New[K comparable, V any](size int) (cache.Cache[K, V], error) {
	return NewWithEvict[K, V](size, nil)
}

// NewWithEvict constructs a fixed size cache with the given eviction
// callback.
func NewWithEvict[K comparable, V any](size int, onEvicted func(key K, value V)) (cache.Cache[K, V], error) {
	lru, err := internal.NewLRU[K, V](size, internal.EvictCallback[K, V](onEvicted))
	if err != nil {
		return nil, err
	}
	c := &Cache[K, V]{
		lru: lru,
	}
	return c, nil
}

// Purge is used to completely clear the cache.
func (c *Cache[K, V]) Purge() {
	c.lock.Lock()
	c.lru.Purge()
	c.lock.Unlock()
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *Cache[K, V]) Add(key K, value V) (updated, evicted bool) {
	c.lock.Lock()
	updated, evicted = c.lru.Add(key, value)
	c.lock.Unlock()
	return
}

// Get looks up a key's value from the cache.
func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	c.lock.Lock()
	value, ok = c.lru.Get(key)
	c.lock.Unlock()
	return
}

// Contains checks if a key is in the cache, without updating the
// recent-ness or deleting it for being stale.
func (c *Cache[K, V]) Contains(key K) bool {
	c.lock.RLock()
	ok := c.lru.Contains(key)
	c.lock.RUnlock()
	return ok
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *Cache[K, V]) Peek(key K) (value V, ok bool) {
	c.lock.RLock()
	value, ok = c.lru.Peek(key)
	c.lock.RUnlock()
	return
}

// ContainsOrAdd checks if a key is in the cache  without updating the
// recent-ness or deleting it for being stale,  and if not, adds the value.
// Returns whether found and whether an eviction occurred.
func (c *Cache[K, V]) ContainsOrAdd(key K, value V) (ok, evicted bool) {
	c.lock.Lock()
	if c.lru.Contains(key) {
		c.lock.Unlock()
		return true, false
	}
	_, evicted = c.lru.Add(key, value)
	c.lock.Unlock()
	return false, evicted
}

// Remove removes the provided key from the cache.
func (c *Cache[K, V]) Remove(key K) {
	c.lock.Lock()
	c.lru.Remove(key)
	c.lock.Unlock()
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache[K, V]) RemoveOldest() {
	c.lock.Lock()
	c.lru.RemoveOldest()
	c.lock.Unlock()
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *Cache[K, V]) Keys() []K {
	c.lock.RLock()
	keys := c.lru.Keys()
	c.lock.RUnlock()
	return keys
}

// Len returns the number of items in the cache.
func (c *Cache[K, V]) Len() int {
	c.lock.RLock()
	l := c.lru.Len()
	c.lock.RUnlock()
	return l
}
