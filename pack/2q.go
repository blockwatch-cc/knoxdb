package pack

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	// Default2QRecentRatio is the ratio of the 2Q cache dedicated
	// to recently added entries that have only been accessed once.
	Default2QRecentRatio = 0.25

	// Default2QGhostEntries is the default ratio of ghost
	// entries kept to track entries recently evicted
	Default2QGhostEntries = 0.50
)

// TwoQueueCache is a thread-safe fixed size 2Q cache.
// 2Q is an enhancement over the standard LRU cache
// in that it tracks both frequently and recently used
// entries separately. This avoids a burst in access to new
// entries from evicting frequently used entries. It adds some
// additional tracking overhead to the standard LRU cache, and is
// computationally about 2x the cost, and adds some metadata over
// head. The ARCCache is similar, but does not require setting any
// parameters.
type TwoQueueCache struct {
	byteSize    int
	maxByteSize int
	recentRatio float64
	ghostRatio  float64

	recent      LRUCache
	frequent    LRUCache
	recentEvict LRUCache
	onEvict     EvictCallback
	lock        sync.RWMutex
}

func (c *TwoQueueCache) GetParams() (int, int, int, int) {
	return c.recent.Len(), c.frequent.Len(), c.recentEvict.Len(), c.byteSize
}

// New2Q creates a new TwoQueueCache using the default
// values for the parameters.
func New2Q(size int) (*TwoQueueCache, error) {
	return New2QParams(size, Default2QRecentRatio, Default2QGhostEntries, nil)
}

func New2QWithEvict(size int, onEvicted func(key string, value *Package)) (*TwoQueueCache, error) {
	return New2QParams(size, Default2QRecentRatio, Default2QGhostEntries, onEvicted)
}

// New2QParams creates a new TwoQueueCache using the provided
// parameter values.
func New2QParams(size int, recentRatio float64, ghostRatio float64, onEvicted func(key string, value *Package)) (*TwoQueueCache, error) {
	if size <= 0 {
		return nil, fmt.Errorf("2qcache: invalid size")
	}
	if recentRatio < 0.0 || recentRatio > 1.0 {
		return nil, fmt.Errorf("2qcache: invalid recent ratio")
	}
	if ghostRatio < 0.0 || ghostRatio > 1.0 {
		return nil, fmt.Errorf("2qcache: invalid ghost ratio")
	}

	// Allocate the LRUs
	recent, err := NewLRU(nil)
	if err != nil {
		return nil, err
	}
	frequent, err := NewLRU(nil)
	if err != nil {
		return nil, err
	}
	recentEvict, err := NewLRU(nil)
	if err != nil {
		return nil, err
	}

	// Initialize the cache
	c := &TwoQueueCache{
		byteSize:    0,
		maxByteSize: size,
		recentRatio: recentRatio,
		ghostRatio:  ghostRatio,

		recent:      recent,
		frequent:    frequent,
		recentEvict: recentEvict,
		onEvict:     onEvicted,
	}
	return c, nil
}

// Get looks up a key's value from the cache.
func (c *TwoQueueCache) Get(key string) (value *Package, ok bool) {
	c.lock.Lock()

	// Check if this is a frequent value
	if pkg, ok := c.frequent.Get(key); ok {
		atomic.AddInt64(&pkg.refCount, 1)
		c.lock.Unlock()
		return pkg, ok
	}

	// If the value is contained in recent, then we
	// promote it to frequent
	if pkg, ok := c.recent.Peek(key); ok {
		c.recent.Remove(key)
		c.frequent.Add(key, pkg)
		atomic.AddInt64(&pkg.refCount, 1)
		c.lock.Unlock()
		return pkg, ok
	}

	// No hit
	c.lock.Unlock()
	return nil, false
}

// Add adds a value to the cache.
func (c *TwoQueueCache) Add(key string, value *Package) (updated, evicted bool) {
	c.lock.Lock()

	c.byteSize += value.HeapSize()
	atomic.AddInt64(&value.refCount, 1)

	// Check if the value is frequently used already,
	// and just update the value
	if val, ok := c.frequent.Peek(key); ok {
		if val != value {
			c.byteSize -= val.HeapSize()
			c.frequent.Add(key, value)
			c.onEvict(key, val)
			evicted = c.ensureSpace()
			updated = true
		}
		c.lock.Unlock()
		return
	}

	// Check if the value is recently used, and promote
	// the value into the frequent list
	if val, ok := c.recent.Peek(key); ok {
		if val != value {
			c.byteSize -= val.HeapSize()
			c.recent.Remove(key)
			c.onEvict(key, val)
			c.frequent.Add(key, value)
			evicted = c.ensureSpace()
			updated = true
		}

		c.lock.Unlock()
		return
	}

	// If the value was recently evicted, add it to the
	// frequently used list
	if c.recentEvict.Contains(key) {
		c.recentEvict.Remove(key)
		c.frequent.Add(key, value)
		evicted = c.ensureSpace()
		c.lock.Unlock()
		return
	}

	// Add to the recently seen list
	c.recent.Add(key, value)
	evicted = c.ensureSpace()
	c.lock.Unlock()
	return
}

// ContainsOrAdd checks if a key is in the cache  without updating the
// recent-ness or deleting it for being stale,  and if not, adds the value.
// Returns whether found and whether an eviction occurred.
func (c *TwoQueueCache) ContainsOrAdd(key string, value *Package) (ok, evicted bool) {
	c.lock.Lock()
	if c.frequent.Contains(key) {
		c.lock.Unlock()
		return true, false
	}
	if c.recent.Contains(key) {
		c.lock.Unlock()
		return true, false
	}
	c.lock.Unlock()
	_, evicted = c.Add(key, value)
	return false, evicted
}

// ensureSpace is used to ensure we have space in the cache
func (c *TwoQueueCache) ensureSpace() (evicted bool) {

	for c.byteSize > c.maxByteSize {
		recentLen := c.recent.Len()
		freqLen := c.frequent.Len()
		recentSize := int(float64(recentLen+freqLen) * c.recentRatio)

		var e bool
		var k string
		var v *Package
		if recentLen > 0 && (recentLen > recentSize) {
			// If the recent buffer is larger than
			// the target, evict from there
			k, v, e = c.recent.RemoveOldest()
			c.recentEvict.Add(k, nil)
		} else {
			// Remove from the frequent list otherwise
			k, v, e = c.frequent.RemoveOldest()
		}
		c.byteSize -= v.HeapSize()
		if e && c.onEvict != nil {
			c.onEvict(k, v)
		}
		evicted = evicted || e
	}

	recentLen := c.recent.Len()
	freqLen := c.frequent.Len()
	evictSize := int(float64(recentLen+freqLen) * c.ghostRatio)

	for evictSize < c.recentEvict.Len() {
		c.recentEvict.RemoveOldest()
	}

	return evicted
}

// Len returns the number of items in the cache.
func (c *TwoQueueCache) Len() int {
	c.lock.RLock()
	l := c.recent.Len() + c.frequent.Len()
	c.lock.RUnlock()
	return l
}

// Keys returns a slice of the keys in the cache.
// The frequently used keys are first in the returned slice.
func (c *TwoQueueCache) Keys() []string {
	c.lock.RLock()
	k1 := c.frequent.Keys()
	k2 := c.recent.Keys()
	c.lock.RUnlock()
	return append(k1, k2...)
}

// Remove removes the provided key from the cache.
func (c *TwoQueueCache) Remove(key string) {
	c.lock.Lock()
	var val *Package
	var ok bool
	if val, ok = c.frequent.Peek(key); !ok {
		val, ok = c.recent.Peek(key)
	}
	if ok {
		c.byteSize -= val.HeapSize()
		if c.onEvict != nil {
			c.onEvict(key, val)
		}
	}

	if c.frequent.Remove(key) {
		c.lock.Unlock()
		return
	}
	if c.recent.Remove(key) {
		c.lock.Unlock()
		return
	}
	if c.recentEvict.Remove(key) {
		c.lock.Unlock()
		return
	}
	c.lock.Unlock()
}

func (c *TwoQueueCache) RemoveOldest() {
	c.lock.Lock()
	key, _, ok := c.recent.GetOldest()
	c.lock.Unlock()
	if ok {
		c.Remove(key)
	}
}

// Purge is used to completely clear the cache.
func (c *TwoQueueCache) Purge() {
	c.lock.Lock()
	k, v, ok := c.recent.RemoveOldest()
	for ok {
		if c.onEvict != nil {
			c.onEvict(k, v)
		}
		k, v, ok = c.recent.RemoveOldest()
	}
	c.recent.Purge()

	k, v, ok = c.frequent.RemoveOldest()
	for ok {
		if c.onEvict != nil {
			c.onEvict(k, v)
		}
		k, v, ok = c.frequent.RemoveOldest()
	}
	c.frequent.Purge()
	c.recentEvict.Purge()
	c.byteSize = 0
	c.lock.Unlock()
}

// Contains is used to check if the cache contains a key
// without updating recency or frequency.
func (c *TwoQueueCache) Contains(key string) bool {
	c.lock.RLock()
	ok := c.frequent.Contains(key) || c.recent.Contains(key)
	c.lock.RUnlock()
	return ok
}

// Peek is used to inspect the cache value of a key
// without updating recency or frequency.
func (c *TwoQueueCache) Peek(key string) (value *Package, ok bool) {
	c.lock.RLock()
	var v *Package
	v, ok = c.frequent.Peek(key)
	if ok {
		value = v
		atomic.AddInt64(&value.refCount, 1)
		c.lock.RUnlock()
		return
	}
	v, ok = c.recent.Peek(key)
	if ok {
		value = v
		atomic.AddInt64(&value.refCount, 1)
	}
	c.lock.RUnlock()
	return
}
