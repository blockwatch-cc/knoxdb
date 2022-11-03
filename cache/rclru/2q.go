package rclru

import (
	"fmt"
	"sync"
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
type TwoQueueCache[K comparable, V RefCountedElem] struct {
	byteSize    int
	maxByteSize int
	recentRatio float64
	ghostRatio  float64

	recent      *LRU[K, V]
	frequent    *LRU[K, V]
	recentEvict *LRU[K, V]
	onEvict     EvictCallback[K, V]
	lock        sync.RWMutex
}

func (c *TwoQueueCache[K, V]) GetParams() (int, int, int, int) {
	return c.recent.Len(), c.frequent.Len(), c.recentEvict.Len(), c.byteSize
}

// New2Q creates a new TwoQueueCache using the default
// values for the parameters.
func New2Q[K comparable, V RefCountedElem](size int) (*TwoQueueCache[K, V], error) {
	return New2QParams[K, V](size, Default2QRecentRatio, Default2QGhostEntries, nil)
}

func New2QWithEvict[K comparable, V RefCountedElem](size int, onEvicted EvictCallback[K, V]) (*TwoQueueCache[K, V], error) {
	return New2QParams[K, V](size, Default2QRecentRatio, Default2QGhostEntries, onEvicted)
}

// New2QParams creates a new TwoQueueCache using the provided
// parameter values.
func New2QParams[K comparable, V RefCountedElem](size int, recentRatio float64, ghostRatio float64, onEvicted EvictCallback[K, V]) (*TwoQueueCache[K, V], error) {
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
	recent, err := NewLRU[K, V](nil)
	if err != nil {
		return nil, err
	}
	frequent, err := NewLRU[K, V](nil)
	if err != nil {
		return nil, err
	}
	recentEvict, err := NewLRU[K, V](nil)
	if err != nil {
		return nil, err
	}

	// Initialize the cache
	c := &TwoQueueCache[K, V]{
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
func (c *TwoQueueCache[K, V]) Get(key K) (val V, ok bool) {
	c.lock.Lock()

	// Check if this is a frequent value
	if val, ok = c.frequent.Get(key); ok {
		val.IncRef()
		c.lock.Unlock()
		return
	}

	// If the value is contained in recent, then we
	// promote it to frequent
	if val, ok = c.recent.Peek(key); ok {
		c.recent.Remove(key)
		c.frequent.Add(key, val)
		val.IncRef()
		c.lock.Unlock()
		return
	}

	// No hit
	c.lock.Unlock()
	return
}

// Add adds a value to the cache.
func (c *TwoQueueCache[K, V]) Add(key K, value V) (updated, evicted bool) {
	c.lock.Lock()

	// FIXME(echa): shouldn't we first check if the value is already cached
	// and only if it is not not, then update the byte size and increment
	// the reference counter?
	c.byteSize += value.HeapSize()
	value.IncRef()

	// Check if the value is frequently used already,
	// and just update the value
	if val, ok := c.frequent.Peek(key); ok {
		// FIXME(echa): why do we have to compare value here? If a key matches,
		// the invariant of a cache is that the value is the same.
		// if val != value {
		c.byteSize -= val.HeapSize()
		c.frequent.Add(key, value)
		// FIXME(echa): is this correct here? isn't ensureSpace() the only
		// point in the code that evicts? FYI, I had to add the `if` because
		// the testcase panicked.
		if c.onEvict != nil {
			c.onEvict(key, val)
		}
		// FIXME(echa): if the value is frequently used already, then we
		// don't have to call ensureSpace because all we did in Add() above
		// was to move the key to the front of the frequent list.
		evicted = c.ensureSpace()
		updated = true
		// }
		c.lock.Unlock()
		return
	}

	// Check if the value is recently used, and promote
	// the value into the frequent list
	if val, ok := c.recent.Peek(key); ok {
		// FIXME(echa): same questions like above
		// if val != value {
		c.byteSize -= val.HeapSize()
		c.recent.Remove(key)
		if c.onEvict != nil {
			c.onEvict(key, val)
		}
		c.frequent.Add(key, value)
		evicted = c.ensureSpace()
		updated = true
		// }

		c.lock.Unlock()
		return
	}

	// If the value was recently evicted, add it to the
	// frequently used list
	if c.recentEvict.Contains(key) {
		c.recentEvict.Remove(key)
		c.frequent.Add(key, value)
		// FIXME(echa): why has this line moved after the Add call?
		evicted = c.ensureSpace()
		c.lock.Unlock()
		return
	}

	// Add to the recently seen list
	c.recent.Add(key, value)
	// FIXME(echa): why has this line moved after the Add call?
	evicted = c.ensureSpace()
	c.lock.Unlock()
	return
}

// ContainsOrAdd checks if a key is in the cache  without updating the
// recent-ness or deleting it for being stale,  and if not, adds the value.
// Returns whether found and whether an eviction occurred.
func (c *TwoQueueCache[K, V]) ContainsOrAdd(key K, value V) (ok, evicted bool) {
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
func (c *TwoQueueCache[K, V]) ensureSpace() (evicted bool) {

	for c.byteSize > c.maxByteSize {
		recentLen := c.recent.Len()
		freqLen := c.frequent.Len()
		recentSize := int(float64(recentLen+freqLen) * c.recentRatio)

		var e bool
		var k K
		var v V
		if recentLen > 0 && (recentLen > recentSize) {
			// If the recent buffer is larger than
			// the target, evict from there
			k, v, e = c.recent.RemoveOldest()
			var null V
			c.recentEvict.Add(k, null)
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
func (c *TwoQueueCache[K, V]) Len() int {
	c.lock.RLock()
	l := c.recent.Len() + c.frequent.Len()
	c.lock.RUnlock()
	return l
}

// Keys returns a slice of the keys in the cache.
// The frequently used keys are first in the returned slice.
func (c *TwoQueueCache[K, V]) Keys() []K {
	c.lock.RLock()
	k1 := c.frequent.Keys()
	k2 := c.recent.Keys()
	c.lock.RUnlock()
	return append(k1, k2...)
}

// Remove removes the provided key from the cache.
func (c *TwoQueueCache[K, V]) Remove(key K) {
	c.lock.Lock()
	var val V
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

func (c *TwoQueueCache[K, V]) RemoveOldest() {
	c.lock.Lock()
	key, _, ok := c.recent.GetOldest()
	c.lock.Unlock()
	if ok {
		c.Remove(key)
	}
}

// Purge is used to completely clear the cache.
func (c *TwoQueueCache[K, V]) Purge() {
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
func (c *TwoQueueCache[K, V]) Contains(key K) bool {
	c.lock.RLock()
	ok := c.frequent.Contains(key) || c.recent.Contains(key)
	c.lock.RUnlock()
	return ok
}

// Peek is used to inspect the cache value of a key
// without updating recency or frequency.
func (c *TwoQueueCache[K, V]) Peek(key K) (value V, ok bool) {
	c.lock.RLock()
	value, ok = c.frequent.Peek(key)
	if ok {
		value.IncRef()
		c.lock.RUnlock()
		return
	}
	value, ok = c.recent.Peek(key)
	if ok {
		value.IncRef()
	}
	c.lock.RUnlock()
	return
}
