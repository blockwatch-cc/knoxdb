package pack

import (
	"fmt"
	"sync"
	"sync/atomic"

	"blockwatch.cc/knoxdb/encoding/block"
)

/*const (
	// Default2QRecentRatio is the ratio of the 2Q cache dedicated
	// to recently added entries that have only been accessed once.
	Default2QRecentRatio = 0.25

	// Default2QGhostEntries is the default ratio of ghost
	// entries kept to track entries recently evicted
	Default2QGhostEntries = 0.50
)*/

// BlockTwoQueueCache is a thread-safe fixed size 2Q cache.
// 2Q is an enhancement over the standard LRU cache
// in that it tracks both frequently and recently used
// entries separately. This avoids a burst in access to new
// entries from evicting frequently used entries. It adds some
// additional tracking overhead to the standard LRU cache, and is
// computationally about 2x the cost, and adds some metadata over
// head. The ARCCache is similar, but does not require setting any
// parameters.
type BlockTwoQueueCache struct {
	byteSize    int
	maxByteSize int
	recentRatio float64
	ghostRatio  float64

	recent      BlockLRUCache
	frequent    BlockLRUCache
	recentEvict BlockLRUCache
	onEvict     BlockEvictCallback
	lock        sync.RWMutex
}

func (c *BlockTwoQueueCache) GetParams() (int, int, int, int) {
	return c.recent.Len(), c.frequent.Len(), c.recentEvict.Len(), c.byteSize
}

// New2Q creates a new BlockTwoQueueCache using the default
// values for the parameters.
func NewBlock2Q(size int) (*BlockTwoQueueCache, error) {
	return NewBlock2QParams(size, Default2QRecentRatio, Default2QGhostEntries, nil)
}

func NewBlock2QWithEvict(size int, onEvicted func(key uint64, value *block.Block)) (*BlockTwoQueueCache, error) {
	return NewBlock2QParams(size, Default2QRecentRatio, Default2QGhostEntries, onEvicted)
}

// New2QParams creates a new BlockTwoQueueCache using the provided
// parameter values.
func NewBlock2QParams(size int, recentRatio float64, ghostRatio float64, onEvicted func(key uint64, value *block.Block)) (*BlockTwoQueueCache, error) {
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
	recent, err := NewBlockLRU(nil)
	if err != nil {
		return nil, err
	}
	frequent, err := NewBlockLRU(nil)
	if err != nil {
		return nil, err
	}
	recentEvict, err := NewBlockLRU(nil)
	if err != nil {
		return nil, err
	}

	// Initialize the cache
	c := &BlockTwoQueueCache{
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
func (c *BlockTwoQueueCache) Get(key uint64) (value *block.Block, ok bool) {
	c.lock.Lock()

	// Check if this is a frequent value
	if pkg, ok := c.frequent.Get(key); ok {
		atomic.AddInt64(&pkg.RefCount, 1)
		c.lock.Unlock()
		return pkg, ok
	}

	// If the value is contained in recent, then we
	// promote it to frequent
	if pkg, ok := c.recent.Peek(key); ok {
		c.recent.Remove(key)
		c.frequent.Add(key, pkg)
		atomic.AddInt64(&pkg.RefCount, 1)
		c.lock.Unlock()
		return pkg, ok
	}

	// No hit
	c.lock.Unlock()
	return nil, false
}

// Add adds a value to the cache.
func (c *BlockTwoQueueCache) Add(key uint64, value *block.Block) (updated, evicted bool) {
	c.lock.Lock()

	c.byteSize += value.HeapSize()
	atomic.AddInt64(&value.RefCount, 1)

	// Check if the value is frequently used already,
	// and just update the value
	if val, ok := c.frequent.Peek(key); ok {
		if val != value {
			c.byteSize -= val.HeapSize()
			c.frequent.Add(key, value)
			if c.onEvict != nil {
				c.onEvict(key, val)
			}
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
			if c.onEvict != nil {
				c.onEvict(key, val)
			}
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
func (c *BlockTwoQueueCache) ContainsOrAdd(key uint64, value *block.Block) (ok, evicted bool) {
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
func (c *BlockTwoQueueCache) ensureSpace() (evicted bool) {

	for c.byteSize > c.maxByteSize {
		recentLen := c.recent.Len()
		freqLen := c.frequent.Len()
		recentSize := int(float64(recentLen+freqLen) * c.recentRatio)

		var e bool
		var k uint64
		var v *block.Block
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
func (c *BlockTwoQueueCache) Len() int {
	c.lock.RLock()
	l := c.recent.Len() + c.frequent.Len()
	c.lock.RUnlock()
	return l
}

// Keys returns a slice of the keys in the cache.
// The frequently used keys are first in the returned slice.
func (c *BlockTwoQueueCache) Keys() []uint64 {
	c.lock.RLock()
	k1 := c.frequent.Keys()
	k2 := c.recent.Keys()
	c.lock.RUnlock()
	return append(k1, k2...)
}

// Remove removes the provided key from the cache.
func (c *BlockTwoQueueCache) Remove(key uint64) {
	c.lock.Lock()
	var val *block.Block
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

func (c *BlockTwoQueueCache) RemoveOldest() {
	c.lock.Lock()
	key, _, ok := c.recent.GetOldest()
	c.lock.Unlock()
	if ok {
		c.Remove(key)
	}
}

// Purge is used to completely clear the cache.
func (c *BlockTwoQueueCache) Purge() {
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
func (c *BlockTwoQueueCache) Contains(key uint64) bool {
	c.lock.RLock()
	ok := c.frequent.Contains(key) || c.recent.Contains(key)
	c.lock.RUnlock()
	return ok
}

// Peek is used to inspect the cache value of a key
// without updating recency or frequency.
func (c *BlockTwoQueueCache) Peek(key uint64) (value *block.Block, ok bool) {
	c.lock.RLock()
	var v *block.Block
	v, ok = c.frequent.Peek(key)
	if ok {
		value = v
		atomic.AddInt64(&value.RefCount, 1)
		c.lock.RUnlock()
		return
	}
	v, ok = c.recent.Peek(key)
	if ok {
		value = v
		atomic.AddInt64(&value.RefCount, 1)
	}
	c.lock.RUnlock()
	return
}
