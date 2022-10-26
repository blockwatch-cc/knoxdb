package pack

import (
	"container/list"

	"blockwatch.cc/knoxdb/encoding/block"
)

// LRUCache is the interface for simple LRU cache.
type BlockLRUCache interface {
	// Adds a value to the cache, returns true if an eviction occurred and
	// updates the "recently used"-ness of the key.
	Add(key uint64, value *block.Block) (updated bool)

	// Returns key's value from the cache and
	// updates the "recently used"-ness of the key. #value, isFound
	Get(key uint64) (value *block.Block, ok bool)

	// Check if a key exsists in cache without updating the recent-ness.
	Contains(key uint64) (ok bool)

	// Returns key's value without updating the "recently used"-ness of the key.
	Peek(key uint64) (value *block.Block, ok bool)

	// Removes a key from the cache.
	Remove(key uint64) bool

	// Removes the oldest entry from cache.
	RemoveOldest() (uint64, *block.Block, bool)

	// Returns the oldest entry from the cache. #key, value, isFound
	GetOldest() (uint64, *block.Block, bool)

	// Returns a slice of the keys in the cache, from oldest to newest.
	Keys() []uint64

	// Returns the number of items in the cache.
	Len() int

	// Clear all cache entries
	Purge()
}

// EvictCallback is used to get a callback when a cache entry is evicted
type BlockEvictCallback func(key uint64, value *block.Block)

// LRU implements a non-thread safe fixed size LRU cache
type BlockLRU struct {
	evictList *list.List
	items     map[uint64]*list.Element
	onEvict   BlockEvictCallback
}

// entry is used to hold a value in the evictList
type blockentry struct {
	key   uint64
	value *block.Block
}

// NewLRU constructs an LRU of the given size
func NewBlockLRU(onEvict BlockEvictCallback) (*BlockLRU, error) {
	c := &BlockLRU{
		evictList: list.New(),
		items:     make(map[uint64]*list.Element),
		onEvict:   onEvict,
	}
	return c, nil
}

// Purge is used to completely clear the cache.
func (c *BlockLRU) Purge() {
	for k, v := range c.items {
		if c.onEvict != nil {
			c.onEvict(k, v.Value.(*blockentry).value)
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *BlockLRU) Add(key uint64, value *block.Block) (updated bool) {
	// Check for existing item
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*blockentry).value = value
		return true
	}

	// Add new item
	ent := &blockentry{key, value}
	entry := c.evictList.PushFront(ent)
	c.items[key] = entry

	return false
}

// Get looks up a key's value from the cache.
func (c *BlockLRU) Get(key uint64) (value *block.Block, ok bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		return ent.Value.(*blockentry).value, true
	}
	return
}

// Contains checks if a key is in the cache, without updating the recent-ness
// or deleting it for being stale.
func (c *BlockLRU) Contains(key uint64) (ok bool) {
	_, ok = c.items[key]
	return ok
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *BlockLRU) Peek(key uint64) (value *block.Block, ok bool) {
	var ent *list.Element
	if ent, ok = c.items[key]; ok {
		return ent.Value.(*blockentry).value, true
	}
	return nil, ok
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *BlockLRU) Remove(key uint64) (present bool) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *BlockLRU) RemoveOldest() (key uint64, value *block.Block, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
		kv := ent.Value.(*blockentry)
		return kv.key, kv.value, true
	}
	return 0, nil, false
}

// GetOldest returns the oldest blockentry
func (c *BlockLRU) GetOldest() (key uint64, value *block.Block, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		kv := ent.Value.(*blockentry)
		return kv.key, kv.value, true
	}
	return 0, nil, false
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *BlockLRU) Keys() []uint64 {
	keys := make([]uint64, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = ent.Value.(*blockentry).key
		i++
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *BlockLRU) Len() int {
	return c.evictList.Len()
}

// removeOldest removes the oldest item from the cache.
func (c *BlockLRU) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *BlockLRU) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*blockentry)
	delete(c.items, kv.key)
	if c.onEvict != nil {
		c.onEvict(kv.key, kv.value)
	}
}
