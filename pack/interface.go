package pack

// LRUCache is the interface for simple LRU cache.
type LRUCache interface {
	// Adds a value to the cache, returns true if an eviction occurred and
	// updates the "recently used"-ness of the key.
	Add(key string, value *Package) (updated bool)

	// Returns key's value from the cache and
	// updates the "recently used"-ness of the key. #value, isFound
	Get(key string) (value *Package, ok bool)

	// Check if a key exsists in cache without updating the recent-ness.
	Contains(key string) (ok bool)

	// Returns key's value without updating the "recently used"-ness of the key.
	Peek(key string) (value *Package, ok bool)

	// Removes a key from the cache.
	Remove(key string) bool

	// Removes the oldest entry from cache.
	RemoveOldest() (string, *Package, bool)

	// Returns the oldest entry from the cache. #key, value, isFound
	GetOldest() (string, *Package, bool)

	// Returns a slice of the keys in the cache, from oldest to newest.
	Keys() []string

	// Returns the number of items in the cache.
	Len() int

	// Clear all cache entries
	Purge()
}
