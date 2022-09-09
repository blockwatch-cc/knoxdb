// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

type Cache interface {
	Purge()
	Add(key interface{}, value *Package) (updated, evicted bool)
	Get(key interface{}) (value *Package, ok bool)
	Contains(key interface{}) bool
	Peek(key interface{}) (value *Package, ok bool)
	ContainsOrAdd(key interface{}, value *Package) (ok, evicted bool)
	Remove(key interface{})
	RemoveOldest()
	Keys() []interface{}
	Len() int
}

func NewNoCache() *NoCache {
	return &NoCache{}
}

type NoCache struct{}

func (n *NoCache) Purge() {}

func (n *NoCache) Add(_ interface{}, _ *Package) (updated, evicted bool) {
	return
}

func (n *NoCache) Get(_ interface{}) (value *Package, ok bool) {
	return
}

func (n *NoCache) Contains(_ interface{}) bool {
	return false
}

func (n *NoCache) Peek(_ interface{}) (value *Package, ok bool) {
	return
}

func (n *NoCache) ContainsOrAdd(key interface{}, value *Package) (ok, evicted bool) {
	return
}

func (n *NoCache) Remove(key interface{}) {}

func (n *NoCache) RemoveOldest() {}

func (n *NoCache) Keys() []interface{} {
	return nil
}

func (n *NoCache) Len() int {
	return 0
}
