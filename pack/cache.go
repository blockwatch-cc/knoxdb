// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

type Cache interface {
	Purge()
	Add(key string, value *Package) (updated, evicted bool)
	Get(key string) (value *Package, ok bool)
	Contains(key string) bool
	Peek(key string) (value *Package, ok bool)
	ContainsOrAdd(key string, value *Package) (ok, evicted bool)
	Remove(key string)
	RemoveOldest()
	Keys() []string
	Len() int
	GetParams() (int, int, int, int)
}

func NewNoCache() *NoCache {
	return &NoCache{}
}

type NoCache struct{}

func (n *NoCache) Purge() {}

func (n *NoCache) Add(_ string, _ *Package) (updated, evicted bool) {
	return
}

func (n *NoCache) Get(_ string) (value *Package, ok bool) {
	return
}

func (n *NoCache) Contains(_ string) bool {
	return false
}

func (n *NoCache) Peek(_ string) (value *Package, ok bool) {
	return
}

func (n *NoCache) ContainsOrAdd(key string, value *Package) (ok, evicted bool) {
	return
}

func (n *NoCache) Remove(key string) {}

func (n *NoCache) RemoveOldest() {}

func (n *NoCache) Keys() []string {
	return nil
}

func (n *NoCache) Len() int {
	return 0
}

func (n *NoCache) GetParams() (int, int, int, int) {
	return 0, 0, 0, 0
}
