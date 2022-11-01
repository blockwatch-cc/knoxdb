// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rclru

type RefCountedElem interface {
	IncRef() int64
	DecRef() int64
	HeapSize() int
}

type Cache interface {
	Purge()
	Add(key string, value RefCountedElem) (updated, evicted bool)
	Get(key string) (value RefCountedElem, ok bool)
	Contains(key string) bool
	Peek(key string) (value RefCountedElem, ok bool)
	ContainsOrAdd(key string, value RefCountedElem) (ok, evicted bool)
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

func (n *NoCache) Add(_ string, _ RefCountedElem) (updated, evicted bool) {
	return
}

func (n *NoCache) Get(_ string) (value RefCountedElem, ok bool) {
	return
}

func (n *NoCache) Contains(_ string) bool {
	return false
}

func (n *NoCache) Peek(_ string) (value RefCountedElem, ok bool) {
	return
}

func (n *NoCache) ContainsOrAdd(key string, value RefCountedElem) (ok, evicted bool) {
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
