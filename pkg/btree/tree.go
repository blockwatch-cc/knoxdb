// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package btree

import (
	"bytes"
	"iter"

	"github.com/RaduBerinde/btreemap"
)

const DefaultBtreeDegree = 32

// ChangeTree defines a change set for a database bucket which consists
// of updates (inserted or updated key/value pairs) and deletions (keys).
type ChangeTree struct {
	tr *btreemap.BTreeMap[[]byte, []byte]
}

func NewChangeTree() ChangeTree {
	tr := btreemap.New[[]byte, []byte](
		DefaultBtreeDegree,
		bytes.Compare,
	)
	return ChangeTree{tr: tr}
}

// Returns true when the tree has been dropped. Used to mark a full
// removal operation from a ChangeForest (a.k.a delete bucket).
func (m ChangeTree) IsNil() bool {
	return m.tr == nil
}

// Drops all content from the tree.
func (m *ChangeTree) Clear() {
	if m.tr != nil {
		m.tr.Clear(true)
	}
}

// Returns total key count.
func (m ChangeTree) Len() int {
	if m.tr == nil {
		return 0
	}
	return m.tr.Len()
}

// Get returns the value and whether the entry is a tombstone, in particular
// - `nil, true`  when key is deleted
// - `val, false` when key is updated
// - `nil, false` when key does not exist
func (m ChangeTree) Get(key []byte) ([]byte, bool) {
	_, val, ok := m.tr.Get(key)
	if !ok {
		return nil, false
	}
	if val == nil {
		return nil, true
	}
	return val, false
}

// Adds or updates key with given value.
func (m ChangeTree) Put(key, value []byte) {
	m.tr.ReplaceOrInsert(key, value)
}

// Removes key and value from the tree if exists.
func (m ChangeTree) Delete(key []byte) {
	m.tr.ReplaceOrInsert(key, nil)
}

// Returns the minimum key, its value and true if it exists.
func (m ChangeTree) Min() ([]byte, []byte, bool) {
	return m.tr.Min()
}

// Returns the maximum key, its value and true if it exists.
func (m ChangeTree) Max() ([]byte, []byte, bool) {
	return m.tr.Max()
}

// Visits all elements in ascending order.
func (m ChangeTree) Seq() iter.Seq2[[]byte, []byte] {
	return m.Scan(nil)
}

// Visits elements with common prefix in ascending order.
func (m ChangeTree) Scan(prefix []byte) iter.Seq2[[]byte, []byte] {
	return Scan(m.tr, prefix)
}

// Visits elements with common prefix in descending order.
func (m ChangeTree) ScanReverse(prefix []byte) iter.Seq2[[]byte, []byte] {
	return ScanReverse(m.tr, prefix)
}

// Visits elements in range [lower,upper) in ascending order.
func (m ChangeTree) ScanRange(lower, upper []byte) iter.Seq2[[]byte, []byte] {
	return ScanRange(m.tr, lower, upper)
}

// Visits elements in range [lower,upper) in descending order.
func (m ChangeTree) ScanRangeReverse(lower, upper []byte) iter.Seq2[[]byte, []byte] {
	return ScanRangeReverse(m.tr, lower, upper)
}

// Merge adds all keys and tombstones from n into m potentially
// replacing keys in m with new content.
func (m ChangeTree) Merge(n ChangeTree) {
	for k, v := range n.Scan(nil) {
		m.tr.ReplaceOrInsert(k, v)
	}
}

// Apply applies all changes collected in m to s removing keys
// with tombstones in m and inserting or replacing keys with values
// in m.
func (m ChangeTree) Apply(s *btreemap.BTreeMap[[]byte, []byte]) {
	for k, v := range m.tr.Ascend(PrefixRange(nil)) {
		if v == nil {
			s.Delete(k)
		} else {
			s.ReplaceOrInsert(k, v)
		}
	}
}

// ChangeForest defines a forest of change trees representing changes across
// multiple database buckets.
type ChangeForest map[uint32]ChangeTree

func (m ChangeForest) Clear() {
	for _, v := range m {
		v.Clear()
	}
	clear(m)
}

func (m ChangeForest) Get(id uint32) (ChangeTree, bool) {
	v, ok := m[id]
	return v, ok
}

func (m ChangeForest) AddOrGet(id uint32) ChangeTree {
	if v, ok := m[id]; ok {
		return v
	}
	c := NewChangeTree()
	m[id] = c
	return c
}

func (m ChangeForest) Drop(id uint32) {
	m[id] = ChangeTree{}
}
