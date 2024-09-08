// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"sort"

	"blockwatch.cc/knoxdb/pkg/slicex"
)

// MetadataIndex implements efficient and scalable pack metadata management as
// well as primary key placement (i.e. best pack selection) for tables and indexes.
//
// Packs are stored and referenced in key order ([4]byte = big endian uint32),
// and primary keys have a global order across packs. This means pk min-max ranges
// may be unordered, but can never overlap.
//
// MetadataIndex can find a pack that may contain a given pk and can select
// the best pack to store a new pk. New or updated packs (i.e. from calls to split
// or storePack) are registered using their pack metadata. Empty packs (Table only)
// must be deregistered before deletion.
//
// MetadataIndex keeps a list of all current pack metadata and a list of all
// removed pack keys in memory. Store() can persist the index to storage.
type MetadataIndex struct {
	packs   PackMetadataList               // list of package identity and block statistics
	minpks  []uint64                       // min statistics for pk field
	maxpks  []uint64                       // max statistics for pk field
	removed *slicex.OrderedNumbers[uint32] // list of keys for removed packs
	pos     []int32                        // list of pack positions sorted by min value
	pki     int
	maxrows int
}

// may be used in {Index|Table}.loadPackHeaders
func NewMetadataIndex(pki, maxrows int) *MetadataIndex {
	return &MetadataIndex{
		packs:   make(PackMetadataList, 0),
		minpks:  make([]uint64, 0),
		maxpks:  make([]uint64, 0),
		removed: slicex.NewOrderedNumbers[uint32](nil),
		pki:     pki,
		pos:     make([]int32, 0),
		maxrows: maxrows,
	}
}

func (l *MetadataIndex) Clear() {
	for _, v := range l.packs {
		l.removed.Insert(v.Key)
	}
	l.packs = l.packs[:0]
	l.minpks = l.minpks[:0]
	l.maxpks = l.maxpks[:0]
	l.pos = l.pos[:0]
}

func (l *MetadataIndex) Reset() {
	l.packs = l.packs[:0]
	l.minpks = l.minpks[:0]
	l.maxpks = l.maxpks[:0]
	l.pos = l.pos[:0]
	l.removed.Values = l.removed.Values[:0]
}

func (l MetadataIndex) NextKey() uint32 {
	if len(l.packs) == 0 {
		return 0
	}
	return l.packs[len(l.packs)-1].Key + 1
}

func (l *MetadataIndex) Len() int {
	return len(l.packs)
}

func (l *MetadataIndex) Count() int {
	var count int
	for i := range l.packs {
		count += l.packs[i].NValues
	}
	return count
}

func (l *MetadataIndex) HeapSize() int {
	sz := szMetadataIndex
	sz += len(l.minpks) * 8
	sz += len(l.maxpks) * 8
	sz += len(l.removed.Values) * 4
	sz += len(l.pos) * 4
	for i := range l.packs {
		sz += l.packs[i].HeapSize()
	}
	return sz
}

func (l *MetadataIndex) TableSize() int {
	var sz int
	for i := range l.packs {
		sz += l.packs[i].StoredSize
	}
	return sz
}

func (l *MetadataIndex) Sort() {
	// sort by min/max/pos -- see testcases
	sort.Slice(l.pos, func(i, j int) bool {
		posi, posj := l.pos[i], l.pos[j]
		mini, maxi := l.minpks[posi], l.maxpks[posi]
		minj, maxj := l.minpks[posj], l.maxpks[posj]
		return mini < minj || (mini == minj && maxi < maxj) || (mini == minj && maxi == maxj && i < j)
	})
}

func (l *MetadataIndex) MinMax(n int) (uint64, uint64) {
	if n >= l.Len() {
		return 0, 0
	}
	return l.minpks[n], l.maxpks[n]
}

func (l *MetadataIndex) MinMaxSorted(n int) (uint64, uint64) {
	if n >= l.Len() {
		return 0, 0
	}
	pos := l.pos[n]
	return l.minpks[pos], l.maxpks[pos]
}

func (l *MetadataIndex) GlobalMinMax() (uint64, uint64) {
	if l.Len() == 0 {
		return 0, 0
	}
	return l.minpks[l.pos[0]], l.maxpks[l.pos[len(l.pos)-1]]
}

func (l *MetadataIndex) MinMaxSlices() ([]uint64, []uint64) {
	return l.minpks, l.maxpks
}

func (l *MetadataIndex) AllPacks() PackMetadataList {
	return l.packs
}

func (l *MetadataIndex) GetPos(i int) (*PackMetadata, bool) {
	if i < 0 || i >= l.Len() {
		return nil, false
	}
	return l.packs[i], true
}

func (l *MetadataIndex) GetSorted(i int) (*PackMetadata, bool) {
	if i < 0 || i >= l.Len() {
		return nil, false
	}
	return l.packs[l.pos[i]], true
}

func (l *MetadataIndex) GetByKey(key uint32) (*PackMetadata, bool) {
	if len(l.packs) == 0 {
		return nil, false
	}

	// check if pack was removed
	if l.removed.Contains(key) {
		return nil, false
	}

	// search for pack
	i := sort.Search(len(l.packs), func(i int) bool { return l.packs[i].Key >= key })
	if i >= len(l.packs) || l.packs[i].Key != key {
		// key is not present at l.packs[i]
		return nil, false
	}
	return l.packs[i], true
}

func (l *MetadataIndex) IsFull(i int) bool {
	if i < 0 || i >= l.Len() {
		return false
	}
	return l.maxrows > 0 && l.packs[i].NValues >= l.maxrows
}

// Called by StorePack with updated pack metadata
func (l *MetadataIndex) AddOrUpdate(m *PackMetadata) {
	m.Dirty = true
	l.removed.Remove(m.Key)
	old, pos, isAdd := l.packs.Add(m)
	var needsort bool

	// keep positions of packs in l.packs in sync with positions stored in l.pos
	if isAdd {
		// appends of new packs can use a more efficient implementation
		if pos > 0 && pos == l.Len()-1 {
			// get the added packs min and the highest max of all managed packs
			newmin := l.packs[pos].Blocks[l.pki].MinValue.(uint64)
			newmax := l.packs[pos].Blocks[l.pki].MaxValue.(uint64)
			lastmax := l.packs[l.pos[len(l.pos)-1]].Blocks[l.pki].MaxValue.(uint64)

			// simple appends of packs with higher min than any existing max does
			// not require re-sorting the pos slice
			needsort = newmin < lastmax

			// append new values
			l.pos = append(l.pos, int32(pos))
			l.minpks = append(l.minpks, newmin)
			l.maxpks = append(l.maxpks, newmax)

		} else {
			// all header positions after `pos` have shifted right, so we must update
			// all corresponding pos as well; for simplicity we rebuild the entire
			// pos slice from scratch

			// grow pos if necessary
			if cap(l.pos) < len(l.packs) {
				l.pos = make([]int32, len(l.packs))
				l.minpks = make([]uint64, len(l.packs))
				l.maxpks = make([]uint64, len(l.packs))
			}
			l.pos = l.pos[:len(l.packs)]
			l.minpks = l.minpks[:len(l.packs)]
			l.maxpks = l.maxpks[:len(l.packs)]
			for i := range l.packs {
				l.minpks[i] = l.packs[i].Blocks[l.pki].MinValue.(uint64)
				l.maxpks[i] = l.packs[i].Blocks[l.pki].MaxValue.(uint64)
				l.pos[i] = int32(i)
			}
			needsort = true
		}
	} else {
		// update min/max pk slices to stay in sync with pack headers
		newmin := l.packs[pos].Blocks[l.pki].MinValue.(uint64)
		newmax := l.packs[pos].Blocks[l.pki].MaxValue.(uint64)
		l.minpks[pos] = newmin
		l.maxpks[pos] = newmax

		// skip sort if min/max values haven't changed

		// TODO(echa): check it is safe to not sort on max change
		// oldmin := old.Blocks[l.pki].MinValue.(uint64)
		// oldmax := old.Blocks[l.pki].MaxValue.(uint64)
		// needsort = oldmin != newmin || oldmax != newmax
		oldmin := old.Blocks[l.pki].MinValue.(uint64)
		needsort = oldmin != newmin
	}
	if needsort {
		l.Sort()
	}
}

// called by storePack when packs are empty (Table only, index packs are never removed)
func (l *MetadataIndex) Remove(key uint32) {
	oldhead, pos := l.packs.RemoveKey(key)
	if pos < 0 {
		return
	}
	// store as dead key
	l.removed.Insert(key)

	// when just the trailing head has been removed we can use a more efficient
	// algorithm because head positions haven't changed
	if pos > 0 && pos == l.Len() {
		// find the pair to remove based on its min value using binary search
		// we know the pair must exist
		min := oldhead.Blocks[l.pki].MinValue.(uint64)
		i := sort.Search(l.Len(), func(i int) bool { return l.minpks[l.pos[i]] >= min })
		// when multiple packs share the same min value, find the correct one based on pos
		for i < l.Len() && l.pos[i] != int32(pos) && l.minpks[l.pos[i]] == min {
			i++
		}
		// remove pos from min/max slices
		l.pos = append(l.pos[:i], l.pos[i+1:]...)
		l.minpks = append(l.minpks[:pos], l.minpks[pos+1:]...)
		l.maxpks = append(l.maxpks[:pos], l.maxpks[pos+1:]...)
	} else {
		// all header positions after `pos` have shifted left, so we must run
		// a full update; for simplicity we rebuild the entire pos slice from scratch
		l.minpks = append(l.minpks[:pos], l.minpks[pos+1:]...)
		l.maxpks = append(l.maxpks[:pos], l.maxpks[pos+1:]...)
		l.pos = l.pos[:len(l.packs)]
		for i := range l.packs {
			l.pos[i] = int32(i)
		}
		l.Sort()
	}
}

// Returns placement hint and pack info for the specified primary key.
//
// Assumes pos list is sorted by min value and pack min/max ranges don't overlap
// this is the case for all index and table packs because placement and split
// algorithms ensure overlaps never exist.
func (l *MetadataIndex) Best(val uint64) (pos int, packmin uint64, packmax uint64, nextmin uint64, isFull bool) {
	count := l.Len()

	// initially we stick to the first pack until split
	if count == 0 {
		return 0, 0, 0, 0, false
	}

	// find first pack with min larger than value
	i := sort.Search(count, func(i int) bool { return l.minpks[l.pos[i]] > val })

	// assign value to the previous pack or the very first pack
	// note that when value is larger than any pack's min value
	// it is assigned to last pack
	if i > 0 {
		i--
	}

	// find min of follower pack
	if i+1 < count {
		nextmin = l.minpks[l.pos[i+1]]
	}

	// return the pack's list position and the corresponding min/max header values
	pos = int(l.pos[i])
	packmin, packmax = l.minpks[pos], l.maxpks[pos]
	isFull = l.maxrows > 0 && l.packs[pos].NValues >= l.maxrows
	return
}

// Returns pack info for the logical follower pack (in min/max sort order).
func (l *MetadataIndex) Next(last int) (pos int, packmin uint64, packmax uint64, nextmin uint64, isFull bool) {
	next := last + 1
	count := l.Len()
	if next >= count {
		return 0, 0, 0, 0, false
	}
	if next+1 < count {
		nextmin = l.minpks[l.pos[next+1]]
	}
	pos = int(l.pos[next])
	packmin, packmax = l.minpks[pos], l.maxpks[pos]
	isFull = l.maxrows > 0 && l.packs[pos].NValues >= l.maxrows
	return
}
