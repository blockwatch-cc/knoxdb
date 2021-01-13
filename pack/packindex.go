// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"sort"

	"blockwatch.cc/knoxdb/vec"
)

// PackIndex implements efficient and scalable pack info/stats management as
// well as primary key placement (i.e. best pack selection).
//
// Packs are stored and referenced in key order ([4]byte = big endian uint32),
// by tables and indexes, but primary keys do not necessarily have a global order
// across packs. This means pk min-max ranges are unordered and we have to map
// from pk to packs in queries and flush operations.
//
// PackIndex takes care of searching a pack that contains a given pk and selects
// the best pack to store a new pk into. New or updated packs (i.e. from calls to split
// or storePack) are registered using their info. When empty packs are deleted
// (Table only) they must be deregistered.
//
// PackIndex keeps a list of all current pack info/stats and a list of all
// removed keys in memory. storePackHeader calls use these lists to update
// the stored representation.
//
type PackIndex struct {
	packs   PackInfoList
	minpks  []uint64
	maxpks  []uint64
	removed []uint32
	pairs   []pair
	pkidx   int
}

type pair struct {
	min uint64 // minimum value used as sort criteria
	pos int    // position in packs list
}

// may be used in {Index|Table}.loadPackHeaders
func NewPackIndex(packs PackInfoList, pkidx int) *PackIndex {
	if packs == nil {
		packs = make(PackInfoList, 0)
	}
	l := &PackIndex{
		packs:   packs,
		minpks:  make([]uint64, len(packs), cap(packs)),
		maxpks:  make([]uint64, len(packs), cap(packs)),
		removed: make([]uint32, 0),
		pkidx:   pkidx,
		pairs:   make([]pair, len(packs), cap(packs)),
	}
	sort.Sort(l.packs)
	for i := range l.packs {
		l.minpks[i] = l.packs[i].Blocks[l.pkidx].MinValue.(uint64)
		l.maxpks[i] = l.packs[i].Blocks[l.pkidx].MaxValue.(uint64)
		l.pairs[i].min = l.minpks[i]
		l.pairs[i].pos = i
	}
	l.Sort()
	return l
}

func (l PackIndex) NextKey() uint32 {
	if len(l.packs) == 0 {
		return 0
	}
	return l.packs[len(l.packs)-1].Key + 1
}

func (l *PackIndex) Len() int {
	return len(l.packs)
}

func (l *PackIndex) Sort() {
	sort.Slice(l.pairs, func(i, j int) bool { return l.pairs[i].min < l.pairs[j].min })
}

func (l *PackIndex) MinMax(n int) (uint64, uint64) {
	if n >= l.Len() {
		return 0, 0
	}
	return l.minpks[n], l.maxpks[n]
}

func (l *PackIndex) GlobalMinMax() (uint64, uint64) {
	if l.Len() == 0 {
		return 0, 0
	}
	pos := l.pairs[len(l.pairs)-1].pos
	return l.minpks[pos], l.maxpks[pos]
}

func (l *PackIndex) MinMaxSlices() ([]uint64, []uint64) {
	return l.minpks, l.maxpks
}

func (l *PackIndex) Get(i int) PackInfo {
	if i < 0 || i >= l.Len() {
		return PackInfo{}
	}
	return l.packs[i]
}

// called by storePack
func (l *PackIndex) AddOrUpdate(head PackInfo) {
	head.dirty = true
	l.removed = vec.Uint32.Remove(l.removed, head.Key)
	old, pos, isAdd := l.packs.Add(head)

	// keep positions of packs in l.packs in sync with positions stored in l.pairs
	// keep min values in l.pairs in sync with primary key min values in packs
	if isAdd {
		var needsort bool

		// appends of new packs can use a more efficient implementation
		if pos > 0 && pos == l.Len()-1 {
			// get the added packs min and the highest max of all managed packs
			newmin := l.packs[pos].Blocks[l.pkidx].MinValue.(uint64)
			newmax := l.packs[pos].Blocks[l.pkidx].MaxValue.(uint64)
			lastmax := l.packs[l.pairs[len(l.pairs)-1].pos].Blocks[l.pkidx].MaxValue.(uint64)

			// simple appends of packs with higher min than any existing max does
			// not require re-sorting the pair slice
			needsort = newmin < lastmax

			// append new pair
			l.pairs = append(l.pairs, pair{
				min: newmin,
				pos: pos,
			})
			l.minpks = append(l.minpks, newmin)
			l.maxpks = append(l.maxpks, newmax)

		} else {
			// all header positions after `pos` have shifted right, so we must update
			// all corresponding pairs as well; for simplicity we rebuild the entire
			// pairs slice from scratch

			// grow pairs if necessary
			if cap(l.pairs) < len(l.packs) {
				l.pairs = make([]pair, len(l.packs))
				l.minpks = make([]uint64, len(l.packs))
				l.maxpks = make([]uint64, len(l.packs))
			}
			l.pairs = l.pairs[:len(l.packs)]
			l.minpks = l.minpks[:len(l.packs)]
			l.maxpks = l.maxpks[:len(l.packs)]
			for i := range l.packs {
				l.minpks[i] = l.packs[i].Blocks[l.pkidx].MinValue.(uint64)
				l.maxpks[i] = l.packs[i].Blocks[l.pkidx].MaxValue.(uint64)
				l.pairs[i].min = l.minpks[i]
				l.pairs[i].pos = i
			}
			needsort = true
		}
		// resort the pairs slice by min value
		if needsort {
			sort.Slice(l.pairs, func(i, j int) bool { return l.pairs[i].min < l.pairs[j].min })
		}
	} else {
		// update min/max pk slices to stay in sync with pack headers
		newmin := l.packs[pos].Blocks[l.pkidx].MinValue.(uint64)
		newmax := l.packs[pos].Blocks[l.pkidx].MaxValue.(uint64)
		l.minpks[pos] = newmin
		l.maxpks[pos] = newmax

		// skip pair update if min value hasn't changed
		oldmin := old.Blocks[l.pkidx].MinValue.(uint64)
		if newmin == oldmin {
			return
		}

		// find and update the pair at position pos; since we know it's old min
		// value we can use binary search; also we know the value exists, so we
		// can skip checking i
		i := sort.Search(l.Len(), func(i int) bool { return l.pairs[i].min >= oldmin })
		if i < l.Len() && l.pairs[i].min == oldmin {
			l.pairs[i].min = newmin
		} else {
			log.Warnf("pack: pack index update mismatch: old-pack=%x new-pack=%x old-min=%d new-min=%d index=%d/%d",
				old.Key, head.Key, old.Blocks[l.pkidx].MinValue.(uint64), newmin, i, l.Len())
		}

		// it's guaranteed that a change in min value cannot make the list unsorted
		// so we can safely skip sorting here
	}
}

// called by storePack when packs are empty (Table only)
func (l *PackIndex) Remove(key uint32) {
	oldhead, pos := l.packs.RemoveKey(key)
	if pos < 0 {
		return
	}
	// store as dead key
	l.removed = vec.Uint32.AddUnique(l.removed, key)

	// remove pos from min/max slices
	l.minpks = append(l.minpks[:pos], l.minpks[pos+1:]...)
	l.maxpks = append(l.maxpks[:pos], l.maxpks[pos+1:]...)

	// when just the trailing head has been removed we can use a more efficient
	// algorithm because head positions haven't changed
	if pos > 0 && pos == l.Len() {
		// find the pair to remove based on its min value using binary search
		// we know the pair must exist
		min := oldhead.Blocks[l.pkidx].MinValue.(uint64)
		i := sort.Search(l.Len(), func(i int) bool { return l.pairs[i].min >= min })
		l.pairs = append(l.pairs[:i], l.pairs[i+1:]...)

	} else {
		// all header positions after `pos` have shifted left, so we must update
		// all corresponding pairs as well; for simplicity we rebuild the entire
		// pairs slice from scratch
		l.pairs = l.pairs[:len(l.packs)]
		for i := range l.packs {
			l.pairs[i].min = l.packs[i].Blocks[l.pkidx].MinValue.(uint64)
			l.pairs[i].pos = i
		}

		// resort the pairs slice by min value
		sort.Slice(l.pairs, func(i, j int) bool { return l.pairs[i].min < l.pairs[j].min })
	}
}

// assumes pair list is sorted by min value and pack min/max ranges don't overlap
// this is the case for all index and table packs because the placement algorithm
// and the splitting algorithm make sure overlaps never exist
func (l *PackIndex) Best(val uint64) (int, uint64, uint64) {
	numpacks := l.Len()

	// initially we stick to the first pack until split
	if numpacks == 0 {
		return 0, 0, 0
	}

	// find first pack with min larger than value
	i := sort.Search(numpacks, func(i int) bool { return l.pairs[i].min > val })

	// assign value to the previous pack or the very first pack
	// note that when value is larger than any pack's min value
	// it is assign to last pack
	if i > 0 {
		i--
	}

	// return the pack's list position and the corresponding min/max header values
	pos := l.pairs[i].pos
	return pos, l.minpks[pos], l.maxpks[pos]
}
