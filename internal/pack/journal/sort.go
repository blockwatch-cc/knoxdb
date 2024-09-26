// Copyright (c) 2018-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"sort"

	"blockwatch.cc/knoxdb/internal/bitset"
)

type dualSorter struct {
	pk []uint64
	id []int
}

func (s dualSorter) Len() int           { return len(s.pk) }
func (s dualSorter) Less(i, j int) bool { return s.pk[i] < s.pk[j] }
func (s dualSorter) Swap(i, j int) {
	s.pk[i], s.pk[j] = s.pk[j], s.pk[i]
	s.id[i], s.id[j] = s.id[j], s.id[i]
}

// On lookup/query we run matching algos on the journal pack which produce a bitset
// of all matches. The algo below takes this bitset and translates it into a pk
// sorted index list.
//
// 1. Cond.MatchPack() -> Bitset (1s at unsorted journal matches)
// 2. Bitset.Indexes() -> []int (positions in unsorted journal)
// 3. data.Column(pkid) -> []uint64 (lookup pks at indexes)
// 4. Joined sort index/pks by pk
// 5. Return pk-sorted index list
func (j *Journal) SortedIndexes(b *bitset.Bitset) ([]int, []uint64) {
	ds := dualSorter{
		pk: make([]uint64, b.Count()),
		id: b.Indexes(nil),
	}
	// fill pks
	pk := j.Data.PkColumn()
	for i, n := range ds.id {
		ds.pk[i] = pk[n]
	}
	sort.Sort(ds)

	// strip all entries that have been marked as deleted (pk == 0)
	firstNonZero := sort.Search(len(ds.pk), func(k int) bool { return ds.pk[k] > 0 })
	ds.id = ds.id[firstNonZero:]
	ds.pk = ds.pk[firstNonZero:]

	// return data pack positions and corresponding pks
	return ds.id, ds.pk
}

func (j *Journal) SortedIndexesReversed(b *bitset.Bitset) ([]int, []uint64) {
	id, pk := j.SortedIndexes(b)
	for i, j := 0, len(id)-1; i < j; i, j = i+1, j-1 {
		id[i], id[j] = id[j], id[i]
		pk[i], pk[j] = pk[j], pk[i]
	}
	return id, pk
}
