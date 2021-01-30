// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"sort"
)

type Item interface {
	ID() uint64
	SetID(uint64)
}

type ItemList []Item

func (l ItemList) Len() int           { return len(l) }
func (l ItemList) Less(i, j int) bool { return l[i].ID() <= l[j].ID() }
func (l ItemList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

func SortItems(l []Item) {
	il := ItemList(l)
	if !sort.IsSorted(il) {
		sort.Sort(il)
	}
}
