// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package btree

import (
	"iter"

	"github.com/RaduBerinde/btreemap"
)

// PrefixRange is a helper to produce lower and upper bounds for
// prefix range scans.
func PrefixRange(prefix []byte) (btreemap.LowerBound[[]byte], btreemap.UpperBound[[]byte]) {
	if len(prefix) == 0 {
		return btreemap.Min[[]byte](), btreemap.Max[[]byte]()
	}
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		if c := prefix[i]; c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return btreemap.GE(prefix), btreemap.LT(limit)
}

// PrefixRangeReverse is a helper to produce lower and upper bounds for
// reverse prefix range scans.
func PrefixRangeReverse(prefix []byte) (btreemap.UpperBound[[]byte], btreemap.LowerBound[[]byte]) {
	if len(prefix) == 0 {
		return btreemap.Max[[]byte](), btreemap.Min[[]byte]()
	}
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		if c := prefix[i]; c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return btreemap.LT(limit), btreemap.GE(prefix)
}

// Scan iterates over keys in a bucket in ascending order
// and returns a sequence of key/value pairs.
func Scan(tr *btreemap.BTreeMap[[]byte, []byte], prefix []byte) iter.Seq2[[]byte, []byte] {
	return tr.Ascend(PrefixRange(prefix))
}

// ScanRange iterates over an explicit range of keys starting at
// a lower bound (inclusive) and ending before an upper bound (exclusive)
// in ascending order.
func ScanRange(tr *btreemap.BTreeMap[[]byte, []byte], start, end []byte) iter.Seq2[[]byte, []byte] {
	a, b := btreemap.Min[[]byte](), btreemap.Max[[]byte]()
	if start != nil {
		a = btreemap.GE(start)
	}
	if end != nil {
		b = btreemap.LT(end)
	}
	return tr.Ascend(a, b)
}

// ScanReverse iterates over keys in a bucket in descending order
// and returns a sequence of key/value pairs.
func ScanReverse(tr *btreemap.BTreeMap[[]byte, []byte], prefix []byte) iter.Seq2[[]byte, []byte] {
	return tr.Descend(PrefixRangeReverse(prefix))
}

// ScanRangeReverse iterates over an explicit range of keys starting at
// a lower bound (inclusive) and ending before an upper bound (exclusive)
// in descending order.
func ScanRangeReverse(tr *btreemap.BTreeMap[[]byte, []byte], start, end []byte) iter.Seq2[[]byte, []byte] {
	a, b := btreemap.Max[[]byte](), btreemap.Min[[]byte]()
	if end != nil {
		a = btreemap.LT(end)
	}
	if start != nil {
		b = btreemap.GE(start)
	}
	return tr.Descend(a, b)
}
