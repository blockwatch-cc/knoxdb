// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package btree

import (
	"bytes"
	"iter"
	"slices"
)

// Merge2 merges two sequence cursors in key order considering tombstone
// semantics. Internally merge uses the moderized Golang iter.Pull framework
// and optimizes around its performance limitations using the ideas from
// https://github.com/achille-roussel/kway-go.
//
// - all sequences must traverse in ascending order
// - the first sequence has higher priority
// - only the first sequence may contain tombstones
//
// For higher order k-merge either compose manually from Merge2 or use
// the slightly less efficient MergeK algorithm below.
func Merge2(seq0, seq1 iter.Seq2[[]byte, []byte]) iter.Seq2[[]byte, []byte] {
	return unbuffer(merge2(ByteKeyCompare, buffer(seq0), buffer(seq1)))
}

// Merge2 but reversed order for descending iteration.
func Merge2R(seq0, seq1 iter.Seq2[[]byte, []byte]) iter.Seq2[[]byte, []byte] {
	return unbuffer(merge2r(ByteKeyCompare, buffer(seq0), buffer(seq1)))
}

// Merge2Compare works like Merge2 except it allows users top specify
// a custom compare function.
func Merge2Compare(cmp func([2][]byte, [2][]byte) (int, bool), seq0, seq1 iter.Seq2[[]byte, []byte]) iter.Seq2[[]byte, []byte] {
	return unbuffer(merge2(cmp, buffer(seq0), buffer(seq1)))
}

// Compares values of both records returning their bytes.Compare result
// and whether the first value is a tombstone. Rationale behind this is
// that when called with two sequences A and B where A is newer than B
// (i.e. A is a transaction changeset and B is a database cursor)
// then a tombstone in A overrides B. It is irrelevant whether B also
// contained a tombstone for the same key or not.
func ByteKeyCompare(a [2][]byte, b [2][]byte) (int, bool) {
	return bytes.Compare(a[0], b[0]), a[1] == nil
}

// pullState holds the state for a pull-based iterator.
type pullState struct {
	nextFunc func() ([]byte, []byte, bool)
	stopFunc func()
	key, val []byte
	ok       bool
}

// MergeK merges multiple iter.Seq2[[]byte, []byte] sequences with
// tombstone skipping. Ties between same keys are broken by selecting
// the first sequence (top or newest layer) where a key appears. Efficiency
// gains are mostly from tracking a primary sequence and only re-evaluating
// the primary when the boundary of a lower layer iterator is crossed.
// This reduces key comparisons to single boundary checks and works best
// for long runs where the primary iterator stays the same.
func MergeK(seqs ...iter.Seq2[[]byte, []byte]) iter.Seq2[[]byte, []byte] {
	seqs = slices.DeleteFunc(seqs, func(fn iter.Seq2[[]byte, []byte]) bool {
		return fn == nil
	})
	switch len(seqs) {
	case 1:
		return seqs[0]
	case 2:
		return Merge2(seqs[0], seqs[1])
	}
	return func(yield func([]byte, []byte) bool) {
		if len(seqs) == 0 {
			return
		}

		// Initialize pull states
		states := make([]pullState, len(seqs))
		for i, seq := range seqs {
			next, stop := iter.Pull2(seq)
			states[i] = pullState{nextFunc: next, stopFunc: stop}
			states[i].key, states[i].val, states[i].ok = next()
		}
		defer func() {
			for i := range states {
				states[i].stopFunc()
			}
		}()

		// Boundary-tracking variables
		primary := -1
		boundary := []byte(nil)

		update := func() {
			var minKey, secKey []byte
			minIdx := -1

			for i := range states {
				if !states[i].ok {
					continue
				}
				if minKey == nil || bytes.Compare(states[i].key, minKey) < 0 {
					// Previous min becomes second
					secKey = minKey
					minKey = states[i].key
					minIdx = i
				} else if secKey == nil || bytes.Compare(states[i].key, secKey) < 0 {
					secKey = states[i].key
				}
			}

			primary = minIdx
			boundary = secKey
		}

		// Initial update
		update()

		for primary != -1 {
			key := states[primary].key
			val := states[primary].val

			// Skip tombstones
			if val != nil {
				if !yield(key, val) {
					return
				}
			}

			// Advance primary
			states[primary].key, states[primary].val, states[primary].ok = states[primary].nextFunc()

			// If primary exhausted or its new key >= boundary, advance secondary iterators <= key, then re-evaluate
			if !states[primary].ok || boundary == nil || (boundary != nil && bytes.Compare(states[primary].key, boundary) >= 0) {
				for i := range states {
					if i != primary && states[i].ok && bytes.Compare(states[i].key, key) <= 0 {
						states[i].key, states[i].val, states[i].ok = states[i].nextFunc()
					}
				}
				update()
			}
		}
	}
}

// MergeKR is the reverse order version of MergeK. It's important that
// input sequences are in reverse order for the output to be correct.
func MergeKR(seqs ...iter.Seq2[[]byte, []byte]) iter.Seq2[[]byte, []byte] {
	seqs = slices.DeleteFunc(seqs, func(fn iter.Seq2[[]byte, []byte]) bool {
		return fn == nil
	})
	switch len(seqs) {
	case 1:
		return seqs[0]
	case 2:
		return Merge2R(seqs[0], seqs[1])
	}
	return func(yield func([]byte, []byte) bool) {
		if len(seqs) == 0 {
			return
		}

		// Initialize pull states
		states := make([]pullState, len(seqs))
		for i, seq := range seqs {
			next, stop := iter.Pull2(seq)
			states[i] = pullState{nextFunc: next, stopFunc: stop}
			states[i].key, states[i].val, states[i].ok = next()
		}
		defer func() {
			for i := range states {
				states[i].stopFunc()
			}
		}()

		// Boundary-tracking variables
		primary := -1
		boundary := []byte(nil)

		update := func() {
			var maxKey, secKey []byte
			maxIdx := -1

			for i := range states {
				if !states[i].ok {
					continue
				}
				if maxKey == nil || bytes.Compare(states[i].key, maxKey) > 0 {
					// Previous max becomes second
					secKey = maxKey
					maxKey = states[i].key
					maxIdx = i
				} else if secKey == nil || bytes.Compare(states[i].key, secKey) > 0 {
					secKey = states[i].key
				}
			}

			primary = maxIdx
			boundary = secKey
		}

		// Initial update
		update()

		for primary != -1 {
			key := states[primary].key
			val := states[primary].val

			// Skip tombstones
			if val != nil {
				if !yield(key, val) {
					return
				}
			}

			// Advance primary
			states[primary].key, states[primary].val, states[primary].ok = states[primary].nextFunc()

			// If primary exhausted or its new key >= boundary, advance secondary iterators <= key, then re-evaluate
			if !states[primary].ok || boundary == nil || (boundary != nil && bytes.Compare(states[primary].key, boundary) <= 0) {
				for i := range states {
					if i != primary && states[i].ok && bytes.Compare(states[i].key, key) >= 0 {
						states[i].key, states[i].val, states[i].ok = states[i].nextFunc()
					}
				}
				update()
			}
		}
	}
}
