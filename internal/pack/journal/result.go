// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"iter"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/xroar"
)

// TODO
// switch from pk centric to rid centric design
// - pks are irrelevant (we won't search for them anymore)
// - rids are the only interesting piece and only if deleted (replaced)
//   -> on query: mask deleted rids from table reader, mask deleted from journal query result
//   -> on merge: mask deleted move to history, remove from main table
//
// switch to vectorized pipeline
// - change to keep pairs of segment + selection vector only
// - build bitmap of deleted/replaced rids (for skip by table reader)
// - on query
//   - use table reader mask to exclude deleted/replaced rids
//   - output full pack with its sel vector as result
//   - last output all journal matches segment by segment
//
// switch to use compressed segment vectors after store (saves mem & time?)
// - keep xmax vector writable (unless segment is complete) so we can set xmax on abort
// - redesign rolling the active segment, flush in background
// - requires we replace a segment's pack after flush/reload
// - hence we must copy matching packs into query result and release on result close
//   -> shared blocks are ref-counted and freed when no longer needed by query result
//   -> cloned pack is a natural place to keep the query specifcic selection vector

type Result struct {
	tomb *xroar.Bitmap   // deleted rids
	pkgs []*pack.Package // pack copies with selection vector
}

func NewResult() *Result {
	return &Result{
		tomb: xroar.New(),
		pkgs: make([]*pack.Package, 0),
	}
}

func (r *Result) Close() {
	for _, pkg := range r.pkgs {
		pkg.Release()
	}
	clear(r.pkgs)
	r.pkgs = nil
	r.tomb = nil
}

func (r *Result) IsEmpty() bool {
	return len(r.pkgs) == 0 && r.tomb.Count() == 0
}

func (r *Result) Len() int {
	var n int
	for _, pkg := range r.pkgs {
		n += len(pkg.Selected())
	}
	return n
}

func (r *Result) TombMask() *xroar.Bitmap {
	if r.tomb.IsEmpty() {
		return nil
	}
	return r.tomb
}

func (r *Result) Append(seg *Segment, bits *bitset.Bitset) {
	// skip without matches
	if bits.None() {
		return
	}

	// create a shallow data pack copy referencing all data blocks as is
	clone := seg.data.Copy()

	// add selection vector unless all records match
	if !bits.All() {
		clone.WithSelection(bits.Indexes(arena.AllocUint32(bits.Count())))
	}

	r.pkgs = append(r.pkgs, clone)
}

func (r *Result) Iterator() iter.Seq[*pack.Package] {
	return func(fn func(*pack.Package) bool) {
		for _, pkg := range r.pkgs {
			if !fn(pkg) {
				return
			}
		}
	}
}

func (r *Result) ReverseIterator() iter.Seq[*pack.Package] {
	return func(fn func(*pack.Package) bool) {
		for i := len(r.pkgs) - 1; i >= 0; i-- {
			if !fn(r.pkgs[i]) {
				return
			}
		}
	}
}
