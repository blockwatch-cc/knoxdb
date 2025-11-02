// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"iter"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/xroar"
)

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
		if sel := pkg.Selected(); sel != nil {
			arena.Free(sel)
		}
		pkg.Release()
	}
	clear(r.pkgs)
	r.pkgs = nil
	r.tomb = nil
}

func (r *Result) IsEmpty() bool {
	return len(r.pkgs) == 0 && r.tomb.None()
}

func (r *Result) Len() int {
	var n int
	for _, pkg := range r.pkgs {
		n += pkg.NumSelected()
	}
	return n
}

func (r *Result) TombMask() *xroar.Bitmap {
	if r.tomb.None() {
		return nil
	}
	return r.tomb
}

func (r *Result) Append(seg *Segment, bits *bitset.Bitset) {
	// create a shallow data pack copy referencing all data blocks as is
	clone := seg.data.Copy()

	// add selection vector unless all records match
	if !bits.All() {
		clone.WithSelection(bits.Indexes(nil))
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
