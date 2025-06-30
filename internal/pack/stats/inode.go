// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"bytes"

	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/pkg/schema"
)

type Node interface {
	Match(*filter.Node, *schema.View) bool
	Bytes() []byte
}

type INode struct {
	meta  []byte // wire encoded stats schema: min/max (keys, data cols), sum(size, n_val)
	dirty bool   // dirty flag
}

func NewINode() *INode {
	return &INode{}
}

func (n INode) Bytes() []byte {
	return n.meta
}

func (n INode) MinKey(view *schema.View) uint32 {
	val, ok := view.Reset(n.meta).GetPhy(STATS_ROW_KEY)
	view.Reset(nil)
	if !ok {
		return 0
	}
	return val.(uint32)
}

func (n INode) Version(view *schema.View) uint32 {
	val, ok := view.Reset(n.meta).GetPhy(STATS_ROW_VERSION)
	view.Reset(nil)
	if !ok {
		return 0
	}
	return val.(uint32)
}

func (n INode) NPacks(view *schema.View) int {
	// (u64) schema id repurposed
	val, ok := view.Reset(n.meta).GetPhy(STATS_ROW_SCHEMA)
	view.Reset(nil)
	if !ok {
		return 0
	}
	return int(val.(uint64))
}

func (n INode) NValues(view *schema.View) uint64 {
	val, ok := view.Reset(n.meta).GetPhy(STATS_ROW_NVALS)
	view.Reset(nil)
	if !ok {
		return 0
	}
	return val.(uint64)
}

func (n INode) Size(view *schema.View) int64 {
	val, ok := view.Reset(n.meta).GetPhy(STATS_ROW_SIZE)
	view.Reset(nil)
	if !ok {
		return 0
	}
	return val.(int64)
}

func (n INode) Get(view *schema.View, i int) (any, bool) {
	val, ok := view.Reset(n.meta).GetPhy(i)
	view.Reset(nil)
	return val, ok
}

func (n *INode) SetVersion(view *schema.View, ver uint32) {
	view.Reset(n.meta).Set(STATS_ROW_VERSION, ver)
	view.Reset(nil)
}

func (n *INode) Update(view *schema.View, wr *schema.Writer, left, right Node) bool {
	// update min/max/sum statistics from left and right children
	// note right may be nil
	if right == nil {
		if bytes.Equal(n.meta, left.Bytes()) {
			// no change
			return false
		}
		// copy left child
		n.meta = bytes.Clone(left.Bytes())
		n.dirty = true
		return true
	}

	// allocate meta buffer when nil
	if n.meta == nil {
		n.meta = make([]byte, wr.Len())
	}

	// merge left and right data when changed
	wr.Reset()

	for i, f := range view.Schema().Exported() {
		typ := f.Type.BlockType()
		lval, _ := view.Reset(left.Bytes()).GetPhy(i)
		rval, _ := view.Reset(right.Bytes()).GetPhy(i)
		vval, _ := view.Reset(n.meta).GetPhy(i)
		switch i {
		case STATS_ROW_KEY:
			// handle data pack key
			// min key is the left subtree's min key
			n.dirty = n.dirty || !typ.EQ(lval, vval)
			wr.Write(i, lval)

		case STATS_ROW_VERSION:
			// keep current value (will update on store)
			wr.Write(i, vval)

		case STATS_ROW_SCHEMA, STATS_ROW_NVALS, STATS_ROW_SIZE:
			// 1: sum data pack count (in u64 field)
			// 2: sum of number of records in data packs
			// 3: sum of disk sizes
			val := typ.Add(lval, rval)
			n.dirty = n.dirty || !typ.EQ(val, vval)
			wr.Write(i, val)

		default:
			// data column statistics
			if (i-STATS_DATA_COL_OFFSET)%2 == 0 {
				// min fields
				minVal := typ.Min(lval, rval)
				n.dirty = n.dirty || !typ.EQ(minVal, vval)
				wr.Write(i, minVal)
			} else {
				// max fields
				maxVal := typ.Max(lval, rval)
				n.dirty = n.dirty || !typ.EQ(maxVal, vval)
				wr.Write(i, maxVal)
			}
		}
	}

	// assemble wire layout
	if n.dirty {
		n.meta = wr.Bytes()
	}
	wr.Reset()

	// release view buffer
	view.Reset(nil)

	return n.dirty
}

func (n INode) Match(flt *filter.Node, view *schema.View) bool {
	view.Reset(n.meta)
	defer view.Reset(nil)
	return Match(flt, &ViewReader{view})
}
