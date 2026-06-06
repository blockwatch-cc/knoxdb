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
	if len(n.meta) == 0 {
		return 0
	}
	val := view.Reset(n.meta).GetPhy(STATS_ROW_KEY)
	view.Reset(nil)
	if val == nil {
		return 0
	}
	return val.(uint32)
}

func (n INode) Version(view *schema.View) uint32 {
	if len(n.meta) == 0 {
		return 0
	}
	val := view.Reset(n.meta).GetPhy(STATS_ROW_VERSION)
	view.Reset(nil)
	if val == nil {
		return 0
	}
	return val.(uint32)
}

func (n INode) NPacks(view *schema.View) int {
	// (u64) schema id repurposed
	if len(n.meta) == 0 {
		return 0
	}
	val := view.Reset(n.meta).GetPhy(STATS_ROW_SCHEMA)
	view.Reset(nil)
	if val == nil {
		return 0
	}
	return int(val.(uint64))
}

func (n INode) NValues(view *schema.View) uint64 {
	if len(n.meta) == 0 {
		return 0
	}
	val := view.Reset(n.meta).GetPhy(STATS_ROW_NVALS)
	view.Reset(nil)
	if val == nil {
		return 0
	}
	return val.(uint64)
}

func (n INode) Size(view *schema.View) int64 {
	if len(n.meta) == 0 {
		return 0
	}
	val := view.Reset(n.meta).GetPhy(STATS_ROW_SIZE)
	view.Reset(nil)
	if val == nil {
		return 0
	}
	return val.(int64)
}

func (n INode) Get(view *schema.View, i int) (any, bool) {
	val := view.Reset(n.meta).GetPhy(i)
	view.Reset(nil)
	return val, val != nil
}

func (n *INode) SetVersion(view *schema.View, ver uint32) {
	view.Reset(n.meta).Set(STATS_ROW_VERSION, ver)
	view.Reset(nil)
}

func (n *INode) Update(view *schema.View, left, right Node) bool {
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
	s := view.Schema()
	if n.meta == nil {
		n.meta = make([]byte, s.MinWireSize)
	}

	// merge left and right data when changed
	wr := s.NewBuffer(1)

	for i, f := range s.Fields {
		typ := filter.ToValueType(f.Type)
		lval := view.Reset(left.Bytes()).GetPhy(i)
		rval := view.Reset(right.Bytes()).GetPhy(i)
		vval := view.Reset(n.meta).GetPhy(i)
		switch i {
		case STATS_ROW_KEY:
			// handle data pack key
			// min key is the left subtree's min key
			n.dirty = n.dirty || !typ.EQ(lval, vval)
			f.WriteValue(wr, lval, LE)

		case STATS_ROW_VERSION:
			// keep current value (will update on store)
			f.WriteValue(wr, vval, LE)

		case STATS_ROW_SCHEMA, STATS_ROW_NVALS, STATS_ROW_SIZE:
			// 1: sum data pack count (in u64 field)
			// 2: sum of number of records in data packs
			// 3: sum of disk sizes
			val := typ.Add(lval, rval)
			n.dirty = n.dirty || !typ.EQ(val, vval)
			f.WriteValue(wr, val, LE)

		default:
			// data column statistics
			if (i-STATS_DATA_COL_OFFSET)%2 == 0 {
				// min fields
				minVal := typ.Min(lval, rval)
				n.dirty = n.dirty || !typ.EQ(minVal, vval)
				f.WriteValue(wr, minVal, LE)
			} else {
				// max fields
				maxVal := typ.Max(lval, rval)
				n.dirty = n.dirty || !typ.EQ(maxVal, vval)
				f.WriteValue(wr, maxVal, LE)
			}
		}
	}

	// assemble wire layout
	if n.dirty {
		n.meta = wr.Bytes()
	}

	// release view buffer
	view.Reset(nil)

	return n.dirty
}

func (n INode) Match(flt *filter.Node, view *schema.View) bool {
	view.Reset(n.meta)
	defer view.Reset(nil)
	return Match(flt, &ViewReader{view})
}
