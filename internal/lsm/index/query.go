// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"bytes"
	"context"
	"fmt"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/bitmap"
)

func (idx *Index) isSupportedMode(m query.FilterMode) bool {
	switch m {
	case types.FilterModeEqual,
		types.FilterModeLt,
		types.FilterModeLe,
		types.FilterModeGt,
		types.FilterModeGe,
		types.FilterModeRange:
		return true
	default:
		return false
	}

}

// This index only supports the following condition types on lookup.
// - complex AND with equal prefix + one extra LT|LE|GT|GE|RG condition
// - simple prefix match (on first index field) for EQ|LT|LE|GT|GE|RG condition
func (idx *Index) CanMatch(c engine.QueryCondition) bool {
	node, ok := c.(*query.FilterTreeNode)
	if !ok || node.OrKind {
		return false
	}

	// simple conditions
	if node.IsLeaf() {
		if idx.schema.Exported()[0].Name != node.Filter.Name {
			return false
		}
		return !idx.IsComposite() && idx.isSupportedMode(node.Filter.Mode)
	}

	// composite conditions
	if !idx.IsComposite() {
		return false
	}

	// Composite AND nodes (at least one condition must match the first index field)
	firstField := idx.schema.Exported()[0]
	for _, v := range node.Children {
		if !v.IsLeaf() {
			continue
		}
		if v.Filter.Name == firstField.Name && idx.isSupportedMode(v.Filter.Mode) {
			return true
		}
	}

	return false
}

var (
	ZERO = []byte{}
	FF   = []byte{0xff}
)

// Process
// - Pre-condition invariants
//   - root node is empty or not leaf
//   - AND nodes are flattened
//
// - NON-LEAF nodes
//   - recurse
//
// - AND nodes
//   - foreach indexes check if we can produce a prefix scan from any condition combi
//   - calculate prefix and run scan -> bitset
//   - mark conditions as processed
//   - append bitset as new condition
//   - continue until no more indexes or no more conditions are left
//
// - OR nodes
//   - handle each child separately
//
// Limitations
// - IN, NI, NE, RE mode conditions cannot use range scans
// - index scans do not consider offset and limit (full index scans are costly)
//
// Cases
// A - AND(C,C) with full index
//
//	> AND(c,c,IN) -> merge bitsets -> scan bitset only
//
// B - AND(C,C) with partial index
//
//	> AND(c,C,IN) -> scan bitset, apply cond tree to each val
//
// C - AND(C,C) no index (or no index matched)
//
//	> AND(C,C) -> full scan, apply cond tree to each val
//
// D - OR(C,C) with full index
//
//	> OR(IN,IN) -> merge bitsets -> scan bitset only
//
// E - OR(C,C) with partial index
//
//	> OR(IN,C) -> full scan, apply cond tree to each val
//
// F - OR(C,C) with no index
//
//	> OR(C,C) -> full scan, apply cond tree to each val
//
// G - OR(AND(C,C),AND(C)) with full index
//
//	> OR(AND(c,c,IN),AND(c,IN)) -> merge bitsets -> scan bitset only
//
// H - OR(AND(C,C),AND(C)) with partial index
//
//	> OR(AND(C,c,IN),AND(C)) -> full scan, apply cond tree to each val
//
// I - OR(AND(C,C),C) with no index
//
//	> OR(AND(C,C),C) -> full scan, apply cond tree to each val
//
// TODO: run LSM index scans in batches & forward through operator tree
// this prevents accumulating large numbers of potential hits when most
// are later discarded by offset/limit
func (idx *Index) Query(ctx context.Context, c engine.QueryCondition) (*bitmap.Bitmap, bool, error) {
	node, ok := c.(*query.FilterTreeNode)
	if !ok {
		return nil, false, fmt.Errorf("invalid condition type %T", c)
	}

	if !node.IsLeaf() {
		return nil, false, fmt.Errorf("invalid branch node")
	}

	// idx.log.Infof("Query %s", node.Filter)

	// encode condition value as big endian key
	f, ok := idx.Schema().FieldById(node.Filter.Index + 1)
	if !ok {
		return nil, false, engine.ErrNoField
	}

	// encode condition value
	var (
		prefix, from, to []byte
		err              error
		buf              = new(bytes.Buffer)
	)
	if node.Filter.Mode == types.FilterModeEqual {
		err = f.Encode(buf, node.Filter.Value, BE)
		prefix = buf.Bytes()
	} else {
		switch node.Filter.Mode {
		case types.FilterModeLt:
			// LT    => scan(0x, to)
			err = f.Encode(buf, node.Filter.Value, BE)
			to = buf.Bytes()

		case types.FilterModeLe:
			// LE    => scan(0x, to)
			err = f.Encode(buf, node.Filter.Value, BE)
			to = store.BytesPrefix(buf.Bytes()).Limit

		case types.FilterModeGt:
			// GT    => scan(from, FF)
			err = f.Encode(buf, node.Filter.Value, BE)
			from = store.BytesPrefix(buf.Bytes()).Limit
			to = bytes.Repeat(FF, buf.Len())

		case types.FilterModeGe:
			// GE    => scan(from, FF)
			err = f.Encode(buf, node.Filter.Value, BE)
			from = buf.Bytes()
			to = bytes.Repeat(FF, buf.Len())

		case types.FilterModeRange:
			// RG    => scan(from, to)
			rg := node.Filter.Value.(query.RangeValue)
			err = f.Encode(buf, rg[0], BE)
			from = bytes.Clone(buf.Bytes())
			if err == nil {
				buf.Reset()
				err = f.Encode(buf, rg[1], BE)
				to = store.BytesPrefix(buf.Bytes()).Limit
			}
		}
	}
	if err != nil {
		return nil, false, err
	}

	// run inside a storage transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, false)
	if err != nil {
		return nil, false, err
	}

	var bits *bitmap.Bitmap
	if len(prefix) > 0 {
		// prefix scan
		bits, _, err = idx.scanTx(ctx, tx, prefix)
	} else {
		// run range scan
		bits, _, err = idx.rangeTx(ctx, tx, from, to)
	}

	if err != nil {
		return nil, false, err
	}

	node.Skip = true

	return bits, false, nil
}

func (idx *Index) QueryComposite(ctx context.Context, c engine.QueryCondition) (*bitmap.Bitmap, bool, error) {
	node, ok := c.(*query.FilterTreeNode)
	if !ok {
		return nil, false, fmt.Errorf("invalid condition type %T", c)
	}

	if node.IsLeaf() {
		return nil, false, fmt.Errorf("invalid leaf node")
	}

	if node.OrKind {
		return nil, false, fmt.Errorf("invalid OR node condition")
	}

	// analyze condition
	var (
		prefix []byte
		extra  *query.FilterTreeNode
	)

	// identify eligible conditions for constructing single or multi-field
	// range scans, this helps optimize some index queries
	eq := make(map[string]*query.FilterTreeNode) // all equal child conditions
	ex := make(map[string]*query.FilterTreeNode) // all eligible extra child conditions
	for _, child := range node.Children {
		f := child.Filter
		switch f.Mode {
		case types.FilterModeEqual:
			eq[f.Name] = child
		case types.FilterModeLt,
			types.FilterModeLe,
			types.FilterModeGt,
			types.FilterModeGe,
			types.FilterModeRange:
			ex[f.Name] = child
		}
	}

	// try combine multiple AND leaf conditions into longer index scans,
	// i.e. see if we can produce an ordered prefix from more than one condition
	//
	buf := new(bytes.Buffer)
	for _, field := range idx.schema.Fields() {
		name := field.Name()
		node, ok := eq[name]
		if !ok {
			// before stopping, check if we can append an extra range condition
			extra = ex[name]
			break
		}
		err := field.Encode(buf, node.Filter.Value, BE)
		if err != nil {
			return nil, false, err
		}
		// set skip flags signalling these conditions have been processed
		node.Skip = true
		delete(eq, name)
	}
	if extra != nil {
		extra.Skip = true
	}
	prefix = bytes.Clone(buf.Bytes())
	buf.Reset()

	// no possible prefix or extra match means the index does not support this query
	if len(prefix) == 0 && extra == nil {
		return nil, false, nil
	}

	// run inside a storage transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, false)
	if err != nil {
		return nil, false, err
	}

	// can use a regular prefix scan over EQ+ condition(s)
	if extra == nil {
		return idx.scanTx(ctx, tx, prefix)
	}

	// handle EQ+ plus extra range condition
	extraField, ok := idx.schema.FieldById(extra.Filter.Index + 1)
	if !ok {
		return nil, false, engine.ErrNoField
	}
	var from, to []byte

	switch extra.Filter.Mode {
	case types.FilterModeLt:
		// LT    => scan(0x, to)
		// EQ+LT => scan(prefix, prefix+to)
		err = extraField.Encode(buf, extra.Filter.Value, BE)
		from = prefix
		prefix = append(prefix, buf.Bytes()...)
		to = prefix

	case types.FilterModeLe:
		// LE    => scan(0x, to)
		// EQ+LE => scan(prefix, prefix+to)
		err = extraField.Encode(buf, extra.Filter.Value, BE)
		from = prefix
		to = store.BytesPrefix(append(prefix, buf.Bytes()...)).Limit

	case types.FilterModeGt:
		// GT    => scan(from, FF)
		// EQ+GT => scan(prefix+from, prefix+FF)
		err = extraField.Encode(buf, extra.Filter.Value, BE)
		from = store.BytesPrefix(append(prefix, buf.Bytes()...)).Limit
		to = bytes.Repeat(FF, len(prefix)+buf.Len())

	case types.FilterModeGe:
		// GE    => scan(from, FF)
		// EQ+GE => scan(prefix+from, prefix+FF)
		err = extraField.Encode(buf, extra.Filter.Value, BE)
		prefix = append(prefix, buf.Bytes()...)
		from = prefix
		to = bytes.Repeat(FF, len(prefix)+buf.Len())

	case types.FilterModeRange:
		// RG    => scan(from, to)
		// EQ+RG => scan(prefix+from, prefix+to)
		err = extraField.Encode(buf, extra.Filter.Value.(query.RangeValue)[0], BE)
		prefix = append(prefix, buf.Bytes()...)
		from = prefix
		if err == nil {
			buf.Reset()
			err = extraField.Encode(buf, extra.Filter.Value.(query.RangeValue)[1], BE)
			to = store.BytesPrefix(append(prefix, buf.Bytes()...)).Limit
		}
	}
	if err != nil {
		return nil, false, err
	}

	return idx.rangeTx(ctx, tx, from, to)
}

func (idx *Index) scanTx(_ context.Context, tx store.Tx, prefix []byte) (*bitmap.Bitmap, bool, error) {
	bits := bitmap.New()
	c := tx.Bucket(idx.key).Range(prefix, store.IndexCursor)
	defer c.Close()
	for ok := c.First(); ok; ok = c.Next() {
		key := c.Key()
		u64 := BE.Uint64(key[len(key)-8:]) // assumes pk is last 8 bytes of key
		bits.Set(u64)
	}
	return &bits, false, nil
}

func (idx *Index) rangeTx(_ context.Context, tx store.Tx, from, to []byte) (*bitmap.Bitmap, bool, error) {
	bits := bitmap.New()
	c := tx.Bucket(idx.key).Cursor(store.IndexCursor)
	defer c.Close()
	for ok := c.Seek(from); ok && bytes.Compare(c.Key(), to) < 0; ok = c.Next() {
		key := c.Key()
		u64 := BE.Uint64(key[len(key)-8:]) // assumes pk is last 8 bytes of key
		bits.Set(u64)
	}
	return &bits, false, nil
}
