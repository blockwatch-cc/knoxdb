// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package table

import (
	"context"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// doLookupRid finds the current row id for a pk that is not deleted. It may be
// kept in a journal segment and may have been not yet committed. We apply
// snapshot visibility rules to hide concurrent (uncommitted transactions).
// If the most recent live (non-deleted) version of a row has been written
// by a different active transaction, this means we found a write-write conflict.
func (t *Table) doLookupRid(ctx context.Context, pk uint64) (uint64, error) {
	// try journal first
	tx := engine.GetTransaction(ctx)
	snap := tx.Snapshot()
	rid, isConflict, ok := t.journal.FindRid(pk, snap)
	if ok {
		// Note: no conflicts as long as we're in single writer mode
		if isConflict {
			tx.Fail(engine.ErrTxConflict)
			return 0, engine.ErrTxConflict
		}
		return rid, nil
	}

	// TODO: use index pk -> rid

	// target is $rid field
	rs, err := t.schema.SelectFieldIds(schema.MetaRid)
	if err != nil {
		return 0, err
	}

	// filter condition (pk field is unique, all deleted records are in history)
	flt, err := query.Equal(t.schema.Pk().Name(), pk).Compile(t.schema)
	if err != nil {
		return 0, err
	}

	// query last update
	p := query.NewQueryPlan().
		WithTable(t).
		WithTag("lookup-rid").
		WithFlags(query.QueryFlagNoIndex).
		WithOrder(types.OrderDesc).
		WithLimit(1).
		WithSchema(rs).
		WithLogger(t.log).
		WithFilters(flt)
	defer p.Close()

	// query iterator
	r := t.NewReader().WithQuery(p)
	defer r.Close()

	// find match
	pkg, err := r.Next(ctx)
	if err != nil {
		return 0, err
	}
	if pkg == nil || pkg.NumSelected() == 0 {
		return 0, nil
	}

	// Note: handle multi-writer conflics (not needed as long as we're single writer)
	// load xmin column value and compare against concurrent txn in our snapshot
	// since all data in table packs has committed, we found a conflict when the
	// row creator is in the snapshot set
	// if snap.IsConflict(pkg.Xmin(int(hits[0]))) {
	// 	tx.Fail(engine.ErrTxConflict)
	// 	return 0, engine.ErrTxConflict
	// }

	// all clear, return rowid
	return pkg.RowId(int(pkg.Selected()[0])), nil
}
