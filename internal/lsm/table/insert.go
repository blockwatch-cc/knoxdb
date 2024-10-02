// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"fmt"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// Table interface
// TODO: better to use typed buffer here []WireMessage or similar
func (t *Table) InsertRows(ctx context.Context, buf []byte) (uint64, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if len(buf) < t.schema.WireSize() {
		return 0, engine.ErrShortMessage
	}

	// open write transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&t.metrics.InsertCalls, 1)

	// keep a pre-image of the state
	firstPk := t.state.Sequence
	state := &t.state

	// cleanup on exit
	defer func() {
		// rollback table state to original value
		if state != nil {
			t.state = *state
		}
	}()

	// split buf into wire messages
	view, buf, _ := schema.NewView(t.schema).Cut(buf)

	// process each message independently, assign PK and insert
	for view.IsValid() {
		// assign primary key by writing directly into wire format buffer
		nextPk := t.state.Sequence
		view.SetPk(nextPk)

		// write value to storage, returns any previous value
		// which we need to update indexes below
		prev, err := t.putTx(tx, engine.Key64Bytes(nextPk), view.Bytes())
		if err != nil {
			return 0, err
		}

		// update indexes, note indexes may be hosted in different
		// db files and engines, so store.Tx may not be relevant
		for _, idx := range t.indexes {
			idx.Add(ctx, prev, view.Bytes())
		}

		// process next message, if any
		view, buf, _ = view.Cut(buf)

		// advance table sequence
		t.state.Sequence++
	}

	// update state in catalog (will commit with main tx)
	t.engine.Catalog().SetState(t.tableId, t.state.ToObjectState())
	state = nil

	// return first primary key assigned
	return firstPk, nil
}

func (t *Table) UpdateRows(ctx context.Context, buf []byte) (uint64, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if len(buf) < t.schema.WireSize() {
		return 0, engine.ErrShortMessage
	}

	// open write transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&t.metrics.UpdateCalls, 1)

	// split buf into wire messages
	view, buf, _ := schema.NewView(t.schema).Cut(buf)
	var n uint64

	// process each message independently, assign PK and insert
	for view.IsValid() {
		// check primary key exists
		pk := view.GetPk()

		// write value to storage, returns any previous value
		// which we need to update indexes below
		prev, err := t.putTx(tx, engine.Key64Bytes(pk), view.Bytes())
		if err != nil {
			return 0, err
		}

		// fail when prev is nil (no previous value exists)
		if prev == nil {
			return 0, fmt.Errorf("update: missing pk %d", pk)
		}

		// update indexes, note indexes may be hosted in different
		// db files and engines, so store.Tx may not be relevant
		for _, idx := range t.indexes {
			idx.Add(ctx, prev, view.Bytes())
		}

		// process next message, if any
		view, buf, _ = view.Cut(buf)
		n++
	}

	return n, nil
}

func (t *Table) ApplyWalRecord(ctx context.Context, rec *wal.Record) error {
	return engine.ErrNotImplemented
}
