// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// design
// - keep table interface as simple as possible
//   - insert and create pks/rids
//   - write wal
//   - write journal
//
// - when full, write journal segment & wal checkpoint, make immutable
// - track active txn per segment and rewrite xmin/xmax metadata when complete
// - on completion, start segment merge
//
// TODO: howto manage state updates/rollbacks on commit/abort (determinism)?
// - commit/abort hooks?
func (t *Table) InsertRows(ctx context.Context, buf []byte) (uint64, error) {
	// check message
	if len(buf) == 0 {
		return 0, nil
	}
	if len(buf) < t.schema.WireSize() {
		return 0, engine.ErrShortMessage
	}

	// check table state
	if t.opts.ReadOnly {
		return 0, engine.ErrTableReadOnly
	}
	atomic.AddInt64(&t.metrics.InsertCalls, 1)

	// obtain shared table lock
	tx := engine.GetTransaction(ctx)
	err := tx.RLock(ctx, t.id)
	if err != nil {
		return 0, err
	}

	// register table for commit/abort callbacks
	tx.Touch(t.id)

	// break message batch into pieces so that each piece fits into the
	// current journal's active segment, then write each piece to wal
	// before inserting it into the journal
	view := schema.NewView(t.schema)

	// remember the first assigned pk
	firstPk := t.state.NextPk

	var count int64
	for len(buf) > 0 {
		var (
			sz   int
			jcap = t.journal.Capacity()
		)

		// split buf into wire messages
		view, vbuf, _ := view.Cut(buf)

		// assign PKs and assemble WAL batch buffer
		for view.IsValid() && jcap > 0 {
			view.SetPk(t.state.NextPk)
			t.state.NextPk++
			jcap--
			count++
			sz += len(view.Bytes())
			view, vbuf, _ = view.Cut(vbuf)
		}

		// write wal batch
		if tx.UseWal() {
			_, err = t.engine.Wal().Write(&wal.Record{
				Type:   wal.RecordTypeInsert,
				Tag:    types.ObjectTagTable,
				Entity: t.id,
				TxID:   tx.Id(),
				Data:   [][]byte{num.EncodeUvarint(t.state.NextRid + 1), buf[:sz]},
			})
			if err != nil {
				break
			}
		}

		// append journal (Note: at this point we don't know if tx will commit)
		view, vbuf, _ = view.Cut(buf[:sz])
		for view.IsValid() {
			t.journal.Insert(tx.Id(), t.state.NextRid+1, view.Bytes())
			t.state.NextRid++
			t.state.NRows++
			view, vbuf, _ = view.Cut(vbuf)
		}

		// advance message buffer
		view.Reset(nil)
		buf = buf[sz:]

		// FIXME: can we find a better call path to rotate&flush the active
		// journal segment?
		// - rotate on insert
		// - remember canFlush
		// - then check and run journal flush which will flush all writable
		//   segments

		// once a journal segment is full
		// - rotate segment, flush to disk
		// - flush other dirty segment's xmeta to disk
		// - write checkpoint record
		if t.journal.Active().IsFull() {
			err = t.flushJournal(ctx)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		return 0, err
	}

	if count == 0 {
		return 0, nil
	}

	atomic.AddInt64(&t.metrics.InsertedTuples, count)

	return firstPk, nil
}
