// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var LE = binary.LittleEndian

// WAL record formats
// - store minimal xmeta for each operation
// - store wire encoded user data
//
// Insert (rids are sequential)
// | wal rec header | rid1 | wire1 | wire2 | ... |
//
// Update (rids are sequential)
// | wal rec header | sz | rid1 | ref1 | ref2 | .. | wire1 | wire2 | ... |
//
// Delete
// | wal rec header | rid1 | pk1 | rid2 |  pk2 | ... |
//
// TODO: consider using varint and min-FOR to save space
func (t *Table) ReplayWal(ctx context.Context) error {
	var xmax uint64 // highest xid seen

	t.log.Debugf("recovering journals from wal lsn %d", t.state.Checkpoint)
	r := t.engine.Wal().NewReader().WithEntity(t.id)
	defer r.Close()
	if err := r.Seek(t.state.Checkpoint); err != nil {
		return err
	}

	// process wal records (a clean shutdown should mean there are
	// no records to handle here)
	var n int
	for {
		rec, err := r.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		n++

		if err := t.ApplyWalRecord(ctx, rec); err != nil {
			return err
		}

		switch rec.Type {
		case wal.RecordTypeCommit, wal.RecordTypeAbort:
			xmax = max(xmax, rec.TxID)
		}
	}

	// abort all remaining pending tx
	skipped := t.journal.AbortActiveTx()

	// flush journal after crash recovery (i.e. some wal records were processed)
	if n+skipped > 0 {
		t.log.Debugf("processed %d wal records, aborted %d txn ", n, skipped)
		if err := t.flushJournal(ctx); err != nil {
			return err
		}
	}

	// track max xid across all tables
	t.engine.UpdateTxHorizon(xmax)

	return nil
}

func (t *Table) ApplyWalRecord(ctx context.Context, rec *wal.Record) error {
	t.log.Debugf("applying wal record %s", rec)
	switch rec.Type {
	case wal.RecordTypeCommit:
		t.journal.CommitTx(rec.TxID)

	case wal.RecordTypeAbort:
		t.journal.AbortTx(rec.TxID)

	case wal.RecordTypeCheckpoint:
		// when we see an own checkpoint record during recovery we know
		// that a crash happened before this checkpoint was saved to db storage.
		// we also know that journals must have already been written with data
		// up until this checkpoint. Hence we can safely reset & reload journals
		// and continue processing messages from here.
		t.journal.Reset()
		err := t.db.Update(func(tx store.Tx) error {
			if err := t.journal.Load(ctx, tx); err != nil {
				return fmt.Errorf("reloading journal: %v", err)
			}
			// update our checkpoint and write to disk again (will sync on commit)
			t.state.Checkpoint = rec.Lsn
			if err := t.state.Store(ctx, tx); err != nil {
				return fmt.Errorf("storing state: %v", err)
			}
			return nil
		})
		if err != nil {
			return err
		}

	case wal.RecordTypeInsert:
		// read data header (first rid)
		buf := rec.Data[0]
		rid, n := num.Uvarint(buf)
		buf = buf[:n]
		var count uint64

		// split buf into wire messages
		view, buf, _ := schema.NewView(t.schema).Cut(buf)
		pk := view.GetPk()
		for view.IsValid() {
			t.journal.Insert(rec.TxID, rid, view.Bytes())
			rid++
			count++
			view, buf, _ = view.Cut(buf)
		}
		view.Reset(nil)
		if len(buf) > 0 {
			return fmt.Errorf("decoding wal record failed: %s", rec)
		}
		t.state.NextPk = pk + count + 1
		t.state.NextRid = rid + 1
		t.state.NRows += count

	case wal.RecordTypeUpdate:
		// decode record
		buf := rec.Data[0]
		sz := int(LE.Uint32(buf))
		head := buf[4:]
		rid, n := num.Uvarint(head)
		head = head[n:]

		// split msg body starting at sz into wire messages
		view, buf, _ := schema.NewView(t.schema).Cut(buf[sz:])
		for view.IsValid() {
			ref, n := num.Uvarint(head)
			head = head[n:]
			t.journal.Update(rec.TxID, rid, view.GetPk(), ref, view.Bytes())
			rid++
			view, buf, _ = view.Cut(buf)
		}
		view.Reset(nil)
		if len(buf) > 0 {
			return fmt.Errorf("decoding wal record failed: %s", rec)
		}
		t.state.NextRid = rid + 1

	case wal.RecordTypeDelete:
		buf := rec.Data[0]
		var nRows uint64
		for len(buf) > 0 {
			rid, n := num.Uvarint(buf)
			buf = buf[n:]
			pk, n := num.Uvarint(buf)
			buf = buf[n:]
			t.journal.Delete(rec.TxID, rid, pk)
			nRows++
		}
		t.state.NRows -= nRows
	}

	return nil
}
