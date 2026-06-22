// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"context"
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// InsertBatch appends new records to journal and WAL. Requires an active
// write transaction which may or may not commit. Encoding schema for the
// batch is already validated to match the current table schema.
//
// Only source of errors is WAL write or system crash. For efficient recovery
// we break the message batch into pieces so that each piece fits into the
// current journal's active segment. This ensures each journal segment aligns
// with a WAL LSN which we can use as recovery checkpoint.
//
// WAL Record format for insert actions
// | rid1 | schema-hash | schema-version | wire1 | wire2 | ... |
//
// Row ids are sequentially assigned, so we only store the first RID for
// each batch. When a transaction aborts, the still invisible row ids are
// reclaimed which is safe under single-writer transaction policy.
// Schema hash/version specifies the schema used to encode the batch.
//
// Transactions can disable WAL mode selectively in which case no WAL records
// are written and inserts go to the journal only.
func (j *Journal) InsertBatch(ctx context.Context, batch *schema.Batch) (uint64, int, error) {
	var (
		tx       = engine.GetTx(ctx)
		xid      = tx.Id()              // id of user tx
		firstPk  = j.tip.tstate.NextPk  // first assigned pk
		firstRid = j.tip.tstate.NextRid // first assigned rid (per wal batch!)
		nextPk   = firstPk
		nextRid  = firstRid
		count    int
		scratch  [binary.MaxVarintLen64]byte
	)

	for batch.Size() > 0 {
		var (
			sz, n int
			jcap  = j.Capacity()
		)

		// split batch into wire messages
		for _, view := range batch.Records() {
			// stop on journal segment bounadry
			if jcap == 0 {
				break
			}

			// assign pk
			view.SetPk(nextPk)

			// add update to journal
			j.tip.InsertRecord(xid, nextRid, view.Buffer())

			// next iteration
			nextRid++
			nextPk++
			jcap--
			n++
			sz += len(view.Buffer())
		}

		// write wal batch
		if tx.UseWal() {
			_, err := tx.Engine().Wal().Write(&wal.Record{
				Type:   wal.RecordTypeInsert,
				Tag:    types.ObjectTagTable,
				Entity: j.id,
				TxID:   xid,
				Data: [][]byte{
					binary.AppendUvarint(scratch[:0], firstRid),
					batch.Header(),
					batch.Bytes()[:sz],
				},
			})
			if err != nil {
				return 0, 0, err
			}
			firstRid = nextRid
		}

		// advance batch buffer
		batch.TrimAt(sz)

		// update object state
		j.tip.tstate.NextPk = nextPk
		j.tip.tstate.NextRid = nextRid
		j.tip.tstate.NRows += uint64(n)
		count += n

		// rotate segment once full
		if tx.UseWal() {
			if err := j.rotateAndCheckpoint(); err != nil {
				return 0, 0, err
			}
		} else {
			j.rotateWhenFull()
		}
	}

	return firstPk, count, nil
}

// Inserts from a pack, either a query result or read-only table pack with
// optional selection vector. May contain a mix of compressed and materialized
// or computed columns. Tx have the ability to turn WAL mode off selectively,
// so we choose appropriate algorithm for each case.
func (j *Journal) InsertPack(ctx context.Context, src *pack.Package) (uint64, int, error) {
	tx := engine.GetTx(ctx)
	xid := tx.Id()
	if tx.UseWal() {
		return j.insertPackWithWal(ctx, src, xid, tx.Engine().Wal())
	} else {
		return j.insertPackNoWal(ctx, src, xid)
	}
}

func (j *Journal) insertPackNoWal(_ context.Context, src *pack.Package, xid types.XID) (uint64, int, error) {
	var (
		state   pack.AppendState
		mode    = pack.WriteModeAll
		nextPk  = j.tip.tstate.NextPk  // first assigned pk
		nextRid = j.tip.tstate.NextRid // first assigned rid (per wal batch!)
		firstPk = nextPk
		n       int
		count   int
	)
	if src.Selected() != nil {
		mode = pack.WriteModeIncludeSelected
	}
	for {
		pos := j.tip.data.Len()

		// call append and use last version of state, returns next state
		n, state = j.tip.data.AppendSelected(src, mode, state)
		count += n

		// write pk, rid, xmin
		pks := j.tip.data.Pks()
		rids := j.tip.data.RowIds()
		xmins := j.tip.data.Xmins()
		for i := pos; i < pos+n; i++ {
			pks.Set(i, nextPk)
			rids.Set(i, nextRid)
			xmins.Set(i, uint64(xid))
			j.tip.NotifyInsert(xid, nextRid)
			nextPk++
			nextRid++
		}

		// update object state
		j.tip.tstate.NextPk = nextPk
		j.tip.tstate.NextRid = nextRid
		j.tip.tstate.NRows += uint64(n)
		count += n

		// rotate segment once full
		j.rotateWhenFull()

		// stop when src is exhausted
		if !state.More() {
			break
		}
	}

	return firstPk, count, nil
}

func (j *Journal) insertPackWithWal(_ context.Context, src *pack.Package, xid types.XID, w *wal.Wal) (uint64, int, error) {
	var (
		view     = schema.NewView(src.Schema()) // view for patching pk
		sel      = src.Selected()               // selection vector, may be nil
		firstPk  = j.tip.tstate.NextPk          // first assigned pk
		firstRid = j.tip.tstate.NextRid         // first assigned rid (per wal batch!)
		nextPk   = firstPk
		nextRid  = firstRid
		count    int
		rec      = &wal.Record{
			Type:   wal.RecordTypeInsert,
			Tag:    types.ObjectTagTable,
			Entity: j.id,
			TxID:   xid,
			Data:   make([][]byte, 3),
		}
		scratch [binary.MaxVarintLen64]byte
		wr      = schema.NewBatchWriter(src.Schema(), src.NumSelected())
	)
	defer wr.Close()

	if sel == nil {
		// write all records when no selection vector is defined
		var i int
		for i < src.Len() {
			n := min(src.Len()-count, j.Capacity())
			batchRid := nextRid

			// 1 create & assign pks, rids, xid, write to journal vectors
			for range n {
				// create wire format for wal write
				start := wr.Len()
				src.ReadWireBuffer(wr.Buffer(), i)
				view.Reset(wr.Bytes()[start:]).SetPk(nextPk)
				j.tip.InsertRecord(xid, nextRid, view.Buffer())
				i++
				nextPk++
				nextRid++
			}

			// 2 write record batch to WAL (in record format)
			batch := wr.Batch()
			rec.Data[0] = binary.AppendUvarint(scratch[:0], batchRid)
			rec.Data[1] = batch.Header()
			rec.Data[2] = batch.Bytes()
			_, err := w.Write(rec)
			if err != nil {
				return 0, 0, err
			}

			// prepare next round
			rec.Data[0] = nil
			rec.Data[1] = nil
			rec.Data[2] = nil
			wr.Reset()

			// update object state
			j.tip.tstate.NextPk = nextPk
			j.tip.tstate.NextRid = nextRid
			j.tip.tstate.NRows += uint64(n)
			count += n

			// rotate segment once full
			if err := j.rotateAndCheckpoint(); err != nil {
				return 0, 0, err
			}
		}

	} else {
		// write selected rows up until capacity limit, continue with next sel each round
		for len(sel) > 0 {
			n := min(len(sel), j.Capacity())
			batchRid := nextRid

			// 1 create & assign pks, rids, xid, write to journal vectors
			for _, v := range sel[:n] {
				start := wr.Len()
				src.ReadWireBuffer(wr.Buffer(), int(v))
				view.Reset(wr.Bytes()[start:]).SetPk(nextPk)
				j.tip.InsertRecord(xid, nextRid, view.Buffer())
				nextPk++
				nextRid++
			}

			// 2 write record batch to WAL (in record format)
			batch := wr.Batch()
			rec.Data[0] = binary.AppendUvarint(scratch[:0], batchRid)
			rec.Data[1] = batch.Header()
			rec.Data[2] = batch.Bytes()
			_, err := w.Write(rec)
			if err != nil {
				return 0, 0, err
			}

			// prepare next round
			rec.Data[0] = nil
			rec.Data[1] = nil
			rec.Data[2] = nil
			sel = sel[n:]
			wr.Reset()

			// update object state
			j.tip.tstate.NextPk = nextPk
			j.tip.tstate.NextRid = nextRid
			j.tip.tstate.NRows += uint64(n)
			count += n

			// rotate segment once full
			if err := j.rotateAndCheckpoint(); err != nil {
				return 0, 0, err
			}
		}
	}

	return firstPk, count, nil
}
