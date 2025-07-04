// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"bytes"
	"context"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// Appends records to journal and WAL and requires an active write transaction.
// At this point it is unclear if the tx will commit, hence the insert is
// tentative and requires a subseqent Abort() or Commit() call.
//
// Only source of errors is WAL write or system crash. For efficient recovery
// we break the message batch into pieces so that each piece fits into the
// current journal's active segment. This ensures each journal segment aligns
// with a WAL LSN which we can use as recovery checkpoint.
//
// Transactions allow to turn WAL mode off selectively. We choose the appropriate
// algorithm for each case.
func (j *Journal) InsertRecords(ctx context.Context, buf []byte) (uint64, int, error) {
	var (
		view     = schema.NewView(j.schema)
		tx       = engine.GetTransaction(ctx)
		xid      = tx.Id()              // id of user tx
		firstPk  = j.tip.tstate.NextPk  // first assigned pk
		firstRid = j.tip.tstate.NextRid // first assigned rid (per wal batch!)
		nextPk   = firstPk
		nextRid  = firstRid
		count    int
	)
	for len(buf) > 0 {
		var (
			sz, n int
			jcap  = j.Capacity()
		)

		// split buf into wire messages
		view, vbuf, _ := view.Cut(buf)

		// assign PKs, insert to active segment and assemble WAL batch buffer
		for view.IsValid() && jcap > 0 {
			view.SetPk(nextPk)
			j.tip.InsertRecord(xid, nextRid, view.Bytes())
			nextRid++
			nextPk++
			jcap--
			n++
			sz += len(view.Bytes())
			view, vbuf, _ = view.Cut(vbuf)
		}
		j.log.Debugf("journal inserted %d records into segment %d", nextRid-firstRid, j.tip.Id())

		// write wal batch
		if tx.UseWal() {
			_, err := tx.Engine().Wal().Write(&wal.Record{
				Type:   wal.RecordTypeInsert,
				Tag:    types.ObjectTagTable,
				Entity: j.id,
				TxID:   xid,
				Data:   [][]byte{num.EncodeUvarint(firstRid), buf[:sz]},
			})
			if err != nil {
				// will likely abort the tx
				return 0, 0, err
			}
			firstRid = nextRid
		}

		// advance message buffer
		view.Reset(nil)
		buf = buf[sz:]

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

	// update shared state on success

	return firstPk, count, nil
}

// Inserts from a pack, either a query result or read-only table pack with
// optional selection vector. May contain a mix of compressed and materialized
// or computed columns. Tx have the ability to turn WAL mode off selectively,
// so we choose appropriate algorithm for each case.
func (j *Journal) InsertPack(ctx context.Context, src *pack.Package) (uint64, int, error) {
	tx := engine.GetTransaction(ctx)
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
		j.rotate()

		// stop when src is exhausted
		if !state.More() {
			break
		}
	}

	return firstPk, count, nil
}

func (j *Journal) insertPackWithWal(_ context.Context, src *pack.Package, xid types.XID, w *wal.Wal) (uint64, int, error) {
	// WAL Record format (rids are sequential)
	// | rid1 | wire1 | wire2 | ... |
	var (
		view     = schema.NewView(j.schema) // view for patching pk
		sel      = src.Selected()           // selection vector, may be nil
		firstPk  = j.tip.tstate.NextPk      // first assigned pk
		firstRid = j.tip.tstate.NextRid     // first assigned rid (per wal batch!)
		nextPk   = firstPk
		nextRid  = firstRid
		count    int
		rec      = &wal.Record{
			Type:   wal.RecordTypeInsert,
			Tag:    types.ObjectTagTable,
			Entity: j.id,
			TxID:   xid,
			Data:   make([][]byte, 1),
		}
	)

	// dimension WAL write buffer (may still with grow with long strings)
	sz := num.MaxVarintLen64
	if sel == nil {
		sz += j.schema.AverageSize() * src.Len()
	} else {
		sz += j.schema.AverageSize() * len(sel)
	}
	buf := arena.AllocBytes(sz)
	msg := bytes.NewBuffer(buf)

	if sel == nil {
		// write all records when no selection vector is defined
		var i int
		for i < src.Len() {
			n := min(src.Len()-count, j.Capacity())

			// 1 create & assign pks, rids, xid, write to journal vectors
			num.WriteUvarint(msg, nextRid)
			for range n {
				// create wire format for wal write
				start := msg.Len()
				if err := src.ReadWireBuffer(msg, i); err != nil {
					return 0, 0, err
				}
				view.Reset(msg.Bytes()[start:]).SetPk(nextPk)
				j.tip.InsertRecord(xid, nextRid, view.Bytes())
				i++
				nextPk++
				nextRid++
			}

			// 2 write record batch to WAL (in record format)
			rec.Data[0] = msg.Bytes()
			_, err := w.Write(rec)
			if err != nil {
				return 0, 0, err
			}

			// prepare next round
			rec.Data[0] = nil
			msg.Reset()
			count += n

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

			// 1 create & assign pks, rids, xid, write to journal vectors
			num.WriteUvarint(msg, nextRid)
			for _, v := range sel[:n] {
				start := msg.Len()
				if err := src.ReadWireBuffer(msg, int(v)); err != nil {
					return 0, 0, err
				}
				view.Reset(msg.Bytes()[start:]).SetPk(nextPk)
				j.tip.InsertRecord(xid, nextRid, view.Bytes())

				nextPk++
				nextRid++
			}

			// 2 write record batch to WAL (in record format)
			rec.Data[0] = msg.Bytes()
			_, err := w.Write(rec)
			if err != nil {
				return 0, 0, err
			}

			// prepare next round
			count += n
			sel = sel[n:]
			rec.Data[0] = nil
			msg.Reset()

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

	arena.Free(buf)

	return firstPk, count, nil
}
