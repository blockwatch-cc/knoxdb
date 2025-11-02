// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"context"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/num"
)

// Deletes selected records from pack and writes WAL records. Src pack is
// typically a read-only pack loaded from disk, but can also be a journal pack.
//
// Requires an active write transaction. At this point it is unclear if the
// tx will commit, hence the delete is tentative and requires a subseqent
// Abort() or Commit() call.
//
// Only source of errors is WAL write or system crash. For efficient recovery
// we break the message batch into pieces so that each piece fits into the
// current journal's active segment. This ensures each journal segment
// aligns with a WAL LSN which we can use as recovery checkpoint.
//
// Transactions allow to turn WAL mode off selectively. We choose the appropriate
// algorithm for each case.
func (j *Journal) DeletePack(ctx context.Context, src *pack.Package) (int, error) {
	tx := engine.GetTx(ctx)
	xid := tx.Id()
	if tx.UseWal() {
		return j.deletePackWithWal(src, xid, tx.Engine().Wal())
	} else {
		return j.deletePackNoWal(src, xid)
	}
}

func (j *Journal) deletePackWithWal(src *pack.Package, xid types.XID, w *wal.Wal) (int, error) {
	// WAL Record format
	// | rid1 | rid2 | ... |
	var (
		sel   = src.Selected() // selection vector, may be nil
		count int
		rec   = &wal.Record{
			Type:   wal.RecordTypeDelete,
			Tag:    types.ObjectTagTable,
			Entity: j.id,
			TxID:   xid,
			Data:   make([][]byte, 1),
		}
		buf []byte // wal write buffer
	)

	// dimension WAL write buffer
	if sel == nil {
		buf = arena.AllocBytes(num.MaxVarintLen64 * src.Len())
	} else {
		buf = arena.AllocBytes(num.MaxVarintLen64 * len(sel))
	}

	if sel == nil {
		// write all records when no selection vector is defined
		it := src.RowIds().Chunks()
		for {
			vals, n := it.NextChunk()
			if n == 0 {
				break
			}

			// process next chunk
			rids := vals[:n]
			for len(rids) > 0 {
				m := min(len(rids), j.TombCapacity())
				// collect rowids for deletion and add to tomb
				for i := range m {
					rid := rids[i]

					// append to WAL record
					buf = num.AppendUvarint(buf, rid)

					// append to tomb, set xmax on rid when in tip segment
					j.tip.NotifyDelete(xid, rid)
				}

				// write WAL
				rec.Data[0] = buf
				_, err := w.Write(rec)
				if err != nil {
					return 0, err
				}

				// prepare next round
				rec.Data[0] = nil
				count += m
				rids = rids[m:]
				buf = buf[:0]

				// update object state
				j.tip.tstate.NRows -= uint64(m)

				// rotate segment once full
				if err := j.rotateAndCheckpoint(); err != nil {
					return 0, err
				}
			}
		}
	} else {
		// write selected rows up until capacity limit, continue with next sel each round
		for len(sel) > 0 {
			n := min(len(sel), j.TombCapacity())
			for _, v := range sel[:n] {
				rid := src.RowId(int(v))

				// append to WAL record
				buf = num.AppendUvarint(buf, rid)

				// append to tomb, set xmax on ref when in tip segment
				j.tip.NotifyDelete(xid, rid)
			}

			// write WAL
			rec.Data[0] = buf
			_, err := w.Write(rec)
			if err != nil {
				return 0, err
			}

			// prepare next round
			rec.Data[0] = nil
			count += n
			sel = sel[n:]
			buf = buf[:0]

			// update object state
			j.tip.tstate.NRows -= uint64(n)

			// rotate segment once full
			if err := j.rotateAndCheckpoint(); err != nil {
				return 0, err
			}
		}
	}

	arena.Free(buf)

	return count, nil
}

func (j *Journal) deletePackNoWal(src *pack.Package, xid types.XID) (int, error) {
	var (
		sel   = src.Selected() // selection vector, may be nil
		count int
	)

	if sel == nil {
		// write all records when no selection vector is defined
		it := src.RowIds().Chunks()
		for {
			vals, n := it.NextChunk()
			if n == 0 {
				break
			}

			// process next chunk
			rids := vals[:n]
			for len(rids) > 0 {
				m := min(len(rids), j.TombCapacity())

				// collect rowids for deletion and add to tomb
				for _, rid := range rids[:m] {
					// append to tomb, set xmax on rid when in tip segment
					j.tip.NotifyDelete(xid, rid)
				}

				// rotate segment once full
				j.rotateWhenFull()

				// prepare next round
				count += m
				rids = rids[m:]

				// update object state
				j.tip.tstate.NRows -= uint64(m)
			}
		}
	} else {
		// write selected rows up until capacity limit, continue with next sel each round
		for len(sel) > 0 {
			n := min(len(sel), j.TombCapacity())

			for _, v := range sel[:n] {
				rid := src.RowId(int(v))

				// append to tomb, set xmax on ref when in tip segment
				j.tip.NotifyDelete(xid, rid)
			}

			// rotate segment once full
			j.rotateWhenFull()

			// prepare next round
			count += n
			sel = sel[n:]

			// update object state
			j.tip.tstate.NRows -= uint64(n)
		}
	}

	return count, nil
}
