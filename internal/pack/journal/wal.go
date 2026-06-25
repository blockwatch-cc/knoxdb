// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/schema"
)

func (j *Journal) ReplayWalRecord(ctx context.Context, rec *wal.Record, rd engine.TableReader) error {
	// j.log.Debugf("journal: apply %s", rec)
	var err error
	switch rec.Type {
	case wal.RecordTypeCommit:
		j.CommitTx(rec.TxID)

	case wal.RecordTypeAbort:
		j.AbortTx(rec.TxID)

	case wal.RecordTypeCheckpoint:
		// each segment starts with a checkpoint
		j.tip.WithLSN(rec.Lsn)

	case wal.RecordTypeInsert:
		err = j.InsertWalBatch(ctx, rec)

	case wal.RecordTypeUpdate:
		err = j.UpdateWalBatch(ctx, rec, rd)

	case wal.RecordTypeDelete:
		err = j.DeleteWalBatch(ctx, rec)
	}
	if err != nil {
		return err
	}

	// try rotate segment once full
	j.rotateWhenFull()

	return nil
}

func (j *Journal) InsertWalBatch(ctx context.Context, rec *wal.Record) error {
	// WAL Record format (rids are sequential)
	// | rid1 | schema-version | schema-hash | wire1 | wire2 | ... |
	var (
		count    uint64
		expectPk = j.tip.tstate.NextPk
		buf      = rec.Data[0]
	)

	// read start rid
	nextRid, n := binary.Uvarint(buf)
	buf = buf[n:]

	// sanity check row id
	if j.tip.tstate.NextRid != nextRid {
		return fmt.Errorf("insert: state rid %d does not match WAL record %d",
			j.tip.tstate.NextRid, nextRid)
	}

	// resolve schema at version
	batch, err := j.resolver.ResolveBatch(ctx, buf)
	if err != nil {
		return err
	}

	// split buf into wire messages
	for _, view := range batch.Records() {
		// check pk is correct
		pk := view.GetPk()
		if pk != expectPk {
			return fmt.Errorf("insert: unexpected pk=%d, expected=%d", pk, expectPk)
		}
		expectPk++

		// fail on overflow, should not happen
		if j.Capacity() == 0 {
			return fmt.Errorf("insert: journal overflow")
		}
		j.tip.InsertRecord(rec.TxID, nextRid, view.Buffer())
		nextRid++
		count++
	}

	j.tip.tstate.NextPk = expectPk
	j.tip.tstate.NextRid = nextRid
	j.tip.tstate.NRows += count
	return nil
}

// WAL Record format (rids are sequential)
// | schema-version | schema-hash | changeset | rid-start | ref1 | wire1 | ... |
func (j *Journal) UpdateWalBatch(ctx context.Context, rec *wal.Record, rd engine.TableReader) error {
	buf := rec.Data[0]
	if len(buf) < schema.BatchHeaderSize {
		return io.ErrShortBuffer
	}

	// resolve schema
	s, err := j.resolver.ResolveSchema(ctx, buf)
	if err != nil {
		return err
	}
	buf = buf[schema.BatchHeaderSize:]

	// read changeset bitset
	l := (s.Len() + 7) / 8
	cset := bitset.NewFromBytes(buf[:l], s.Len())
	buf = buf[l:]

	// read start rowid
	rid, n := binary.Uvarint(buf)
	buf = buf[n:]

	// sanity check row id
	if j.tip.tstate.NextRid != rid {
		return fmt.Errorf("update: state rid %d does not match WAL record %d",
			j.tip.tstate.NextRid, rid)
	}

	if cset.Count() == s.Len() {
		var (
			view    = schema.NewView(s)
			nextRid = j.tip.tstate.NextRid
		)
		for len(buf) > 0 {
			// decode ref
			ref, n := binary.Uvarint(buf)
			buf = buf[n:]

			// decode record
			view, buf, _ = view.Cut(buf)

			// append to journal
			j.tip.UpdateRecord(rec.TxID, nextRid, ref, view.Buffer())
			nextRid++

			// ensure amount of updates fits into current journal tip
			if j.Capacity() == 0 {
				// should not happen
				return fmt.Errorf("update: num updates is larger than journal capacity")
			}
		}
		j.tip.tstate.NextRid = nextRid

	} else {
		// make change schema (for parsing change records)
		cids := make([]uint16, 0, cset.Count())
		cols := make([]int, 0, cset.Count())
		for i := range cset.Ones() {
			cids = append(cids, s.Fields[i].Id)
			cols = append(cols, i)
		}

		cschema, err := s.SelectIds(cids...)
		if err != nil {
			// should not happen
			return fmt.Errorf("update: make change schema: %v", err)
		}

		// decode refs from WAL record and construct a query mask
		var (
			tmp  = buf
			refs = xroar.New()
			recs = make(map[uint64]int)
			view = schema.NewView(cschema)
			c    int
		)
		for len(buf) > 0 {
			// decode ref
			ref, n := binary.Uvarint(buf)
			buf = buf[n:]
			c += n
			refs.Set(ref)
			recs[ref] = c
			// skip record
			view, buf, _ = view.Cut(buf)
			c += view.Len()
		}
		buf = tmp

		// ensure amount of updates fits into current journal tip
		if len(recs) > j.Capacity() {
			// should not happen
			return fmt.Errorf("update: no space for %d updates in %d journal capacity",
				len(recs), j.Capacity())
		}

		// run query visiting all packs with matches
		rd.WithMask(refs, engine.ReadModeIncludeMask)
		for {
			pkg, err := rd.Next(ctx)
			if err != nil {
				return err
			}
			if pkg == nil {
				break
			}

			// materialize columns in the change set
			for _, col := range cols {
				pkg.MaterializeBlock(col)
			}

			// patch records
			for _, row := range pkg.Selected() {
				rid := pkg.RowId(int(row))
				ofs, ok := recs[rid]
				if !ok {
					// should not happen
					return fmt.Errorf("update: found invalid original rid=%d", rid)
				}

				// set values
				view.Reset(buf[ofs:])
				for i, col := range cols {
					pkg.Block(col).Set(int(row), view.Get(i))
				}

				// remove patched update
				delete(recs, rid)
			}

			// append changed records to journal (will set new rowid, xid, ref)
			_, err = j.updatePackNoWal(pkg, rec.TxID)
			if err != nil {
				return fmt.Errorf("replay update: %v", err)
			}
		}

		// sanity check we have applied changes to all records found in WAL
		if len(recs) > 0 {
			// should not happen
			return fmt.Errorf("update: %d unhandled records", len(recs))
		}
	}
	return nil
}

func (j *Journal) DeleteWalBatch(ctx context.Context, rec *wal.Record) error {
	var (
		nDeleted uint64
		buf      = rec.Data[0]
	)

	for len(buf) > 0 && j.Capacity() > 0 {
		rid, n := binary.Uvarint(buf)
		buf = buf[n:]

		// append to tomb, set xmax on rid when in tip segment
		j.tip.NotifyDelete(rec.TxID, rid)

		nDeleted++
	}

	// fail on overflow, should not happen
	if len(buf) > 0 {
		return fmt.Errorf("delete: journal overflow")
	}

	j.tip.tstate.NRows -= nDeleted

	return nil
}
