// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/internal/xroar"
)

func (e *Engine) writeWalRecord(ctx context.Context, typ wal.RecordType, o Object) error {
	buf, err := o.Encode(typ)
	if err != nil {
		return err
	}
	rec := &wal.Record{
		Type:   typ,
		Tag:    types.ObjectTagDatabase,
		Entity: e.dbId,
		TxID:   GetTransaction(ctx).id,
		Data:   buf,
	}
	_, err = e.wal.Write(rec)
	return err
}

// find the highest checkpoint across all containers to start reading the WAL on startup
func (e *Engine) maxWalCheckpoint() (maxLsn wal.LSN) {
	for _, v := range e.tables {
		lsn := v.State().Checkpoint
		if lsn == 0 {
			continue
		}
		maxLsn = max(maxLsn, lsn)
	}
	for _, v := range e.stores {
		lsn := v.State().Checkpoint
		if lsn == 0 {
			continue
		}
		maxLsn = max(maxLsn, lsn)
	}
	return
}

// find the lowest checkpoint across all containers to start reading the WAL on startup
func (e *Engine) minWalCheckpoint() (minLsn wal.LSN) {
	for _, v := range e.tables {
		lsn := v.State().Checkpoint
		if lsn == 0 {
			continue
		}
		if minLsn == 0 {
			minLsn = lsn
		} else {
			minLsn = max(minLsn, lsn)
		}
	}
	for _, v := range e.stores {
		lsn := v.State().Checkpoint
		if lsn == 0 {
			continue
		}
		if minLsn == 0 {
			minLsn = lsn
		} else {
			minLsn = max(minLsn, lsn)
		}
	}
	return
}

func (e *Engine) getWalCheckpoint(entity uint64) (lsn wal.LSN) {
	t, ok := e.tables[entity]
	if ok {
		return t.State().Checkpoint
	}
	s, ok := e.stores[entity]
	if ok {
		return s.State().Checkpoint
	}
	return 0
}

// TODO: make sure all checkpoints for all tables exist
//   - problem: committed tx are missing from wal because the file was broken/truncated
//     -> what to do here?
func (e *Engine) recoverWal(ctx context.Context) error {
	// find the minimum non-zero checkpoint across all catalog objects
	// we directly access catalog state without lock because this function
	// runs non-concurrent on engine init
	minLsn := e.minWalCheckpoint()

	e.log.Infof("recover journals from wal lsn %d", minLsn)

	// 1st pass - read committed tx ids
	r := e.wal.NewReader().WithType(wal.RecordTypeCommit)
	defer r.Close()
	if err := r.Seek(minLsn); err != nil {
		return err
	}
	committed := xroar.NewBitmap()
	for {
		rec, err := r.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		e.log.Debugf("Committed tx %d", rec.TxID)
		committed.Set(rec.TxID)
	}
	r.Close()

	// 2nd pass - read records skipping records with uncommitted or aborted data
	r = e.wal.NewReader()
	defer r.Close()
	if err := r.Seek(minLsn); err != nil {
		return err
	}

	// init tx id and horizon
	e.xmin = 0

	for {
		rec, err := r.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// skip uncommitted data
		if !committed.Contains(rec.TxID) {
			continue
		}

		// skip commit records (we implicitly treat all recovered data as commited)
		if rec.Type == wal.RecordTypeCommit {
			e.xmin = max(e.xmin, rec.TxID)
			continue
		}

		// skip already checkpointed data (containers have their own checkpoints)
		if rec.Lsn < e.getWalCheckpoint(rec.Entity) {
			continue
		}

		// TODO: handle catalog object creation, update and deletion
		e.log.Debugf("Record %s", rec)

		// dispatch records to receivers
		switch rec.Tag {
		case types.ObjectTagDatabase:
			// catalog changes can create/update/drop database objects
			err = e.applyWalRecord(ctx, rec)

		case types.ObjectTagTable:
			// table insert, update, delete, checkpoint
			table, ok := e.GetTable(rec.Entity)
			if ok {
				err = table.ApplyWalRecord(ctx, rec)
			} else {
				err = ErrNoTable
			}
		case types.ObjectTagStore:
			// kv store record: insert, update, delete, checkpoint
			store, ok := e.GetStore(rec.Entity)
			if ok {
				err = store.ApplyWalRecord(ctx, rec)
			} else {
				err = ErrNoStore
			}
		// case types.ObjectTagStream:
		// TODO: cdc stream
		default:
			err = fmt.Errorf("unexpected wal record: %s", rec)
		}
		if err != nil {
			return err
		}
	}

	// next tx id is one higher than the last seen id from the wal
	e.xnext = e.xmin + 1

	return nil
}

func (e *Engine) applyWalRecord(ctx context.Context, rec *wal.Record) error {
	obj, err := e.decodeWalRecord(rec.Data, rec.Type)
	if err != nil {
		return err
	}
	switch rec.Type {
	case wal.RecordTypeInsert:
		err = obj.Create(ctx)
	case wal.RecordTypeUpdate:
		err = obj.Update(ctx)
	case wal.RecordTypeDelete:
		err = obj.Drop(ctx)
	default:
		err = fmt.Errorf("unexpected wal record: %s", rec)
	}
	return err
}

func (e *Engine) decodeWalRecord(buf []byte, typ wal.RecordType) (Object, error) {
	var obj Object
	switch types.ObjectTag(buf[0]) {
	case types.ObjectTagTable:
		obj = &TableObject{engine: e}
	case types.ObjectTagStore:
		obj = &StoreObject{engine: e}
	case types.ObjectTagEnum:
		obj = &EnumObject{engine: e}
	case types.ObjectTagIndex:
		obj = &IndexObject{engine: e}
	// case types.ObjectTagView:
	// 	obj = &ViewObject{engine: e}
	// case types.ObjectTagStream:
	// 	obj = &StreamObject{engine: e}
	// case types.ObjectTagSnapshot:
	// 	obj = &SnapshotObject{engine: e}
	default:
		return nil, ErrInvalidObjectType
	}
	if err := obj.Decode(buf, typ); err != nil {
		return nil, err
	}
	return obj, nil
}
