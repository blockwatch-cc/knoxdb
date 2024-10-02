// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"sort"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/bitmap"
)

type TxHook func(Context) error

type Tx struct {
	engine   *Engine
	id       uint64
	dbTx     map[store.DB]store.Tx
	catTx    store.Tx
	snap     *types.Snapshot     // isolation snapshot
	touched  map[uint64]struct{} // oids we have written to (tables, stores)
	onCommit []TxHook
	onAbort  []TxHook

	// TODO: close/drop/truncate/compact/alter integration
	// - should we close the table before? then reopen private, action and reopen again?
	// - or should we lock the table? then the lock must be checked on each access

	// TODO tx isolation
	// - keep private tx-scoped journal per table?
	// - write hidden txid col to journal and skip read when txid > self? last committed?
	// - write to wal only, on commit apply wal to journal (!! violates read own writes)
	// - journal insert: lock sequence access (or lock table) because num rows unknown
	//   - or make nrows known in wire format and pre-reserve sequence space
	//   - if insert fails sequences may be lost (!! determinism)

	// TODO tx conflict detection
	// - keep list of written PKs per table in tx (how to know, journal?)
	// - keep aggregate list of written PKs in engine
	// - add written pk to both lists on write (in journal, update/delete calls only)
	// - check if pk exists in global list but not local list
	//   - if not in local and in global: -> conflict, abort current tx
	//   - if in local and global: -> second update/undelete/etc -> allow
	// - on tx close (commit or abort) AND NOT engine list with local list clearing touched pks
	writes map[uint64]bitmap.Bitmap

	// TODO tx deadlock detection (on writable tx)
	// - live tx are registered with engine
	// - A prevent multiple tx from opening storage db tx (or take any locks)
	//     in a pre-defined order (e.g. by mem address)
	// - B actively check in StoreTx if any other live tx has as open write tx
	//     and abort the caller
	//     - could use a map in engine that holds all dbs (interface ptr) with
	//       currently open write tx
}

// TxList is a list of transactions sorted by txid
type TxList []*Tx

func (t *TxList) Add(tx *Tx) {
	txs := *t
	i := sort.Search(len(txs), func(i int) bool { return txs[i].id > tx.id })
	txs = append(txs, nil)
	copy(txs[i+1:], txs[i:])
	*t = txs
}

func (t *TxList) Del(tx *Tx) {
	txs := *t
	i := sort.Search(len(txs), func(i int) bool { return txs[i].id >= tx.id })
	copy(txs[i:], txs[i+1:])
	txs[len(txs)-1] = nil
	txs = txs[:len(txs)-1]
	*t = txs
}

func (e *Engine) NewTransaction() *Tx {
	tx := &Tx{
		engine: e,
		id:     0, // read-only tx do not use an id
		dbTx:   make(map[store.DB]store.Tx),
		snap:   e.NewSnapshot(),
	}
	return tx
}

func (t *Tx) Id() uint64 {
	return t.id
}

func (t *Tx) Snapshot() *types.Snapshot {
	return t.snap
}

func (t *Tx) Engine() *Engine {
	if t == nil {
		return nil
	}
	return t.engine
}

func (t *Tx) Noop() error {
	return nil
}

func (t *Tx) Close() {
	if len(t.touched) > 0 {
		t.engine.mu.Lock()
		t.engine.txs.Del(t)
		if len(t.engine.txs) > 0 {
			t.engine.xmin = t.engine.txs[0].id
		} else {
			t.engine.xmin = t.engine.xnext - 1
		}
		t.engine.mu.Unlock()
		clear(t.touched)
	}
	clear(t.dbTx)
	clear(t.writes)
	t.catTx = nil
	t.snap = nil
}

func (t *Tx) Touch(key uint64) {
	if len(t.touched) == 0 {
		// generate txid on first write
		t.engine.mu.Lock()
		t.id = atomic.AddUint64(&t.engine.xnext, 1)
		t.engine.txs.Add(t)
		t.engine.mu.Unlock()

		// update snapshot
		t.snap.WithId(t.id)
		t.touched = make(map[uint64]struct{})
	}
	t.touched[key] = struct{}{}
}

func (t *Tx) OnCommit(fn TxHook) {
	if t.onCommit != nil {
		t.onCommit = append([]TxHook{fn}, t.onCommit...)
	} else {
		t.onCommit = []TxHook{fn}
	}
}

func (t *Tx) OnAbort(fn TxHook) {
	if t.onAbort != nil {
		t.onAbort = append([]TxHook{fn}, t.onAbort...)
	} else {
		t.onAbort = []TxHook{fn}
	}
}

func (t *Tx) Commit(ctx context.Context) error {
	if t == nil {
		return nil
	}
	// write commit record to wal
	_, err := t.engine.wal.Write(&wal.Record{
		Type:   wal.RecordTypeCommit,
		Tag:    types.ObjectTagDatabase,
		Entity: t.engine.dbId,
		TxID:   t.id,
	})
	if err != nil {
		return err
	}

	// sync wal
	if err := t.engine.wal.Sync(); err != nil {
		return err
	}

	// run callbacks
	for _, fn := range t.onCommit {
		if err := fn(ctx); err != nil {
			t.engine.log.Errorf("Tx 0x%016x commit: %v", t.id, err)
		}
	}

	// TODO: refactor when all stores are journaled
	// close and write storage tx
	for _, tx := range t.dbTx {
		var e error
		if tx.IsWriteable() {
			e = tx.Commit()
		} else {
			e = tx.Rollback()
		}
		if e != nil && err == nil {
			err = e
		}
	}
	if err := t.engine.cat.CommitState(t); err != nil {
		return err
	}
	if t.catTx != nil {
		var e error
		if t.catTx.IsWriteable() {
			e = t.catTx.Commit()
		} else {
			e = t.catTx.Rollback()
		}
		if e != nil && err == nil {
			err = e
		}
	}
	t.Close()
	return err
}

func (t *Tx) Abort(ctx context.Context) error {
	if t == nil {
		return nil
	}

	// write abort record to wal (no sync required, tx data is ignored if lost)
	_, err := t.engine.wal.Write(&wal.Record{
		Type:   wal.RecordTypeAbort,
		Tag:    types.ObjectTagDatabase,
		Entity: t.engine.dbId,
		TxID:   t.id,
	})
	if err != nil {
		return err
	}

	// run callbacks
	for _, fn := range t.onCommit {
		if err := fn(ctx); err != nil {
			t.engine.log.Errorf("Tx 0x%016x commit: %v", t.id, err)
		}
	}

	// close storage tx
	for _, tx := range t.dbTx {
		if e := tx.Rollback(); e != nil && err == nil {
			err = e
		}
	}
	if err := t.engine.cat.RollbackState(t); err != nil {
		return err
	}
	if t.catTx != nil {
		if err := t.catTx.Rollback(); err != nil {
			return err
		}
	}
	t.Close()
	return err
}

func (t *Tx) StoreTx(db store.DB, write bool) (store.Tx, error) {
	if t == nil {
		return nil, ErrNoTx
	}
	tx, ok := t.dbTx[db]
	if ok {
		if write && !tx.IsWriteable() {
			// cancel and upgrade tx
			if err := tx.Rollback(); err != nil {
				return nil, err
			}
			delete(t.dbTx, db)
		} else {
			return tx, nil
		}
	}
	tx, err := db.Begin(write)
	if err != nil {
		return nil, err
	}
	t.dbTx[db] = tx
	return tx, nil
}

func (t *Tx) CatalogTx(db store.DB, write bool) (store.Tx, error) {
	if t == nil {
		return nil, ErrNoTx
	}
	if t.catTx != nil {
		if write && !t.catTx.IsWriteable() {
			// cancel and upgrade tx
			if err := t.catTx.Rollback(); err != nil {
				return nil, err
			}
			t.catTx = nil
		} else {
			return t.catTx, nil
		}
	}
	tx, err := db.Begin(write)
	if err != nil {
		return nil, err
	}
	t.catTx = tx
	return tx, nil
}

// Commits and reopens a writeable storage tx. This may be used to flush verly large
// transactions to storage in incremental pieces.
func (t *Tx) Continue(tx store.Tx) (store.Tx, error) {
	db := tx.DB()
	tx, err := store.CommitAndContinue(tx)
	if err != nil {
		delete(t.dbTx, db)
		return nil, err
	}
	t.dbTx[db] = tx
	return tx, nil
}
