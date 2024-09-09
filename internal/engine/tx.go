// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/pkg/bitmap"
)

type Tx struct {
	engine  *Engine
	id      uint64
	dbTx    map[store.DB]store.Tx
	catTx   store.Tx
	touched map[uint64]struct{}

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

func (e *Engine) NewTransaction() *Tx {
	e.mu.Lock()
	tx := &Tx{
		engine:  e,
		id:      atomic.AddUint64(&e.nextTxId, 1),
		dbTx:    make(map[store.DB]store.Tx),
		touched: make(map[uint64]struct{}),
	}
	e.txs[tx.id] = tx
	e.mu.Unlock()
	return tx
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
	clear(t.dbTx)
	clear(t.touched)
	clear(t.writes)
	t.catTx = nil
	t.engine.mu.Lock()
	delete(t.engine.txs, t.id)
	t.engine.mu.Unlock()
}

func (t *Tx) Touch(key uint64) {
	t.touched[key] = struct{}{}
}

func (t *Tx) Commit() error {
	if t == nil {
		return nil
	}
	var err error
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

func (t *Tx) Abort() error {
	if t == nil {
		return nil
	}
	var err error
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
