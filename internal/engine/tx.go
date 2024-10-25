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

type TxFlags byte

const (
	TxFlagsReadOnly     TxFlags = 1 << iota
	TxFlagsCatalog              // txn made changes to catalog
	TxFlagsSerializable         // use serializable snapshot isolation level (TODO)
	TxFlagsDeferred             // wait for safe snapshot (TODO)
	TxFlagsConflict             // conflict detected, abort on commit
)

func (f TxFlags) IsReadOnly() bool     { return f&TxFlagsReadOnly > 0 }
func (f TxFlags) IsCatalog() bool      { return f&TxFlagsCatalog > 0 }
func (f TxFlags) IsSerializable() bool { return f&TxFlagsSerializable > 0 }
func (f TxFlags) IsDeferred() bool     { return f&TxFlagsDeferred > 0 }
func (f TxFlags) IsConflict() bool     { return f&TxFlagsConflict > 0 }

type TxHook func(Context) error

type Tx struct {
	id       uint64
	engine   *Engine
	dbTx     map[store.DB]store.Tx
	catTx    store.Tx
	snap     *types.Snapshot     // isolation snapshot
	touched  map[uint64]struct{} // oids we have written to (tables, stores)
	onCommit []TxHook
	onAbort  []TxHook
	ctx      context.Context
	cancel   context.CancelCauseFunc

	// TODO: concurrency control config option: optimistic/deterministic
	// single writer: rollback sequences -> determinism
	// multi writer: don't roll back

	// TODO: close/drop/truncate/compact/alter integration
	// - need lock manager, table ops take a lock (granularity table)
	//   - scope: db, table, pk range, row
	//   - owner: txid
	//   - entity u64
	//   - data [2]u64
	// - can wait on lock release (use sync.Cond)
	// - deterministic tx can lock the entire db (or table?)

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

	// flags
	flags TxFlags
}

// TxList is a list of transactions sorted by txid
type TxList []*Tx

func (t *TxList) Add(tx *Tx) {
	txs := *t
	i := sort.Search(len(txs), func(i int) bool { return txs[i].id > tx.id })
	txs = append(txs, nil)
	copy(txs[i+1:], txs[i:])
	txs[i] = tx
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

func mergeFlags(x []TxFlags) (f TxFlags) {
	for _, v := range x {
		f |= v
	}
	return
}

func (e *Engine) NewTransaction(flags ...TxFlags) *Tx {
	tx := &Tx{
		id:     0, // read-only tx do not use an id
		engine: e,
		dbTx:   make(map[store.DB]store.Tx),
		flags:  mergeFlags(flags),
		ctx:    context.Background(),
		cancel: func(error) {},
	}
	if tx.flags.IsReadOnly() {
		// only create snapshot for read transactions (don't pollute id space)
		e.mu.RLock()
		tx.snap = e.NewSnapshot(0)
		e.mu.RUnlock()
	} else {
		// generate txid for write transactions and store in global tx list
		e.mu.Lock()
		tx.id = atomic.AddUint64(&e.xnext, 1)
		tx.snap = e.NewSnapshot(tx.id)
		e.txs.Add(tx)
		e.mu.Unlock()
	}
	e.log.Debugf("New tx %d", tx.id)

	return tx
}

func (t *Tx) WithContext(ctx context.Context) *Tx {
	t.ctx, t.cancel = context.WithCancelCause(ctx)
	return t
}

func (t *Tx) WithFlags(flags ...TxFlags) *Tx {
	for _, f := range flags {
		t.flags |= f
	}
	return t
}

func (t *Tx) IsReadOnly() bool {
	return t.flags.IsReadOnly()
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

func (t *Tx) Err() error {
	if t == nil {
		return ErrNoTx
	}
	return context.Cause(t.ctx)
}

func (t *Tx) Close() {
	// remove from global tx list (read-only tx run without id)
	if t.id > 0 {
		t.engine.mu.Lock()
		t.engine.txs.Del(t)
		if len(t.engine.txs) > 0 {
			t.engine.xmin = t.engine.txs[0].id
		} else {
			t.engine.xmin = t.engine.xnext - 1
		}
		t.engine.mu.Unlock()
	}

	// release all locks
	t.engine.lm.Done(t.id)

	// cleanup
	clear(t.touched)
	clear(t.dbTx)
	clear(t.writes)
	t.catTx = nil
	t.snap = nil
	t.flags = 0
	t.id = 0
	t.engine = nil

	// reset all (TODO: implement tx reuse/pooling)
	t.dbTx = nil
	t.touched = nil
	t.onCommit = nil
	t.onAbort = nil
	t.writes = nil
	t.cancel(ErrTxClosed)
}

func (t *Tx) Lock(ctx context.Context, oid uint64) error {
	if t == nil {
		return ErrNoTx
	}
	if err := t.Err(); err != nil {
		return err
	}
	return t.engine.lm.Lock(ctx, t.id, LockModeExclusive, oid)
}

func (t *Tx) RLock(ctx context.Context, oid uint64) error {
	if t == nil {
		return ErrNoTx
	}
	if err := t.Err(); err != nil {
		return err
	}
	return t.engine.lm.Lock(ctx, t.id, LockModeShared, oid)
}

func (t *Tx) Touch(key uint64) {
	if len(t.touched) == 0 {
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

func (t *Tx) Commit() error {
	if t == nil {
		return ErrNoTx
	}
	if err := t.Err(); err != nil {
		return err
	}

	t.engine.log.Debugf("Commit tx %d", t.id)

	// write wal record
	_, err := t.engine.wal.WriteAndSync(&wal.Record{
		Type:   wal.RecordTypeCommit,
		Tag:    types.ObjectTagDatabase,
		Entity: t.engine.dbId,
		TxID:   t.id,
	})
	if err != nil {
		return err
	}

	// commit all touched objects, this will:
	// - write wal commit records
	// - sync wals
	// - update journal segments
	for oid, _ := range t.touched {
		err := t.engine.CommitTx(t.ctx, oid, t.id)
		if err != nil {
			return err
		}
	}

	// commit catalog
	if t.flags.IsCatalog() {
		err := t.engine.cat.CommitTx(t.ctx, t.id)
		if err != nil {
			return err
		}
	}

	// run callbacks
	for _, fn := range t.onCommit {
		if err := fn(t.ctx); err != nil {
			t.engine.log.Errorf("Tx 0x%016x commit: %v", t.id, err)
		}
	}

	// close and write storage tx
	for _, tx := range t.dbTx {
		var err error
		if tx.IsWriteable() {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			return err
		}
	}

	// handle catalog updates
	if t.catTx != nil {
		var err error
		if t.catTx.IsWriteable() {
			err = t.catTx.Commit()
		} else {
			err = t.catTx.Rollback()
		}
		if err != nil {
			return err
		}
	}
	t.Close()
	return nil
}

func (t *Tx) Abort() error {
	if t == nil {
		return ErrNoTx
	}

	if err := t.Err(); err != nil {
		return err
	}

	// don't log read only tx
	if t.id != 0 && t.engine.wal != nil {
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

		// send abort to all touched objects; write wal abort records,
		// update journal segments
		for oid, _ := range t.touched {
			err := t.engine.AbortTx(t.ctx, oid, t.id)
			if err != nil {
				return err
			}
		}

		// abort and update catalog wal
		if t.flags.IsCatalog() {
			err := t.engine.cat.AbortTx(t.ctx, t.id)
			if err != nil {
				return err
			}
		}
	}

	// close storage tx
	var err error
	for _, tx := range t.dbTx {
		if e := tx.Rollback(); e != nil && err == nil {
			err = e
		}
	}

	// close catalog tx
	if t.catTx != nil {
		if e := t.catTx.Rollback(); e != nil && err == nil {
			err = e
		}
	}

	// run callbacks
	for _, fn := range t.onAbort {
		if err := fn(t.ctx); err != nil {
			t.engine.log.Errorf("Tx 0x%016x abort: %v", t.id, err)
		}
	}

	// cleanup tx
	t.Close()
	return err
}

func (t *Tx) kill() {
	// TODO
	// - like abort() but called from another goroutine on engine shutdown
	// - should prevent race conditions with other code altering tx data
	// - write wal record
	// - run abort callbacks
	// - free locks
	// - abort storage txn
	// - prevent running commit/abort functions
	//
	// scenarios
	// - tx owner goroutine is waiting or executing database code
	//   -> needs context cancel with cause
	// - tx owner goroutine is in user code
	//   -> needs flag & check on each entry to a tx func (return error ErrTxKilled)
	t.cancel(ErrDatabaseShutdown)
}

func (t *Tx) StoreTx(db store.DB, write bool) (store.Tx, error) {
	if t == nil {
		return nil, ErrNoTx
	}
	if err := t.Err(); err != nil {
		return nil, err
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
	if err := t.Err(); err != nil {
		return nil, err
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
	t.flags |= TxFlagsCatalog
	return tx, nil
}

// Commits and reopens a writeable storage tx. This may be used to flush verly large
// transactions to storage in incremental pieces.
func (t *Tx) Continue(tx store.Tx) (store.Tx, error) {
	if err := t.Err(); err != nil {
		return nil, err
	}
	db := tx.DB()
	tx, err := store.CommitAndContinue(tx)
	if err != nil {
		delete(t.dbTx, db)
		return nil, err
	}
	t.dbTx[db] = tx
	return tx, nil
}
