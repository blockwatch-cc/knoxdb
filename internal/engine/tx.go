// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"errors"
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

const ReadTxOffset uint64 = 1 << 63

func (f TxFlags) IsReadOnly() bool     { return f&TxFlagsReadOnly > 0 }
func (f TxFlags) IsCatalog() bool      { return f&TxFlagsCatalog > 0 }
func (f TxFlags) IsSerializable() bool { return f&TxFlagsSerializable > 0 }
func (f TxFlags) IsDeferred() bool     { return f&TxFlagsDeferred > 0 }
func (f TxFlags) IsConflict() bool     { return f&TxFlagsConflict > 0 }

type TxHook func(Context) error

type Tx struct {
	id       uint64                  // unique tx id (read-only txn use alternative range)
	engine   *Engine                 // reference to engine
	dbTx     map[store.DB]store.Tx   // storage tx for table dbs
	catTx    store.Tx                // separate storage tx for catalog db
	snap     *types.Snapshot         // isolation snapshot
	touched  map[uint64]struct{}     // oids we have written to (tables, stores)
	onCommit []TxHook                // list of callbacks to execute before storage sync
	onAbort  []TxHook                // list of callbacks to execute before storage sync
	ctx      context.Context         // derived context so tx is cancellable
	cancel   context.CancelCauseFunc // cancel tx with this function

	// TODO: concurrency control config option: optimistic/deterministic
	// single writer: rollback sequences -> determinism
	// multi writer: don't roll back

	// TODO: close/drop/truncate/compact/alter integration
	// - use lock manager
	// - should deterministic tx exclusive lock the entire db (or table?)

	// TODO tx isolation

	// TODO tx conflict detection
	// - keep list of written PKs per table in tx (how to know, journal?)
	// - keep aggregate list of written PKs in engine
	// - add written pk to both lists on write (in journal, update/delete calls only)
	// - check if pk exists in global list but not local list
	//   - if not in local and in global: -> conflict, abort current tx
	//   - if in local and global: -> second update/undelete/etc -> allow
	// - on tx close (commit or abort) AND NOT engine list with local list clearing touched pks
	writes map[uint64]bitmap.Bitmap

	// flags
	flags TxFlags
}

// TxList is a list of transactions sorted by txid
type TxList []*Tx

func (t *TxList) Add(tx *Tx) {
	txs := *t
	switch l := len(txs); l {
	case 0:
		txs = append(txs, tx)
	default:
		if txs[l-1].id < tx.id {
			txs = append(txs, tx)
		} else {
			i := sort.Search(len(txs), func(i int) bool { return txs[i].id > tx.id })
			txs = append(txs, nil)
			copy(txs[i+1:], txs[i:])
			txs[i] = tx
		}
	}
	*t = txs
}

func (t *TxList) Del(tx *Tx) {
	txs := *t
	switch l := len(txs); l {
	case 1:
		txs = txs[:0]
	default:
		if txs[l-1].id == tx.id {
			txs = txs[:l-1]
		} else {
			i := sort.Search(len(txs), func(i int) bool { return txs[i].id >= tx.id })
			copy(txs[i:], txs[i+1:])
			txs[l-1] = nil
			txs = txs[:l-1]
		}
	}
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
		// create snapshot for read transactions (don't pollute xid space, use virtual id)
		e.mu.Lock()
		tx.id = atomic.AddUint64(&e.vnext, 1)
		tx.snap = e.NewSnapshot(0)
		e.txs.Add(tx)
		e.mu.Unlock()
	} else {
		// generate txid for write transactions and store in global tx list
		e.mu.Lock()
		tx.id = atomic.AddUint64(&e.xnext, 1)
		tx.snap = e.NewSnapshot(tx.id)
		e.txs.Add(tx)
		e.mu.Unlock()
	}
	e.log.Tracef("New tx %d", tx.id)

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

func (t *Tx) IsClosed() bool {
	return t.engine == nil
}

func (t *Tx) HasWritten() bool {
	return len(t.touched) > 0 || t.flags.IsCatalog()
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
	if t.IsClosed() {
		return
	}

	// FIXME: avoid lock, maybe use channel to signal to engine that tx
	// can be removed and xmin update should happen

	// lock engine
	t.engine.mu.Lock()
	defer t.engine.mu.Unlock()

	// remove from global tx list
	t.engine.txs.Del(t)

	// update xmin, without active tx we use xnext (horion: anything smaller is final)
	t.engine.xmin = t.engine.xnext

	// use first active read/write xid if any
	if len(t.engine.txs) > 0 {
		for _, tx := range t.engine.txs {
			if tx.IsReadOnly() {
				continue
			}
			t.engine.xmin = tx.id
			break
		}
	}

	// release all locks
	t.engine.log.Tracef("Unlock tx %d", t.id)
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
	t.engine.log.Tracef("Lock tx %d", t.id)
	return t.engine.lm.Lock(ctx, t.id, LockModeExclusive, oid)
}

func (t *Tx) Unlock() {
	if t == nil {
		return
	}
	t.engine.log.Tracef("Unlock tx %d", t.id)
	t.engine.lm.Done(t.id)
}

func (t *Tx) RLock(ctx context.Context, oid uint64) error {
	if t == nil {
		return ErrNoTx
	}
	if err := t.Err(); err != nil {
		return err
	}
	t.engine.log.Tracef("Rlock tx %d", t.id)
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

	if t.IsClosed() {
		return nil
	}

	defer t.Close()

	t.engine.log.Tracef("Commit tx %d", t.id)

	// don't log read only tx or tx without activity
	if t.HasWritten() && t.engine.wal != nil {
		_, err := t.engine.wal.WriteAndSync(&wal.Record{
			Type:   wal.RecordTypeCommit,
			Tag:    types.ObjectTagDatabase,
			Entity: t.engine.dbId,
			TxID:   t.id,
		})
		if err != nil {
			t.Fail(err)
		}
	}

	// commit all touched objects, this will update journal segments
	for oid := range t.touched {
		err := t.engine.CommitTx(t.ctx, oid, t.id)
		if err != nil {
			t.Fail(err)
		}
	}

	// commit catalog
	if t.flags.IsCatalog() {
		err := t.engine.cat.CommitTx(t.ctx, t.id)
		if err != nil {
			t.Fail(err)
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
			t.Fail(err)
		}
	}
	clear(t.dbTx)

	// close and write catalog updates
	if t.catTx != nil {
		var err error
		if t.catTx.IsWriteable() {
			err = t.catTx.Commit()
		} else {
			err = t.catTx.Rollback()
		}
		if err != nil {
			t.Fail(err)
		}
		t.catTx = nil
	}

	return t.Err()
}

func (t *Tx) Abort() error {
	if t == nil {
		return ErrNoTx
	}

	if t.IsClosed() {
		return nil
	}

	defer t.Close()

	t.engine.log.Tracef("Abort tx %d", t.id)

	// don't log read only tx, tx without activity
	if t.HasWritten() && t.engine.wal != nil {
		// write abort record to wal (no sync required, tx data is ignored if lost)
		_, err := t.engine.wal.Write(&wal.Record{
			Type:   wal.RecordTypeAbort,
			Tag:    types.ObjectTagDatabase,
			Entity: t.engine.dbId,
			TxID:   t.id,
		})
		if err != nil {
			t.Fail(err)
		}
	}

	// send abort to all touched objects; write wal abort records,
	// update journal segments
	for oid := range t.touched {
		err := t.engine.AbortTx(t.ctx, oid, t.id)
		if err != nil {
			t.Fail(err)
		}
	}

	// abort and update catalog wal
	if t.flags.IsCatalog() {
		err := t.engine.cat.AbortTx(t.ctx, t.id)
		if err != nil {
			t.Fail(err)
		}
	}

	// run callbacks
	for _, fn := range t.onAbort {
		if err := fn(t.ctx); err != nil {
			t.engine.log.Errorf("Tx 0x%016x abort: %v", t.id, err)
		}
	}

	// close storage tx
	for _, tx := range t.dbTx {
		if err := tx.Rollback(); err != nil {
			t.Fail(err)
		}
	}
	clear(t.dbTx)

	// close catalog tx
	if t.catTx != nil {
		if err := t.catTx.Rollback(); err != nil {
			t.Fail(err)
		}
		t.catTx = nil
	}

	// return the first error or nil if everything when ok
	return t.Err()
}

func (t *Tx) Fail(err error) {
	if errors.Is(err, ErrTxConflict) {
		t.flags |= TxFlagsConflict
	}
	t.cancel(err)
}

// func (t *Tx) kill() {
// 	// TODO
// 	// - like abort() but called from another goroutine on engine shutdown
// 	// - should prevent race conditions with other code altering tx data
// 	// - write wal record
// 	// - run abort callbacks
// 	// - free locks
// 	// - abort storage txn
// 	// - prevent running commit/abort functions
// 	//
// 	// scenarios
// 	// - tx owner goroutine is waiting or executing database code
// 	//   -> needs context cancel with cause
// 	// - tx owner goroutine is in user code
// 	//   -> needs flag & check on each entry to a tx func (return error ErrTxKilled)
// 	t.cancel(ErrDatabaseShutdown)
// }

func (t *Tx) Kill(err error) error {
	// cancel context
	t.cancel(err)
	err = nil

	// close storage tx
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

	// release locks
	t.engine.lm.Done(t.id)

	// remove from tx list (requires Kill is called under lock)
	t.engine.txs.Del(t)

	// clear data
	clear(t.touched)
	clear(t.dbTx)
	clear(t.writes)
	t.catTx = nil
	t.engine = nil

	return err
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
