// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/store"
	"blockwatch.cc/knoxdb/pkg/util"
)

type TxFlags uint16

const (
	TxFlagReadOnly TxFlags = 1 << iota
	TxFlagNoWal            // do not write wal
	TxFlagNoSync           // write wal but do not fsync
	TxFlagNoWait           // don't block in single writer mode
	TxFlagCatalog          // txn made changes to catalog (internal)
	TxFlagConflict         // conflict detected, abort on commit (internal)
	TxFlagAborted          // set when aborted or killed (internal)
	TxFlagClosed           // set on close (internal)

	// multi-writer support
	TxFlagDelaySync    // batch wal fsync requests
	TxFlagSerializable // use serializable snapshot isolation level (TODO)
	TxFlagDeferred     // wait for safe snapshot (TODO)
)

func (f TxFlags) IsReadOnly() bool     { return f&TxFlagReadOnly > 0 }
func (f TxFlags) IsCatalog() bool      { return f&TxFlagCatalog > 0 }
func (f TxFlags) IsConflict() bool     { return f&TxFlagConflict > 0 }
func (f TxFlags) IsClosed() bool       { return f&TxFlagClosed > 0 }
func (f TxFlags) IsAborted() bool      { return f&TxFlagAborted > 0 }
func (f TxFlags) IsNoWal() bool        { return f&TxFlagNoWal > 0 }
func (f TxFlags) IsNoSync() bool       { return f&TxFlagNoSync > 0 }
func (f TxFlags) IsNoWait() bool       { return f&TxFlagNoWait > 0 }
func (f TxFlags) IsDelaySync() bool    { return f&TxFlagDelaySync > 0 }
func (f TxFlags) IsSerializable() bool { return f&TxFlagSerializable > 0 }
func (f TxFlags) IsDeferred() bool     { return f&TxFlagDeferred > 0 }

func mergeFlags(x ...TxFlags) (f TxFlags) {
	for _, v := range x {
		f |= v
	}
	return
}

type TxHook func(Context) error

const (
	ReadTxOffset = types.ReadTxOffset
	rtFlags      = TxFlagCatalog | TxFlagConflict | TxFlagClosed | TxFlagAborted
)

type Tx struct {
	closing  atomic.Uint32           // atomic preventing race on close
	uflags   TxFlags                 // static user flags
	rtflags  TxFlags                 // runtime updatable flags
	ctx      context.Context         // derived context so tx is cancellable
	cancel   context.CancelCauseFunc // cancel tx with this function
	id       types.XID               // unique tx id (read-only txn use alternative range)
	engine   *Engine                 // reference to engine
	catTx    store.Tx                // storage tx for catalog db
	snap     *types.Snapshot         // isolation snapshot
	touched  map[uint64]struct{}     // oids we have written to (tables, stores)
	onCommit []TxHook                // callbacks to run before storage sync
	onAbort  []TxHook                // callbacks to run before storage sync
}

func (e *Engine) InitTxHorizon(xid types.XID) {
	e.xmin.Store(xid + 1)
	e.xid.Store(xid)
}

// Moves xid horizon ahead. Called during wal replay and when
// a write tx closes. Note: single writer design.
func (e *Engine) UpdateTxHorizon(xid types.XID) {
	for {
		xmin := e.xmin.Load()
		if e.xmin.CompareAndSwap(xmin, xid+1) {
			return
		}
	}
}

func (e *Engine) BeginTransaction(ctx context.Context, flags ...TxFlags) (context.Context, *Tx, func() error, func() error, error) {
	// merge flags
	uflags := mergeFlags(flags...)

	// prevent duplicates, return noops when an outer call frame controls tx
	if tx := GetTx(ctx); tx != nil {
		// check compatibility
		if tx.IsReadOnly() && !uflags.IsReadOnly() {
			return ctx, tx, noop, noop, ErrTxReadonly
		}

		// allow catalog flag update
		for _, f := range flags {
			if f == TxFlagCatalog {
				tx.WithFlags(f)
			}
		}

		// return error in case tx is already canceled
		return ctx, tx, noop, noop, tx.Err()
	}

	// check readonly state
	if e.IsReadOnly() && !uflags.IsReadOnly() {
		return ctx, nil, noop, noop, ErrDatabaseReadOnly
	}

	// check engine shutdown state
	if e.IsShutdown() {
		return ctx, nil, noop, noop, ErrDatabaseShutdown
	}

	// apply engine nosync
	if e.opts.NoSync {
		uflags |= TxFlagNoSync
	}

	// potentially wait
	ok := true
	if uflags.IsReadOnly() {
		// enforce deferred flag
		if uflags.IsDeferred() {
			switch {
			case e.opts.TxWaitTimeout > 0:
				select {
				case _, ok = <-e.xtoken:
				case <-time.After(e.opts.TxWaitTimeout):
					return ctx, nil, noop, noop, ErrTxTimeout
				}
			default:
				_, ok = <-e.xtoken
			}
		}
	} else {
		// enforce single writer tx
		switch {
		case uflags.IsNoWait():
			select {
			case _, ok = <-e.xtoken:
			default:
				return ctx, nil, noop, noop, ErrTxConflict
			}
		case e.opts.TxWaitTimeout > 0:
			select {
			case _, ok = <-e.xtoken:
			case <-time.After(e.opts.TxWaitTimeout):
				return ctx, nil, noop, noop, ErrTxTimeout
			}
		default:
			_, ok = <-e.xtoken
		}
	}

	// channel was closed during wait or engine is in shutdown.
	// note because the channel is buffered we may consume the
	// token on shutdown without seeing the closed flag, hence
	// and explict check for shutdown is needed
	if !ok || e.IsShutdown() {
		return ctx, nil, noop, noop, ErrDatabaseShutdown
	}

	// create new tx
	tx := e.newTransaction(ctx, uflags)

	// return writer token after deferred reader wait
	if uflags.IsDeferred() {
		e.xtoken <- struct{}{}
	}

	return tx.ctx, tx, tx.Commit, tx.Abort, nil
}

func (e *Engine) newTransaction(ctx context.Context, uflags TxFlags) *Tx {
	// alloc a fresh tx object
	tx := &Tx{
		engine: e,
		uflags: uflags,
	}

	// extend user context with a cancel cause func; this new context
	// will be canceled when the transaction finished
	tctx, tcancel := context.WithCancelCause(ctx)

	// link tx and engine to the merged context, use cancel func
	tx.ctx = WithTx(WithEngine(tctx, e), tx)
	tx.cancel = tcancel

	// refcount
	e.xwg.Add(1)

	// derive xid and MVCC snapshot
	if uflags.IsReadOnly() {
		// create snapshot for read transactions
		// use virtual id to not pollute xid space
		tx.id = e.vid.Add(1)  // generate new read-only (v)xid
		xmin := e.xmin.Load() // read current xmin horizon first
		xid := e.xid.Load()   // read current write xid last
		if xid == xmin {
			// when xmin == xid there is a concurrent write tx
			// in progress, use its id to init xmax to the next
			// next not yet assigned xid and remember xid as active.
			// because reading xid and xmin are two independent atomic
			// reads, a concurrently starting write tx could have have
			// moved xid ahead of xmin, multiple write tx could have
			// even finished leaving us with a stale xmin horizon here.
			// in either case we can assume a write tx is or was active
			// after we read xmin and we keep using it in the snapshot
			tx.snap = types.NewSnapshot(0, xmin, xid+1)
			tx.snap.AddActive(xid)
		} else {
			// without concurrent write tx init both xmin and xmax
			// to a future yet unassigned xid and mark snapshot safe
			tx.snap = types.NewSnapshot(0, xid+1, xid+1)
		}
	} else {
		// generate xid for write transactions, at this point we hold
		// the single writer token
		tx.id = e.xid.Add(1)
		tx.snap = types.NewSnapshot(tx.id, tx.id, tx.id+1)
	}

	// e.log.Tracef("New tx %s", tx.id)

	return tx
}

func noop() error {
	return nil
}

func (t *Tx) close() {
	// prevent double close (e.g. when wal write on commit fails)
	if t.engine == nil {
		return
	}

	// close context if not done already
	t.cancel(ErrTxClosed)
	t.rtflags |= TxFlagClosed

	e := t.engine

	// release all locks (during back-pressure this may have happened
	// already in which case tihs call becomes a noop)
	// e.log.Tracef("Unlock tx %s", tx.id)
	e.lm.Done(t.id)

	// for write tx move horizon and release the writer token
	if !t.IsReadOnly() {
		// update xmin when write tx closes
		e.UpdateTxHorizon(t.id)

		// wake up next waiting writer
		e.xtoken <- struct{}{}
	}
	e.xwg.Done()

	// cleanup, but keep id and flags
	clear(t.touched)
	t.snap.Close()
	t.engine = nil
	t.catTx = nil
	t.snap = nil
	t.touched = nil
	t.onCommit = nil
	t.onAbort = nil
}

func (t *Tx) fail(err error) {
	if errors.Is(err, ErrTxConflict) {
		t.rtflags |= TxFlagConflict
	}
	t.cancel(err)
}

func (t *Tx) Id() types.XID {
	return t.id
}

func (t *Tx) Snapshot() *types.Snapshot {
	return t.snap
}

func (t *Tx) Context() context.Context {
	return t.ctx
}

func (t *Tx) Engine() *Engine {
	return t.engine
}

func (t *Tx) Err() error {
	return context.Cause(t.ctx)
}

func (t *Tx) WithFlags(flags ...TxFlags) *Tx {
	for _, f := range flags {
		if f&rtFlags > 0 {
			t.rtflags |= f
		} else {
			t.uflags |= f
		}
	}
	return t
}

func (t *Tx) IsReadOnly() bool {
	return t.uflags.IsReadOnly()
}

func (t *Tx) IsClosed() bool {
	return t.rtflags.IsClosed()
}

func (t *Tx) IsAborted() bool {
	return t.rtflags.IsClosed() && t.rtflags.IsAborted()
}

func (t *Tx) IsCommitted() bool {
	return t.rtflags.IsClosed() && !t.rtflags.IsAborted()
}

func (t *Tx) UseWal() bool {
	return !t.uflags.IsNoWal() &&
		t.engine.wal != nil &&
		(len(t.touched) > 0 || t.rtflags.IsCatalog())
}

func (t *Tx) Lock(ctx context.Context, oid uint64) error {
	if t.IsClosed() {
		return t.Err()
	}
	if err := t.Err(); err != nil {
		return err
	}
	// t.engine.log.Tracef("Lock tx %s", t.id)
	return t.engine.lm.Lock(ctx, t.id, LockModeExclusive, oid)
}

func (t *Tx) Unlock() {
	if t.IsClosed() {
		return
	}
	// t.engine.log.Tracef("Unlock tx %s", t.id)
	t.engine.lm.Done(t.id)
}

func (t *Tx) RLock(ctx context.Context, oid uint64) error {
	if t.IsClosed() {
		return t.Err()
	}
	if err := t.Err(); err != nil {
		return err
	}
	// t.engine.log.Tracef("Rlock tx %s", t.id)
	return t.engine.lm.Lock(ctx, t.id, LockModeShared, oid)
}

func (t *Tx) Touch(key uint64) {
	if len(t.touched) == 0 {
		t.touched = make(map[uint64]struct{})
	}
	t.touched[key] = struct{}{}
}

func (t *Tx) Touched(key uint64) bool {
	_, ok := t.touched[key]
	return ok
}

func (t *Tx) OnCommit(fn TxHook) {
	t.onCommit = append(t.onCommit, fn)
}

func (t *Tx) OnAbort(fn TxHook) {
	t.onAbort = append(t.onAbort, fn)
}

func (t *Tx) Commit() error {
	// prevent race with concurrent Commit/Abort
	if !t.closing.CompareAndSwap(0, 1) {
		return t.Err()
	}

	// check for shutdown and fail
	if t.engine.IsShutdown() {
		t.fail(ErrDatabaseShutdown)
		return t.abort()
	}

	// close read-only tx
	if t.IsReadOnly() {
		return t.abort()
	}

	// free resources on exit
	defer t.close()

	// t.engine.log.Tracef("Commit tx %s", t.id)

	// don't log tx without activity
	if t.UseWal() {
		rec := &wal.Record{
			Type:   wal.RecordTypeCommit,
			Tag:    types.ObjectTagDatabase,
			Entity: t.engine.dbId,
			TxID:   t.id,
		}
		var (
			err error
			fut *util.Future
		)
		switch {
		case t.uflags.IsNoSync():
			_, err = t.engine.wal.Write(rec)
		case t.uflags.IsDelaySync():
			_, fut, err = t.engine.wal.WriteAndSchedule(rec)
			if err == nil {
				fut.Wait()
				err = fut.Err()
			}
		default:
			_, err = t.engine.wal.WriteAndSync(rec)
		}

		// any WAL write failure indicates we cannot return success
		// to the user and instead must roll back pending changes.
		// capture the error and call abort
		if err != nil {
			t.fail(err)
			return t.abort()
		}
	}

	// point of no return
	t.rtflags |= TxFlagClosed

	var waitList []WaitCh

	// commit all touched objects, this will update journal segments
	for oid := range t.touched {
		if wait := t.engine.CommitTx(t.ctx, oid, t.id); wait != nil {
			waitList = append(waitList, wait)
		}
	}

	// apply catalog actions and set checkpoints, an error here is
	// fatal as WAL is already written
	if t.rtflags.IsCatalog() {
		if err := t.engine.cat.CommitTx(t.ctx, t.id); err != nil {
			t.engine.log.Errorf("Tx %s commit catalog: %v", t.id, err)
			t.fail(err)
		}
	}

	// run callbacks as stack (backwards), print error only
	for _, fn := range slices.Backward(t.onCommit) {
		if err := fn(t.ctx); err != nil {
			t.engine.log.Errorf("Tx %s commit callback: %v", t.id, err)
			t.fail(err)
		}
	}

	// finalize catalog updates (commit will turn into rollback
	// on read-only storage tx), print error only
	if t.catTx != nil {
		if err := t.catTx.Commit(); err != nil {
			t.engine.log.Errorf("Tx %s commit catalog tx: %v", t.id, err)
			t.fail(err)
		}
		t.catTx = nil
	}

	// return tx error if there was any up until this point,
	// this includes engine shutdown error
	if err := t.Err(); err != nil {
		return err
	}

	// block on backpressure from full table journals unless
	// the user requested this tx not to wait (we use the
	// nowait flag for this purpose)
	if len(waitList) > 0 {
		if t.uflags.IsNoWait() {
			// don't fail tx but still inform user to wait
			return ErrTxBackoff
		}

		// release locks
		t.engine.lm.Done(t.id)

		// wait for back-pressure to reduce, but at most TxWaitTimeout
		to := t.engine.opts.TxWaitTimeout
		if to > 0 {
			start := time.Now()
		waitloop:
			// wait for either a timeout or backpressure to ease
			for _, wait := range waitList {
				select {
				case <-wait:
				case <-time.After(to - time.Since(start)):
					// negative duration will fire immediately
					break waitloop
				}
			}
		} else {
			// wait until all pending merge tasks from back-pressured
			// journals have completed
			for _, wait := range waitList {
				<-wait
			}
		}
	}

	return nil
}

func (t *Tx) Abort() error {
	// prevent race with concurrent Commit/Abort (e.g. called by
	// context after func on shutdown which happens from a Goroutine)
	if !t.closing.CompareAndSwap(0, 1) {
		return t.Err()
	}

	// catch double-close
	if t.IsClosed() {
		return t.Err()
	}

	// check for shutdown and fail
	if t.engine.IsShutdown() {
		t.fail(ErrDatabaseShutdown)
	}

	return t.abort()
}

func (t *Tx) abort() error {
	// cleanup on exit
	defer t.close()

	// point of no return
	t.rtflags |= TxFlagAborted | TxFlagClosed

	// t.engine.log.Tracef("Abort tx %d", t.id)

	// don't log tx without activity
	if t.UseWal() && !t.IsReadOnly() {
		// write abort record to wal (no sync required, tx data is ignored if lost)
		rec := &wal.Record{
			Type:   wal.RecordTypeAbort,
			Tag:    types.ObjectTagDatabase,
			Entity: t.engine.dbId,
			TxID:   t.id,
		}
		var (
			err error
			fut *util.Future
		)
		switch {
		case t.uflags.IsNoSync():
			_, err = t.engine.wal.Write(rec)
		case t.uflags.IsDelaySync():
			_, fut, err = t.engine.wal.WriteAndSchedule(rec)
			if err == nil {
				fut.Wait()
				err = fut.Err()
			}
		default:
			_, err = t.engine.wal.WriteAndSync(rec)
		}

		// capture WAL write error and continue the abort process
		if err != nil {
			t.fail(err)
		}
	}

	// send abort to all touched objects which will update journal segments
	for oid := range t.touched {
		t.engine.AbortTx(t.ctx, oid, t.id)
	}

	// abort catalog actions
	if t.rtflags.IsCatalog() {
		if err := t.engine.cat.AbortTx(t.ctx, t.id); err != nil {
			t.engine.log.Errorf("Tx %s abort catalog: %v", t.id, err)
			t.fail(err)
		}
	}

	// run callbacks as stack (backwards), print error only
	for _, fn := range slices.Backward(t.onAbort) {
		if err := fn(t.ctx); err != nil {
			t.engine.log.Errorf("Tx %s abort callback: %v", t.id, err)
			t.fail(err)
		}
	}

	// close catalog tx, print error only
	if t.catTx != nil {
		if err := t.catTx.Rollback(); err != nil {
			t.engine.log.Errorf("Tx %s rollback catalog tx: %v", t.id, err)
			t.fail(err)
		}
		t.catTx = nil
	}

	// return the first error or nil
	return t.Err()
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
	var (
		tx  store.Tx
		err error
	)
	if write {
		tx, err = db.Begin(store.WithTxWrite())
	} else {
		tx, err = db.Begin()
	}
	if err != nil {
		return nil, err
	}
	t.catTx = tx
	t.rtflags |= TxFlagCatalog
	return tx, nil
}
