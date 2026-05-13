// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"errors"
	"io"
	"sync"

	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/pkg/store"
	"blockwatch.cc/knoxdb/pkg/util"
)

// db wraps a boltdb database instance and implements the store.DB interface.
type db struct {
	mu    sync.Mutex     // tx open lock
	wg    sync.WaitGroup // close wait
	store *bolt.DB       // the database
	opts  store.Options  // options copy
}

// Ensure db implements the store.DB interface.
var _ store.DB = (*db)(nil)

// Type returns the database's driver name.
func (db *db) Type() string {
	return dbType
}

// Path returns the path where the current database is stored.
func (db *db) Path() string {
	return db.store.Path()
}

func (db *db) IsReadOnly() bool {
	return db.store.IsReadOnly()
}

// IsZeroCopyRead returns true if keys and values on Get and from Iterators
// are only valid within the current transaction (or iterator step).
func (db *db) IsZeroCopyRead() bool {
	return true
}

// begin starts a new transaction and returns its handle
func (db *db) begin(options ...store.TxOption) (*tx, error) {
	// apply options
	var opts store.TxOptions
	for _, o := range options {
		o(&opts)
	}

	// lock tx creation to sync with close
	db.mu.Lock()
	defer db.mu.Unlock()

	// check db is not closed
	if db.store == nil {
		return nil, store.ErrDatabaseClosed
	}

	// start bolt tx
	bx, err := db.store.Begin(opts.Writable)
	if err != nil {
		return nil, wrap(err)
	}

	// refcount
	db.wg.Add(1)

	// alloc new wrapped tx
	if ix := txPool.Get(); ix != nil {
		t := ix.(*tx)
		t.db = db
		t.tx = bx
		t.sync = opts.Sync && db.opts.NoSync
		return t, nil
	} else {
		return &tx{db: db, tx: bx}, nil
	}
}

// Begin starts a read-only or read-write boltdb transaction. Multiple
// read-only transactions can exist concurrently while read-write transaction
// are mutually exclusive with read-only and other read-write transactions.
// The call will block when a conflicting transaction is in progress.
// The transaction must be closed by calling Rollback or Commit to free
// resources and locks. Failure to do so will result in unclaimed memory
// and deadlocks.
func (db *db) Begin(opts ...store.TxOption) (store.Tx, error) {
	return db.begin(opts...)
}

// View invokes the passed function in the context of a read-only
// transaction with the root bucket as namespace. Any error
// returned from the user-supplied function will abort the transaction.
func (db *db) View(fn func(store.Tx) error) error {
	// Start a read-only transaction.
	t, err := db.begin()
	if err != nil {
		return err
	}

	// Since the user-provided function might panic, ensure the transaction
	// releases all locks and resources. There is no guarantee the caller
	// won't use recover and keep going. Thus, the database must still be
	// in a usable state on panics due to caller issues.
	defer rollbackOnPanic(t)

	t.managed = true
	err = fn(t)
	t.managed = false
	if err != nil {
		// The error is ignored here because nothing was written yet
		// and regardless of a rollback failure, the tx is closed anyways.
		_ = t.Rollback()
		return err
	}

	return t.Rollback()
}

// Update invokes the passed function in the context of a managed read-write
// transaction with the root bucket as namespace. Any errors returned from
// the user-supplied function will cause the transaction to be rolled back and
// are returned from this function. On success the transaction is committed.
func (db *db) Update(fn func(store.Tx) error) error {
	// Start a read-write transaction.
	t, err := db.begin(store.WithTxWrite())
	if err != nil {
		return err
	}

	// Since the user-provided function might panic, ensure the transaction
	// releases all locks and resources. There is no guarantee the caller
	// won't use recover and keep going. Thus, the database must still be
	// in a usable state on panics due to caller issues.
	defer rollbackOnPanic(t)

	t.managed = true
	err = fn(t)
	t.managed = false
	if err != nil {
		// The error is ignored here because nothing was written yet
		// and regardless of a rollback failure, the tx is closed now
		// anyways.
		_ = t.Rollback()
		return err
	}

	return t.Commit()
}

// Close shuts down the database and syncs all data. It will block
// until all database transactions have been committed or rolled back.
func (db *db) Close() error {
	// be exclusive with tx begin
	db.mu.Lock()
	defer db.mu.Unlock()

	// check for earlier or concurrent close
	if db.store == nil {
		return store.ErrDatabaseClosed
	}

	// wait for all tx to complete
	db.wg.Wait()

	// sync and cleanup
	var err error
	if db.opts.NoSync {
		err = wrap(db.store.Sync())
	}
	err2 := wrap(db.store.Close())
	db.store = nil
	return errors.Join(err, err2)
}

func (db *db) Sync() error {
	return wrap(db.store.Sync())
}

// Snapshot exports a database backup to a writer. It runs inside a
// read-only transaction which ensures a consistent snapshot is written.
func (db *db) Snapshot(w io.Writer) error {
	// backup may run in parallel to any tx and will be using a snapshot copy
	err := db.store.View(func(tx *bolt.Tx) error {
		db.opts.Log.Debugf("Exporting database of size %s (this may take a while)...",
			util.ByteSize(tx.Size()))
		n, err := tx.WriteTo(w)
		if err != nil {
			return err
		}
		db.opts.Log.Debugf("Successfully wrote %s of data.", util.ByteSize(n))
		return nil
	})
	return wrap(err)
}

// Restore loads a database from a backup copy. It must be called on a pristine
// database (no content) and while no concurrent transactions is running.
func (db *db) Restore(r io.Reader) error {
	// not implemented; to do so implement
	// - close bolt db (waiting for any open tx)
	// - restore/overwrite file with reader contents
	// - open bolt db from restored file
	return store.ErrNotImplemented
}

// rollbackOnPanic rolls the passed transaction back if the code in the calling
// function panics. This is needed since the lock on a transaction must be
// released and a panic in called code would prevent that from happening.
//
// NOTE: This can only be handled manually for managed transactions since they
// control the life-cycle of the transaction. Callers using manual transactions
// must ensure the transaction is rolled back on panic as well. Otherwise the
// the database will deadlock on close.
func rollbackOnPanic(t *tx) {
	// note: runtime.Goexit used in testing.Fail does not panic but
	// still unwinds all defered functions
	err := recover()

	t.managed = false
	_ = t.Rollback()

	// re-panic
	if err != nil {
		panic(err)
	}
}
