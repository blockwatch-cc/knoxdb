// Copyright (c) 2024-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package mem

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/google/btree"

	"blockwatch.cc/knoxdb/pkg/store"
)

type Item struct {
	Key []byte
	Val []byte
}

func (a Item) Less(b Item) bool {
	return bytes.Compare(a.Key, b.Key) < 0
}

// db is an ephemeral in-memory key-value database which implements the store.DB interface.
// All database access is performed through managed transactions.
type db struct {
	writeLock sync.RWMutex // Single writer, multiple readers. Also used for close.
	seqLock   sync.RWMutex // Guard access to sequences.
	closed    bool         // Is the database closed?
	opts      store.Options
	manifest  *store.Manifest
	store     *btree.BTreeG[Item]          // the database (btree)
	bucketIds map[string][bucketIdLen]byte // bucket key to id map
	sequences map[string]*sequence         // map of open sequences
}

func (db *db) bucketName(id [bucketIdLen]byte) string {
	for n, k := range db.bucketIds {
		if k == id {
			return n
		}
	}
	return ""
}

// Enforce db implements the store.DB interface.
var _ store.DB = (*db)(nil)

// Registry stores open memdb instances
var registry sync.Map

// Type returns the database driver type the current database instance was
// created with.
func (*db) Type() string {
	return dbType
}

func (db *db) IsReadOnly() bool {
	return db.opts.Readonly
}

// IsZeroCopyRead returns true if keys and values on Get and from Cursors
// are only valid within the current transaction (or iterator step).
func (*db) IsZeroCopyRead() bool {
	return true
}

// Path returns the path where the current database is stored.
func (db *db) Path() string {
	return db.opts.Path
}

// Sequence creates a new managed sequence stored in the sequences bucket.
func (db *db) Sequence(key []byte, lease uint64) (store.Sequence, error) {
	db.seqLock.RLock()
	if seq, ok := db.sequences[string(key)]; ok {
		db.seqLock.RUnlock()
		return seq, nil
	}
	db.seqLock.RUnlock()
	seq := &sequence{
		key: string(key),
		db:  db,
	}
	seq.seq.Store(1)
	db.seqLock.Lock()
	db.sequences[seq.key] = seq
	db.seqLock.Unlock()
	return seq, nil
}

// begin starts a new transaction and returns its internal handle
func (db *db) begin(writable bool) (*transaction, error) {
	// Whenever a new transaction is started, grab a read or write lock against
	// the database. This ensures write transactions are isolated, multiple read
	// transactions can run concurrently and close waits for all transactions to
	// complete.
	//
	// This lock will not be released until the transaction is closed (via
	// Rollback or Commit).
	if writable {
		// Enforce single writer property to make transactions serializable.
		// Without this global lock we would have to implement concurrency
		// control on tx commit which is unnecessary for this simple db driver.
		// fmt.Printf("Wlock\n%s", string(debug.Stack()))
		db.writeLock.Lock()

		// cross-check the db was not closed while waiting for the lock.
		if db.closed {
			db.writeLock.Unlock()
			return nil, store.ErrDatabaseClosed
		}
	} else {
		// Readers must also acquire a lock to make writes atomic.
		// fmt.Printf("Rlock\n%s", string(debug.Stack()))
		db.writeLock.RLock()

		// cross-check the db was not closed while waiting for the lock.
		if db.closed {
			db.writeLock.RUnlock()
			return nil, store.ErrDatabaseClosed
		}
	}

	tx := &transaction{
		writable: writable,
		db:       db,
	}
	if writable {
		tx.updates = make(map[string][]byte)
		tx.deletes = make(map[string]struct{})
	}
	return tx, nil
}

// Begin starts a transaction which is either read-only or read-write depending
// on the specified flag.  Multiple read-only transactions can be started
// simultaneously while only a single read-write transaction can be started at a
// time.  The call will block when starting a read-write transaction when one is
// already open.
//
// NOTE: The transaction must be closed by calling Rollback or Commit on it when
// it is no longer needed.  Failure to do so will result in unclaimed memory.
func (db *db) Begin(writable bool) (store.Tx, error) {
	return db.begin(writable)
}

// rollbackOnPanic rolls the passed transaction back if the code in the calling
// function panics.  This is needed since the mutex on a transaction must be
// released and a panic in called code would prevent that from happening.
//
// NOTE: This can only be handled manually for managed transactions since they
// control the life-cycle of the transaction.  As the documentation on Begin
// calls out, callers opting to use manual transactions will have to ensure the
// transaction is rolled back on panic if it desires that functionality as well
// or the database will fail to close since the read-lock will never be
// released.
func rollbackOnPanic(tx *transaction) {
	// note: runtime.Goexit used in testing.Fail does not panic but
	// still unwinds all defered functions
	err := recover()
	if err != nil {
		tx.db.opts.Log.Error(err)
	}

	tx.managed = false
	_ = tx.Rollback()

	if err != nil {
		panic(err)
	}
}

// View invokes the passed function in the context of a managed read-only
// transaction with the root bucket for the namespace.  Any errors returned from
// the user-supplied function are returned from this function.
//
// This function is part of the store.DB interface implementation.
func (db *db) View(fn func(store.Tx) error) error {
	// Start a read-only transaction.
	tx, err := db.begin(false)
	if err != nil {
		return err
	}

	// Since the user-provided function might panic, ensure the transaction
	// releases all mutexes and resources.  There is no guarantee the caller
	// won't use recover and keep going.  Thus, the database must still be
	// in a usable state on panics due to caller issues.
	defer rollbackOnPanic(tx)

	tx.managed = true
	err = fn(tx)
	tx.managed = false
	if err != nil {
		// The error is ignored here because nothing was written yet
		// and regardless of a rollback failure, the tx is closed now
		// anyways.
		_ = tx.Rollback()
		return err
	}

	return tx.Rollback()
}

// Update invokes the passed function in the context of a managed read-write
// transaction with the root bucket for the namespace.  Any errors returned from
// the user-supplied function will cause the transaction to be rolled back and
// are returned from this function.  Otherwise, the transaction is committed
// when the user-supplied function returns a nil error.
//
// This function is part of the store.DB interface implementation.
func (db *db) Update(fn func(store.Tx) error) error {
	// Start a read-write transaction.
	tx, err := db.begin(true)
	if err != nil {
		return err
	}

	// Since the user-provided function might panic, ensure the transaction
	// releases all mutexes and resources.  There is no guarantee the caller
	// won't use recover and keep going.  Thus, the database must still be
	// in a usable state on panics due to caller issues.
	defer rollbackOnPanic(tx)

	tx.managed = true
	err = fn(tx)
	tx.managed = false
	if err != nil {
		// The error is ignored here because nothing was written yet
		// and regardless of a rollback failure, the tx is closed now
		// anyways.
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// Close cleanly shuts down the database and syncs all data.  It will block
// until all database transactions have been finalized (rolled back or
// committed).
//
// This function is part of the store.DB interface implementation.
func (db *db) Close() error {
	// Since all transactions have a read lock on this mutex, this will
	// cause Close to wait for all readers to complete.
	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	if db.closed {
		return store.ErrDatabaseClosed
	}

	db.close()
	return nil
}

func (db *db) close() {
	// don't clear when persist flag is set
	if db.opts.KeepOnClose {
		db.closed = true
		return
	}

	// drop all sequences and buckets
	clear(db.sequences)
	db.sequences = nil
	clear(db.bucketIds)
	db.bucketIds = nil

	// NOTE: Since the close lock waits for all transactions to finish and
	// prevents any new ones from being started, it is safe to clear the
	// underlying btree here.
	db.store.Clear(false)

	if db.manifest.Name != "" {
		db.opts.Log.Debugf("%s database closed successfully.", db.manifest.Name)
	} else {
		db.opts.Log.Debugf("database closed successfully.")
	}
	db.closed = true
	db.store = nil
	registry.Delete(db.opts.Path)
}

func (db *db) Sync() error {
	return nil
}

// Database maintenance functions

// Export all database contents as protobuf data.
func (*db) Dump(_ io.Writer) error {
	return store.ErrNotImplemented
}

// Should be called on a database that is not running any other
// concurrent transactions while it is running.
func (*db) Restore(_ io.Reader) error {
	return store.ErrNotImplemented
}

// Should be called on a database that is not running any concurrent tx.
func (*db) GC(_ context.Context, _ float64) error {
	return store.ErrNotImplemented
}
