// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package mem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"

	"blockwatch.cc/knoxdb/internal/store"
)

type Options struct {
	ReadOnly bool
	Persist  bool // keep in memory after close

	// Fault simulation
	SimulateReadErrors   bool
	SimulateWriteErrors  bool
	SimulateFlushErrors  bool
	SimulateBitRotErrors bool

	// Introspection / fault injection interface
	GetCallback    func(k, v []byte) []byte
	PutCallback    func(k, v []byte) ([]byte, []byte, error)
	DeleteCallback func(k []byte) ([]byte, error)
}

var defaultOptions = &Options{}

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
	dbPath    string       // name only, this db does not store persistent data
	opts      *Options
	manifest  store.Manifest
	store     *btree.BTreeG[Item]          // the database (btree)
	bucketIds map[string][bucketIdLen]byte // bucket key to id map
	sequences map[string]*sequence         // map of open sequences
}

// Enforce db implements the store.DB interface.
var _ store.DB = (*db)(nil)

// Registry stores open memdb instances
var registry sync.Map

// Type returns the database driver type the current database instance was
// created with.
//
// This function is part of the store.DB interface implementation.
func (_ *db) Type() string {
	return dbType
}

func (db *db) IsReadOnly() bool {
	return db.opts.ReadOnly
}

// IsZeroCopyRead returns true if keys and values on Get and from Cursors
// are only valid within the current transaction (or iterator step).
func (_ *db) IsZeroCopyRead() bool {
	return true
}

// Path returns the path where the current database is stored.
//
// This function is part of the store.DB interface implementation.
func (db *db) Path() string {
	return db.dbPath
}

// Manifest returns the current database manifest metadata.
//
// This function is part of the store.DB interface implementation.
func (db *db) Manifest() (store.Manifest, error) {
	return db.manifest, nil
}

// SetManifest overwrites the current database manifest.
//
// This function is part of the store.DB interface implementation.
func (db *db) SetManifest(manifest store.Manifest) error {
	db.writeLock.Lock()
	defer db.writeLock.Unlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}
	// we only allow some fields to be overwritten
	db.manifest.Name = manifest.Name
	db.manifest.Version = manifest.Version
	db.manifest.Label = manifest.Label
	db.manifest.Schema = manifest.Schema
	return nil
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

// begin is the implementation function for the Begin database method.  See its
// documentation for more details.
//
// This function is only separate because it returns the internal transaction
// which is used by the managed transaction code while the database method
// returns the interface.
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
		db.writeLock.Lock()

		// cross-check the db was not closed while waiting for the lock.
		if db.closed {
			db.writeLock.Unlock()
			return nil, makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
		}
	} else {
		// Readers must also acquire a lock to make writes atomic.
		db.writeLock.RLock()

		// cross-check the db was not closed while waiting for the lock.
		if db.closed {
			db.writeLock.RUnlock()
			return nil, makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
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
//
// This function is part of the store.DB interface implementation.
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
		log.Error(err)
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
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	return db.close()
}

func (db *db) close() error {
	// don't clear when persist flag is set
	if db.opts.Persist {
		return nil
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
		log.Debugf("%s database closed successfully.", strings.Title(db.manifest.Name))
	} else {
		log.Debugf("Database closed successfully.")
	}
	db.closed = true
	db.store = nil
	db.opts = nil
	return nil
}

func (db *db) Sync() error {
	// noop
	return nil
}

// initDB creates the initial buckets and values used by the package.  This is
// mainly in a separate function for testing purposes.
func initDB(db *db) error {
	// init manifest
	now := time.Now().UTC()
	db.manifest = store.Manifest{
		CreatedAt: now,
	}

	// init buckets
	db.bucketIds["root"] = [bucketIdLen]byte{}

	return nil
}

// openDB opens the database at the provided path.  store.ErrDbDoesNotExist
// is returned if the database doesn't exist and the create flag is not set.
func openDB(dbPath string, opts *Options, create bool) (store.DB, error) {
	val, ok := registry.Load(dbPath)
	if !ok && !create {
		str := fmt.Sprintf("database %q does not exist", dbPath)
		return nil, makeDbErr(store.ErrDbDoesNotExist, str, nil)
	}
	if val != nil {
		return val.(*db), nil
	}

	db := &db{
		store:     btree.NewG[Item](2, func(a, b Item) bool { return bytes.Compare(a.Key, b.Key) < 0 }),
		dbPath:    dbPath,
		opts:      opts,
		sequences: make(map[string]*sequence),
		bucketIds: make(map[string][bucketIdLen]byte),
	}

	log.Debug("Initializing database...")
	if err := initDB(db); err != nil {
		db.Close()
		return nil, err
	}

	registry.Store(dbPath, db)

	return db, nil
}

// Database maintenance functions

// Export all database contents as protobuf data.
func (_ *db) Dump(_ io.Writer) error {
	// not implemented
	return nil
}

// Should be called on a database that is not running any other
// concurrent transactions while it is running.
func (_ *db) Restore(_ io.Reader) error {
	// not implemented
	return nil
}

// Should be called on a database that is not running any concurrent tx.
func (_ *db) GC(_ context.Context, _ float64) error {
	// not implemented
	return nil
}

// walkFunc is the type of the function called for keys (buckets and "normal"
// values) discovered by Walk. keys is the list of keys to descend to the bucket
// owning the discovered key/value pair k/v.
type walkFunc func(keys [][]byte, k, v []byte, seq uint64) error

// walk walks recursively the bolt database db, calling walkFn for each key it finds.
func walk(db *db, walkFn walkFunc) error {
	return db.View(func(tx store.Tx) error {
		return tx.Root().ForEachBucket(func(name []byte, b store.Bucket) error {
			return walkBucket(b, nil, name, nil, 0, walkFn)
		})
	})
}

func walkBucket(b store.Bucket, keypath [][]byte, k, v []byte, seq uint64, fn walkFunc) error {
	// Execute callback.
	if err := fn(keypath, k, v, seq); err != nil {
		return err
	}

	// If this is not a bucket then stop.
	if v != nil {
		return nil
	}

	// Iterate over each child key/value.
	keypath = append(keypath, k)
	return b.ForEach(func(k, v []byte) error {
		if v == nil {
			bkt := b.Bucket(k)
			return walkBucket(bkt, keypath, k, nil, 0, fn)
		}
		return walkBucket(b, keypath, k, v, 0, fn)
	})
}
