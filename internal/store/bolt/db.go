// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/pkg/util"
)

// db wraps a boltdb instance and implements the store.DB interface.
// All database access is performed through transactions which are managed.
type db struct {
	// seqLock   sync.RWMutex   // Guard access to sequences.
	closeLock sync.RWMutex // Make database close block while txns active.
	// txLock    sync.Mutex     // block creating new tx during backup/restore.
	activeTx sync.WaitGroup // count active tx (needed for backup/restore).
	closed   bool           // Is the database closed?
	store    *bolt.DB       // the database
	opts     *bolt.Options
	dbPath   string
}

// Enforce db implements the store.DB interface.
var _ store.DB = (*db)(nil)

// Type returns the database driver type the current database instance was
// created with.
//
// This function is part of the store.DB interface implementation.
func (db *db) Type() string {
	return dbType
}

func (db *db) IsReadOnly() bool {
	return db.store.IsReadOnly()
}

// IsZeroCopyRead returns true if keys and values on Get and from Cursors
// are only valid within the current transaction (or iterator step).
func (db *db) IsZeroCopyRead() bool {
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
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return store.Manifest{}, makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}
	return getManifest(db.store)
}

// SetManifest overwrites the current database manifest.
//
// This function is part of the store.DB interface implementation.
func (db *db) SetManifest(manifest store.Manifest) error {
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}
	mft, err := getManifest(db.store)
	if err != nil {
		return err
	}
	// we only allow some fields to be overwritten
	mft.Name = manifest.Name
	mft.Version = manifest.Version
	mft.Label = manifest.Label
	mft.Schema = manifest.Schema
	return putManifest(db.store, mft)
}

func getManifest(bdb *bolt.DB) (store.Manifest, error) {
	var mft store.Manifest
	err := bdb.View(func(dbTx *bolt.Tx) error {
		mftBucket := dbTx.Bucket(manifestBucketKeyName)
		if mftBucket == nil {
			return makeDbErr(store.ErrInvalid, "invalid database: missing manifest", nil)
		}
		buf := mftBucket.Get(manifestKey)
		if buf != nil {
			return json.Unmarshal(buf, &mft)
		}
		return nil
	})
	if err != nil {
		return mft, err
	}
	return mft, nil
}

func putManifest(bdb *bolt.DB, manifest store.Manifest) error {
	buf, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	return bdb.Update(func(dbTx *bolt.Tx) error {
		return dbTx.Bucket(manifestBucketKeyName).Put(manifestKey, buf)
	})
}

// Sequence creates a new managed sequence stored in the sequences bucket.
func (db *db) Sequence(key []byte, lease uint64) (store.Sequence, error) {
	return &sequence{
		db:  db,
		key: copySlice(key),
	}, nil
}

// begin is the implementation function for the Begin database method.  See its
// documentation for more details.
//
// This function is only separate because it returns the internal transaction
// which is used by the managed transaction code while the database method
// returns the interface.
func (db *db) begin(writable bool) (*transaction, error) {
	// Whenever a new transaction is started, grab a read lock against the
	// database to ensure Close will wait for the transaction to finish.
	// This lock will not be released until the transaction is closed (via
	// Rollback or Commit).
	db.closeLock.RLock()
	if db.closed {
		db.closeLock.RUnlock()
		return nil, makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// db.txLock.Lock()
	// defer db.txLock.Unlock()
	dbTx, err := db.store.Begin(writable)
	if err != nil {
		db.closeLock.RUnlock()
		return nil, convertErr("begin tx", err)
	}
	db.activeTx.Add(1)
	tx := &transaction{
		writable: writable,
		db:       db,
		tx:       dbTx,
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
// func rollbackOnPanic(tx *transaction) {
// 	if err := recover(); err != nil {
// 		tx.managed = false
// 		_ = tx.Rollback()
// 		panic(err)
// 	}
// }

// View invokes the passed function in the context of a managed read-only
// transaction with the root bucket for the namespace.  Any errors returned from
// the user-supplied function are returned from this function.
//
// This function is part of the store.DB interface implementation.
func (db *db) View(fn func(store.Tx) error) error {
	// check for close and hold close lock
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// count active tx
	db.activeTx.Add(1)
	defer db.activeTx.Done()

	var err error
	dberr := db.store.View(func(tx *bolt.Tx) error {
		dbtx := &transaction{
			writable: false,
			db:       db,
			tx:       tx,
		}
		err = fn(dbtx)
		return nil
	})
	if dberr != nil {
		return convertErr("view tx", dberr)
	}
	return err
}

// Update invokes the passed function in the context of a managed read-write
// transaction with the root bucket for the namespace.  Any errors returned from
// the user-supplied function will cause the transaction to be rolled back and
// are returned from this function.  Otherwise, the transaction is committed
// when the user-supplied function returns a nil error.
//
// This function is part of the store.DB interface implementation.
func (db *db) Update(fn func(store.Tx) error) error {
	// check for close and hold close lock
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// count active tx
	db.activeTx.Add(1)
	defer db.activeTx.Done()

	var err error
	dberr := db.store.Update(func(tx *bolt.Tx) error {
		dbtx := &transaction{
			writable: true,
			db:       db,
			tx:       tx,
		}
		err = fn(dbtx)
		return nil
	})
	if dberr != nil {
		return convertErr("update tx", dberr)
	}
	return err
}

// Close cleanly shuts down the database and syncs all data.  It will block
// until all database transactions have been finalized (rolled back or
// committed).
//
// This function is part of the store.DB interface implementation.
func (db *db) Close() error {
	// Since all transactions have a read lock on this mutex, this will
	// cause Close to wait for all readers to complete.
	db.closeLock.Lock()
	defer db.closeLock.Unlock()

	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	return db.close()
}

func (db *db) close() error {
	mft, err := getManifest(db.store)
	if err != nil {
		return err
	}

	// NOTE: Since the above lock waits for all transactions to finish and
	// prevents any new ones from being started, it is safe to close the
	// underlying db here.
	if err := db.store.Close(); err != nil {
		return convertErr("close", err)
	}
	if mft.Name != "" {
		log.Debugf("%s database closed successfully.", mft.Name)
	} else {
		log.Debugf("Database closed successfully.")
	}
	db.closed = true
	db.store = nil
	db.opts = nil
	return nil
}

func (db *db) Sync() error {
	db.closeLock.Lock()
	defer db.closeLock.Unlock()

	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	return db.store.Sync()
}

// filesExists reports whether the named file or directory exists.
func fileExists(name string) (bool, error) {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// existsDB checks whether a database file exists at path. it does not
// check whether the file is locked or valid.
func existsDB(dbPath string) (bool, error) {
	return fileExists(dbPath)
}

// initDB creates the initial buckets and values used by the package.  This is
// mainly in a separate function for testing purposes.
func initDB(db *bolt.DB) error {
	// init manifest
	now := time.Now().UTC()
	mft := store.Manifest{
		CreatedAt: now,
	}
	buf, err := json.Marshal(mft)
	if err != nil {
		return err
	}

	// init sequences bucket
	err = db.Update(func(dbTx *bolt.Tx) error {
		mftBucket, err := dbTx.CreateBucketIfNotExists(manifestBucketKeyName)
		if err != nil {
			return err
		}
		return mftBucket.Put(manifestKey, buf)
	})
	if err != nil {
		return convertErr("init db", err)
	}
	return nil
}

// openDB opens the database at the provided path.  store.ErrDbDoesNotExist
// is returned if the database doesn't exist and the create flag is not set.
func openDB(dbPath string, opts *bolt.Options, create bool) (store.DB, error) {
	dbExists, err := fileExists(dbPath)
	if err != nil {
		return nil, convertErr("exist db", err)
	}

	if !create && !dbExists {
		str := fmt.Sprintf("database file %q does not exist", dbPath)
		return nil, makeDbErr(store.ErrDbDoesNotExist, str, nil)
	}

	if create && dbExists {
		str := fmt.Sprintf("database file %q exists", dbPath)
		return nil, makeDbErr(store.ErrDbExists, str, nil)
	}

	// Ensure the full path to the database exists.
	if !dbExists {
		// The error can be ignored here since the call to
		// bolt.Open will fail if the directory couldn't be
		// created.
		_ = os.MkdirAll(filepath.Dir(dbPath), 0700)
	}

	// bolt will create any non-existing database file automatically
	bdb, err := bolt.Open(dbPath, 0600, opts)
	if err != nil {
		return nil, convertErr("open", err)
	}
	if create && !dbExists {
		log.Debug("Initializing database...")
		if err := initDB(bdb); err != nil {
			bdb.Close()
			return nil, convertErr("init db", err)
		}
	} else {
		// read manifest
		mft, err := getManifest(bdb)
		if err != nil {
			bdb.Close()
			return nil, err
		}
		if mft.Name != "" {
			log.Debugf("%s database opened successfully.", mft.Name)
		} else {
			log.Debug("Database opened successfully.")
		}
	}
	return &db{store: bdb, dbPath: dbPath, opts: opts}, nil
}

// Database maintenance functions

// Export all database contents as protobuf data.
func (db *db) Dump(w io.Writer) error {
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// backup may run in parallel to any tx and will be using a snapshot copy
	err := db.store.View(func(dbTx *bolt.Tx) error {
		log.Debugf("Exporting database of size %s (this may take a while)...",
			util.ByteSize(dbTx.Size()).String())
		n, err := dbTx.WriteTo(w)
		if err != nil {
			return err
		}
		log.Debugf("Successfully wrote %s of data.", util.ByteSize(n).String())
		return nil
	})
	if err != nil {
		return convertErr("dump db", err)
	}
	return nil
}

// Should be called on a database that is not running any other
// concurrent transactions while it is running.
func (db *db) Restore(r io.Reader) error {
	// not implemented; to do so implement
	// - close bolt db (waiting for any open tx)
	// - restore/overwrite file with reader contents
	// - open bolt db from restored file
	return nil
}

// Should be called on a database that is not running any concurrent tx.
//
// Garbage collect database. This will create a new file, stream all keys into
// that file, replace the existing DB with the new file and reopen the DB.
func (db *db) GC(ctx context.Context, ratio float64) error {
	// hold close lock
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// make wait interruptable
	run := make(chan struct{})
	go func() {
		// wait for tx to finish
		db.activeTx.Wait()
		run <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-run:
	}

	// prevent parallel GC
	db.activeTx.Add(1)
	defer db.activeTx.Done()

	// init
	start := time.Now()
	srcPath := db.dbPath
	dstPath := db.dbPath + ".temp"
	fi, err := os.Stat(srcPath)
	if err != nil {
		return convertErr("cannot stat source db", err)
	}
	initialSize := fi.Size()

	// Open destination database.
	dstOpts := *db.opts
	dstOpts.ReadOnly = false
	dst, err := bolt.Open(dstPath, fi.Mode(), &dstOpts)
	if err != nil {
		return convertErr("cannot open compaction db", err)
	}

	defer func(dst *bolt.DB, dstPath string) {
		if err != nil {
			dst.Close()
			os.Remove(dstPath)
		}
	}(dst, dstPath)

	// Run compaction.
	log.Infof("[GC] Compacting database %s (%s).", db.dbPath, util.ByteSize(initialSize))
	if err = compact(ctx, dst, db.store, compactTxSize, ratio); err != nil {
		return convertErr("compact db", err)
	}

	// Report stats on new size.
	fi, err = os.Stat(dstPath)
	if err != nil {
		return convertErr("cannot stat destination db", err)
	} else if fi.Size() == 0 {
		err = fmt.Errorf("zero size after compaction")
		return convertErr("compact db", err)
	}
	log.Infof("[GC] Database %s successfully compacted %s -> %s (gain=%.2fx) in %s.",
		db.dbPath,
		util.ByteSize(initialSize),
		util.ByteSize(fi.Size()),
		float64(initialSize)/float64(fi.Size()),
		time.Since(start))

	// replace db - point of no return
	// also, don't overwrite err to avoid triggering defer
	if err := dst.Close(); err != nil {
		return convertErr("close after compact", err)
	}
	if err := db.store.Close(); err != nil {
		return convertErr("close after compact", err)
	}
	db.closed = true
	if err := os.Rename(srcPath, srcPath+".backup"); err != nil {
		return convertErr("rename source db", err)
	}
	if err := os.Rename(dstPath, srcPath); err != nil {
		return convertErr("rename compacted db", err)
	}
	db.store, err = bolt.Open(srcPath, 0600, db.opts)
	if err != nil {
		return convertErr("open compacted db", err)
	}
	log.Debugf("[GC] Database %s reopened successfully.", db.dbPath)
	db.closed = false

	// when all is good, remove the old database, ignoring errors
	os.Remove(srcPath + ".backup")
	log.Info("[GC] Using compacted database from now.")
	return nil
}

func compact(ctx context.Context, dst, src *bolt.DB, txMaxSize int64, fillPercent float64) error {
	// commit regularly, or we'll run out of memory for large datasets if using one transaction.
	var size int64
	tx, err := dst.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := walk(src, func(keys [][]byte, k, v []byte, seq uint64) error {
		// On each key/value, check if we have exceeded tx size.
		sz := int64(len(k) + len(v))
		if size+sz > txMaxSize && txMaxSize != 0 {
			// Commit previous transaction.
			if err := tx.Commit(); err != nil {
				return err
			}

			// check context
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Start new transaction.
			tx, err = dst.Begin(true)
			if err != nil {
				return err
			}
			size = 0
		}
		size += sz

		// Create bucket on the root transaction if this is the first level.
		nk := len(keys)
		if nk == 0 {
			bkt, err := tx.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Create buckets on subsequent levels, if necessary.
		b := tx.Bucket(keys[0])
		if nk > 1 {
			for _, k := range keys[1:] {
				b = b.Bucket(k)
			}
		}

		// Fill the entire page for best compaction.
		b.FillPercent = fillPercent

		// If there is no value then this is a bucket call.
		if v == nil {
			bkt, err := b.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Otherwise treat it as a key/value pair.
		return b.Put(k, v)
	}); err != nil {
		return err
	}

	return tx.Commit()
}

// walkFunc is the type of the function called for keys (buckets and "normal"
// values) discovered by Walk. keys is the list of keys to descend to the bucket
// owning the discovered key/value pair k/v.
type walkFunc func(keys [][]byte, k, v []byte, seq uint64) error

// walk walks recursively the bolt database db, calling walkFn for each key it finds.
func walk(db *bolt.DB, walkFn walkFunc) error {
	return db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return walkBucket(b, nil, name, nil, b.Sequence(), walkFn)
		})
	})
}

func walkBucket(b *bolt.Bucket, keypath [][]byte, k, v []byte, seq uint64, fn walkFunc) error {
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
			return walkBucket(bkt, keypath, k, nil, bkt.Sequence(), fn)
		}
		return walkBucket(b, keypath, k, v, b.Sequence(), fn)
	})
}
