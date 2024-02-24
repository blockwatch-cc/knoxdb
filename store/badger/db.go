// Copyright (c) 2018 - 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package badger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"blockwatch.cc/knoxdb/store"
	"blockwatch.cc/knoxdb/util"
	"github.com/dgraph-io/badger/v4"
)

// db wraps a Badger instance and implements the store.DB interface.
// All database access is performed through transactions which are managed.
type db struct {
	seqLock   sync.RWMutex         // Guard access to sequences.
	closeLock sync.RWMutex         // Make database close block while txns active.
	txLock    sync.Mutex           // block creating new tx during backup/restore.
	activeTx  sync.WaitGroup       // count active tx (needed for backup/restore).
	closed    bool                 // Is the database closed?
	store     *badger.DB           // the database
	sequences map[string]*sequence // map of open sequences
	dbPath    string               // storage path
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
	return db.store.Opts().ReadOnly
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
	return putManifest(db.store, mft)
}

func getManifest(bdb *badger.DB) (store.Manifest, error) {
	var mft store.Manifest
	err := bdb.View(func(dbTx *badger.Txn) error {
		mftKey := bucketizedKey(metadataBucketID, manifestKey)
		item, err := dbTx.Get(mftKey)
		if err != nil {
			return err
		}
		if buf, _ := item.ValueCopy(nil); buf != nil {
			return json.Unmarshal(buf, &mft)
		}
		return nil
	})
	if err != nil {
		return mft, convertErr("get manifest", err)
	}
	return mft, nil
}

func putManifest(bdb *badger.DB, manifest store.Manifest) error {
	buf, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	err = bdb.Update(func(dbTx *badger.Txn) error {
		mftKey := bucketizedKey(metadataBucketID, manifestKey)
		return dbTx.Set(mftKey, buf)
	})
	if err != nil {
		return convertErr("put manifest", err)
	}
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
	seqKey := bucketizedKey(sequenceBucketID, key)
	dbseq, err := db.store.GetSequence(seqKey, lease)
	if err != nil {
		return nil, err
	}
	log.Tracef("Opening sequence %s", string(key))
	seq := &sequence{
		key: copySlice(key),
		db:  db,
		seq: dbseq,
	}
	db.seqLock.Lock()
	db.sequences[string(key)] = seq
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
	// Whenever a new transaction is started, grab a read lock against the
	// database to ensure Close will wait for the transaction to finish.
	// This lock will not be released until the transaction is closed (via
	// Rollback or Commit).
	db.closeLock.RLock()
	if db.closed {
		db.closeLock.RUnlock()
		return nil, makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// The metadata and block index buckets are internal-only buckets, so
	// they have defined IDs.
	db.txLock.Lock()
	defer db.txLock.Unlock()
	db.activeTx.Add(1)
	tx := &transaction{
		writable: writable,
		db:       db,
		tx:       db.store.NewTransaction(writable),
	}
	tx.metaBucket = &bucket{tx: tx, id: metadataBucketID}
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
	if err := recover(); err != nil {
		tx.managed = false
		_ = tx.Rollback()
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
	db.closeLock.Lock()
	defer db.closeLock.Unlock()

	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// write manifest
	mft, err := getManifest(db.store)
	if err != nil {
		return err
	}
	if !db.store.Opts().ReadOnly {
		if err := putManifest(db.store, mft); err != nil {
			return err
		}
	}
	log.Infof("Closing %s database (this may take a while)...", strings.Title(mft.Name))

	// close all sequences
	for _, v := range db.sequences {
		// use internal release function that does not alter the db map
		v.release()
	}
	db.sequences = make(map[string]*sequence)

	// NOTE: Since the above lock waits for all transactions to finish and
	// prevents any new ones from being started, it is safe to close the
	// underlying db here.
	if err := db.store.Close(); err != nil {
		return convertErr("close", err)
	}
	if mft.Name != "" {
		log.Infof("%s database closed successfully.", strings.Title(mft.Name))
	} else {
		log.Debugf("Database closed successfully.")
	}
	db.closed = true
	db.store = nil
	return nil
}

// filesExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// initDB creates the initial buckets and values used by the package.  This is
// mainly in a separate function for testing purposes.
func initDB(db *badger.DB) error {
	// init manifest
	now := time.Now().UTC()
	mft := store.Manifest{
		CreatedAt: now,
	}
	if err := putManifest(db, mft); err != nil {
		return err
	}

	// init sequence
	seqKey := bucketizedKey(sequenceBucketID, curBucketIDKeyName)
	seq, err := db.GetSequence(seqKey, 2)
	if err != nil {
		return err
	}
	// reserve the first ID (0) for top-level metadata bucket
	seq.Next()

	// reserve the second ID (1) for top-level sequences bucket
	seq.Next()

	// release the sequence
	seq.Release()

	return nil
}

// openDB opens the database at the provided path.  store.ErrDbDoesNotExist
// is returned if the database doesn't exist and the create flag is not set.
func openDB(dbPath, dbValuePath string, create bool) (store.DB, error) {
	if dbValuePath != "" {
		dbValuePath = dbPath
	}
	dbExists := fileExists(dbPath)
	dbValueExists := fileExists(dbValuePath)

	if !create && !dbExists {
		str := fmt.Sprintf("database %q does not exist", dbPath)
		return nil, makeDbErr(store.ErrDbDoesNotExist, str, nil)
	}

	if !create && !dbValueExists {
		str := fmt.Sprintf("database value store %q does not exist", dbValuePath)
		return nil, makeDbErr(store.ErrDbDoesNotExist, str, nil)
	}

	// Ensure the full path to the database exists.
	if !dbExists {
		// The error can be ignored here since the call to
		// badger.Open will fail if the directory couldn't be
		// created.
		_ = os.MkdirAll(dbPath, 0700)
	}

	// Ensure the full path to the database value log files exists.
	if !dbValueExists {
		// The error can be ignored here since the call to
		// badger.Open will fail if the directory couldn't be
		// created.
		_ = os.MkdirAll(dbValuePath, 0700)
	}

	// Open the metadata database (will create it if needed). Use Badger
	// default options.
	// MemTableSize:        64 << 20,
	// BaseTableSize:       2 << 20,
	// BaseLevelSize:       10 << 20,
	// TableSizeMultiplier: 2,
	// LevelSizeMultiplier: 10,
	// MaxLevels:           7,
	// NumGoroutines:       8,
	// MetricsEnabled:      true,
	// NumCompactors:           4, // at least 2 compactors.
	// NumLevelZeroTables:      5,
	// NumLevelZeroTablesStall: 15,
	// NumMemtables:            5,
	// BloomFalsePositive:      0.01,
	// BlockSize:               4 * 1024,
	// SyncWrites:              false,
	// NumVersionsToKeep:       1,
	// CompactL0OnClose:        false,
	// VerifyValueChecksum:     false,
	// Compression:             options.Snappy,
	// BlockCacheSize:          256 << 20,
	// IndexCacheSize:          0,
	// ZSTDCompressionLevel: 1,
	// ValueLogFileSize: 1<<30 - 1,
	// ValueLogMaxEntries: 1000000,
	// VLogPercentile: 0.0,
	// ValueThreshold: maxValueThreshold,
	// Logger:                        defaultLogger(INFO),
	// EncryptionKey:                 []byte{},
	// EncryptionKeyRotationDuration: 10 * 24 * time.Hour, // Default 10 days.
	// DetectConflicts:               true,
	// NamespaceOffset:               -1,
	opts := badger.DefaultOptions(dbPath).
		WithDetectConflicts(false)
	// WithLogger(log.Log) // TODO: add WarningF(string, ...any) func

	log.Info("Opening database (this may take a while)...")
	bdb, err := badger.Open(opts)
	if err != nil {
		return nil, convertErr("open db", err)
	}
	if create && !dbExists {
		log.Info("Initializing store...")
		if err := initDB(bdb); err != nil {
			bdb.Close()
			return nil, convertErr("init db", err)
		}
	} else {
		// update manifest
		mft, err := getManifest(bdb)
		if err != nil {
			bdb.Close()
			return nil, err
		}
		if err := putManifest(bdb, mft); err != nil {
			bdb.Close()
			return nil, err
		}
		if mft.Name != "" {
			log.Infof("%s database opened successfully.", strings.Title(mft.Name))
		} else {
			log.Info("Database opened successfully.")
		}
	}
	return &db{store: bdb, dbPath: dbPath, sequences: make(map[string]*sequence)}, nil
}

// Database maintenance functions

// Export all database contents as protobuf data.
func (db *db) Dump(w io.Writer) error {
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// let new transactions wait until backup is finished
	db.txLock.Lock()
	defer db.txLock.Unlock()

	// wait for all active transactions to finish
	db.activeTx.Wait()

	// dump full db contents to writer
	_, err := db.store.Backup(w, 0)
	return err
}

// Should be called on a database that is not running any other
// concurrent transactions while it is running.
func (db *db) Restore(r io.Reader) error {
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// let new transactions wait until backup is finished
	db.txLock.Lock()
	defer db.txLock.Unlock()

	// wait for all active transactions to finish
	db.activeTx.Wait()

	// load db data from reader
	return db.store.Load(r, 0)
}

// Garbage collect old versions of keys and compress value log.
func (db *db) GC(ctx context.Context, ratio float64) error {
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// let new transactions wait until backup is finished
	db.txLock.Lock()
	defer db.txLock.Unlock()

	// make wait interruptable
	run := make(chan struct{})
	go func() {
		// wait for all active transactions to finish
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

	// first, purge old versions of keys
	start := time.Now()
	lsm, vlog := db.store.Size()
	log.Info("[GC] Flatting sstables.")
	if err := db.store.Flatten(8); err != nil {
		return err
	}
	defer func() {
		lsm2, vlog2 := db.store.Size()
		diff := (lsm - lsm2) + (vlog - vlog2)
		if diff > 0 {
			log.Infof("[GC] Reclaimed %s in %s",
				util.ByteSize(diff),
				time.Since(start))
		} else {
			log.Infof("[GC] No compaction possible. Database grew by %s in %s",
				util.ByteSize(-diff),
				time.Since(start))

		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// then, compact value log (set discardRatio to 0.5, thus
	// indicating that a file be rewritten if half the space can be discarded)
	log.Info("[GC] Compacting value log.")
	if err := db.store.RunValueLogGC(ratio); err != nil && err != badger.ErrNoRewrite {
		return err
	}
	return nil
}
