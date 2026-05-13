// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package mem

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"sync"

	"blockwatch.cc/knoxdb/pkg/btree"
	"blockwatch.cc/knoxdb/pkg/store"
	"github.com/RaduBerinde/btreemap"
	"github.com/echa/log"
)

// db is a transient in-memory key-value database which implements
// the store.DB interface. It uses a single btree for storage, abstracts
// buckets using a sorted key prefix and keeps bucket names in an extra
// memory map for lookup.
type db struct {
	mu      sync.RWMutex                       // single writer lock
	wg      sync.WaitGroup                     // close wait
	opts    store.Options                      // config options
	store   *btreemap.BTreeMap[[]byte, []byte] // the database (btree)
	buckets map[string]uint32                  // bucket key to id map cache
	log     log.Logger                         // db logger instance
	closed  bool                               // Is the database closed?
}

// Ensure db implements the store.DB interface.
var _ store.DB = (*db)(nil)

func (*db) Type() string {
	return dbType
}

func (db *db) IsReadOnly() bool {
	return db.opts.Readonly
}

// IsZeroCopyRead returns true if keys and values on Get and from Iterators
// are only valid within the current transaction (or iterator step).
func (*db) IsZeroCopyRead() bool {
	return true
}

// Path returns the path where the current database is stored.
func (db *db) Path() string {
	return db.opts.Path
}

func (db *db) Begin(opts ...store.TxOption) (store.Tx, error) {
	return db.begin(opts...)
}

func (db *db) begin(options ...store.TxOption) (*tx, error) {
	// check db is not closed
	if db.closed {
		return nil, store.ErrDatabaseClosed
	}

	// apply options
	var opts store.TxOptions
	for _, o := range options {
		o(&opts)
	}

	// alloc new tx
	var t *tx
	if ix := txPool.Get(); ix != nil {
		t = ix.(*tx)
	} else {
		t = &tx{}
	}
	t.db = db
	t.flags = NewTxFlags(opts)

	// Whenever a new write transaction starts, we take a write lock against
	// the database. This lock will not be released until the transaction
	// is closed (via Rollback or Commit). This ensures write transactions
	// are exclusive, but multiple read transactions can run concurrently
	// using snapshots. Close waits on the waitgroup until all transactions
	// complete.
	if opts.Writable {
		// Enforce single writer property to make transactions serializable.
		// Without this global lock we would have to implement conflict
		// resolution on commit.
		if opts.NoWait {
			if !db.mu.TryLock() {
				db.wg.Done()
				return nil, store.ErrTxWouldBlock
			}
		} else {
			db.mu.Lock()
		}

		// refcount tx after obtaining lock
		db.wg.Add(1)

		// cross-check the db was not closed while waiting for the lock.
		if db.closed {
			db.mu.Unlock()
			db.wg.Done()
			return nil, store.ErrDatabaseClosed
		}

		// alloc change data
		t.pending = btree.NewChangeTree()

		// use the original database state as snapshot
		t.snap = db.store

	} else {
		// ensure we see a concurrent close
		db.mu.RLock()
		defer db.mu.RUnlock()

		// refcount tx after obtaining lock
		db.wg.Add(1)

		// cross-check the db is not closed
		if db.closed {
			db.wg.Done()
			return nil, store.ErrDatabaseClosed
		}

		// snapshot the database state
		t.snap = db.store.Clone()
	}

	return t, nil
}

// rollbackOnPanic rolls the passed transaction back if the code in the calling
// function panics. This is needed since the lock on a transaction must be
// released and a panic in called code would prevent that from happening.
//
// NOTE: This can only be handled manually for managed transactions since they
// control the life-cycle of the transaction. Callers using manual transactions
// must ensure the transaction is rolled back on panic as well. Otherwise the
// the database will deadlock on close.
func rollbackOnPanic(tx *tx) {
	// note: runtime.Goexit used in testing.Fail does not panic but
	// still unwinds all defered functions
	err := recover()

	tx.flags &^= TxFlagManaged
	_ = tx.Rollback()

	// re-panic
	if err != nil {
		tx.db.log.Error(err)
		panic(err)
	}
}

// View invokes the passed function in the context of a managed read-only
// transaction with the root bucket as namespace. Any errors returned from
// the user-supplied function are returned from this function.
func (db *db) View(fn func(store.Tx) error) error {
	// Start a read-only transaction.
	tx, err := db.begin()
	if err != nil {
		return err
	}

	// Since the user-provided function might panic, ensure the transaction
	// releases all mutexes and resources.  There is no guarantee the caller
	// won't use recover and keep going.  Thus, the database must still be
	// in a usable state on panics due to caller issues.
	defer rollbackOnPanic(tx)

	tx.flags |= TxFlagManaged
	err = fn(tx)
	tx.flags &^= TxFlagManaged
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
// transaction with the root bucket as namespace. Any errors returned from
// the user-supplied function will cause the transaction to be rolled back and
// are returned from this function. Otherwise, the transaction is committed
// when the user-supplied function returns a nil error.
func (db *db) Update(fn func(store.Tx) error) error {
	// Start a read-write transaction.
	tx, err := db.begin(store.WithTxWrite())
	if err != nil {
		return err
	}

	// Since the user-provided function might panic, ensure the transaction
	// releases all mutexes and resources.  There is no guarantee the caller
	// won't use recover and keep going.  Thus, the database must still be
	// in a usable state on panics due to caller issues.
	defer rollbackOnPanic(tx)

	tx.flags |= TxFlagManaged
	err = fn(tx)
	tx.flags &^= TxFlagManaged
	if err != nil {
		// The error is ignored here because nothing was written yet
		// and regardless of a rollback failure, the tx is closed now
		// anyways.
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// Close cleanly shuts down the database. It blocks until all transactions
// have been finalized (rolled back or committed).
func (db *db) Close() error {
	// Wait for an existing writer and block new writer from starting.
	db.mu.Lock()
	defer db.mu.Unlock()

	// check for concurrent close
	if db.closed {
		return store.ErrDatabaseClosed
	}

	// wait for all tx to complete
	db.wg.Wait()

	// release resources
	db.close()
	return nil
}

func (db *db) close() {
	// don't clear when persist flag is set
	if db.opts.KeepOnClose {
		db.closed = true
		return
	}

	// NOTE: Since the close lock waits for all transactions to finish and
	// prevents any new ones from being started, it is safe to clear the
	// underlying btree here.
	registry.Delete(db.opts.Path)

	// clear resources and drop references
	db.store.Clear(true)
	db.closed = true
	db.store = nil
	clear(db.buckets)
	db.buckets = nil
}

// Sync is a noop for this in-memory database driver.
func (db *db) Sync() error {
	return nil
}

// Snapshot exports all database contents for backup.
func (db *db) Snapshot(w io.Writer) (err error) {
	if db.closed {
		return store.ErrDatabaseClosed
	}

	// refcount
	db.wg.Add(1)
	defer db.wg.Done()

	// use a read only snapshot
	wr := bufio.NewWriter(w)
	db.store.Clone().AscendFunc(
		btreemap.Min[[]byte](),
		btreemap.Max[[]byte](),
		func(key []byte, val []byte) bool {
			// write key and val length as varint
			var v [binary.MaxVarintLen32]byte
			if _, err = wr.Write(v[:binary.PutUvarint(v[:], uint64(len(key)))]); err != nil {
				return false
			}
			if _, err = wr.Write(v[:binary.PutUvarint(v[:], uint64(len(val)))]); err != nil {
				return false
			}

			// write key and value
			if _, err = wr.Write(key); err != nil {
				return false
			}
			if _, err = wr.Write(val); err != nil {
				return false
			}
			return true
		})
	return wr.Flush()
}

// Restore imports an earlier snapshot. It must be called on a pristine
// database (with no content) and no concurrent transactions.
func (db *db) Restore(r io.Reader) (err error) {
	// need a write lock
	db.mu.Lock()
	defer db.mu.Unlock()

	// refcount
	db.wg.Add(1)
	defer db.wg.Done()

	// fail when closed
	if db.closed {
		return store.ErrDatabaseClosed
	}

	// fail when data exists
	if db.store.Len() > 0 {
		return store.ErrDatabaseNotEmpty
	}

	// read back from stream
	rd := bufio.NewReader(r)
	for {
		// read key and value len
		var klen, vlen uint64
		if klen, err = binary.ReadUvarint(rd); err != nil {
			break
		}
		if klen == 0 {
			err = store.ErrKeyRequired
			break
		}
		if vlen, err = binary.ReadUvarint(rd); err != nil {
			break
		}

		// read key and value
		var n int
		key := make([]byte, klen)
		if n, err = rd.Read(key); err != nil {
			break
		} else if n != int(klen) {
			err = io.ErrShortBuffer
			break
		}
		val := make([]byte, vlen)
		if n, err = rd.Read(val); err != nil {
			break
		} else if n != int(vlen) {
			err = io.ErrShortBuffer
			break
		}

		// insert into btree
		db.store.ReplaceOrInsert(key, val)
	}

	// clear inconsistent tree on read error, EOF is expected
	if err != nil {
		if !errors.Is(err, io.EOF) {
			db.store.Clear(true)
		} else {
			err = nil
		}
	}
	return
}

func (db *db) Scan(prefix []byte) iter.Seq2[[]byte, []byte] {
	return btree.Scan(db.store, prefix)
}

func (db *db) ScanReverse(prefix []byte) iter.Seq2[[]byte, []byte] {
	return btree.ScanReverse(db.store, prefix)
}

func (db *db) ScanRange(start, end []byte) iter.Seq2[[]byte, []byte] {
	return btree.ScanRange(db.store, start, end)
}

func (db *db) ScanRangeReverse(start, end []byte) iter.Seq2[[]byte, []byte] {
	return btree.ScanRangeReverse(db.store, start, end)
}
