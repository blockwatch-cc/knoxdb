// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package mem

import (
	"iter"
	"strings"
	"sync"

	"blockwatch.cc/knoxdb/pkg/store"
	"github.com/RaduBerinde/btreemap"
)

var txPool = sync.Pool{}

type TxFlags int

const (
	TxFlagReadOnly  TxFlags = 0         // 0 read-only tx
	TxFlagReadWrite TxFlags = 1 << iota // 1 read-write tx
	TxFlagNoWait                        // 2 don't block on mutex
	TxFlagManaged                       // 4 tx is managed
	TxFlagClosed                        // 8 tx is closed
)

func NewTxFlags(o store.TxOptions) TxFlags {
	var f TxFlags
	if o.Writable {
		f |= TxFlagReadWrite
	}
	if o.NoWait {
		f |= TxFlagNoWait
	}
	return f
}

// tx represents a database transaction. It is either read-only or
// read-write. The transaction tracks all changes in a pair of btrees
// which are merged with the main btree store on commit.
type tx struct {
	db    *db                                // DB instance the tx was created from.
	snap  *btreemap.BTreeMap[[]byte, []byte] // cow or read only snapshot
	flags TxFlags                            // flags control tx features and lifecycle
}

// Ensure tx implements the store.Tx interface.
var _ store.Tx = (*tx)(nil)

func (tx *tx) DB() store.DB {
	return tx.db
}

func (tx *tx) IsWriteable() bool {
	return tx.flags&TxFlagReadWrite > 0
}

func (tx *tx) IsClosed() bool {
	return tx.flags&TxFlagClosed > 0
}

func (tx *tx) IsManaged() bool {
	return tx.flags&TxFlagManaged > 0
}

// Bucket returns the bucket with given name.
func (tx *tx) Bucket(key []byte) (store.Bucket, error) {
	// Ensure transaction state is valid.
	if tx.IsClosed() {
		return nil, store.ErrTxClosed
	}
	id, ok := tx.db.buckets[string(bucketizedKey(0, key))]
	if !ok {
		return nil, store.ErrBucketNotFound
	}
	return &bucket{tx: tx, id: id}, nil
}

// BucketPath returns a top-level or nested bucket with a given path
// or nil and ErrBucketNotFound if a bucket does not exits along the path.
func (tx *tx) BucketPath(path ...[]byte) (store.Bucket, error) {
	// Ensure transaction state is valid.
	if tx.IsClosed() {
		return nil, store.ErrTxClosed
	}

	// Ensure path
	if len(path) == 0 {
		return nil, store.ErrIncompatibleValue
	}

	// find nested bucket
	var (
		id uint32
		ok bool
	)
	for _, key := range path {
		id, ok = tx.db.buckets[string(bucketizedKey(id, key))]
		if !ok {
			return nil, store.ErrBucketNotFound
		}
	}
	return &bucket{id: id, tx: tx}, nil
}

// Buckets returns an iterator for top-level buckets.
func (tx *tx) Buckets() iter.Seq2[[]byte, store.Bucket] {
	return func(yield func([]byte, store.Bucket) bool) {
		if tx.IsClosed() {
			return
		}
		// walk direct nested buckets
		prefix := store.UnsafeString([]byte{0})
		for n, id := range tx.db.buckets {
			if !strings.HasPrefix(n, prefix) {
				continue
			}
			if !yield([]byte(n[1:]), &bucket{tx: tx, id: id}) {
				return
			}
		}
	}
}

// CreateBucket creates and returns a new top-level bucket with the given key.
// If the bucket already exists it is returned without error.
func (tx *tx) CreateBucket(key []byte, _ ...store.BucketOption) (store.Bucket, error) {
	root := bucket{tx: tx}
	return root.CreateBucket(key)
}

// DeleteBucket removes a top-level bucket with the given key including
// all nested buckets and keys.
func (tx *tx) DeleteBucket(key []byte) error {
	root := bucket{tx: tx}
	return root.DeleteBucket(key)
}

// Commit commits all changes that have been made to different buckets
// and to in-memory btree.
func (tx *tx) Commit() error {
	// Ensure transaction state is valid.
	if tx.IsClosed() {
		return store.ErrTxClosed
	}

	// Prevent commits on managed transactions.
	if tx.IsManaged() {
		return store.ErrTxManaged
	}

	// Ensure the transaction is writable.
	if !tx.IsWriteable() {
		return tx.Rollback()
	}

	// promote snapshot
	tx.db.store = tx.snap
	tx.snap = nil
	tx.flags = TxFlagClosed
	tx.db.mu.Unlock()
	tx.db.wg.Done()
	tx.db = nil
	txPool.Put(tx)

	return nil
}

// Rollback undoes all changes that have been made in this transaction.
// It simply clears pending changes
func (tx *tx) Rollback() (err error) {
	// Ensure transaction state is valid.
	if tx.IsClosed() {
		return store.ErrTxClosed
	}

	// Prevent rollbacks on managed transactions.
	if tx.IsManaged() {
		err = store.ErrTxManaged
	}

	// Release write lock when the transaction is writable.
	if tx.IsWriteable() {
		tx.db.mu.Unlock()
	}

	// cleanup
	tx.snap = nil
	tx.flags = TxFlagClosed
	tx.db.wg.Done()
	tx.db = nil
	txPool.Put(tx)

	return err
}

func (tx *tx) get(key []byte) ([]byte, error) {
	// lookup in btree snapshot
	_, val, ok := tx.snap.Get(key)
	if !ok {
		return nil, store.ErrKeyNotFound
	}
	return val, nil
}

func (tx *tx) put(key, value []byte) {
	tx.snap.ReplaceOrInsert(key, value)
}

func (tx *tx) del(key []byte) {
	tx.snap.Delete(key)
}
