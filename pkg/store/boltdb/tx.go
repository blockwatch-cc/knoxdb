// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"errors"
	"io"
	"iter"
	"sync"

	"blockwatch.cc/knoxdb/pkg/store"
	bolt "go.etcd.io/bbolt"
	bolterr "go.etcd.io/bbolt/errors"
)

var txPool = sync.Pool{}

// tx represents a read-only or read-write database transaction.
type tx struct {
	db      *db      // DB instance the tx was created from.
	tx      *bolt.Tx // the DB transaction
	managed bool     // true when managed
	sync    bool     // sync on commit
}

// Ensure transaction implements the store.Tx interface.
var _ store.Tx = (*tx)(nil)

func (tx *tx) IsWriteable() bool {
	return tx.tx.Writable()
}

func (tx *tx) IsClosed() bool {
	return tx.tx == nil
}

func (tx *tx) IsManaged() bool {
	return tx.managed
}

func (tx *tx) DB() store.DB {
	return tx.db
}

// Bucket returns a top-level bucket with given name or nil if no
// such bucket exists.
func (tx *tx) Bucket(key []byte) (store.Bucket, error) {
	// Ensure transaction state is valid.
	if tx.IsClosed() {
		return nil, store.ErrTxClosed
	}
	b := tx.tx.Bucket(key)
	if b == nil {
		return nil, store.ErrBucketNotFound
	}
	b.FillPercent = tx.db.opts.PageFill
	return &bucket{tx: tx, bucket: b}, nil
}

// CreateBucket creates and returns a new top-level bucket with the given key.
// If the bucket already exists it is returned without error.
func (tx *tx) CreateBucket(key []byte, _ ...store.BucketOption) (store.Bucket, error) {
	child, err := tx.tx.CreateBucket(key)
	if err != nil {
		// use bucket if exists
		if errors.Is(err, bolterr.ErrBucketExists) {
			return tx.Bucket(key)
		}
		return nil, wrap(err)
	}
	child.FillPercent = tx.db.opts.PageFill
	return &bucket{tx: tx, bucket: child}, nil
}

// DeleteBucket removes a top-level bucket with the given key including
// all nested buckets and keys.
func (tx *tx) DeleteBucket(key []byte) error {
	return wrap(tx.tx.DeleteBucket(key))
}

// Buckets returns an iterator for top-level buckets.
func (tx *tx) Buckets() iter.Seq2[[]byte, store.Bucket] {
	return func(yield func([]byte, store.Bucket) bool) {
		if tx.IsClosed() {
			return
		}
		tx.tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			b.FillPercent = tx.db.opts.PageFill
			if !yield(name, &bucket{tx: tx, bucket: b}) {
				return io.EOF
			}
			return nil
		})
	}
}

// Commit atomically commits all changes made by the transaction
// to the database.
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

	// Commit tx data.
	err := wrap(tx.tx.Commit())

	// sync when requested
	if err == nil && tx.sync {
		err = wrap(tx.db.Sync())
	}

	// free resources
	tx.db.wg.Done()
	tx.tx = nil
	tx.db = nil
	tx.managed = false
	tx.sync = false
	txPool.Put(tx)

	// return commit error, if any
	return err
}

// Rollback discards all changes made during the transaction.
func (tx *tx) Rollback() (err error) {
	// Ensure transaction state is valid.
	if tx.IsClosed() {
		return store.ErrTxClosed
	}

	// Prevent rollbacks on managed transactions.
	if tx.IsManaged() {
		return store.ErrTxManaged
	}

	// Clear pending changes when the transaction is writable.
	err = wrap(tx.tx.Rollback())

	// free resources
	tx.db.wg.Done()
	tx.tx = nil
	tx.db = nil
	tx.managed = false
	tx.sync = false
	txPool.Put(tx)

	// return rollback error, if nay
	return err
}
