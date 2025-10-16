// Copyright (c) 2018-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"fmt"

	"blockwatch.cc/knoxdb/pkg/store"
	bolt "go.etcd.io/bbolt"
)

// transaction represents a database transaction.  It can either be read-only or
// read-write and implements the store.Bucket interface.  The transaction
// provides a root bucket against which all read and writes occur.
type transaction struct {
	db      *db      // DB instance the tx was created from.
	tx      *bolt.Tx // the DB transaction
	managed bool     // Is the transaction managed by this driver?
}

// Enforce transaction implements the store.Tx interface.
var _ store.Tx = (*transaction)(nil)

func (tx *transaction) IsWriteable() bool {
	return tx.tx.Writable()
}

// Root returns the top-most bucket for all metadata storage.
func (tx *transaction) Root() store.Bucket {
	return &bucket{tx: tx, bucket: nil}
}

// Root returns the bucket with given name.
func (tx *transaction) Bucket(key []byte) store.Bucket {
	b := tx.tx.Bucket(key)
	if b == nil {
		return nil
	}
	b.FillPercent = tx.db.opts.PageFill
	return &bucket{tx: tx, bucket: b}
}

// close marks the transaction closed then releases any pending data.
func (tx *transaction) close(doRollback bool) {
	if doRollback {
		tx.tx.Rollback()
	}
	tx.db = nil
	tx.tx = nil
	tx.managed = false
}

// Commit commits all changes that have been made to the root metadata bucket
// and all of its sub-buckets to the database cache which is periodically synced
// to persistent storage.  In addition, it commits all new blocks directly to
// persistent storage bypassing the db cache.  Blocks can be rather large, so
// this help increase the amount of cache available for the metadata updates and
// is safe since blocks are immutable.
//
// This function is part of the store.Tx interface implementation.
func (tx *transaction) Commit() error {
	// Prevent commits on managed transactions.
	if tx.managed {
		return fmt.Errorf("managed transaction commit not allowed")
	}

	// Ensure the transaction is writable.
	if !tx.tx.Writable() {
		tx.close(true)
		return store.ErrTxReadonly
	}

	// Write pending data.
	err := tx.tx.Commit()
	if err != nil {
		tx.close(true)
	}

	return wrap(err)
}

// Rollback discards all changes made during the transaction.
func (tx *transaction) Rollback() error {
	// Prevent rollbacks on managed transactions.
	if tx.managed {
		return fmt.Errorf("managed transaction rollback not allowed")
	}

	err := tx.tx.Rollback()
	tx.close(false)
	return wrap(err)
}

func (tx *transaction) DB() store.DB {
	return tx.db
}
