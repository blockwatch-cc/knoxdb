// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"encoding/json"

	"blockwatch.cc/knoxdb/store"
	bolt "go.etcd.io/bbolt"
)

// transaction represents a database transaction.  It can either be read-only or
// read-write and implements the store.Bucket interface.  The transaction
// provides a root bucket against which all read and writes occur.
type transaction struct {
	managed  bool     // Is the transaction managed by this driver?
	closed   bool     // Is the transaction closed?
	writable bool     // Is the transaction writable?
	db       *db      // DB instance the tx was created from.
	tx       *bolt.Tx // the DB transaction
}

// Enforce transaction implements the store.Tx interface.
var _ store.Tx = (*transaction)(nil)

// checkClosed returns an error if the the database or transaction is closed.
func (tx *transaction) checkClosed() error {
	// The transaction is no longer valid if it has been closed.
	if tx.closed {
		return makeDbErr(store.ErrTxClosed, errTxClosedStr, nil)
	}
	return nil
}

// Root returns the top-most bucket for all metadata storage.
//
// This function is part of the store.Tx interface implementation.
func (tx *transaction) Root() store.Bucket {
	return &bucket{tx: tx, bucket: nil}
}

// Root returns the bucket with given name.
func (tx *transaction) Bucket(key []byte) store.Bucket {
	b := tx.tx.Bucket(key)
	if b == nil {
		return nil
	}
	return &bucket{tx: tx, bucket: b}
}

// Manifest returns the current database manifest metadata.
//
// This function is part of the store.DB interface implementation.
func (tx *transaction) Manifest() (store.Manifest, error) {
	var mft store.Manifest
	if err := tx.checkClosed(); err != nil {
		return mft, err
	}
	buf := tx.tx.Bucket(manifestBucketKeyName).Get(manifestKey)
	if buf != nil {
		if err := json.Unmarshal(buf, &mft); err != nil {
			return mft, err
		}
	}
	return mft, nil
}

// SetManifest overwrites the current database manifest.
//
// This function is part of the store.DB interface implementation.
func (tx *transaction) SetManifest(manifest store.Manifest) error {
	if err := tx.checkClosed(); err != nil {
		return err
	}
	mft, err := tx.Manifest()
	if err != nil {
		return err
	}
	// we only allow some fields to be overwritten
	mft.Name = manifest.Name
	mft.Version = manifest.Version
	mft.Label = manifest.Label
	mft.Schema = manifest.Schema
	buf, err := json.Marshal(mft)
	if err != nil {
		return err
	}
	if err := tx.tx.Bucket(manifestBucketKeyName).Put(manifestKey, buf); err != nil {
		return convertErr("set manifest", err)
	}
	return nil
}

// close marks the transaction closed then releases any pending data.
func (tx *transaction) close() {
	if !tx.writable {
		tx.tx.Rollback()
	}
	tx.closed = true
	tx.db.activeTx.Done()
	tx.db.closeLock.RUnlock()
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
		tx.close()
		panic("managed transaction commit not allowed")
	}

	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return err
	}

	// Regardless of whether the commit succeeds, the transaction is closed
	// on return.
	defer tx.close()

	// Ensure the transaction is writable.
	if !tx.writable {
		str := "Commit requires a writable database transaction"
		return makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Write pending data.  The function will rollback if any errors occur.
	if err := tx.tx.Commit(); err != nil {
		return convertErr("commit tx", err)
	}
	return nil
}

// Rollback undoes all changes that have been made to the root bucket and all of
// its sub-buckets.
//
// This function is part of the store.Tx interface implementation.
func (tx *transaction) Rollback() error {
	// Prevent rollbacks on managed transactions.
	if tx.managed {
		tx.close()
		panic("managed transaction rollback not allowed")
	}

	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return err
	}

	if err := tx.tx.Rollback(); err != nil {
		return convertErr("rollback tx", err)
	}
	tx.close()
	return nil
}

func (tx *transaction) DB() store.DB {
	return tx.db
}
