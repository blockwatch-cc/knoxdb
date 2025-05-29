// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package mem

import (
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/store"
)

// transaction represents a database transaction.  It can either be read-only or
// read-write and implements the store.Bucket interface.  The transaction
// provides a root bucket against which all read and writes occur.
type transaction struct {
	managed  bool // Is the transaction managed by this driver?
	closed   bool // Is the transaction closed?
	writable bool // Is the transaction writable?
	db       *db  // DB instance the tx was created from.
	updates  map[string][]byte
	deletes  map[string]struct{}
}

// Enforce transaction implements the store.Tx interface.
var _ store.Tx = (*transaction)(nil)

// checkClosed returns an error if the the database or transaction is closed.
func (tx *transaction) checkClosed() error {
	// The transaction is no longer valid if it has been closed.
	if tx.closed {
		return makeDbErr(store.ErrTxClosed, errTxClosedStr)
	}
	return nil
}

// checkWriteable returns an error if the the database or transaction is closed.
func (tx *transaction) checkWriteable() error {
	if !tx.writable {
		return makeDbErr(store.ErrTxNotWritable, "tx is not writeable")
	}
	return nil
}

// nextBucketID returns the next bucket ID to use for creating a new bucket.
//
// NOTE: This function must only be called on a writable transaction.  Since it
// is an internal helper function, it does not check.
func (tx *transaction) nextBucketID() ([bucketIdLen]byte, error) {
	// init from full length (assuming there is no gap)
	nextId := len(tx.db.bucketIds) + 1

	// find a gap in bucket id map
	ids := bitset.New(1 << bucketIdLen).One()
	defer ids.Close()
	for _, v := range tx.db.bucketIds {
		var buf [8]byte
		copy(buf[8-bucketIdLen:], v[:])
		ids.Unset(int(binary.BigEndian.Uint64(buf[:])))
	}
	// use the first missing id
	if ids.Count() > 0 {
		if pos, ok := ids.Iterate(-1, make([]int, 1)); ok {
			nextId = pos[0]
		}
	}
	if nextId > 1<<uint(8*bucketIdLen) {
		return [bucketIdLen]byte{}, makeDbErr(store.ErrTxConflict, "bucket sequence overflow")
	}
	var buf [8]byte
	var id [bucketIdLen]byte
	binary.BigEndian.PutUint64(buf[:], uint64(nextId))
	copy(id[:], buf[8-bucketIdLen:8])
	return id, nil
}

func (tx *transaction) IsWriteable() bool {
	return tx.writable
}

// Root returns the top-most bucket for all metadata storage.
//
// This function is part of the store.Tx interface implementation.
func (tx *transaction) Root() store.Bucket {
	return &bucket{tx: tx, id: [bucketIdLen]byte{}, key: []byte("root")}
}

// Bucket returns the bucket with given name.
func (tx *transaction) Bucket(key []byte) store.Bucket {
	effectiveKey := bucketizedKey([bucketIdLen]byte{}, key)
	id, ok := tx.db.bucketIds[string(effectiveKey)]
	if !ok {
		return nil
	}
	return &bucket{tx: tx, id: id, key: effectiveKey}
}

// Manifest returns the current database manifest metadata.
//
// This function is part of the store.DB interface implementation.
func (tx *transaction) Manifest() (mft store.Manifest, err error) {
	if err = tx.checkClosed(); err != nil {
		return
	}
	mft = tx.db.manifest
	return
}

// SetManifest overwrites the current database manifest.
//
// This function is part of the store.DB interface implementation.
func (tx *transaction) SetManifest(manifest store.Manifest) error {
	if err := tx.checkClosed(); err != nil {
		return err
	}
	if err := tx.checkWriteable(); err != nil {
		return err
	}

	// we only allow some fields to be overwritten
	tx.db.manifest.Name = manifest.Name
	tx.db.manifest.Version = manifest.Version
	tx.db.manifest.Label = manifest.Label
	tx.db.manifest.Schema = manifest.Schema
	return nil
}

// close marks the transaction closed then releases any pending data.
func (tx *transaction) close() {
	tx.closed = true
	clear(tx.updates)
	tx.updates = nil
	clear(tx.deletes)
	tx.deletes = nil

	// free locks
	if tx.writable {
		// fmt.Printf("Wunlock\n%s", string(debug.Stack()))
		tx.db.writeLock.Unlock()
	} else {
		// fmt.Printf("Runlock\n%s", string(debug.Stack()))
		tx.db.writeLock.RUnlock()
	}
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
		return makeDbErr(store.ErrTxNotWritable, str)
	}

	// Write (merge) pending updates and deletes.
	for k := range tx.deletes {
		tx.db.store.Delete(Item{[]byte(k), nil})
	}
	for k, v := range tx.updates {
		tx.db.store.ReplaceOrInsert(Item{[]byte(k), v})
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

	// we have not yet altered any data in the db, so rollback is a noop
	tx.close()
	return nil
}

func (tx *transaction) DB() store.DB {
	return tx.db
}
