// Copyright (c) 2018 - 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package badger

import (
	"encoding/binary"
	"encoding/json"

	"blockwatch.cc/knoxdb/internal/store"
	"github.com/dgraph-io/badger/v4"
)

// transaction represents a database transaction.  It can either be read-only or
// read-write and implements the store.Bucket interface.  The transaction
// provides a root bucket against which all read and writes occur.
type transaction struct {
	managed    bool        // Is the transaction managed by this driver?
	closed     bool        // Is the transaction closed?
	writable   bool        // Is the transaction writable?
	db         *db         // DB instance the tx was created from.
	tx         *badger.Txn // the DB transaction
	metaBucket *bucket     // The root metadata bucket.
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

// hasKey returns whether or not the provided key exists in the database while
// taking into account the current transaction state.
func (tx *transaction) hasKey(key []byte) bool {
	// Consult the underlying store. returns ErrKeyNotFound on fail
	_, err := tx.tx.Get(key)
	return err == nil
}

// putKey adds the provided key to the list of keys to be updated in the
// database when the transaction is committed.
//
// NOTE: This function must only be called on a writable transaction.  Since it
// is an internal helper function, it does not check.
func (tx *transaction) putKey(key, value []byte) error {
	return tx.tx.Set(key, value)
}

// fetchKey attempts to fetch the provided key from the database cache (and
// hence underlying database) while taking into account the current transaction
// state.  Returns nil if the key does not exist.
func (tx *transaction) fetchKey(key []byte) ([]byte, error) {
	// Consult the underlying store. Ignore ErrKeyNotFound for compatibility
	// with other drivers.
	item, err := tx.tx.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// deleteKey adds the provided key to the list of keys to be deleted from the
// database when the transaction is committed.  The notify iterators flag is
// useful to delay notifying iterators about the changes during bulk deletes.
//
// NOTE: This function must only be called on a writable transaction.  Since it
// is an internal helper function, it does not check.
func (tx *transaction) deleteKey(key []byte) error {
	return tx.tx.Delete(key)
}

// nextBucketID returns the next bucket ID to use for creating a new bucket.
//
// NOTE: This function must only be called on a writable transaction.  Since it
// is an internal helper function, it does not check.
func (tx *transaction) nextBucketID() ([bucketIdLen]byte, error) {
	// Load the currently highest used bucket ID.
	seqKey := bucketizedKey(sequenceBucketID, curBucketIDKeyName)
	seq, err := tx.db.store.GetSequence(seqKey, 1)
	if err != nil {
		return [bucketIdLen]byte{}, err
	}
	defer seq.Release()
	nextId, err := seq.Next()
	if err != nil {
		return [bucketIdLen]byte{}, err
	}
	if nextId > 1<<uint(8*bucketIdLen) {
		return [bucketIdLen]byte{}, makeDbErr(store.ErrTxConflict, "bucket sequence overflow", nil)
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, nextId)
	var id [bucketIdLen]byte
	copy(id[:], buf[8-bucketIdLen:8])
	return id, nil
}

// Bucket returns a bucket under the top-most bucket.
//
// This function is part of the store.Tx interface implementation.
func (tx *transaction) Root() store.Bucket {
	return tx.metaBucket
}

// Root returns the bucket with given name.
func (tx *transaction) Bucket(key []byte) store.Bucket {
	return tx.Root().Bucket(key)
}

// Manifest returns the current database manifest metadata.
//
// This function is part of the store.DB interface implementation.
func (tx *transaction) Manifest() (store.Manifest, error) {
	var mft store.Manifest
	if err := tx.checkClosed(); err != nil {
		return mft, err
	}
	mftKey := bucketizedKey(metadataBucketID, manifestKey)
	item, err := tx.tx.Get(mftKey)
	if err == nil {
		return mft, err
	}
	if buf, _ := item.ValueCopy(nil); buf != nil {
		err = json.Unmarshal(buf, &mft)
	}
	return mft, err
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
	buf, _ := json.Marshal(mft)
	mftKey := bucketizedKey(metadataBucketID, manifestKey)
	if err := tx.tx.Set(mftKey, buf); err != nil {
		return convertErr("set manifest", err)
	}
	return nil
}

// close marks the transaction closed then releases any pending data.
func (tx *transaction) close() {
	tx.closed = true
	if tx.writable {
		tx.db.activeTx.Done()
	}
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

	tx.tx.Discard()
	tx.close()
	return nil
}

func (tx *transaction) DB() store.DB {
	return tx.db
}
