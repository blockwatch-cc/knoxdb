// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"blockwatch.cc/knoxdb/store"
	bolt "go.etcd.io/bbolt"
)

// bucket is an internal type used to represent a collection of key/value pairs
// and implements the store.Bucket interface.
type bucket struct {
	tx     *transaction
	bucket *bolt.Bucket
}

// Enforce bucket implements the store.Bucket interface.
var _ store.Bucket = (*bucket)(nil)

// Bucket retrieves a nested bucket with the given key.  Returns nil if
// the bucket does not exist.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Bucket(key []byte) store.Bucket {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	// fetch bucket
	var child *bolt.Bucket
	if b.bucket == nil {
		child = b.tx.tx.Bucket(key)
	} else {
		child = b.bucket.Bucket(key)
	}
	if child == nil {
		return nil
	}

	return &bucket{tx: b.tx, bucket: child}
}

// CreateBucket creates and returns a new nested bucket with the given key.
//
// Returns the following errors as required by the interface contract:
//   - ErrBucketExists if the bucket already exists
//   - ErrBucketNameRequired if the key is empty
//   - ErrIncompatibleValue if the key is otherwise invalid for the particular
//     implementation
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) CreateBucket(key []byte) (store.Bucket, error) {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "create bucket requires a writable database transaction"
		return nil, makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		str := "create bucket requires a key"
		return nil, makeDbErr(store.ErrBucketNameRequired, str, nil)
	}

	var child *bolt.Bucket
	var err error
	if b.bucket == nil {
		child, err = b.tx.tx.CreateBucket(key)
	} else {
		child, err = b.bucket.CreateBucket(key)
	}
	if err != nil {
		return nil, convertErr("create bucket", err)
	}

	return &bucket{tx: b.tx, bucket: child}, nil
}

// CreateBucketIfNotExists creates and returns a new nested bucket with the
// given key if it does not already exist.
//
// Returns the following errors as required by the interface contract:
//   - ErrBucketNameRequired if the key is empty
//   - ErrIncompatibleValue if the key is otherwise invalid for the particular
//     implementation
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) CreateBucketIfNotExists(key []byte) (store.Bucket, error) {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "create bucket requires a writable database transaction"
		return nil, makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	var child *bolt.Bucket
	var err error
	if b.bucket == nil {
		child, err = b.tx.tx.CreateBucketIfNotExists(key)
	} else {
		child, err = b.bucket.CreateBucketIfNotExists(key)
	}
	if err != nil {
		return nil, convertErr("create non-exist bucket", err)
	}
	return &bucket{tx: b.tx, bucket: child}, nil
}

// DeleteBucket removes a nested bucket with the given key.
//
// Returns the following errors as required by the interface contract:
//   - ErrBucketNotFound if the specified bucket does not exist
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) DeleteBucket(key []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "delete bucket requires a writable database transaction"
		return makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Remove the nested bucket from the bucket index.  Any buckets nested
	// under it were already removed above.
	var err error
	if b.bucket == nil {
		err = b.tx.tx.DeleteBucket(key)
	} else {
		err = b.bucket.DeleteBucket(key)
	}
	if err != nil {
		return convertErr("delete bucket", err)
	}
	return nil
}

// Cursor returns a new cursor, allowing for iteration over the bucket's
// key/value pairs in forward or backward order.
//
// You must seek to a position using the First, Last, or Seek functions before
// calling the Next, Prev, Key, or Value functions.  Failure to do so will
// result in the same return values as an exhausted cursor, which is false for
// the Prev and Next functions and nil for Key and Value functions.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Cursor(_ ...store.CursorOptions) store.Cursor {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return &cursor{bucket: b}
	}

	// Create the cursor for either the root bucket or a nested bucket.
	if b.bucket == nil {
		return &cursor{bucket: b, currentIter: b.tx.tx.Cursor()}
	} else {
		return &cursor{bucket: b, currentIter: b.bucket.Cursor()}
	}
}

// Range returns a new ranged cursor, allowing for iteration over the
// bucket's key/value pairs (and nested buckets) that satisfy the prefix
// condition in forward or backward order.
//
// This cursor automatically seeks to the first key that satisfies prefix
// stops when the next key does not match the prefix. Its sufficient to
// only use Next, but you can reset the cursor with First, Last and Seek,
// however, calls to these functions consider the original prefix.
func (b *bucket) Range(prefix []byte, _ ...store.CursorOptions) store.Cursor {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return &cursor{bucket: b}
	}

	// Create the cursor for either the root bucket or a nested bucket.
	if b.bucket == nil {
		return &cursor{bucket: b, currentIter: b.tx.tx.Cursor(), keyRange: store.BytesPrefix(prefix)}
	} else {
		return &cursor{bucket: b, currentIter: b.bucket.Cursor(), keyRange: store.BytesPrefix(prefix)}
	}
}

// ForEach invokes the passed function with every key/value pair in the bucket.
// This does not include nested buckets or the key/value pairs within those
// nested buckets.
//
// WARNING: It is not safe to mutate data while iterating with this method.
// Doing so may cause the underlying cursor to be invalidated and return
// unexpected keys and/or values.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) ForEach(fn func(k, v []byte) error) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	var err error
	if b.bucket == nil {
		// root bucket only contains buckets and no keys to iterate
		return nil
	} else {
		err = b.bucket.ForEach(fn)
	}
	if err != nil {
		return convertErr("foreach", err)
	}
	return nil
}

// ForEachBucket invokes the passed function with the key of every nested bucket
// in the current bucket.  This does not include any nested buckets within those
// nested buckets.
//
// WARNING: It is not safe to mutate data while iterating with this method.
// Doing so may cause the underlying cursor to be invalidated and return
// unexpected keys.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) ForEachBucket(fn func(k []byte, b store.Bucket) error) error {
	err := b.ForEach(func(name, val []byte) error {
		if val != nil {
			return nil
		}
		dbBucket := b.bucket.Bucket(name)
		bucket := &bucket{tx: b.tx, bucket: dbBucket}
		return fn(name, bucket)
	})
	if err != nil {
		return convertErr("foreach bucket", err)
	}
	return nil
}

// Writable returns whether or not the bucket is writable.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Writable() bool {
	return b.tx.writable
}

// Put saves the specified key/value pair to the bucket.  Keys that do not
// already exist are added and keys that already exist are overwritten.
//
// Returns the following errors as required by the interface contract:
//   - ErrKeyRequired if the key is empty
//   - ErrIncompatibleValue if the key is the same as an existing bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Put(key, value []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "setting a key requires a writable database transaction"
		return makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		str := "put requires a key"
		return makeDbErr(store.ErrKeyRequired, str, nil)
	}

	if err := b.bucket.Put(key, value); err != nil {
		return convertErr("put", err)
	}
	return nil
}

// Get returns the value for the given key.  Returns nil if the key does not
// exist in this bucket.  An empty slice is returned for keys that exist but
// have no value assigned.
//
// NOTE: The value returned by this function is only valid during a transaction.
// Attempting to access it after a transaction has ended results in undefined
// behavior.  Additionally, the value must NOT be modified by the caller.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Get(key []byte) []byte {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	// Nothing to return if there is no key.
	if len(key) == 0 {
		return nil
	}

	return b.bucket.Get(key)
}

// Delete removes the specified key from the bucket.  Deleting a key that does
// not exist does not return an error.
//
// Returns the following errors as required by the interface contract:
//   - ErrKeyRequired if the key is empty
//   - ErrIncompatibleValue if the key is the same as an existing bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Delete(key []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "deleting a value requires a writable database transaction"
		return makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Nothing to do if there is no key.
	if len(key) == 0 {
		return nil
	}

	if err := b.bucket.Delete(key); err != nil {
		return convertErr("delete", err)
	}
	return nil
}

func (b *bucket) NextSequence() (uint64, error) {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return 0, err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "deleting a value requires a writable database transaction"
		return 0, makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	val, err := b.bucket.NextSequence()
	if err != nil {
		return 0, convertErr("next sequence", err)
	}
	return val, nil
}

func (b *bucket) FillPercent(p float64) {
	b.bucket.FillPercent = p
}

func (b *bucket) Stats() store.BucketStats {
	stats := store.BucketStats{}
	if err := b.tx.checkClosed(); err != nil {
		return stats
	}
	internalStats := b.bucket.Stats()
	stats.KeyN = internalStats.KeyN
	stats.BucketN = internalStats.BucketN
	stats.Size = internalStats.BranchAlloc + internalStats.LeafAlloc
	return stats
}
