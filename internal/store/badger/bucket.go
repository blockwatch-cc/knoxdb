// Copyright (c) 2018 - 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package badger

import (
	"fmt"

	"blockwatch.cc/knoxdb/internal/store"
)

// bucket is an internal type used to represent a collection of key/value pairs
// and implements the store.Bucket interface.
type bucket struct {
	tx  *transaction
	id  [bucketIdLen]byte
	key []byte
	seq store.Sequence
}

// Enforce bucket implements the store.Bucket interface.
var _ store.Bucket = (*bucket)(nil)

// bucketIndexKey returns the actual key to use for storing and retrieving a
// child bucket in the bucket index.  This is required because additional
// information is needed to distinguish nested buckets with the same name.
func bucketIndexKey(parentID [bucketIdLen]byte, key []byte) []byte {
	// The serialized bucket index key format is:
	//   <bucketindexprefix><parentbucketid><bucketname>
	indexKey := make([]byte, len(bucketIndexPrefix)+bucketIdLen+len(key))
	copy(indexKey, bucketIndexPrefix)
	copy(indexKey[len(bucketIndexPrefix):], parentID[:])
	copy(indexKey[len(bucketIndexPrefix)+bucketIdLen:], key)
	return indexKey
}

// bucketizedKey returns the actual key to use for storing and retrieving a key
// for the provided bucket ID.  This is required because bucketizing is handled
// through the use of a unique prefix per bucket.
func bucketizedKey(bucketID [bucketIdLen]byte, key []byte) []byte {
	// The serialized block index key format is:
	//   <bucketid><key>
	bKey := make([]byte, bucketIdLen+len(key))
	copy(bKey, bucketID[:])
	copy(bKey[bucketIdLen:], key)
	return bKey
}

// Bucket retrieves a nested bucket with the given key.  Returns nil if
// the bucket does not exist.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Bucket(key []byte) store.Bucket {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	// Attempt to fetch child bucket id from cache.
	cache := b.tx.db.bucketIds.Load()
	if cache != nil {
		if id, ok := cache.(map[string][bucketIdLen]byte)[string(key)]; ok {
			return &bucket{tx: b.tx, key: key, id: id}
		}
	}

	// Attempt to fetch the ID for the child bucket.  The bucket does not
	// exist if the bucket index entry does not exist.
	childID, err := b.tx.fetchKey(bucketizedKey(metadataBucketID, bucketIndexKey(b.id, key)))
	if err != nil {
		b.tx.db.log.Errorf("read bucket %s: %v", string(key), err)
		return nil
	}
	if childID == nil {
		return nil
	}

	childBucket := &bucket{tx: b.tx, key: key}
	copy(childBucket.id[:], childID)

	// Update cache with an extended map, copy current contents
	next := make(map[string][bucketIdLen]byte)
	if cache != nil {
		for n, v := range cache.(map[string][bucketIdLen]byte) {
			next[n] = v
		}
	}
	next[string(key)] = childBucket.id
	b.tx.db.bucketIds.Store(next)

	return childBucket
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
		return nil, makeDbErr(store.ErrTxNotWritable, str)
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		str := "create bucket requires a key"
		return nil, makeDbErr(store.ErrBucketNameRequired, str)
	}

	// Ensure bucket does not already exist.
	bidxKey := bucketizedKey(metadataBucketID, bucketIndexKey(b.id, key))
	if b.tx.hasKey(bidxKey) {
		str := "bucket already exists"
		return nil, makeDbErr(store.ErrBucketExists, str)
	}

	// Find the appropriate next bucket ID to use for the new bucket.
	childID, err := b.tx.nextBucketID()
	if err != nil {
		return nil, err
	}

	// Add the new bucket to the bucket index.
	b.tx.db.log.Debugf("Creating bucket %s with id 0x%x", string(key), childID)
	if err := b.tx.putKey(bidxKey, childID[:]); err != nil {
		str := fmt.Sprintf("failed to create bucket with key %q", key)
		return nil, convertErr(str, err)
	}
	return &bucket{tx: b.tx, id: childID, key: copySlice(key)}, nil
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
		return nil, makeDbErr(store.ErrTxNotWritable, str)
	}

	// Return existing bucket if it already exists, otherwise create it.
	if bucket := b.Bucket(key); bucket != nil {
		return bucket, nil
	}
	return b.CreateBucket(key)
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
		return makeDbErr(store.ErrTxNotWritable, str)
	}

	// Attempt to fetch the ID for the child bucket.  The bucket does not
	// exist if the bucket index entry does not exist.  In the case of the
	// special internal block index, keep the fixed ID.
	bidxKey := bucketizedKey(metadataBucketID, bucketIndexKey(b.id, key))
	childID, _ := b.tx.fetchKey(bidxKey)
	if childID == nil {
		str := fmt.Sprintf("bucket %q does not exist", key)
		return makeDbErr(store.ErrBucketNotFound, str)
	}

	b.tx.db.log.Debugf("Deleting bucket %s with id 0x%x", string(key), childID)

	// Remove all nested buckets and their keys.
	childIDs := [][]byte{childID}
	for len(childIDs) > 0 {
		childID = childIDs[len(childIDs)-1]
		childIDs = childIDs[:len(childIDs)-1]

		b.tx.db.log.Debugf("Deleting nested bucket id 0x%x", childID)

		// experimental: check this does not produce a deadlock
		err := b.tx.db.store.DropPrefix(childID)
		if err != nil {
			return convertErr("drop prefix", err)
		}

		// Delete all keys in the nested bucket.
		// keyCursor := newCursor(b, childID, ctKeys)
		// for ok := keyCursor.First(); ok; ok = keyCursor.Next() {
		// 	b.tx.deleteKey(keyCursor.rawKey())
		// }
		// cursorFinalizer(keyCursor)

		// Iterate through all nested buckets.
		bucketCursor := newCursor(b, childID, ctBuckets, store.ForwardCursor)
		for ok := bucketCursor.First(); ok; ok = bucketCursor.Next() {
			// Push the id of the nested bucket onto the stack for
			// the next iteration.
			childID := bucketCursor.rawValue()
			childIDs = append(childIDs, childID)

			// Remove the nested bucket from the bucket index.
			b.tx.deleteKey(bucketCursor.rawKey())
		}
		bucketCursor.Close()
	}

	// Remove bucket sequence
	if b.seq != nil {
		if err := b.seq.Release(); err != nil {
			return convertErr("release sequence", err)
		}
		seqKey := bucketizedKey(sequenceBucketID, b.key)
		if err := b.tx.tx.Delete(seqKey); err != nil {
			return convertErr("delete sequence", err)
		}
	}

	// Remove the nested bucket from the bucket index.  Any buckets nested
	// under it were already removed above.
	b.tx.deleteKey(bidxKey)

	// Invalidate bucket id cache
	b.tx.db.bucketIds.Store(make(map[string][bucketIdLen]byte))

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
func (b *bucket) Cursor(opts ...store.CursorOptions) store.Cursor {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return &cursor{bucket: b}
	}

	// use default opts when unset
	o := store.ForwardCursor
	if len(opts) > 0 {
		o = opts[0]
	}

	// Create the cursor. The user must ensure to close all cursors
	// before tx commit or rollback.
	return newCursor(b, b.id[:], ctKeys, o)
}

// Range returns a new ranged cursor, allowing for iteration over the
// bucket's key/value pairs (and nested buckets) that satisfy the prefix
// condition in forward or backward order.
//
// This cursor automatically seeks to the first key that satisfies prefix
// stops when the next key does not match the prefix. Its sufficient to
// only use Next, but you can reset the cursor with First, Last and Seek,
// however, calls to these functions consider the original prefix.
func (b *bucket) Range(prefix []byte, opts ...store.CursorOptions) store.Cursor {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return &cursor{bucket: b}
	}

	// use default opts when unset
	o := store.ForwardCursor
	if len(opts) > 0 {
		o = opts[0]
	}

	// Create the cursor with custom prefix. User must close this cursor
	// before tx commit/rollback.
	return newCursor(b, append(b.id[:], prefix...), ctKeys, o)
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

	// Invoke the callback for each cursor item.  Return the error returned
	// from the callback when it is non-nil.
	c := newCursor(b, b.id[:], ctKeys, store.ForwardCursor)
	defer c.Close()
	for ok := c.First(); ok; ok = c.Next() {
		err := fn(c.Key(), c.Value())
		if err != nil {
			return err
		}
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
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Invoke the callback for each cursor item.  Return the error returned
	// from the callback when it is non-nil.
	c := newCursor(b, b.id[:], ctBuckets, store.ForwardCursor)
	defer c.Close()
	for ok := c.First(); ok; ok = c.Next() {
		bucket := &bucket{tx: b.tx, key: copySlice(c.Key())}
		err := fn(c.Key(), bucket)
		if err != nil {
			return err
		}
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
		return makeDbErr(store.ErrTxNotWritable, str)
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		str := "put requires a key"
		return makeDbErr(store.ErrKeyRequired, str)
	}
	if err := b.tx.putKey(bucketizedKey(b.id, key), value); err != nil {
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

	val, _ := b.tx.fetchKey(bucketizedKey(b.id, key))
	return val
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
		return makeDbErr(store.ErrTxNotWritable, str)
	}

	// Nothing to do if there is no key.
	if len(key) == 0 {
		return nil
	}

	if err := b.tx.deleteKey(bucketizedKey(b.id, key)); err != nil {
		return convertErr("del", err)
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
		return 0, makeDbErr(store.ErrTxNotWritable, str)
	}

	if b.seq == nil {
		var err error
		b.seq, err = b.tx.db.Sequence(b.key, 1000)
		if err != nil {
			return 0, convertErr("next sequence", err)
		}
	}
	return b.seq.Next()
}

func (b *bucket) FillPercent(p float64) {
	// unsupported
}

func (b *bucket) Stats() store.BucketStats {
	stats := store.BucketStats{
		BucketN: 1,
	}
	if err := b.tx.checkClosed(); err != nil {
		return stats
	}

	if err := b.ForEachBucket(func(_ []byte, _ store.Bucket) error {
		stats.BucketN++
		return nil
	}); err != nil {
		return stats
	}

	// LSM+valuelog table size on disk (approximate)
	ondisk, _ := b.tx.db.store.EstimateSize(b.id[:])
	stats.Size = int(ondisk)

	// counting keys is too expensive
	return stats
}
