// Copyright (c) 2018-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"blockwatch.cc/knoxdb/pkg/store"
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
func (b *bucket) Bucket(key []byte) store.Bucket {
	var child *bolt.Bucket
	if b.bucket == nil {
		child = b.tx.tx.Bucket(key)
	} else {
		child = b.bucket.Bucket(key)
	}
	if child == nil {
		return nil
	}
	child.FillPercent = b.tx.db.opts.PageFill
	return &bucket{tx: b.tx, bucket: child}
}

// CreateBucket creates and returns a new nested bucket with the given key.
func (b *bucket) CreateBucket(key []byte) (store.Bucket, error) {
	var (
		child *bolt.Bucket
		err   error
	)
	if b.bucket == nil {
		child, err = b.tx.tx.CreateBucket(key)
	} else {
		child, err = b.bucket.CreateBucket(key)
	}
	if err != nil {
		return nil, wrap(err)
	}
	child.FillPercent = b.tx.db.opts.PageFill
	return &bucket{tx: b.tx, bucket: child}, nil
}

// CreateBucketIfNotExists creates and returns a new nested bucket with the
// given key if it does not already exist.
func (b *bucket) CreateBucketIfNotExists(key []byte) (store.Bucket, error) {
	var (
		child *bolt.Bucket
		err   error
	)
	if b.bucket == nil {
		child, err = b.tx.tx.CreateBucketIfNotExists(key)
	} else {
		child, err = b.bucket.CreateBucketIfNotExists(key)
	}
	if err != nil {
		return nil, wrap(err)
	}
	child.FillPercent = b.tx.db.opts.PageFill
	return &bucket{tx: b.tx, bucket: child}, nil
}

// DeleteBucket removes a nested bucket with the given key.
func (b *bucket) DeleteBucket(key []byte) error {
	var err error
	if b.bucket == nil {
		err = b.tx.tx.DeleteBucket(key)
	} else {
		err = b.bucket.DeleteBucket(key)
	}
	return wrap(err)
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
	// Create the cursor for either the root bucket or a nested bucket.
	if b.bucket == nil {
		return &cursor{bucket: b, it: b.tx.tx.Cursor()}
	} else {
		return &cursor{bucket: b, it: b.bucket.Cursor()}
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
	if b.bucket == nil {
		return &cursor{bucket: b, it: b.tx.tx.Cursor(), keyRange: store.BytesPrefix(prefix)}
	} else {
		return &cursor{bucket: b, it: b.bucket.Cursor(), keyRange: store.BytesPrefix(prefix)}
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
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
func (b *bucket) ForEach(fn func(k, v []byte) error) error {
	var err error
	if b.bucket == nil {
		// root bucket only contains buckets and no keys to iterate
		return nil
	} else {
		err = b.bucket.ForEach(fn)
	}
	return wrap(err)
}

// ForEachBucket invokes the passed function with the key of every nested bucket
// in the current bucket.  This does not include any nested buckets within those
// nested buckets.
//
// WARNING: It is not safe to mutate data while iterating with this method.
// Doing so may cause the underlying cursor to be invalidated and return
// unexpected keys.
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
func (b *bucket) ForEachBucket(fn func(k []byte, b store.Bucket) error) error {
	err := b.bucket.ForEachBucket(func(name []byte) error {
		return fn(name, &bucket{tx: b.tx, bucket: b.bucket.Bucket(name)})
	})
	return wrap(err)
}

// Writable returns whether or not the bucket is writable.
func (b *bucket) Writable() bool {
	return b.tx.tx.Writable()
}

// Put saves the specified key/value pair to the bucket.  Keys that do not
// already exist are added and keys that already exist are overwritten.
func (b *bucket) Put(key, value []byte) error {
	return wrap(b.bucket.Put(key, value))
}

// Get returns the value for the given key.  Returns nil if the key does not
// exist in this bucket.  An empty slice is returned for keys that exist but
// have no value assigned.
//
// NOTE: The value returned by this function is only valid during a transaction.
// Attempting to access it after a transaction has ended results in undefined
// behavior.  Additionally, the value must NOT be modified by the caller.
func (b *bucket) Get(key []byte) []byte {
	// Nothing to return if there is no key.
	if len(key) == 0 {
		return nil
	}
	return b.bucket.Get(key)
}

// Delete removes the specified key from the bucket.  Deleting a key that does
// not exist does not return an error.
func (b *bucket) Delete(key []byte) error {
	// Nothing to do if there is no key.
	if len(key) == 0 {
		return nil
	}

	return wrap(b.bucket.Delete(key))
}

func (b *bucket) NextSequence() (uint64, error) {
	val, err := b.bucket.NextSequence()
	if err != nil {
		return 0, wrap(err)
	}
	return val, nil
}

func (b *bucket) FillPercent(p float64) {
	b.bucket.FillPercent = p
}

func (b *bucket) Stats() store.BucketStats {
	stats := store.BucketStats{}
	internalStats := b.bucket.Stats()
	stats.KeyN = internalStats.KeyN
	stats.BucketN = internalStats.BucketN
	stats.Size = internalStats.BranchAlloc + internalStats.LeafAlloc
	return stats
}
