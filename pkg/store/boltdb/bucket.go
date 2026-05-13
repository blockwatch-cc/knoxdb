// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"bytes"
	"errors"
	"io"
	"iter"

	"blockwatch.cc/knoxdb/pkg/store"
	bolt "go.etcd.io/bbolt"
	bolterr "go.etcd.io/bbolt/errors"
)

// bucket is an internal type used to represent a collection of key/value pairs
// and implements the store.Bucket interface.
type bucket struct {
	tx     *tx
	bucket *bolt.Bucket
}

// Ensure bucket implements the store.Bucket interface.
var _ store.Bucket = (*bucket)(nil)

// Bucket retrieves a nested bucket with the given key. Returns nil if
// the bucket does not exist.
func (b *bucket) Bucket(key []byte) (store.Bucket, error) {
	child := b.bucket.Bucket(key)
	if child == nil {
		return nil, store.ErrBucketNotFound
	}
	child.FillPercent = b.tx.db.opts.PageFill
	return &bucket{tx: b.tx, bucket: child}, nil
}

// Buckets returns an iterator for nested buckets.
func (b *bucket) Buckets() iter.Seq2[[]byte, store.Bucket] {
	return func(yield func([]byte, store.Bucket) bool) {
		if b.tx.IsClosed() {
			return
		}
		b.bucket.ForEachBucket(func(name []byte) error {
			child := b.bucket.Bucket(name)
			child.FillPercent = b.tx.db.opts.PageFill
			if !yield(name, &bucket{tx: b.tx, bucket: child}) {
				return io.EOF
			}
			return nil
		})
	}
}

// CreateBucket creates and returns a new nested bucket with the given key.
// If the bucket already exists it is returned without error.
func (b *bucket) CreateBucket(key []byte, _ ...store.BucketOption) (store.Bucket, error) {
	child, err := b.bucket.CreateBucket(key)
	if err != nil {
		// use bucket if exists
		if errors.Is(err, bolterr.ErrBucketExists) {
			return b.Bucket(key)
		}
		return nil, wrap(err)
	}
	child.FillPercent = b.tx.db.opts.PageFill
	return &bucket{tx: b.tx, bucket: child}, nil
}

// DeleteBucket removes a nested bucket with the given key including
// all nested buckets and keys.
func (b *bucket) DeleteBucket(key []byte) error {
	return wrap(b.bucket.DeleteBucket(key))
}

// Writable returns whether or not the bucket is writable.
func (b *bucket) Writable() bool {
	return b.tx.tx.Writable()
}

// Put saves the specified key/value pair to the bucket. Keys that do not
// already exist are added and keys that already exist are overwritten.
func (b *bucket) Put(key, value []byte) error {
	return wrap(b.bucket.Put(key, value))
}

// Get returns the value for the given key. Returns nil if the key does not
// exist in this bucket. An empty slice is returned for keys that exist but
// have no value assigned.
//
// NOTE: The value returned by this function is only valid during a transaction.
// Attempting to access it after a transaction has ended results in undefined
// behavior. Additionally, the value must NOT be modified by the caller.
func (b *bucket) Get(key []byte) ([]byte, error) {
	// Ensure transaction state is valid.
	if b.tx.IsClosed() {
		return nil, store.ErrTxClosed
	}

	// Ignore empty keys.
	if len(key) == 0 {
		return nil, store.ErrKeyRequired
	}

	val := b.bucket.Get(key)
	if val == nil {
		return nil, store.ErrKeyNotFound
	}
	return val, nil
}

// Delete removes the specified key from the bucket. Deleting a key that does
// not exist does not return an error.
func (b *bucket) Delete(key []byte) error {
	return wrap(b.bucket.Delete(key))
}

// Scan iterates over keys in a bucket in ascending order
// and returns a sequence of key/value pairs.
func (b *bucket) Scan(prefix []byte) iter.Seq2[[]byte, []byte] {
	return b.ScanRange(store.PrefixRange(prefix))
}

// ScanRange iterates over an explicit range of keys starting at
// a lower bound (inclusive) and ending before an upper bound (exclusive)
// in ascending order.
func (b *bucket) ScanRange(start, end []byte) iter.Seq2[[]byte, []byte] {
	if b.tx.IsClosed() {
		return func(fn func([]byte, []byte) bool) {} // noop sequence
	}
	return func(yield func([]byte, []byte) bool) {
		c := b.bucket.Cursor()
		if start != nil {
			for k, v := c.Seek(start); k != nil && (end == nil || bytes.Compare(k, end) < 0); k, v = c.Next() {
				// skip nested buckets
				if v == nil {
					continue
				}
				if !yield(k, v) {
					return
				}
			}
		} else {
			for k, v := c.First(); k != nil && (end == nil || bytes.Compare(k, end) < 0); k, v = c.Next() {
				// skip nested buckets
				if v == nil {
					continue
				}
				if !yield(k, v) {
					return
				}
			}
		}
	}
}

// ScanReverse iterates over keys in a bucket in descending order
// and returns a sequence of key/value pairs.
func (b *bucket) ScanReverse(prefix []byte) iter.Seq2[[]byte, []byte] {
	return b.ScanRangeReverse(store.PrefixRange(prefix))
}

// ScanRangeReverse iterates over an explicit range of keys starting at
// a lower bound (inclusive) and ending before an upper bound (exclusive)
// in descending order.
func (b *bucket) ScanRangeReverse(start, end []byte) iter.Seq2[[]byte, []byte] {
	if b.tx.IsClosed() {
		return func(fn func([]byte, []byte) bool) {} // noop sequence
	}
	return func(yield func([]byte, []byte) bool) {
		c := b.bucket.Cursor()
		if end != nil {
			k, v := c.Seek(end)
			if k != nil && bytes.Compare(k, end) >= 0 {
				k, v = c.Prev()
			}
			for ; k != nil && (start == nil || bytes.Compare(k, start) >= 0); k, v = c.Prev() {
				// skip nested buckets
				if v == nil {
					continue
				}
				if !yield(k, v) {
					return
				}
			}
		} else {
			for k, v := c.Last(); k != nil && (start == nil || bytes.Compare(k, start) >= 0); k, v = c.Prev() {
				// skip nested buckets
				if v == nil {
					continue
				}
				if !yield(k, v) {
					return
				}
			}
		}
	}
}

// SearchGE returns the first key and its value that is greater or
// equal to the search key or an error when no key was found.
// It works like a binary search in ascending order.
func (b *bucket) SearchGE(key []byte) ([]byte, []byte, error) {
	if b.tx.IsClosed() {
		return nil, nil, store.ErrTxClosed
	}
	c := b.bucket.Cursor()
	if k, v := c.Seek(key); k != nil {
		return k, v, nil
	}
	return nil, nil, store.ErrKeyNotFound
}

// SearchLE returns the last key and its value that is less or
// equal to the search key or an error when no key was found.
// It works like a binary search in descending order.
func (b *bucket) SearchLE(key []byte) ([]byte, []byte, error) {
	if b.tx.IsClosed() {
		return nil, nil, store.ErrTxClosed
	}
	c := b.bucket.Cursor()

	// seek the first key equal or greater
	k, v := c.Seek(key)

	// reverse one key if not equal
	if !bytes.Equal(key, k) {
		k, v = c.Prev()
	}
	if k != nil {
		return k, v, nil
	}
	return nil, nil, store.ErrKeyNotFound
}

// Stats returns bucket statistics
func (b *bucket) Stats() store.BucketStats {
	stats := b.bucket.Stats()
	return store.BucketStats{
		NKeys:    stats.KeyN,
		NBuckets: stats.BucketN + stats.InlineBucketN,
		NBytes:   stats.BranchAlloc + stats.LeafAlloc,
	}
}
