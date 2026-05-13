// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package mem

import (
	"iter"
	"maps"
	"slices"
	"strings"

	"blockwatch.cc/knoxdb/pkg/btree"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/store"
)

const root = "root"

// bucket is an internal type used to represent a collection of key/value pairs
// and implements the store.Bucket interface.
type bucket struct {
	tx *tx
	id uint32
}

// Ensure bucket implements the store.Bucket interface.
var _ store.Bucket = (*bucket)(nil)

// Writable returns whether or not the bucket is writable.
func (b *bucket) Writable() bool {
	return b.tx.IsWriteable()
}

// Bucket retrieves a nested bucket with the given key. Returns nil if
// the bucket does not exist.
func (b *bucket) Bucket(key []byte) (store.Bucket, error) {
	// Ensure transaction state is valid.
	if b.tx.IsClosed() {
		return nil, store.ErrTxClosed
	}

	// Attempt to fetch the ID for the child bucket. The bucket does not
	// exist if the bucket index entry does not exist.
	childKey := bucketizedKey(b.id, key)
	childID, ok := b.tx.db.buckets[string(childKey)]
	if !ok {
		return nil, store.ErrBucketNotFound
	}

	return &bucket{tx: b.tx, id: childID}, nil
}

// Buckets returns an iterator for nested buckets.
func (b *bucket) Buckets() iter.Seq2[[]byte, store.Bucket] {
	return func(yield func([]byte, store.Bucket) bool) {
		if b.tx.IsClosed() {
			return
		}

		// walk direct nested buckets
		prefix := store.UnsafeString(num.EncodeUvarint(b.id))
		for n, id := range b.tx.db.buckets {
			if !strings.HasPrefix(n, prefix) {
				continue
			}
			if !yield([]byte(n[len(prefix):]), &bucket{tx: b.tx, id: id}) {
				return
			}
		}
	}
}

// CreateBucket creates and returns a new nested bucket with the given key.
// If the bucket already exists it returns it without error.
func (b *bucket) CreateBucket(key []byte, _ ...store.BucketOption) (store.Bucket, error) {
	// Ensure transaction state is valid.
	if b.tx.IsClosed() {
		return nil, store.ErrTxClosed
	}

	// Ensure the transaction is writable.
	if !b.tx.IsWriteable() {
		return nil, store.ErrTxReadonly
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		return nil, store.ErrKeyRequired
	}

	// Check if bucket exists.
	bkey := bucketizedKey(b.id, key)
	if bid, ok := b.tx.db.buckets[string(bkey)]; ok {
		return &bucket{tx: b.tx, id: bid}, nil
	}

	// Find the appropriate next bucket ID to use for the new bucket.
	bid, err := b.tx.db.nextBucketID()
	if err != nil {
		return nil, err
	}

	// Add the new bucket to the bucket index.
	b.tx.db.buckets[string(bkey)] = bid

	return &bucket{tx: b.tx, id: bid}, nil
}

// DeleteBucket removes a nested bucket with the given key. T
func (b *bucket) DeleteBucket(key []byte) error {
	// Ensure transaction state is valid.
	if b.tx.IsClosed() {
		return store.ErrTxClosed
	}

	// Ensure the transaction is writable.
	if !b.tx.IsWriteable() {
		return store.ErrTxReadonly
	}

	// Attempt to fetch the ID for the child bucket. The bucket does not
	// exist if the bucket index entry does not exist.
	bkey := bucketizedKey(b.id, key)
	bid, ok := b.tx.db.buckets[string(bkey)]
	if !ok {
		return store.ErrBucketNotFound
	}

	// Remove all nested buckets and their keys.
	toRemove := []uint32{bid}
	for len(toRemove) > 0 {
		// pop next bucket id from stack
		id := toRemove[len(toRemove)-1]
		toRemove = toRemove[:len(toRemove)-1]

		// Delete all keys through tx.pending
		prefix := num.EncodeUvarint(id)
		for k := range b.tx.db.Scan(prefix) {
			b.tx.pending.Delete(k)
		}

		// Collect nested buckets.
		sprefix := store.UnsafeString(prefix)
		for n, v := range b.tx.db.buckets {
			if !strings.HasPrefix(n, sprefix) {
				continue
			}

			// Push id onto the stack for the next iteration.
			toRemove = append(toRemove, v)

			// Remove from bucket index.
			delete(b.tx.db.buckets, n)
		}
	}

	// Remove the outermost bucket from index last.
	delete(b.tx.db.buckets, string(bkey))

	return nil
}

// Put saves a key/value pair to the bucket. Keys that do not
// already exist are added and keys that already exist are overwritten.
func (b *bucket) Put(key, value []byte) error {
	// Ensure transaction state is valid.
	if b.tx.IsClosed() {
		return store.ErrTxClosed
	}

	// Ensure the transaction is writable.
	if !b.tx.IsWriteable() {
		return store.ErrTxReadonly
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		return store.ErrKeyRequired
	}

	// Add to tx changeset
	b.tx.put(bucketizedKey(b.id, key), value)

	return nil
}

// Get returns the value for the given key. Returns nil if the key does not
// exist in this bucket. An empty slice is returned for keys that exist but
// have no value assigned.
func (b *bucket) Get(key []byte) ([]byte, error) {
	// Ensure transaction state is valid.
	if b.tx.IsClosed() {
		return nil, store.ErrTxClosed
	}

	// Ignore empty keys.
	if len(key) == 0 {
		return nil, store.ErrKeyRequired
	}

	return b.tx.get(bucketizedKey(b.id, key))
}

// Delete removes the specified key from the bucket. Deleting a key that does
// not exist does not return an error.
func (b *bucket) Delete(key []byte) error {
	// Ensure transaction state is valid.
	if b.tx.IsClosed() {
		return store.ErrTxClosed
	}

	// Ensure the transaction is writable.
	if !b.tx.IsWriteable() {
		return store.ErrTxReadonly
	}

	// Nothing to do if there is no key.
	if len(key) == 0 {
		return store.ErrKeyRequired
	}

	// Register key for delete.
	b.tx.del(bucketizedKey(b.id, key))

	return nil
}

// Scan iterates over keys in a bucket in ascending order
// and returns a sequence of key/value pairs.
func (b *bucket) Scan(prefix []byte) iter.Seq2[[]byte, []byte] {
	if b.tx.IsClosed() {
		return func(fn func([]byte, []byte) bool) {} // noop sequence
	}

	// add bucket prefix id to prefix key
	prefix = bucketizedKey(b.id, prefix)

	// strip bucket id from key prefixes on return
	if b.tx.IsWriteable() {
		return store.TrimKeyPrefix(
			num.UvarintLen(b.id),
			btree.Merge2(
				b.tx.pending.Scan(prefix),
				b.tx.db.Scan(prefix),
			),
		)
	} else {
		return store.TrimKeyPrefix(
			num.UvarintLen(b.id),
			b.tx.db.Scan(prefix),
		)
	}
}

// ScanReverse iterates over keys in a bucket in descending order
// and returns a sequence of key/value pairs.
func (b *bucket) ScanReverse(prefix []byte) iter.Seq2[[]byte, []byte] {
	if b.tx.IsClosed() {
		return func(fn func([]byte, []byte) bool) {} // noop sequence
	}

	// add bucket prefix id to prefix key
	prefix = bucketizedKey(b.id, prefix)

	// strip bucket id from key prefixes on return
	if b.tx.IsWriteable() {
		return store.TrimKeyPrefix(
			num.UvarintLen(b.id),
			btree.Merge2R(
				b.tx.pending.ScanReverse(prefix),
				b.tx.db.ScanReverse(prefix),
			),
		)
	} else {
		return store.TrimKeyPrefix(
			num.UvarintLen(b.id),
			b.tx.db.ScanReverse(prefix),
		)
	}
}

// ScanRange iterates over an explicit range of keys starting at
// a lower bound (inclusive) and ending before an upper bound (exclusive)
// in ascending order.
func (b *bucket) ScanRange(start, end []byte) iter.Seq2[[]byte, []byte] {
	if b.tx.IsClosed() {
		return func(fn func([]byte, []byte) bool) {} // noop sequence
	}

	// add bucket prefix id to range keys
	if start != nil {
		start = bucketizedKey(b.id, start)
	}
	if end != nil {
		end = bucketizedKey(b.id, end)
	} else {
		end = store.NextKey(bucketizedKey(b.id, end))
	}

	// strip bucket id from key prefixes on return
	if b.tx.IsWriteable() {
		return store.TrimKeyPrefix(
			num.UvarintLen(b.id),
			btree.Merge2(
				b.tx.pending.ScanRange(start, end),
				b.tx.db.ScanRange(start, end),
			),
		)
	} else {
		return store.TrimKeyPrefix(
			num.UvarintLen(b.id),
			b.tx.db.ScanRange(start, end),
		)
	}
}

// ScanRangeReverse iterates over an explicit range of keys starting at
// a lower bound (inclusive) and ending before an upper bound (exclusive)
// in descending order.
func (b *bucket) ScanRangeReverse(start, end []byte) iter.Seq2[[]byte, []byte] {
	if b.tx.IsClosed() {
		return func(fn func([]byte, []byte) bool) {} // noop sequence
	}

	// add bucket prefix id to range keys
	if start != nil {
		start = bucketizedKey(b.id, start)
	}
	if end != nil {
		end = bucketizedKey(b.id, end)
	} else {
		end = store.NextKey(bucketizedKey(b.id, end))
	}

	// strip bucket id from key prefixes on return
	if b.tx.IsWriteable() {
		return store.TrimKeyPrefix(
			num.UvarintLen(b.id),
			btree.Merge2R(
				b.tx.pending.ScanRangeReverse(start, end),
				b.tx.db.ScanRangeReverse(start, end),
			),
		)
	} else {
		return store.TrimKeyPrefix(
			num.UvarintLen(b.id),
			b.tx.db.ScanRangeReverse(start, end),
		)
	}
}

// SearchGE returns the first key and its value that is greater or
// equal to the search key or an error when no key was found.
// It works like a binary search in ascending order.
func (b *bucket) SearchGE(key []byte) ([]byte, []byte, error) {
	if b.tx.IsClosed() {
		return nil, nil, store.ErrTxClosed
	}

	for k, v := range b.ScanRange(key, nil) {
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

	for k, v := range b.ScanRangeReverse(nil, store.NextKey(key)) {
		return k, v, nil
	}

	return nil, nil, store.ErrKeyNotFound
}

// nextBucketID returns the next bucket ID to use for creating a new bucket.
// This function must only be called from a writable transaction.
// Since it is an internal helper function, it does not check.
func (db *db) nextBucketID() (uint32, error) {
	// get and sort all bucket ids
	ids := slices.Collect(maps.Values(db.buckets))
	slices.Sort(ids)

	// all ids in use, generate next
	if l := len(ids); l-1 == int(ids[l-1]) {
		return uint32(l), nil
	}

	// find the first gap (Note: id 0 always exists)
	for i, id := range ids[1:] {
		if ids[i]+1 != id {
			return ids[i] + 1, nil
		}
	}

	return 0, store.ErrDatabaseFull
}

// bucketizedKey returns the actual key to use for storing and retrieving
// a key in the bucket. It prefixes user keys with the varint encoding of
// the bucket id. This encoding is unique and sortable.
// The serialized bucketized key format is: <varint(bucketid)><key>
func bucketizedKey(id uint32, key []byte) []byte {
	bkey := make([]byte, num.UvarintLen(id)+len(key))
	n := num.PutUvarint(bkey, uint64(id))
	n += copy(bkey[n:], key)
	return bkey[:n]
}

// Stats returns simple bucket statistics.
func (b *bucket) Stats() (stats store.BucketStats) {
	if b.tx.IsClosed() {
		return
	}

	// count direct nested buckets
	prefix := store.UnsafeString(num.EncodeUvarint(b.id))
	for n := range b.tx.db.buckets {
		if !strings.HasPrefix(n, prefix) {
			continue
		}
		stats.NBuckets++
	}

	// count all keys
	for k, v := range b.Scan(nil) {
		stats.NKeys++
		stats.NBytes += len(k) + len(v)
	}

	return
}
