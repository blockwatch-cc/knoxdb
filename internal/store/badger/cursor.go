// Copyright (c) 2018 - 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package badger

import (
	"bytes"

	"blockwatch.cc/knoxdb/internal/store"
	"github.com/dgraph-io/badger/v4"
)

// PrefetchValues: true,
// PrefetchSize:   100,
// Reverse:        false,
// AllVersions:    false,
var defaultIteratorOpts = badger.DefaultIteratorOptions

// cursor is an internal type used to represent a cursor over key/value pairs
// and nested buckets of a bucket and implements the store.Cursor interface.
type cursor struct {
	bucket      *bucket
	currentIter *badger.Iterator
	keyRange    *store.Range
}

// Enforce cursor implements the store.Cursor interface.
var _ store.Cursor = (*cursor)(nil)

// Bucket returns the bucket the cursor was created for.
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) Bucket() store.Bucket {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	return c.bucket
}

func (c *cursor) Close() {
	if c.currentIter != nil {
		c.currentIter.Close()
		c.currentIter = nil
	}
}

// Delete removes the current key/value pair the cursor is at without
// invalidating the cursor.
//
// Returns the following errors as required by the interface contract:
//   - ErrIncompatibleValue if attempted when the cursor points to a nested
//     bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) Delete() error {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return err
	}

	// Error if the cursor is exhausted.
	if !c.currentIter.Valid() {
		str := "cursor is exhausted"
		return makeDbErr(store.ErrIncompatibleValue, str, nil)
	}

	// Do not allow buckets to be deleted via the cursor.
	key := c.currentIter.Item().Key()
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		str := "buckets may not be deleted from a cursor"
		return makeDbErr(store.ErrIncompatibleValue, str, nil)
	}

	c.bucket.tx.deleteKey(copySlice(key))
	return nil
}

// First positions the cursor at the first key/value pair and returns whether or
// not the pair exists.
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) First() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Seek to the first key.
	c.currentIter.Seek(c.keyRange.Start)
	return c.currentIter.Valid() && c.currentIter.ValidForPrefix(c.keyRange.Start)
}

// Not supported.
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) Last() bool {
	return false
}

// Next moves the cursor one key/value pair forward and returns whether or not
// the pair exists.
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) Next() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Nothing to return if cursor is exhausted.
	if !c.currentIter.Valid() {
		return false
	}

	// Move the current iterator to the next entry.
	c.currentIter.Next()

	if !c.currentIter.Valid() {
		return false
	}

	// check iterator range
	return bytes.Compare(c.currentIter.Item().Key(), c.keyRange.Limit) <= 0
}

// Not supported
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) Prev() bool {
	return false
}

// Seek positions the cursor at the first key/value pair that is greater than or
// equal to the passed seek key.  Returns false if no suitable key was found.
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) Seek(seek []byte) bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Seek to the provided key in both the database and pending iterators
	// then choose the iterator that is both valid and has the larger key.
	// seekKey := append(bucketizedKey(c.bucket.id, c.keyRange.Start), seek...)
	c.currentIter.Seek(bucketizedKey(c.bucket.id, seek))
	return c.currentIter.Valid()
}

// rawKey returns the current key the cursor is pointing to without stripping
// the current bucket prefix or bucket index prefix.
func (c *cursor) rawKey() []byte {
	// Nothing to return if cursor is exhausted.
	if !c.currentIter.Valid() {
		return nil
	}

	return c.currentIter.Item().KeyCopy(nil)
}

// Key returns the current key the cursor is pointing to.
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) Key() []byte {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	// Nothing to return if cursor is exhausted.
	if !c.currentIter.Valid() {
		return nil
	}

	// Slice out the actual key name and make a copy since it is no longer
	// valid after iterating to the next item.
	//
	// The key is after the bucket index prefix and parent ID when the
	// cursor is pointing to a nested bucket.
	key := c.currentIter.Item().KeyCopy(nil)
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		return key[len(bucketIndexPrefix)+bucketIdLen:]
	}

	// The key is after the bucket ID when the cursor is pointing to a
	// normal entry.
	return key[len(c.bucket.id):]
}

// rawValue returns the current value the cursor is pointing to without
// stripping and without filtering bucket index values.
func (c *cursor) rawValue() []byte {
	// Nothing to return if cursor is exhausted.
	if !c.currentIter.Valid() {
		return nil
	}

	val, err := c.currentIter.Item().ValueCopy(nil)
	if err != nil {
		return nil
	}

	return val
}

// Value returns the current value the cursor is pointing to.  This will be nil
// for nested buckets.
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) Value() []byte {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	// Nothing to return if cursor is exhausted.
	if !c.currentIter.Valid() {
		return nil
	}

	// Return nil for the value when the cursor is pointing to a nested
	// bucket.
	key := c.currentIter.Item().Key()
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		return nil
	}

	val, err := c.currentIter.Item().ValueCopy(nil)
	if err != nil {
		return nil
	}

	return val
}

// cursorType defines the type of cursor to create.
type cursorType int

// The following constants define the allowed cursor types.
const (
	// ctKeys iterates through all of the keys in a given bucket.
	ctKeys cursorType = iota

	// ctBuckets iterates through all directly nested buckets in a given
	// bucket.
	ctBuckets
)

// newCursor returns a new cursor for the given bucket, bucket ID, and cursor
// type.
//
// NOTE: The caller is responsible for calling the cursorFinalizer function on
// the returned cursor.
func newCursor(b *bucket, bucketID []byte, cursorTyp cursorType, o store.CursorOptions) *cursor {
	var (
		iter     *badger.Iterator
		keyRange *store.Range
	)

	opts := badger.IteratorOptions{
		PrefetchSize:   o.PrefetchSize,
		PrefetchValues: o.PrefetchValues,
		Reverse:        o.Reverse,
	}

	switch cursorTyp {
	case ctKeys:
		keyRange = store.BytesPrefix(bucketID)
		iter = b.tx.tx.NewIterator(opts)

	case ctBuckets:
		// The serialized bucket index key format is:
		//   <bucketindexprefix><parentbucketid><bucketname>

		// Create an iterator for the database prefixed by the bucket
		// index identifier and the provided bucket ID.
		prefix := make([]byte, len(bucketIndexPrefix)+bucketIdLen*2)
		copy(prefix, metadataBucketID[:])
		copy(prefix, bucketIndexPrefix)
		copy(prefix[len(bucketIndexPrefix):], bucketID)
		keyRange = store.BytesPrefix(prefix)
		iter = b.tx.tx.NewIterator(opts)
	}

	// Create the cursor using the iterators.
	return &cursor{bucket: b, currentIter: iter, keyRange: keyRange}
}
