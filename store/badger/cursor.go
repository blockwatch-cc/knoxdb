// Copyright (c) 2018 - 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package badger

import (
	"bytes"

	"blockwatch.cc/knoxdb/store"
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
	if c.currentIter == nil {
		str := "cursor is exhausted"
		return makeDbErr(store.ErrIncompatibleValue, str, nil)
	}

	// Do not allow buckets to be deleted via the cursor.
	key := c.currentIter.Item().Key()
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		str := "buckets may not be deleted from a cursor"
		return makeDbErr(store.ErrIncompatibleValue, str, nil)
	}

	c.bucket.tx.deleteKey(copySlice(key), true)
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
	return c.currentIter.Valid()
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
	if c.currentIter == nil {
		return false
	}

	// Move the current iterator to the next entry.
	c.currentIter.Next()

	// check iterator range
	if bytes.Compare(c.currentIter.Item().Key(), c.keyRange.Limit) > 0 {
		return false
	}

	return c.currentIter.Valid()
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
	seekKey := bucketizedKey(c.bucket.id, seek)
	c.currentIter.Seek(seekKey)
	return c.currentIter.Valid()
}

// rawKey returns the current key the cursor is pointing to without stripping
// the current bucket prefix or bucket index prefix.
func (c *cursor) rawKey() []byte {
	// Nothing to return if cursor is exhausted.
	if c.currentIter == nil {
		return nil
	}

	return copySlice(c.currentIter.Item().Key())
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
	if c.currentIter == nil {
		return nil
	}

	// Slice out the actual key name and make a copy since it is no longer
	// valid after iterating to the next item.
	//
	// The key is after the bucket index prefix and parent ID when the
	// cursor is pointing to a nested bucket.
	key := c.currentIter.Item().Key()
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		key = key[len(bucketIndexPrefix)+bucketIdLen:]
		return copySlice(key)
	}

	// The key is after the bucket ID when the cursor is pointing to a
	// normal entry.
	key = key[len(c.bucket.id):]
	return copySlice(key)
}

// rawValue returns the current value the cursor is pointing to without
// stripping without filtering bucket index values.
func (c *cursor) rawValue() []byte {
	// Nothing to return if cursor is exhausted.
	if c.currentIter == nil {
		return nil
	}

	val, err := c.currentIter.Item().ValueCopy(nil)
	if err != nil {
		log.Tracef("value read error for key %s: %v",
			c.currentIter.Item().Key(), err)
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
	if c.currentIter == nil {
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
		log.Tracef("value read error for key %s: %v",
			c.currentIter.Item().Key(), err)
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

	// ctFull iterates through both the keys and the directly nested buckets
	// in a given bucket.
	// ctFull
)

// cursorFinalizer is either invoked when a cursor is being garbage collected or
// called manually to ensure the underlying cursor iterators are released.
func cursorFinalizer(c *cursor) {
	c.currentIter.Close()
	c.currentIter = nil
}

// newCursor returns a new cursor for the given bucket, bucket ID, and cursor
// type.
//
// NOTE: The caller is responsible for calling the cursorFinalizer function on
// the returned cursor.
func newCursor(b *bucket, bucketID []byte, cursorTyp cursorType) *cursor {
	var (
		iter     *badger.Iterator
		keyRange *store.Range
	)

	switch cursorTyp {
	case ctKeys:
		keyRange = store.BytesPrefix(bucketID)
		iter = b.tx.tx.NewIterator(defaultIteratorOpts)

	case ctBuckets:
		// The serialized bucket index key format is:
		//   <bucketindexprefix><parentbucketid><bucketname>

		// Create an iterator for the database prefixed by the bucket
		// index identifier and the provided bucket ID.
		prefix := make([]byte, len(bucketIndexPrefix)+bucketIdLen)
		copy(prefix, bucketIndexPrefix)
		copy(prefix[len(bucketIndexPrefix):], bucketID)
		keyRange = store.BytesPrefix(prefix)
		iter = b.tx.tx.NewIterator(defaultIteratorOpts)

		// case ctFull:
		// unsupported
		//  fallthrough
		// default:
		//  // The serialized bucket index key format is:
		//  //   <bucketindexprefix><parentbucketid><bucketname>
		//  prefix := make([]byte, len(bucketIndexPrefix)+bucketIdLen)
		//  copy(prefix, bucketIndexPrefix)
		//  copy(prefix[len(bucketIndexPrefix):], bucketID)
		//  bucketRange := util.BytesPrefix(prefix)
		//  keyRange := util.BytesPrefix(bucketID)

		//  // Since both keys and buckets are needed from the database,
		//  // create an individual iterator for each prefix and then create
		//  // a merged iterator from them.
		//  dbKeyIter := b.tx.snapshot.NewIterator(keyRange)
		//  dbBucketIter := b.tx.snapshot.NewIterator(bucketRange)
		//  iters := []iterator.Iterator{dbKeyIter, dbBucketIter}
		//  dbIter = iterator.NewMergedIterator(iters,
		//      comparer.DefaultComparer, true)

		//  // Since both keys and buckets are needed from the pending keys,
		//  // create an individual iterator for each prefix and then create
		//  // a merged iterator from them.
		//  // pendingKeyIter := newLdbTreapIter(b.tx, keyRange)
		//  // pendingBucketIter := newLdbTreapIter(b.tx, bucketRange)
		//  // iters = []iterator.Iterator{pendingKeyIter, pendingBucketIter}
		//  // pendingIter = iterator.NewMergedIterator(iters,
		//  //  comparer.DefaultComparer, true)
	}

	// Create the cursor using the iterators.
	return &cursor{bucket: b, currentIter: iter, keyRange: keyRange}
}
