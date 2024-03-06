// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"bytes"

	"blockwatch.cc/knoxdb/store"
	bolt "go.etcd.io/bbolt"
)

// cursor is an internal type used to represent a cursor over key/value pairs
// and nested buckets of a bucket and implements the store.Cursor interface.
type cursor struct {
	bucket      *bucket
	currentIter *bolt.Cursor
	keyRange    *store.Range
	key         []byte
	val         []byte
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

// Close
func (c *cursor) Close() {
	c.bucket = nil
	c.currentIter = nil
	c.keyRange = nil
	c.key = nil
	c.val = nil
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

	if err := c.currentIter.Delete(); err != nil {
		return convertErr("delete on cursor", err)
	}
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

	// Nothing to return if cursor is exhausted.
	if c.currentIter == nil {
		return false
	}

	var k, v []byte
	if c.keyRange != nil {
		k, v = c.currentIter.Seek(c.keyRange.Start)
	} else {
		k, v = c.currentIter.First()
	}
	c.key = copySlice(k)
	c.val = copySlice(v)
	return c.key != nil
}

// Not supported.
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) Last() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Nothing to return if cursor is exhausted.
	if c.currentIter == nil {
		return false
	}

	var k, v []byte
	if c.keyRange != nil {
		_, _ = c.currentIter.Seek(c.keyRange.Limit)
		k, v = c.currentIter.Prev()
	} else {
		k, v = c.currentIter.Last()
	}

	c.key = copySlice(k)
	c.val = copySlice(v)
	return c.key != nil
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
	k, v := c.currentIter.Next()
	if c.keyRange != nil && bytes.Compare(k, c.keyRange.Limit) <= 0 {
		k, v = nil, nil
	}

	c.key = copySlice(k)
	c.val = copySlice(v)
	return c.key != nil
}

// Not supported
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) Prev() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Nothing to return if cursor is exhausted.
	if c.currentIter == nil {
		return false
	}

	// Move the current iterator to the next entry.
	k, v := c.currentIter.Prev()
	if c.keyRange != nil && bytes.Compare(k, c.keyRange.Start) < 0 {
		k, v = nil, nil
	}

	c.key = copySlice(k)
	c.val = copySlice(v)
	return c.key != nil
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

	if c.keyRange != nil {
		seek = append(c.keyRange.Start, seek...)
	}

	k, v := c.currentIter.Seek(seek)
	c.key = copySlice(k)
	c.val = copySlice(v)
	return c.key != nil
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

	return c.key
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

	return c.val
}
