// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"bytes"

	"blockwatch.cc/knoxdb/internal/store"
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

	if c.keyRange != nil {
		c.key, c.val = c.currentIter.Seek(c.keyRange.Start)
	} else {
		c.key, c.val = c.currentIter.First()
	}
	return c.key != nil
}

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

	if c.keyRange != nil {
		_, _ = c.currentIter.Seek(c.keyRange.Limit)
		c.key, c.val = c.currentIter.Prev()
	} else {
		c.key, c.val = c.currentIter.Last()
	}

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
	c.key, c.val = c.currentIter.Next()
	if c.keyRange != nil && bytes.Compare(c.key, c.keyRange.Limit) <= 0 {
		c.key, c.val = nil, nil
	}

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
	c.key, c.val = c.currentIter.Prev()
	if c.keyRange != nil && bytes.Compare(c.key, c.keyRange.Start) < 0 {
		c.key, c.val = nil, nil
	}

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

	c.key, c.val = c.currentIter.Seek(seek)
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
