// Copyright (c) 2018-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"bytes"

	"blockwatch.cc/knoxdb/pkg/store"
	bolt "go.etcd.io/bbolt"
)

// cursor is an internal type used to represent a cursor over key/value pairs
// and nested buckets of a bucket and implements the store.Cursor interface.
type cursor struct {
	bucket   *bucket
	it       *bolt.Cursor
	keyRange *store.Range
	key      []byte
	val      []byte
}

// Enforce cursor implements the store.Cursor interface.
var _ store.Cursor = (*cursor)(nil)

// Bucket returns the bucket the cursor was created for.
func (c *cursor) Bucket() store.Bucket {
	return c.bucket
}

// Close
func (c *cursor) Close() {
	c.bucket = nil
	c.it = nil
	c.keyRange = nil
	c.key = nil
	c.val = nil
}

// Delete removes the current key/value pair the cursor is at without
// invalidating the cursor.
func (c *cursor) Delete() error {
	// Error if the cursor is exhausted.
	if c.it == nil {
		return store.ErrInvalidCursor
	}
	return wrap(c.it.Delete())
}

// First positions the cursor at the first key/value pair and returns whether or
// not the pair exists.
func (c *cursor) First() bool {
	// Nothing to return if cursor is exhausted.
	if c.it == nil {
		return false
	}

	if c.keyRange != nil {
		c.key, c.val = c.it.Seek(c.keyRange.Start)
	} else {
		c.key, c.val = c.it.First()
	}
	return c.key != nil
}

// Last moves the cursor at the last key/value pair and returns whether or not
// the pair exists.
func (c *cursor) Last() bool {
	// Nothing to return if cursor is exhausted.
	if c.it == nil {
		return false
	}

	if c.keyRange != nil {
		_, _ = c.it.Seek(c.keyRange.Limit)
		c.key, c.val = c.it.Prev()
	} else {
		c.key, c.val = c.it.Last()
	}

	return c.key != nil
}

// Next moves the cursor one key/value pair forward and returns whether or not
// the pair exists.
func (c *cursor) Next() bool {
	// Nothing to return if cursor is exhausted.
	if c.it == nil {
		return false
	}

	// Move the current iterator to the next entry.
	c.key, c.val = c.it.Next()
	if c.keyRange != nil && bytes.Compare(c.key, c.keyRange.Limit) <= 0 {
		c.key, c.val = nil, nil
	}

	return c.key != nil
}

// This function is part of the store.Cursor interface implementation.
func (c *cursor) Prev() bool {
	// Nothing to return if cursor is exhausted.
	if c.it == nil {
		return false
	}

	// Move the current iterator to the next entry.
	c.key, c.val = c.it.Prev()
	if c.keyRange != nil && bytes.Compare(c.key, c.keyRange.Start) < 0 {
		c.key, c.val = nil, nil
	}

	return c.key != nil
}

// Seek positions the cursor at the first key/value pair that is greater than or
// equal to the passed seek key.  Returns false if no suitable key was found.
func (c *cursor) Seek(seek []byte) bool {
	if c.keyRange != nil {
		seek = append(c.keyRange.Start, seek...)
	}

	c.key, c.val = c.it.Seek(seek)
	return c.key != nil
}

// Key returns the current key the cursor is pointing to.
func (c *cursor) Key() []byte {
	// Nothing to return if cursor is exhausted.
	if c.it == nil {
		return nil
	}

	return c.key
}

// Value returns the current value the cursor is pointing to.  This will be nil
// for nested buckets.
func (c *cursor) Value() []byte {
	// Nothing to return if cursor is exhausted.
	if c.it == nil {
		return nil
	}

	return c.val
}
