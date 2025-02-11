// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package mem

import (
	"bytes"

	"blockwatch.cc/knoxdb/internal/store"
)

// cursor is an internal type used to represent a cursor over key/value pairs
// and nested buckets of a bucket and implements the store.Cursor interface.
type cursor struct {
	bucket   *bucket
	current  []byte
	keyRange *store.Range
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
	c.keyRange = nil
	c.current = nil
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

	// Ensure the transaction is writable.
	if !c.bucket.tx.writable {
		str := "create bucket requires a writable database transaction"
		return makeDbErr(store.ErrTxNotWritable, str)
	}

	// Error if the cursor is exhausted.
	if c.current == nil {
		str := "cursor is exhausted"
		return makeDbErr(store.ErrIncompatibleValue, str)
	}

	// mark for deletion
	c.bucket.tx.deletes[string(c.current)] = struct{}{}
	return nil
}

// First positions the cursor at the first key/value pair and returns whether or
// not the pair exists. Only committed database entries are visited, keys updated
// or removed during the current transaction are ignored.
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) First() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Seek to the first key.
	c.current = nil
	c.bucket.tx.db.store.AscendGreaterOrEqual(Item{c.keyRange.Start, nil}, func(t Item) bool {
		if bytes.HasPrefix(c.current, c.keyRange.Start) {
			c.current = t.Key
		} else {
			c.current = nil
		}
		return false
	})

	return len(c.current) > 0
}

// Last moves the cursor at the last key/value pair and returns whether or not
// the pair exists.
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) Last() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Seek to the last key.
	c.current = nil
	c.bucket.tx.db.store.DescendLessOrEqual(Item{c.keyRange.Limit, nil}, func(t Item) bool {
		// skip limit value if it exists
		if bytes.Equal(t.Key, c.keyRange.Limit) {
			return true
		}

		// use next lower value if prefix matches
		if bytes.HasPrefix(t.Key, c.keyRange.Start) {
			c.current = t.Key
		} else {
			c.current = nil
		}
		return false
	})

	return len(c.current) > 0
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
	if c.current == nil {
		return false
	}

	// Move the current iterator to the next entry.
	c.bucket.tx.db.store.AscendGreaterOrEqual(Item{c.current, nil}, func(t Item) bool {
		// skip the current value
		if bytes.Equal(t.Key, c.current) {
			return true
		}
		// use next value if prefix matches
		if bytes.Compare(t.Key, c.keyRange.Limit) <= 0 {
			c.current = t.Key
		} else {
			c.current = nil
		}
		return false
	})

	return len(c.current) > 0
}

// This function is part of the store.Cursor interface implementation.
func (c *cursor) Prev() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Nothing to return if cursor is exhausted.
	if c.current == nil {
		return false
	}

	// Move the current iterator to the next entry.
	c.bucket.tx.db.store.DescendLessOrEqual(Item{c.current, nil}, func(t Item) bool {
		// skip the current value
		if bytes.Equal(t.Key, c.current) {
			return true
		}
		if bytes.Compare(t.Key, c.keyRange.Start) >= 0 {
			c.current = t.Key
		} else {
			c.current = nil
		}
		return false
	})

	return len(c.current) > 0
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

	// Seek to the provided key in both the database and pending updates
	seek = bucketizedKey(c.bucket.id, seek)
	c.bucket.tx.db.store.AscendGreaterOrEqual(Item{seek, nil}, func(t Item) bool {
		if bytes.HasPrefix(t.Key, seek) {
			c.current = t.Key
		} else {
			c.current = nil
		}
		return false
	})

	return len(c.current) > 0
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
	if c.current == nil {
		return nil
	}

	// Slice out the key suffix name. Note this key is only valid
	// until the cursor is updated.
	return c.current[len(c.bucket.id):]
}

// Value returns the current value the cursor is pointing to.
//
// This function is part of the store.Cursor interface implementation.
func (c *cursor) Value() []byte {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	// Nothing to return if cursor is exhausted.
	if c.current == nil {
		return nil
	}

	item, _ := c.bucket.tx.db.store.Get(Item{c.current, nil})
	return item.Val
}

// newCursor returns a new cursor for the given bucket, and key prefix.
//
// NOTE: The caller is responsible for calling the cursorFinalizer function on
// the returned cursor.
func newCursor(b *bucket, prefix []byte) *cursor {
	return &cursor{bucket: b, keyRange: store.BytesPrefix(prefix)}
}
