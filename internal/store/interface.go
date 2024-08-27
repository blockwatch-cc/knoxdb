// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// Parts of this interface were inspired heavily by the excellent boltdb project
// at https://github.com/boltdb/bolt by Ben B. Johnson.

package store

import (
	"context"
	"fmt"
	"io"
	"time"
)

// Cursor represents a cursor over key/value pairs and nested buckets of a
// bucket.
//
// Note that open cursors are not tracked on bucket changes and any
// modifications to the bucket, with the exception of Cursor.Delete, invalidates
// the cursor.  After invalidation, the cursor must be repositioned, or the keys
// and values returned may be unpredictable. Keys added or removed during the
// current transaction may not be visible to cursors depending on the
// underlying database implementation.
type Cursor interface {
	// Bucket returns the bucket the cursor was created for.
	Bucket() Bucket

	// Delete removes the current key/value pair the cursor is at without
	// invalidating the cursor.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//   - ErrIncompatibleValue if attempted when the cursor points to a
	//     nested bucket
	//   - ErrTxNotWritable if attempted against a read-only transaction
	//   - ErrTxClosed if the transaction has already been closed
	Delete() error

	// First positions the cursor at the first key/value pair and returns
	// whether or not the pair exists.
	First() bool

	// Last positions the cursor at the last key/value pair and returns
	// whether or not the pair exists.
	Last() bool

	// Next moves the cursor one key/value pair forward and returns whether
	// or not the pair exists.
	Next() bool

	// Prev moves the cursor one key/value pair backward and returns whether
	// or not the pair exists.
	Prev() bool

	// Seek positions the cursor at the first key/value pair that is greater
	// than or equal to the passed seek key.  Returns whether or not such key
	// exists.
	Seek(seek []byte) bool

	// Key returns the current key the cursor is pointing to.
	Key() []byte

	// Value returns the current value the cursor is pointing to.  This will
	// be nil for nested buckets.
	Value() []byte

	// Close must be called to closes each open cursor before the transaction
	// commits or rolls back.
	Close()
}

type CursorOptions struct {
	PrefetchSize   int
	PrefetchValues bool
	Reverse        bool
}

var (
	ForwardCursor = CursorOptions{
		PrefetchSize:   100,
		PrefetchValues: true,
	}
	ReverseCursor = CursorOptions{
		PrefetchSize:   100,
		PrefetchValues: true,
		Reverse:        true,
	}
	IndexCursor = CursorOptions{
		PrefetchSize:   16,
		PrefetchValues: false,
	}
)

type BucketStats struct {
	KeyN    int // number of keys/value pairs
	BucketN int // total number of buckets including the top bucket
	Size    int // total bucket size in bytes
}

// Bucket represents a collection of key/value pairs.
type Bucket interface {
	// Bucket retrieves a nested bucket with the given key.  Returns nil if
	// the bucket does not exist.
	Bucket(key []byte) Bucket

	// CreateBucket creates and returns a new nested bucket with the given
	// key.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//   - ErrBucketExists if the bucket already exists
	//   - ErrBucketNameRequired if the key is empty
	//   - ErrIncompatibleValue if the key is otherwise invalid for the
	//     particular implementation
	//   - ErrTxNotWritable if attempted against a read-only transaction
	//   - ErrTxClosed if the transaction has already been closed
	CreateBucket(key []byte) (Bucket, error)

	// CreateBucketIfNotExists creates and returns a new nested bucket with
	// the given key if it does not already exist.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//   - ErrBucketNameRequired if the key is empty
	//   - ErrIncompatibleValue if the key is otherwise invalid for the
	//     particular implementation
	//   - ErrTxNotWritable if attempted against a read-only transaction
	//   - ErrTxClosed if the transaction has already been closed
	CreateBucketIfNotExists(key []byte) (Bucket, error)

	// DeleteBucket removes a nested bucket with the given key.  This also
	// includes removing all nested buckets and keys under the bucket being
	// deleted.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//   - ErrBucketNotFound if the specified bucket does not exist
	//   - ErrTxNotWritable if attempted against a read-only transaction
	//   - ErrTxClosed if the transaction has already been closed
	DeleteBucket(key []byte) error

	// ForEach invokes the passed function with every key/value pair in the
	// bucket.  This does not include nested buckets or the key/value pairs
	// within those nested buckets.
	//
	// WARNING: It is not safe to mutate data while iterating with this
	// method.  Doing so may cause the underlying cursor to be invalidated
	// and return unexpected keys and/or values.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//   - ErrTxClosed if the transaction has already been closed
	//
	// NOTE: The slices returned by this function are only valid during a
	// transaction.  Attempting to access them after a transaction has ended
	// results in undefined behavior.  Additionally, the slices must NOT
	// be modified by the caller.  These constraints prevent additional data
	// copies and allows support for memory-mapped database implementations.
	ForEach(func(k, v []byte) error) error

	// ForEachBucket invokes the passed function with the key of every
	// nested bucket in the current bucket.  This does not include any
	// nested buckets within those nested buckets.
	//
	// WARNING: It is not safe to mutate data while iterating with this
	// method.  Doing so may cause the underlying cursor to be invalidated
	// and return unexpected keys and/or values.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//   - ErrTxClosed if the transaction has already been closed
	//
	// NOTE: The keys returned by this function are only valid during a
	// transaction.  Attempting to access them after a transaction has ended
	// results in undefined behavior.  This constraint prevents additional
	// data copies and allows support for memory-mapped database
	// implementations.
	ForEachBucket(func(k []byte, b Bucket) error) error

	// Cursor returns a new cursor, allowing for iteration over the bucket's
	// key/value pairs and nested buckets in forward or backward order.
	//
	// You must seek to a position using the First, Last, or Seek functions
	// before calling the Next, Prev, Key, or Value functions.  Failure to
	// do so will result in the same return values as an exhausted cursor,
	// which is false for the Prev and Next functions and nil for Key and
	// Value functions.
	Cursor(...CursorOptions) Cursor

	// Range returns a new ranged cursor, allowing for iteration over the
	// bucket's key/value pairs (and nested buckets) that satisfy the prefix
	// condition in forward or backward order.
	//
	// You must seek to a position using the First, Last, or Seek functions
	// before calling the Next, Prev, Key, or Value functions.  Failure to
	// do so will result in the same return values as an exhausted cursor,
	// which is false for the Prev and Next functions and nil for Key and
	// Value functions.
	Range(prefix []byte, opts ...CursorOptions) Cursor

	// Writable returns whether or not the bucket is writable.
	Writable() bool

	// Put saves the specified key/value pair to the bucket.  Keys that do
	// not already exist are added and keys that already exist are
	// overwritten.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//   - ErrKeyRequired if the key is empty
	//   - ErrIncompatibleValue if the key is the same as an existing bucket
	//   - ErrTxNotWritable if attempted against a read-only transaction
	//   - ErrTxClosed if the transaction has already been closed
	//
	// NOTE: The slices passed to this function must NOT be modified by the
	// caller.  This constraint prevents the requirement for additional data
	// copies and allows support for memory-mapped database implementations.
	Put(key, value []byte) error

	// Get returns the value for the given key.  Returns nil if the key does
	// not exist in this bucket.  An empty slice is returned for keys that
	// exist but have no value assigned.
	//
	// NOTE: The value returned by this function is only valid during a
	// transaction.  Attempting to access it after a transaction has ended
	// results in undefined behavior.  Additionally, the value must NOT
	// be modified by the caller.  These constraints prevent additional data
	// copies and allows support for memory-mapped database implementations.
	Get(key []byte) []byte

	// Delete removes the specified key from the bucket.  Deleting a key
	// that does not exist does not return an error.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//   - ErrKeyRequired if the key is empty
	//   - ErrIncompatibleValue if the key is the same as an existing bucket
	//   - ErrTxNotWritable if attempted against a read-only transaction
	//   - ErrTxClosed if the transaction has already been closed
	Delete(key []byte) error

	// Unique sequence associated with a bucket. Use it to generate unique
	// id's for items inside the bucket.
	NextSequence() (uint64, error)

	// FillPercent lets a caller set the fill percentage for a bucket to optimize
	// storage allocation when keys are known to be sequential.
	FillPercent(float64)

	// Stats returns bucket statistics
	Stats() BucketStats
}

type Sequence interface {
	Next() (uint64, error)
	Release() error
}

// Tx represents a database transaction.  It can either by read-only or
// read-write.  The transaction provides a metadata bucket against which all
// read and writes occur.
//
// As would be expected with a transaction, no changes will be saved to the
// database until it has been committed.  The transaction will only provide a
// view of the database at the time it was created.  Transactions should not be
// long running operations.
type Tx interface {
	// Root returns the top-most root bucket for all metadata storage.
	Root() Bucket

	// Bucket returns the bucket with given name.
	Bucket(key []byte) Bucket

	// Manifest returns the current database manifest metadata.
	Manifest() (Manifest, error)

	// SetManifest overwrites the current database manifest.
	SetManifest(manifest Manifest) error

	// Commit commits all changes that have been made to the metadata or
	// block storage.  Depending on the backend implementation this could be
	// to a cache that is periodically synced to persistent storage or
	// directly to persistent storage.  In any case, all transactions which
	// are started after the commit finishes will include all changes made
	// by this transaction.  Calling this function on a managed transaction
	// will result in a panic.
	Commit() error

	// Rollback undoes all changes that have been made to the metadata or
	// block storage.  Calling this function on a managed transaction will
	// result in a panic.
	Rollback() error

	// DB returns the current database for the transaction.
	DB() DB
}

type Manifest struct {
	Name      string    `json:"name"`
	Version   int       `json:"version"`
	Label     string    `json:"label"`
	Schema    string    `json:"schema"`
	CreatedAt time.Time `json:"createdAt"`
}

func (m Manifest) IsValid() bool {
	return m.Version > 0
}

func (m Manifest) Validate(name, label, schema string, version int) error {
	if name != "*" && name != m.Name {
		return fmt.Errorf("manifest name: expected=%s, actual=%s", name, m.Name)
	}
	if label != "*" && label != m.Label {
		return fmt.Errorf("manifest label: expected=%s, actual=%s", label, m.Label)
	}
	if schema != "*" && schema != m.Schema {
		return fmt.Errorf("manifest schema: expected=%s, actual=%s", schema, m.Schema)
	}
	if version > 0 && version != m.Version {
		return fmt.Errorf("manifest version: expected=%s, actual=%s", version, m.Version)
	}
	return nil
}

// DB provides a generic interface that is used to store KV indexes and KV data.
// This interface is intended to be agnostic to the actual mechanism
// used for backend data storage.  The RegisterDriver function can be used to
// add a new backend data storage method.
//
// This interface is divided into two distinct categories of functionality.
//
// The first category is atomic metadata storage with bucket support.  This is
// accomplished through the use of database transactions.
//
// The second category is generic block storage.  This functionality is
// intentionally separate because the mechanism used for block storage may or
// may not be the same mechanism used for metadata storage.  For example, it is
// often more efficient to store the block data as flat files while the metadata
// is kept in a database.  However, this interface aims to be generic enough to
// support blocks in the database too, if needed by a particular backend.
type DB interface {
	// Type returns the database driver type the current database instance
	// was created with.
	Type() string

	// Path returns the path where the current database is stored.
	Path() string

	// IsReadOnly returns true if the database was opened in readonly mode
	// and no write transactions are supported.
	IsReadOnly() bool

	// IsZeroCopyRead returns true if keys and values on Get and from Cursors
	// are NOT safe to use without copy.
	IsZeroCopyRead() bool

	// Manifest returns the current database manifest metadata.
	Manifest() (Manifest, error)

	// SetManifest overwrites the current database manifest.
	SetManifest(manifest Manifest) error

	// Begin starts a transaction which is either read-only or read-write
	// depending on the specified flag.  Multiple read-only transactions
	// can be started simultaneously while only a single read-write
	// transaction can be started at a time.  The call will block when
	// starting a read-write transaction when one is already open.
	//
	// NOTE: The transaction must be closed by calling Rollback or Commit on
	// it when it is no longer needed.  Failure to do so can result in
	// unclaimed memory and/or inablity to close the database due to locks
	// depending on the specific database implementation.
	Begin(writable bool) (Tx, error)

	// View invokes the passed function in the context of a managed
	// read-only transaction.  Any errors returned from the user-supplied
	// function are returned from this function.
	//
	// Calling Rollback or Commit on the transaction passed to the
	// user-supplied function will result in a panic.
	View(fn func(tx Tx) error) error

	// Update invokes the passed function in the context of a managed
	// read-write transaction.  Any errors returned from the user-supplied
	// function will cause the transaction to be rolled back and are
	// returned from this function.  Otherwise, the transaction is committed
	// when the user-supplied function returns a nil error.
	//
	// Calling Rollback or Commit on the transaction passed to the
	// user-supplied function will result in a panic.
	Update(fn func(tx Tx) error) error

	// Create a new sequence to generate sequential ids. Remember to close
	// a sequence by calling Release().
	Sequence(key []byte, lease uint64) (Sequence, error)

	// Triggers a full garbage collection cycle, removing deleted keys and old
	// versions of values from the underlying database. This process is synchronous.
	// It first waits until all concurrent transactions are closed before it run
	// file compaction and reclaims free space. This may take a long time
	// and generate spikes in iops while data files are rewritten on disk.
	GC(ctx context.Context, ratio float64) error

	// Writes the entire database to io.Writer. During this process the
	// database may or may not be available for concurrent read and write
	// transaction depending on the driver.
	Dump(io.Writer) error

	// Restores database from io.Reader. During this process the database
	// is unavailable for concurrent read and write transactions.
	Restore(io.Reader) error

	// Close cleanly shuts down the database and syncs all data.  It will
	// block until all database transactions have been finalized (rolled
	// back or committed).
	Close() error
}
