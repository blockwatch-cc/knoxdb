// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store

import (
	"io"
	"iter"
)

// DB provides a generic interface for key/value datastores. It attempts
// to remain independent from custom features to support a broad range of
// backends (on-disk LSM and btree engines, caches, servers). Users can
// call RegisterDriver to add more backends.
type DB interface {
	// IsReadOnly returns true if the database was opened in readonly mode
	// and no write transactions are supported.
	IsReadOnly() bool

	// IsZeroCopyRead returns true if keys and values on Get and from Cursors
	// are NOT safe to use without copy.
	IsZeroCopyRead() bool

	// Begin starts a transaction which is either read-only or read-write
	// depending on the specified flag. Multiple read-only transactions
	// can be started simultaneously while read-write is mutually exclusive
	// with other writes and may be mutually exclusive with other reads as well.
	// The call will block and wait until other transactions finish.
	//
	// The transaction must be closed by calling Rollback or Commit on
	// when it is no longer needed. Failure to do so can result in memory
	// leaks and/or inablity to open more transactions or close the database
	// due to locks depending on the driver.
	Begin(...TxOption) (Tx, error)

	// View invokes the passed function in the context of a managed
	// read-only transaction. Any errors returned from the user-supplied
	// function are returned from this method.
	//
	// Calling Rollback or Commit on the transaction passed to the
	// user-supplied function will result in an error.
	View(fn func(tx Tx) error) error

	// Update invokes the passed function in the context of a managed
	// read-write transaction.  Any errors returned from the user-supplied
	// function will cause the transaction to be rolled back and are
	// returned from this method. Otherwise, the transaction is committed.
	//
	// Calling Rollback or Commit on the transaction passed to the
	// user-supplied function will result in an error.
	Update(fn func(tx Tx) error) error

	// Sync calls fdatasync to make changes to the database durable. It
	// is not necessary to call Sync during normal options, however if you
	// open the database with the NoSync flag then it allows you to execute
	// sync when convenient.
	Sync() error
}

// DBManager implements a superset of DB aimed at parts of a codebase
// that manages the database. It is split from the user DB interface to
// avoid mistakes.
type DBManager interface {
	// inherit the DB interface
	DB

	// Type returns the driver name for the current database instance.
	Type() string

	// Path returns the filesystem path where the current database is stored.
	Path() string

	// Writes the entire database to io.Writer. During this process the
	// database may or may not be available for concurrent read and write
	// transaction depending on the driver.
	Snapshot(io.Writer) error

	// Restores the database from io.Reader. During this process the
	// database is unavailable for concurrent read and write transactions.
	// The database must be pristine, i.e. contain no content. Otherwise
	// restore will return an error.
	Restore(io.Reader) error

	// Close cleanly shuts down the database and syncs all data to diskl.
	// It blocks until all database transactions have been finalized (rolled
	// back or committed).
	Close() error
}

// TxOption defines a configuration option function type
type TxOption func(*TxOptions)

// TxOptions defines configuration options for a transaction. Some drivers
// may ignore certain options when not supported.
type TxOptions struct {
	Writable bool
	NoWait   bool
	Sync     bool
}

// Enables writable transaction.
func WithTxWrite() TxOption {
	return func(o *TxOptions) {
		o.Writable = true
	}
}

// Fails with ErrTxWouldBlock when a concurrent transaction is running
// on db.Begin()
func WithTxNoWait() TxOption {
	return func(o *TxOptions) {
		o.NoWait = true
	}
}

// Selectively enables backend flush/fsync on commit when database
// is opened in NoSync mode. NoSync may speed up commits but may lead
// to data loss on system crash. This option lets users selectively
// enforce a fsync to disk when convenient.
func WithTxSync() TxOption {
	return func(o *TxOptions) {
		o.Sync = true
	}
}

// Tx represents a database transaction. It is either read-only or read-write.
// The transaction provides access to buckets that can hold keys and
// nested buckets. Changes made during a read-write transaction become
// atomically visible on commit only. From the context of the read-write
// transaction the changes are however visible immediatly (read-your-own-writes)
// A read-only transaction provides a view of the database at the
// time it was created. Depending on the backend, new read-write transactions
// may have to wait until all open read-only transactions finish. For this
// reason transactions should be as short as possible.
type Tx interface {
	// DB returns the current database for the transaction.
	DB() DB

	// IsWriteable returns true when the transaction can write to the database.
	IsWriteable() bool

	// CreateBucket creates and returns a new top-level bucket with the
	// given key. If the bucket already exists it is returned without error.
	// Calling this method may have immediate effect on the underlying
	// database even without committing the transaction.
	CreateBucket(key []byte, opts ...BucketOption) (Bucket, error)

	// DeleteBucket removes a top-level bucket with the given key. This
	// includes removing all nested buckets and keys inside the bucket.
	// Calling this method may have immediate effect on the underlying
	// database even without committing the transaction.
	DeleteBucket(key []byte) error

	// Bucket returns the top-level bucket with a given name or nil and
	// ErrBucketNotFound if a bucket with this name does not exist.
	Bucket(key []byte) (Bucket, error)

	// Buckets returns an iterator for top-level buckets.
	Buckets() iter.Seq2[[]byte, Bucket]

	// Commit commits all changes made during a read-write transaction.
	// Calling this method on a managed transaction will result in
	// an error. Calling this method on a read-only transaction has
	// the same effect as calling Rollback.
	Commit() error

	// Rollback undoes all changes made during a read-write transaction
	// or closes a read-only transaction. Calling this method on a
	// managed transaction will result in an error.
	Rollback() error
}

// BucketOption defines a configuration option function type
type BucketOption func(*BucketOptions)

// BucketOptions defines configuration options for a transaction. Some drivers
// may ignore certain options when not supported.
type BucketOptions struct {
	Fill float64
}

func WithBucketFill(f float64) BucketOption {
	return func(o *BucketOptions) {
		o.Fill = f
	}
}

type BucketStats struct {
	NKeys    int // number of keys/value pairs
	NBuckets int // total number of buckets including the top bucket
	NBytes   int // total bucket size in bytes
}

// Bucket represents a collection of key/value pairs. In addition
// buckets can cold nested buckets.
type Bucket interface {
	// Writable returns whether or not the bucket is writable.
	Writable() bool

	// Stats returns bucket statistics
	Stats() BucketStats

	// Bucket retrieves a nested bucket with the given key. Returns nil
	// and ErrBucketNotFound if the bucket does not exist.
	Bucket(key []byte) (Bucket, error)

	// Buckets returns an iterator for nested buckets.
	Buckets() iter.Seq2[[]byte, Bucket]

	// CreateBucket creates and returns a new nested bucket with the given
	// key. If the bucket already exists it is returned without error.
	// Calling this method may have immediate effect on the underlying
	// database even without committing the transaction.
	CreateBucket(key []byte, opts ...BucketOption) (Bucket, error)

	// DeleteBucket removes a nested bucket with the given key. This
	// includes removing all nested buckets and keys inside the bucket.
	// Calling this method may have immediate effect on the underlying
	// database even without committing the transaction.
	DeleteBucket(key []byte) error

	// Put stores a key/value pair to the bucket. Keys that do not already
	// exist are added and keys that already exist are overwritten. The
	// buffer slices passed to this function must NOT be modified by the
	// caller until the transaction ends. This constraint prevents internal
	// data copies for better performance control.
	Put(key, value []byte) error

	// Get returns the value for the given key. It returns nil and an error
	// if the key does not exist in this bucket or otherwise the key could
	// not be fetched due to driver or database error. An empty slice is
	// returned for keys that exist but have no value assigned. The returned
	// value is only valid during a transaction. Attempting to access it
	// after a transaction has ended results in undefined behavior.
	// Additionally, the value must NOT be modified by the caller. These
	// constraints prevent internal data copies for better performance control.
	Get(key []byte) ([]byte, error)

	// Delete removes the specified key from the bucket. Deleting a key
	// that does not exist does not return an error. The key buffer passed
	// to this function must NOT be modified by the caller until the
	// transaction ends. This constraint prevents internal data copies
	// for better performance control.
	Delete(key []byte) error

	// Scan iterates over keys in a bucket and returns a sequence
	// of key, value pairs. Typical consumption is a for-range loop,
	// but the returned function can also be called directly. When
	// prefix is nil, scan visits all keys. When prefix has non-zero
	// length all keys with a common prefix are visited. Scan replaces
	// cursor first / next loops with a modern alternative.
	Scan(prefix []byte) iter.Seq2[[]byte, []byte]

	// ScanRange iterates over an explicit range of keys starting at
	// a lower bound (inclusive) and ending before an upper bound
	// (exclusive). When bounds are nil they are interpreted as min
	// or max value. ScanRange provides a modern alternative to
	// cursor Seek/Next loop patterns for forward iteration.
	ScanRange(start, end []byte) iter.Seq2[[]byte, []byte]

	// ScanReverse iterates over keys in a bucket in descending order
	// and returns a sequence of key, value pairs. Typical consumption
	// is a for-range loop, but the returned function can also be called
	// directly. When prefix is nil, scan visits all keys. When prefix
	// has non-zero length all keys with a common prefix are visited.
	// ScanReverse replaces cursor Last/Prev loops with a modern alternative.
	ScanReverse(prefix []byte) iter.Seq2[[]byte, []byte]

	// ScanRangeReverse iterates over an explicit range of keys in
	// descending order. The range defines a lower bound (inclusive)
	// and upper bound (exclusive). When bounds are nil they are
	// interpreted as min or max value. ScanRangeReverse provides
	// a modern alternative to cursor Seek/Prev loop patterns for
	// backward iteration.
	ScanRangeReverse(start, end []byte) iter.Seq2[[]byte, []byte]

	// SearchGE returns the first key and its value that is greater or
	// equal to the search key or an error when no key was found.
	// It works like a binary search in ascending order.
	SearchGE(key []byte) ([]byte, []byte, error)

	// SearchLE returns the last key and its value that is less or
	// equal to the search key or an error when no key was found.
	// It works like a binary search in descending order.
	SearchLE(key []byte) ([]byte, []byte, error)
}
