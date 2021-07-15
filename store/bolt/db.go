// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package boltdb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/store"
	"blockwatch.cc/knoxdb/util"
)

// Bolt Limits
// Max Tx size: 15% of MaxTableSize, default = 0.15*64M = 10M
// Max key update count per Tx: default ~ 100k

var (
	// byteOrder is the preferred byte order used through the database.
	// Sometimes big endian will be used to allow ordered byte
	// sortable integer values.
	byteOrder = binary.LittleEndian

	// max size of compact transactions
	compactTxSize int64 = 1048576

	// manifestKey is the name of the top-level manifest key.
	manifestKey           = []byte("manifest")
	manifestBucketKeyName = []byte("manifest")
)

// Common error strings.
const (
	// errDbNotOpenStr is the text to use for the store.ErrDbNotOpen
	// error code.
	errDbNotOpenStr = "database is not open"

	// errDbReadOnlyStr is the text to use for the store.ErrDbTxNotWriteable
	// error code.
	errDbReadOnlyStr = "database is in read-only mode"

	// errTxClosedStr is the text to use for the store.ErrTxClosed error
	// code.
	errTxClosedStr = "database tx is closed"
)

// makeDbErr creates a store.Error given a set of arguments.
func makeDbErr(c store.ErrorCode, desc string, err error) store.Error {
	return store.Error{ErrorCode: c, Description: desc, Err: err}
}

// convertErr converts the passed badger error into a database error with an
// equivalent error code  and the passed description.  It also sets the passed
// error as the underlying error.
func convertErr(desc string, boltErr error) store.Error {
	// Use the driver-specific error code by default.  The code below will
	// update this with the converted error if it's recognized.
	var code = store.ErrDriverSpecific

	switch boltErr {
	// Database corruption errors.
	case bolt.ErrChecksum:
		code = store.ErrCorruption

		// Database open/create errors. Most badger errors are dynamic and
		// difficult to dissect, so we pass them as driver-specific.
		//  code = store.ErrDbDoesNotExist
	case bolt.ErrDatabaseOpen:
		code = store.ErrDbAlreadyOpen
	case bolt.ErrDatabaseNotOpen:
		code = store.ErrDbNotOpen
	case bolt.ErrInvalid:
		code = store.ErrInvalid
	case bolt.ErrVersionMismatch:
		code = store.ErrInvalid
		// case bolt.ErrTimeout:

	// Transaction errors.
	case bolt.ErrTxNotWritable:
		code = store.ErrTxNotWritable
	case bolt.ErrTxClosed:
		code = store.ErrTxClosed
	case bolt.ErrDatabaseReadOnly:
		code = store.ErrTxNotWritable
	case bolt.ErrBucketNotFound:
		code = store.ErrBucketNotFound
	case bolt.ErrBucketExists:
		code = store.ErrBucketExists
	case bolt.ErrBucketNameRequired:
		code = store.ErrBucketNameRequired
	case bolt.ErrKeyTooLarge:
		code = store.ErrKeyTooLarge
	case bolt.ErrValueTooLarge:
		code = store.ErrValueTooLarge
	case bolt.ErrIncompatibleValue:
		code = store.ErrIncompatibleValue

	case bolt.ErrKeyRequired:
		code = store.ErrKeyRequired
	}

	return store.Error{ErrorCode: code, Description: desc, Err: boltErr}
}

// copySlice returns a copy of the passed slice.  This is mostly used to copy
// badger iterator keys and values since they are only valid until the iterator
// is moved instead of during the entirety of the transaction.
func copySlice(slice []byte) []byte {
	if slice == nil {
		return nil
	}
	ret := make([]byte, len(slice))
	copy(ret, slice)
	return ret
}

// cursor is an internal type used to represent a cursor over key/value pairs
// and nested buckets of a bucket and implements the store.Cursor interface.
type cursor struct {
	bucket      *bucket
	currentIter *bolt.Cursor
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

	k, v := c.currentIter.First()
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

	k, v := c.currentIter.Last()
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

// bucket is an internal type used to represent a collection of key/value pairs
// and implements the store.Bucket interface.
type bucket struct {
	tx     *transaction
	bucket *bolt.Bucket
}

// Enforce bucket implements the store.Bucket interface.
var _ store.Bucket = (*bucket)(nil)

// Bucket retrieves a nested bucket with the given key.  Returns nil if
// the bucket does not exist.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Bucket(key []byte) store.Bucket {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	// fetch bucket
	var child *bolt.Bucket
	if b.bucket == nil {
		child = b.tx.tx.Bucket(key)
	} else {
		child = b.bucket.Bucket(key)
	}
	if child == nil {
		return nil
	}

	return &bucket{tx: b.tx, bucket: child}
}

// CreateBucket creates and returns a new nested bucket with the given key.
//
// Returns the following errors as required by the interface contract:
//   - ErrBucketExists if the bucket already exists
//   - ErrBucketNameRequired if the key is empty
//   - ErrIncompatibleValue if the key is otherwise invalid for the particular
//     implementation
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) CreateBucket(key []byte) (store.Bucket, error) {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "create bucket requires a writable database transaction"
		return nil, makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		str := "create bucket requires a key"
		return nil, makeDbErr(store.ErrBucketNameRequired, str, nil)
	}

	var child *bolt.Bucket
	var err error
	if b.bucket == nil {
		child, err = b.tx.tx.CreateBucket(key)
	} else {
		child, err = b.bucket.CreateBucket(key)
	}
	if err != nil {
		return nil, convertErr("create bucket", err)
	}

	return &bucket{tx: b.tx, bucket: child}, nil
}

// CreateBucketIfNotExists creates and returns a new nested bucket with the
// given key if it does not already exist.
//
// Returns the following errors as required by the interface contract:
//   - ErrBucketNameRequired if the key is empty
//   - ErrIncompatibleValue if the key is otherwise invalid for the particular
//     implementation
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) CreateBucketIfNotExists(key []byte) (store.Bucket, error) {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "create bucket requires a writable database transaction"
		return nil, makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	var child *bolt.Bucket
	var err error
	if b.bucket == nil {
		child, err = b.tx.tx.CreateBucketIfNotExists(key)
	} else {
		child, err = b.bucket.CreateBucketIfNotExists(key)
	}
	if err != nil {
		return nil, convertErr("create non-exist bucket", err)
	}
	return &bucket{tx: b.tx, bucket: child}, nil
}

// DeleteBucket removes a nested bucket with the given key.
//
// Returns the following errors as required by the interface contract:
//   - ErrBucketNotFound if the specified bucket does not exist
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) DeleteBucket(key []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "delete bucket requires a writable database transaction"
		return makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Remove the nested bucket from the bucket index.  Any buckets nested
	// under it were already removed above.
	var err error
	if b.bucket == nil {
		err = b.tx.tx.DeleteBucket(key)
	} else {
		err = b.bucket.DeleteBucket(key)
	}
	if err != nil {
		return convertErr("delete bucket", err)
	}
	return nil
}

// Cursor returns a new cursor, allowing for iteration over the bucket's
// key/value pairs in forward or backward order.
//
// You must seek to a position using the First, Last, or Seek functions before
// calling the Next, Prev, Key, or Value functions.  Failure to do so will
// result in the same return values as an exhausted cursor, which is false for
// the Prev and Next functions and nil for Key and Value functions.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Cursor() store.Cursor {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return &cursor{bucket: b}
	}

	// Create the cursor for either the root bucket or a nested bucket.
	if b.bucket == nil {
		return &cursor{bucket: b, currentIter: b.tx.tx.Cursor()}
	} else {
		return &cursor{bucket: b, currentIter: b.bucket.Cursor()}
	}
}

// ForEach invokes the passed function with every key/value pair in the bucket.
// This does not include nested buckets or the key/value pairs within those
// nested buckets.
//
// WARNING: It is not safe to mutate data while iterating with this method.
// Doing so may cause the underlying cursor to be invalidated and return
// unexpected keys and/or values.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) ForEach(fn func(k, v []byte) error) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	var err error
	if b.bucket == nil {
		// root bucket only contains buckets and no keys to iterate
		return nil
	} else {
		err = b.bucket.ForEach(fn)
	}
	if err != nil {
		return convertErr("foreach", err)
	}
	return nil
}

// ForEachBucket invokes the passed function with the key of every nested bucket
// in the current bucket.  This does not include any nested buckets within those
// nested buckets.
//
// WARNING: It is not safe to mutate data while iterating with this method.
// Doing so may cause the underlying cursor to be invalidated and return
// unexpected keys.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) ForEachBucket(fn func(k []byte, b store.Bucket) error) error {
	err := b.ForEach(func(name, val []byte) error {
		if val != nil {
			return nil
		}
		dbBucket := b.bucket.Bucket(name)
		bucket := &bucket{tx: b.tx, bucket: dbBucket}
		return fn(name, bucket)
	})
	if err != nil {
		return convertErr("foreach bucket", err)
	}
	return nil
}

// Writable returns whether or not the bucket is writable.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Writable() bool {
	return b.tx.writable
}

// Put saves the specified key/value pair to the bucket.  Keys that do not
// already exist are added and keys that already exist are overwritten.
//
// Returns the following errors as required by the interface contract:
//   - ErrKeyRequired if the key is empty
//   - ErrIncompatibleValue if the key is the same as an existing bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Put(key, value []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "setting a key requires a writable database transaction"
		return makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		str := "put requires a key"
		return makeDbErr(store.ErrKeyRequired, str, nil)
	}

	if err := b.bucket.Put(key, value); err != nil {
		return convertErr("put", err)
	}
	return nil
}

// Get returns the value for the given key.  Returns nil if the key does not
// exist in this bucket.  An empty slice is returned for keys that exist but
// have no value assigned.
//
// NOTE: The value returned by this function is only valid during a transaction.
// Attempting to access it after a transaction has ended results in undefined
// behavior.  Additionally, the value must NOT be modified by the caller.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Get(key []byte) []byte {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	// Nothing to return if there is no key.
	if len(key) == 0 {
		return nil
	}

	return b.bucket.Get(key)
}

// Delete removes the specified key from the bucket.  Deleting a key that does
// not exist does not return an error.
//
// Returns the following errors as required by the interface contract:
//   - ErrKeyRequired if the key is empty
//   - ErrIncompatibleValue if the key is the same as an existing bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Delete(key []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "deleting a value requires a writable database transaction"
		return makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Nothing to do if there is no key.
	if len(key) == 0 {
		return nil
	}

	if err := b.bucket.Delete(key); err != nil {
		return convertErr("delete", err)
	}
	return nil
}

func (b *bucket) NextSequence() (uint64, error) {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return 0, err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "deleting a value requires a writable database transaction"
		return 0, makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	val, err := b.bucket.NextSequence()
	if err != nil {
		return 0, convertErr("next sequence", err)
	}
	return val, nil
}

func (b *bucket) FillPercent(p float64) {
	b.bucket.FillPercent = p
}

func (b *bucket) Stats() store.BucketStats {
	stats := store.BucketStats{}
	if err := b.tx.checkClosed(); err != nil {
		return stats
	}
	internalStats := b.bucket.Stats()
	stats.KeyN = internalStats.KeyN
	stats.BucketN = internalStats.BucketN
	return stats
}

// sequence represents a database sequence. It can be used to generate
// unique ids for objects to decrease the memory required to store keys
// in database indexes.
type sequence struct {
	db  *db
	key []byte
}

// Enforce sequence implements the store.Sequence interface.
var _ store.Sequence = (*sequence)(nil)

// Next returns a new id from the sequences id space.
func (s *sequence) Next() (uint64, error) {
	var val uint64
	err := s.db.Update(func(dbTx store.Tx) error {
		bucket := dbTx.Bucket(s.key)
		var err error
		val, err = bucket.NextSequence()
		return err
	})
	return val, err
}

func (s *sequence) Release() error {
	return nil
}

// transaction represents a database transaction.  It can either be read-only or
// read-write and implements the store.Bucket interface.  The transaction
// provides a root bucket against which all read and writes occur.
type transaction struct {
	managed  bool     // Is the transaction managed by this driver?
	closed   bool     // Is the transaction closed?
	writable bool     // Is the transaction writable?
	db       *db      // DB instance the tx was created from.
	tx       *bolt.Tx // the DB transaction
}

// Enforce transaction implements the store.Tx interface.
var _ store.Tx = (*transaction)(nil)

// checkClosed returns an error if the the database or transaction is closed.
func (tx *transaction) checkClosed() error {
	// The transaction is no longer valid if it has been closed.
	if tx.closed {
		return makeDbErr(store.ErrTxClosed, errTxClosedStr, nil)
	}
	return nil
}

// Root returns the top-most bucket for all metadata storage.
//
// This function is part of the store.Tx interface implementation.
func (tx *transaction) Root() store.Bucket {
	return &bucket{tx: tx, bucket: nil}
}

// Root returns the bucket with given name.
func (tx *transaction) Bucket(key []byte) store.Bucket {
	b := tx.tx.Bucket(key)
	if b == nil {
		return nil
	}
	return &bucket{tx: tx, bucket: b}
}

// Manifest returns the current database manifest metadata.
//
// This function is part of the store.DB interface implementation.
func (tx *transaction) Manifest() (store.Manifest, error) {
	var mft store.Manifest
	if err := tx.checkClosed(); err != nil {
		return mft, err
	}
	buf := tx.tx.Bucket(manifestBucketKeyName).Get(manifestKey)
	if buf != nil {
		if err := json.Unmarshal(buf, &mft); err != nil {
			return mft, err
		}
	}
	return mft, nil
}

// SetManifest overwrites the current database manifest.
//
// This function is part of the store.DB interface implementation.
func (tx *transaction) SetManifest(manifest store.Manifest) error {
	if err := tx.checkClosed(); err != nil {
		return err
	}
	mft, err := tx.Manifest()
	if err != nil {
		return err
	}
	// we only allow some fields to be overwritten
	mft.Name = manifest.Name
	mft.Version = manifest.Version
	mft.Label = manifest.Label
	mft.Schema = manifest.Schema
	buf, err := json.Marshal(mft)
	if err != nil {
		return err
	}
	if err := tx.tx.Bucket(manifestBucketKeyName).Put(manifestKey, buf); err != nil {
		return convertErr("set manifest", err)
	}
	return nil
}

// close marks the transaction closed then releases any pending data.
func (tx *transaction) close() {
	if !tx.writable {
		tx.tx.Rollback()
	}
	tx.closed = true
	tx.db.activeTx.Done()
	tx.db.closeLock.RUnlock()
}

// Commit commits all changes that have been made to the root metadata bucket
// and all of its sub-buckets to the database cache which is periodically synced
// to persistent storage.  In addition, it commits all new blocks directly to
// persistent storage bypassing the db cache.  Blocks can be rather large, so
// this help increase the amount of cache available for the metadata updates and
// is safe since blocks are immutable.
//
// This function is part of the store.Tx interface implementation.
func (tx *transaction) Commit() error {
	// Prevent commits on managed transactions.
	if tx.managed {
		tx.close()
		panic("managed transaction commit not allowed")
	}

	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return err
	}

	// Regardless of whether the commit succeeds, the transaction is closed
	// on return.
	defer tx.close()

	// Ensure the transaction is writable.
	if !tx.writable {
		str := "Commit requires a writable database transaction"
		return makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Write pending data.  The function will rollback if any errors occur.
	if err := tx.tx.Commit(); err != nil {
		return convertErr("commit tx", err)
	}
	return nil
}

// Rollback undoes all changes that have been made to the root bucket and all of
// its sub-buckets.
//
// This function is part of the store.Tx interface implementation.
func (tx *transaction) Rollback() error {
	// Prevent rollbacks on managed transactions.
	if tx.managed {
		tx.close()
		panic("managed transaction rollback not allowed")
	}

	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return err
	}

	if err := tx.tx.Rollback(); err != nil {
		return convertErr("rollback tx", err)
	}
	tx.close()
	return nil
}

func (tx *transaction) DB() store.DB {
	return tx.db
}

// db wraps a Badger instance and implements the store.DB interface.
// All database access is performed through transactions which are managed.
type db struct {
	seqLock   sync.RWMutex   // Guard access to sequences.
	closeLock sync.RWMutex   // Make database close block while txns active.
	txLock    sync.Mutex     // block creating new tx during backup/restore.
	activeTx  sync.WaitGroup // count active tx (needed for backup/restore).
	closed    bool           // Is the database closed?
	store     *bolt.DB       // the database
	opts      *bolt.Options
	dbPath    string
}

// Enforce db implements the store.DB interface.
var _ store.DB = (*db)(nil)

// Type returns the database driver type the current database instance was
// created with.
//
// This function is part of the store.DB interface implementation.
func (db *db) Type() string {
	return dbType
}

// Path returns the path where the current database is stored.
//
// This function is part of the store.DB interface implementation.
func (db *db) Path() string {
	return db.dbPath
}

// Manifest returns the current database manifest metadata.
//
// This function is part of the store.DB interface implementation.
func (db *db) Manifest() (store.Manifest, error) {
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return store.Manifest{}, makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}
	return getManifest(db.store)
}

// SetManifest overwrites the current database manifest.
//
// This function is part of the store.DB interface implementation.
func (db *db) SetManifest(manifest store.Manifest) error {
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}
	mft, err := getManifest(db.store)
	if err != nil {
		return err
	}
	// we only allow some fields to be overwritten
	mft.Name = manifest.Name
	mft.Version = manifest.Version
	mft.Label = manifest.Label
	mft.Schema = manifest.Schema
	return putManifest(db.store, mft)
}

func getManifest(bdb *bolt.DB) (store.Manifest, error) {
	var mft store.Manifest
	err := bdb.View(func(dbTx *bolt.Tx) error {
		mftBucket := dbTx.Bucket(manifestBucketKeyName)
		if mftBucket == nil {
			return makeDbErr(store.ErrInvalid, "invalid database: missing manifest", nil)
		}
		buf := mftBucket.Get(manifestKey)
		if buf != nil {
			return json.Unmarshal(buf, &mft)
		}
		return nil
	})
	if err != nil {
		return mft, err
	}
	return mft, nil
}

func putManifest(bdb *bolt.DB, manifest store.Manifest) error {
	buf, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	return bdb.Update(func(dbTx *bolt.Tx) error {
		return dbTx.Bucket(manifestBucketKeyName).Put(manifestKey, buf)
	})
}

// Sequence creates a new managed sequence stored in the sequences bucket.
//
func (db *db) Sequence(key []byte, lease uint64) (store.Sequence, error) {
	return &sequence{
		db:  db,
		key: copySlice(key),
	}, nil
}

// begin is the implementation function for the Begin database method.  See its
// documentation for more details.
//
// This function is only separate because it returns the internal transaction
// which is used by the managed transaction code while the database method
// returns the interface.
func (db *db) begin(writable bool) (*transaction, error) {
	// Whenever a new transaction is started, grab a read lock against the
	// database to ensure Close will wait for the transaction to finish.
	// This lock will not be released until the transaction is closed (via
	// Rollback or Commit).
	db.closeLock.RLock()
	if db.closed {
		db.closeLock.RUnlock()
		return nil, makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// db.txLock.Lock()
	// defer db.txLock.Unlock()
	dbTx, err := db.store.Begin(writable)
	if err != nil {
		db.closeLock.RUnlock()
		return nil, convertErr("begin tx", err)
	}
	db.activeTx.Add(1)
	tx := &transaction{
		writable: writable,
		db:       db,
		tx:       dbTx,
	}
	return tx, nil
}

// Begin starts a transaction which is either read-only or read-write depending
// on the specified flag.  Multiple read-only transactions can be started
// simultaneously while only a single read-write transaction can be started at a
// time.  The call will block when starting a read-write transaction when one is
// already open.
//
// NOTE: The transaction must be closed by calling Rollback or Commit on it when
// it is no longer needed.  Failure to do so will result in unclaimed memory.
//
// This function is part of the store.DB interface implementation.
func (db *db) Begin(writable bool) (store.Tx, error) {
	return db.begin(writable)
}

// rollbackOnPanic rolls the passed transaction back if the code in the calling
// function panics.  This is needed since the mutex on a transaction must be
// released and a panic in called code would prevent that from happening.
//
// NOTE: This can only be handled manually for managed transactions since they
// control the life-cycle of the transaction.  As the documentation on Begin
// calls out, callers opting to use manual transactions will have to ensure the
// transaction is rolled back on panic if it desires that functionality as well
// or the database will fail to close since the read-lock will never be
// released.
// func rollbackOnPanic(tx *transaction) {
// 	if err := recover(); err != nil {
// 		tx.managed = false
// 		_ = tx.Rollback()
// 		panic(err)
// 	}
// }

// View invokes the passed function in the context of a managed read-only
// transaction with the root bucket for the namespace.  Any errors returned from
// the user-supplied function are returned from this function.
//
// This function is part of the store.DB interface implementation.
func (db *db) View(fn func(store.Tx) error) error {
	// check for close and hold close lock
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// count active tx
	db.activeTx.Add(1)
	defer db.activeTx.Done()

	var err error
	dberr := db.store.View(func(tx *bolt.Tx) error {
		dbtx := &transaction{
			writable: false,
			db:       db,
			tx:       tx,
		}
		err = fn(dbtx)
		return nil
	})
	if dberr != nil {
		return convertErr("view tx", dberr)
	}
	return err
}

// Update invokes the passed function in the context of a managed read-write
// transaction with the root bucket for the namespace.  Any errors returned from
// the user-supplied function will cause the transaction to be rolled back and
// are returned from this function.  Otherwise, the transaction is committed
// when the user-supplied function returns a nil error.
//
// This function is part of the store.DB interface implementation.
func (db *db) Update(fn func(store.Tx) error) error {
	// check for close and hold close lock
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// count active tx
	db.activeTx.Add(1)
	defer db.activeTx.Done()

	var err error
	dberr := db.store.Update(func(tx *bolt.Tx) error {
		dbtx := &transaction{
			writable: true,
			db:       db,
			tx:       tx,
		}
		err = fn(dbtx)
		return nil
	})
	if dberr != nil {
		return convertErr("update tx", dberr)
	}
	return err
}

// Close cleanly shuts down the database and syncs all data.  It will block
// until all database transactions have been finalized (rolled back or
// committed).
//
// This function is part of the store.DB interface implementation.
func (db *db) Close() error {
	// Since all transactions have a read lock on this mutex, this will
	// cause Close to wait for all readers to complete.
	db.closeLock.Lock()
	defer db.closeLock.Unlock()

	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	return db.close()
}

func (db *db) close() error {
	mft, err := getManifest(db.store)
	if err != nil {
		return err
	}
	if !db.store.IsReadOnly() {
		// write manifest
		mft.IsLocked = false
		if err := putManifest(db.store, mft); err != nil {
			return err
		}
	}

	// NOTE: Since the above lock waits for all transactions to finish and
	// prevents any new ones from being started, it is safe to close the
	// underlying db here.
	if err := db.store.Close(); err != nil {
		return convertErr("close", err)
	}
	if mft.Name != "" {
		log.Debugf("%s database closed successfully.", strings.Title(mft.Name))
	} else {
		log.Debugf("Database closed successfully.")
	}
	db.closed = true
	db.store = nil
	db.opts = nil
	return nil
}

// filesExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// initDB creates the initial buckets and values used by the package.  This is
// mainly in a separate function for testing purposes.
func initDB(db *bolt.DB) error {
	// init manifest
	now := time.Now().UTC()
	mft := store.Manifest{
		CreatedAt: now,
		IsLocked:  true,
	}
	buf, err := json.Marshal(mft)
	if err != nil {
		return err
	}

	// init sequences bucket
	err = db.Update(func(dbTx *bolt.Tx) error {
		mftBucket, err := dbTx.CreateBucketIfNotExists(manifestBucketKeyName)
		if err != nil {
			return err
		}
		return mftBucket.Put(manifestKey, buf)
	})
	if err != nil {
		return convertErr("init db", err)
	}
	return nil
}

// openDB opens the database at the provided path.  store.ErrDbDoesNotExist
// is returned if the database doesn't exist and the create flag is not set.
func openDB(dbPath string, opts *bolt.Options, create bool) (store.DB, error) {
	dbExists := fileExists(dbPath)

	if !create && !dbExists {
		str := fmt.Sprintf("database file %q does not exist", dbPath)
		return nil, makeDbErr(store.ErrDbDoesNotExist, str, nil)
	}

	if create && dbExists {
		str := fmt.Sprintf("database file %q exists", dbPath)
		return nil, makeDbErr(store.ErrDbExists, str, nil)
	}

	// Ensure the full path to the database exists.
	if !dbExists {
		// The error can be ignored here since the call to
		// badger.Open will fail if the directory couldn't be
		// created.
		_ = os.MkdirAll(filepath.Dir(dbPath), 0700)
	}

	// bolt will create any non-existing database file automatically
	bdb, err := bolt.Open(dbPath, 0600, opts)
	if err != nil {
		return nil, convertErr("open db", err)
	}
	if create && !dbExists {
		log.Debug("Initializing database...")
		if err := initDB(bdb); err != nil {
			bdb.Close()
			return nil, convertErr("init db", err)
		}
	} else {
		// update manifest
		mft, err := getManifest(bdb)
		if err != nil {
			bdb.Close()
			return nil, err
		}
		if !bdb.IsReadOnly() {
			mft.IsLocked = true
			if err := putManifest(bdb, mft); err != nil {
				bdb.Close()
				return nil, err
			}
		}
		if mft.Name != "" {
			log.Debugf("%s database opened successfully.", strings.Title(mft.Name))
		} else {
			log.Debug("Database opened successfully.")
		}
	}
	return &db{store: bdb, dbPath: dbPath, opts: opts}, nil
}

// Database maintenance functions

// Export all database contents as protobuf data.
func (db *db) Dump(w io.Writer) error {
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// backup may run in parallel to any tx and will be using a snapshot copy
	err := db.store.View(func(dbTx *bolt.Tx) error {
		log.Debugf("Exporting database of size %s (this may take a while)...",
			util.ByteSize(dbTx.Size()).String())
		n, err := dbTx.WriteTo(w)
		if err != nil {
			return err
		}
		log.Debugf("Successfully wrote %s of data.", util.ByteSize(n).String())
		return nil
	})
	if err != nil {
		return convertErr("dump db", err)
	}
	return nil
}

// Should be called on a database that is not running any other
// concurrent transactions while it is running.
func (db *db) Restore(r io.Reader) error {
	// not implemented; to do so implement
	// - close bolt db (waiting for any open tx)
	// - restore/overwrite file with reader contents
	// - open bolt db from restored file
	return nil
}

// Should be called on a database that is not running any concurrent tx.
//
// Garbage collect database. This will create a new file, stream all keys into
// that file, replace the existing DB with the new file and reopen the DB.
func (db *db) GC(ctx context.Context, ratio float64) error {
	// hold close lock
	db.closeLock.RLock()
	defer db.closeLock.RUnlock()
	if db.closed {
		return makeDbErr(store.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	// make wait interruptable
	run := make(chan struct{})
	go func() {
		// wait for tx to finish
		db.activeTx.Wait()
		run <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-run:
	}

	// prevent parallel GC
	db.activeTx.Add(1)
	defer db.activeTx.Done()

	// init
	start := time.Now()
	srcPath := db.dbPath
	dstPath := db.dbPath + ".temp"
	fi, err := os.Stat(srcPath)
	if err != nil {
		return convertErr("cannot stat source db", err)
	}
	initialSize := fi.Size()

	// Open destination database.
	dstOpts := *db.opts
	dstOpts.ReadOnly = false
	dst, err := bolt.Open(dstPath, fi.Mode(), &dstOpts)
	if err != nil {
		return convertErr("cannot open compaction db", err)
	}

	defer func(dst *bolt.DB, dstPath string) {
		if err != nil {
			dst.Close()
			os.Remove(dstPath)
		}
	}(dst, dstPath)

	// Run compaction.
	log.Infof("[GC] Compacting database %s (%s).", db.dbPath, util.ByteSize(initialSize))
	if err = compact(ctx, dst, db.store, compactTxSize, ratio); err != nil {
		return convertErr("compact db", err)
	}

	// Report stats on new size.
	fi, err = os.Stat(dstPath)
	if err != nil {
		return convertErr("cannot stat destination db", err)
	} else if fi.Size() == 0 {
		err = fmt.Errorf("zero size after compaction")
		return convertErr("compact db", err)
	}
	log.Infof("[GC] Database %s successfully compacted %s -> %s (gain=%.2fx) in %s.",
		db.dbPath,
		util.ByteSize(initialSize),
		util.ByteSize(fi.Size()),
		float64(initialSize)/float64(fi.Size()),
		time.Since(start))

	// replace db - point of no return
	// also, don't overwrite err to avoid triggering defer
	if err := dst.Close(); err != nil {
		return convertErr("close after compact", err)
	}
	if err := db.store.Close(); err != nil {
		return convertErr("close after compact", err)
	}
	db.closed = true
	if err := os.Rename(srcPath, srcPath+".backup"); err != nil {
		return convertErr("rename source db", err)
	}
	if err := os.Rename(dstPath, srcPath); err != nil {
		return convertErr("rename compacted db", err)
	}
	db.store, err = bolt.Open(srcPath, 0600, db.opts)
	if err != nil {
		return convertErr("open compacted db", err)
	}
	// update manifest
	if !db.store.IsReadOnly() {
		mft, err := getManifest(db.store)
		if err != nil {
			db.store.Close()
			return convertErr("get manifest after compact", err)
		}
		mft.IsLocked = true
		if err := putManifest(db.store, mft); err != nil {
			db.store.Close()
			return convertErr("store manifest after compact", err)
		}
	}
	log.Debugf("[GC] Database %s reopened successfully.", db.dbPath)
	db.closed = false

	// when all is good, remove the old database, ignoring errors
	os.Remove(srcPath + ".backup")
	log.Info("[GC] Using compacted database from now.")
	return nil
}

func compact(ctx context.Context, dst, src *bolt.DB, txMaxSize int64, fillPercent float64) error {
	// commit regularly, or we'll run out of memory for large datasets if using one transaction.
	var size int64
	tx, err := dst.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := walk(src, func(keys [][]byte, k, v []byte, seq uint64) error {
		// On each key/value, check if we have exceeded tx size.
		sz := int64(len(k) + len(v))
		if size+sz > txMaxSize && txMaxSize != 0 {
			// Commit previous transaction.
			if err := tx.Commit(); err != nil {
				return err
			}

			// check context
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Start new transaction.
			tx, err = dst.Begin(true)
			if err != nil {
				return err
			}
			size = 0
		}
		size += sz

		// Create bucket on the root transaction if this is the first level.
		nk := len(keys)
		if nk == 0 {
			bkt, err := tx.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Create buckets on subsequent levels, if necessary.
		b := tx.Bucket(keys[0])
		if nk > 1 {
			for _, k := range keys[1:] {
				b = b.Bucket(k)
			}
		}

		// Fill the entire page for best compaction.
		b.FillPercent = fillPercent

		// If there is no value then this is a bucket call.
		if v == nil {
			bkt, err := b.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Otherwise treat it as a key/value pair.
		return b.Put(k, v)
	}); err != nil {
		return err
	}

	return tx.Commit()
}

// walkFunc is the type of the function called for keys (buckets and "normal"
// values) discovered by Walk. keys is the list of keys to descend to the bucket
// owning the discovered key/value pair k/v.
type walkFunc func(keys [][]byte, k, v []byte, seq uint64) error

// walk walks recursively the bolt database db, calling walkFn for each key it finds.
func walk(db *bolt.DB, walkFn walkFunc) error {
	return db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return walkBucket(b, nil, name, nil, b.Sequence(), walkFn)
		})
	})
}

func walkBucket(b *bolt.Bucket, keypath [][]byte, k, v []byte, seq uint64, fn walkFunc) error {
	// Execute callback.
	if err := fn(keypath, k, v, seq); err != nil {
		return err
	}

	// If this is not a bucket then stop.
	if v != nil {
		return nil
	}

	// Iterate over each child key/value.
	keypath = append(keypath, k)
	return b.ForEach(func(k, v []byte) error {
		if v == nil {
			bkt := b.Bucket(k)
			return walkBucket(bkt, keypath, k, nil, bkt.Sequence(), fn)
		}
		return walkBucket(b, keypath, k, v, b.Sequence(), fn)
	})
}
