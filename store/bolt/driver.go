// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"bytes"
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/store"
	logpkg "github.com/echa/log"
)

// Bolt Limits
// Max Tx size: 15% of MaxTableSize, default = 0.15*64M = 10M
// Max key update count per Tx: default ~ 100k

var log = logpkg.Disabled

const (
	dbType = "bolt"
)

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
// user data to internal structures. Cursor and Get calls return zero-copy data
// which is inly valid during a transaction.
func copySlice(slice []byte) []byte {
	return bytes.Clone(slice)
}

// parseArgs parses the arguments from the database Open/Create methods.
func parseArgs(funcName string, args ...interface{}) (string, *bolt.Options, error) {
	if len(args) < 1 {
		return "", nil, fmt.Errorf("invalid arguments to %s.%s -- "+
			"expected database path and optional options", dbType,
			funcName)
	}

	dbPath, ok := args[0].(string)
	if !ok {
		return "", nil, fmt.Errorf("first argument to %s.%s is invalid -- "+
			"expected database path string", dbType, funcName)
	}

	if len(args) == 1 || args[1] == nil {
		return dbPath, nil, nil
	}

	opts, ok := args[1].(*bolt.Options)
	if !ok {
		return "", nil, fmt.Errorf("second argument to %s.%s is invalid -- "+
			"expected database options, got %T", dbType, funcName, args[1])
	}

	return dbPath, opts, nil
}

// openDBDriver is the callback provided during driver registration that opens
// an existing database for use.
func openDBDriver(args ...interface{}) (store.DB, error) {
	dbPath, opts, err := parseArgs("Open", args...)
	if err != nil {
		return nil, err
	}

	return openDB(dbPath, opts, false)
}

// createDBDriver is the callback provided during driver registration that
// creates, initializes, and opens a database for use.
func createDBDriver(args ...interface{}) (store.DB, error) {
	dbPath, opts, err := parseArgs("Create", args...)
	if err != nil {
		return nil, err
	}

	return openDB(dbPath, opts, true)
}

// useLogger is the callback provided during driver registration that sets the
// current logger to the provided one.
func useLogger(logger logpkg.Logger) {
	log = logger
}

func init() {
	// Register the driver.
	driver := store.Driver{
		DbType:    dbType,
		Create:    createDBDriver,
		Open:      openDBDriver,
		UseLogger: useLogger,
	}
	if err := store.RegisterDriver(driver); err != nil {
		panic(fmt.Sprintf("Failed to register database driver '%s': %v",
			dbType, err))
	}
}
