// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package mem

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"blockwatch.cc/knoxdb/store"
	logpkg "github.com/echa/log"
)

var log = logpkg.Disabled

const (
	dbType = "mem"

	// bucketIdLen is the length in bytes for bucket ids
	bucketIdLen int = 1
)

var (
	// byteOrder is the preferred byte order used through the database.
	// Sometimes big endian will be used to allow ordered byte
	// sortable integer values.
	byteOrder = binary.LittleEndian

	// bucketIndexPrefix is the prefix used for all entries in the bucket
	// index.
	bucketIndexPrefix = []byte("bidx")
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

// copySlice returns a copy of the passed slice.  This is mostly used to copy
// user data to internal structures. Cursor and Get calls return zero-copy data
// which is only valid during a transaction.
func copySlice(slice []byte) []byte {
	return bytes.Clone(slice)
}

// parseArgs parses the arguments from the database Open/Create methods.
func parseArgs(funcName string, args ...any) (string, *Options, error) {
	if len(args) < 1 {
		return "", nil, fmt.Errorf("invalid arguments to %s.%s -- expected database path",
			dbType, funcName)
	}

	dbPath, ok := args[0].(string)
	if !ok {
		return "", nil, fmt.Errorf("first argument to %s.%s is invalid -- "+
			"expected database path string", dbType, funcName)
	}

	if len(args) == 1 || args[1] == nil {
		return dbPath, defaultOptions, nil
	}

	opts, ok := args[1].(*Options)
	if !ok {
		return "", nil, fmt.Errorf("second argument to %s.%s is invalid -- "+
			"expected database options, got %T", dbType, funcName, args[1])
	}

	return dbPath, opts, nil
}

// openDBDriver is the callback provided during driver registration that opens
// an existing database for use.
func openDBDriver(args ...any) (store.DB, error) {
	dbPath, opts, err := parseArgs("Open", args...)
	if err != nil {
		return nil, err
	}

	return openDB(dbPath, opts, false)
}

// createDBDriver is the callback provided during driver registration that
// creates, initializes, and opens a database for use.
func createDBDriver(args ...any) (store.DB, error) {
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
