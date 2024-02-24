// Copyright (c) 2018 - 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package badger

import (
	"encoding/binary"
	"fmt"

	"blockwatch.cc/knoxdb/store"
	"github.com/dgraph-io/badger/v4"
	logpkg "github.com/echa/log"
)

var log = logpkg.Disabled

const (
	dbType = "badger"

	// bucketIdLen is the length in bytes for bucket ids
	bucketIdLen int = 1
)

var (
	// byteOrder is the preferred byte order used through the store.
	// Sometimes big endian will be used to allow ordered byte
	// sortable integer values.
	byteOrder = binary.LittleEndian

	// bucketIndexPrefix is the prefix used for all entries in the bucket
	// index.
	bucketIndexPrefix = []byte("bidx")

	// curBucketIDKeyName is the name of the key used to keep track of the
	// current bucket ID counter.
	curBucketIDKeyName = []byte("bidx-cbid")

	// metadataBucketID is the ID of the top-level metadata bucket.
	// It is the value 0 encoded as an unsigned big-endian uint32.
	metadataBucketID = [bucketIdLen]byte{}

	// sequenceBucketId is the prefix used for all sequences generated
	// by the store.
	sequenceBucketID = [bucketIdLen]byte{1}

	// manifestKey is the name of the top-level manifest key.
	manifestKey = []byte("manifest")
)

// Common error strings.
const (
	// errDbNotOpenStr is the text to use for the store.ErrDbNotOpen
	// error code.
	errDbNotOpenStr = "database is not open"

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
func convertErr(desc string, bdbErr error) store.Error {
	// Use the driver-specific error code by default.  The code below will
	// update this with the converted error if it's recognized.
	var code = store.ErrDriverSpecific

	switch bdbErr {
	// Database open/create errors.
	case badger.ErrDBClosed:
		code = store.ErrDbNotOpen

	// Transaction errors.
	case badger.ErrConflict:
		code = store.ErrTxConflict
	case badger.ErrReadOnlyTxn:
		code = store.ErrTxNotWritable
	case badger.ErrDiscardedTxn:
		code = store.ErrTxClosed
	case badger.ErrEmptyKey:
		code = store.ErrKeyRequired
	}

	return store.Error{ErrorCode: code, Description: desc, Err: bdbErr}
}

// copySlice returns a copy of the passed slice.  This is mostly used to copy
// badger iterator keys and values since they are only valid until the iterator
// is moved instead of during the entirety of the transaction.
func copySlice(slice []byte) []byte {
	ret := make([]byte, len(slice))
	copy(ret, slice)
	return ret
}

// parseArgs parses the arguments from the database Open/Create methods.
func parseArgs(funcName string, args ...interface{}) (string, string, error) {
	if len(args) < 1 {
		return "", "", fmt.Errorf("invalid arguments to %s.%s -- "+
			"expected database path and optional block network", dbType,
			funcName)
	}

	dbPath, ok := args[0].(string)
	if !ok {
		return "", "", fmt.Errorf("first argument to %s.%s is invalid -- "+
			"expected database path string", dbType, funcName)
	}

	if len(args) == 1 || args[1] == nil {
		return dbPath, dbPath, nil
	}

	dbValuePath, ok := args[1].(string)
	if !ok {
		return "", "", fmt.Errorf("second argument to %s.%s is invalid -- "+
			"expected database value file path", dbType, funcName)
	}

	return dbPath, dbValuePath, nil
}

// openDBDriver is the callback provided during driver registration that opens
// an existing database for use.
func openDBDriver(args ...interface{}) (store.DB, error) {
	dbPath, dbValuePath, err := parseArgs("Open", args...)
	if err != nil {
		return nil, err
	}

	return openDB(dbPath, dbValuePath, false)
}

// createDBDriver is the callback provided during driver registration that
// creates, initializes, and opens a database for use.
func createDBDriver(args ...interface{}) (store.DB, error) {
	dbPath, dbValuePath, err := parseArgs("Create", args...)
	if err != nil {
		return nil, err
	}

	return openDB(dbPath, dbValuePath, true)
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
		panic(fmt.Sprintf("Failed to regiser database driver '%s': %v",
			dbType, err))
	}
}
