// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package boltdb

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/packdb-pro/store"
	logpkg "github.com/echa/log"
)

var log = logpkg.Disabled

const (
	dbType = "bolt"
)

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
