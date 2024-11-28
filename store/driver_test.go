// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store_test

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/store"
	_ "blockwatch.cc/knoxdb/store/bolt"
)

var (
	// ignoreDbTypes are types which should be ignored when running tests
	// that iterate all supported DB types.  This allows some tests to add
	// bogus drivers for testing purposes while still allowing other tests
	// to easily iterate all supported drivers.
	ignoreDbTypes = map[string]bool{"createopenfail": true}
)

// checkDbError ensures the passed error is a database.Error with an error code
// that matches the passed  error code.
func checkDbError(t *testing.T, testName string, gotErr error, wantErrCode store.ErrorCode) bool {
	dbErr, ok := gotErr.(store.Error)
	if !ok {
		t.Errorf("%s: unexpected error type - got %T, want %T",
			testName, gotErr, store.Error{})
		return false
	}
	if dbErr.ErrorCode != wantErrCode {
		t.Errorf("%s: unexpected error code - got %s (%s), want %s",
			testName, dbErr.ErrorCode, dbErr.Description,
			wantErrCode)
		return false
	}

	return true
}

// TestAddDuplicateDriver ensures that adding a duplicate driver does not
// overwrite an existing one.
func TestAddDuplicateDriver(t *testing.T) {
	supportedDrivers := store.SupportedDrivers()
	if len(supportedDrivers) == 0 {
		t.Errorf("no backends to test")
		return
	}
	dbType := supportedDrivers[0]

	// bogusCreateDB is a function which acts as a bogus create and open
	// driver function and intentionally returns a failure that can be
	// detected if the interface allows a duplicate driver to overwrite an
	// existing one.
	bogusCreateDB := func(args ...interface{}) (store.DB, error) {
		return nil, fmt.Errorf("duplicate driver allowed for database "+
			"type [%v]", dbType)
	}

	// Create a driver that tries to replace an existing one.  Set its
	// create and open functions to a function that causes a test failure if
	// they are invoked.
	driver := store.Driver{
		DbType: dbType,
		Create: bogusCreateDB,
		Open:   bogusCreateDB,
	}
	testName := "duplicate driver registration"
	err := store.RegisterDriver(driver)
	if !checkDbError(t, testName, err, store.ErrDbTypeRegistered) {
		return
	}
}

// TestCreateOpenFail ensures that errors which occur while opening or closing
// a database are handled properly.
func TestCreateOpenFail(t *testing.T) {
	// bogusCreateDB is a function which acts as a bogus create and open
	// driver function that intentionally returns a failure which can be
	// detected.
	dbType := "createopenfail"
	openError := fmt.Errorf("failed to create or open database for "+
		"database type [%v]", dbType)
	bogusCreateDB := func(args ...interface{}) (store.DB, error) {
		return nil, openError
	}

	// Create and add driver that intentionally fails when created or opened
	// to ensure errors on database open and create are handled properly.
	driver := store.Driver{
		DbType: dbType,
		Create: bogusCreateDB,
		Open:   bogusCreateDB,
	}
	store.RegisterDriver(driver)

	// Ensure creating a database with the new type fails with the expected
	// error.
	_, err := store.Create(dbType)
	if err != openError {
		t.Errorf("expected error not received - got: %v, want %v", err,
			openError)
		return
	}

	// Ensure opening a database with the new type fails with the expected
	// error.
	_, err = store.Open(dbType)
	if err != openError {
		t.Errorf("expected error not received - got: %v, want %v", err,
			openError)
		return
	}
}

// TestCreateOpenUnsupported ensures that attempting to create or open an
// unsupported database type is handled properly.
func TestCreateOpenUnsupported(t *testing.T) {
	// Ensure creating a database with an unsupported type fails with the
	// expected error.
	testName := "create with unsupported database type"
	dbType := "unsupported"
	_, err := store.Create(dbType)
	if !checkDbError(t, testName, err, store.ErrDbUnknownType) {
		return
	}

	// Ensure opening a database with the an unsupported type fails with the
	// expected error.
	testName = "open with unsupported database type"
	_, err = store.Open(dbType)
	if !checkDbError(t, testName, err, store.ErrDbUnknownType) {
		return
	}
}
