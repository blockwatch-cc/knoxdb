// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"errors"
)

var (
	ErrNoDatabase = errors.New("knox: database not found")
	ErrNoEngine   = errors.New("knox: engine does not exist")
	ErrNoDriver   = errors.New("knox: driver does not exist")
	ErrNoTable    = errors.New("knox: table does not exist")
	ErrNoStore    = errors.New("knox: store does not exist")
	ErrNoIndex    = errors.New("knox: index does not exist")
	ErrNoEnum     = errors.New("knox: enum does not exist")
	ErrNoColumn   = errors.New("knox: column does not exist")
	ErrNoBucket   = errors.New("knox: bucket does not exist")
	ErrNoPk       = errors.New("knox: primary key not defined")
	ErrNoKey      = errors.New("knox: key not found")
	ErrNoField    = errors.New("knox: field does not exist")
	ErrNoTx       = errors.New("knox: missing transaction")
	ErrNilValue   = errors.New("knox: nil value passed")

	ErrDatabaseExists   = errors.New("knox: database already exists")
	ErrDatabaseReadOnly = errors.New("knox: database is read-only")
	ErrDatabaseClosed   = errors.New("knox: database is closed")
	ErrDatabaseCorrupt  = errors.New("knox: database file corrupt")
	ErrDatabaseShutdown = errors.New("knox: database is shutting down")

	ErrTableExists       = errors.New("knox: table already exists")
	ErrStoreExists       = errors.New("knox: store already exists")
	ErrIndexExists       = errors.New("knox: index already exists")
	ErrEnumExists        = errors.New("knox: enum already exists")
	ErrResultClosed      = errors.New("knox: result already closed")
	ErrInvalidObjectType = errors.New("knox: invalid object type")
	ErrTableDropWithRefs = errors.New("knox: table is referenced")
	ErrTableReadOnly     = errors.New("knox: table is read-only")

	EndStream = errors.New("end stream")

	ErrTxConflict     = errors.New("transaction conflict")
	ErrTxReadonly     = errors.New("transaction is read-only")
	ErrTxClosed       = errors.New("transaction is closed")
	ErrShortMessage   = errors.New("short message buffer")
	ErrNotImplemented = errors.New("feature not implemented")
)
