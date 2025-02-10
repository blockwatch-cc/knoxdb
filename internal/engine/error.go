// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"errors"
)

var (
	ErrNoDatabase = errors.New("database not found")
	ErrNoEngine   = errors.New("engine does not exist")
	ErrNoDriver   = errors.New("driver does not exist")
	ErrNoTable    = errors.New("table does not exist")
	ErrNoStore    = errors.New("store does not exist")
	ErrNoIndex    = errors.New("index does not exist")
	ErrNoEnum     = errors.New("enum does not exist")
	ErrNoColumn   = errors.New("column does not exist")
	ErrNoBucket   = errors.New("bucket does not exist")
	ErrNoPk       = errors.New("primary key not defined")
	ErrNoKey      = errors.New("key not found")
	ErrNoField    = errors.New("field does not exist")
	ErrNoTx       = errors.New("missing transaction")
	ErrNilValue   = errors.New("nil value passed")

	ErrDatabaseExists   = errors.New("database already exists")
	ErrDatabaseReadOnly = errors.New("database is read-only")
	ErrDatabaseClosed   = errors.New("database is closed")
	ErrDatabaseCorrupt  = errors.New("database file corrupt")
	ErrDatabaseShutdown = errors.New("database is shutting down")

	ErrTableExists       = errors.New("table already exists")
	ErrStoreExists       = errors.New("store already exists")
	ErrIndexExists       = errors.New("index already exists")
	ErrEnumExists        = errors.New("enum already exists")
	ErrEnumInUse         = errors.New("enum is referenced")
	ErrResultClosed      = errors.New("result already closed")
	ErrInvalidObjectType = errors.New("invalid object type")
	ErrInvalidId         = errors.New("invalid object id")
	ErrTableDropWithRefs = errors.New("table is referenced")
	ErrTableReadOnly     = errors.New("table is read-only")
	ErrRecordNotFound    = errors.New("record not found")

	EndStream = errors.New("end stream")

	ErrTxConflict     = errors.New("transaction conflict")
	ErrTxReadonly     = errors.New("transaction is read-only")
	ErrTxClosed       = errors.New("transaction is closed")
	ErrShortMessage   = errors.New("short message buffer")
	ErrTooManyTasks   = errors.New("too many running tasks")
	ErrNotImplemented = errors.New("feature not implemented")
)
