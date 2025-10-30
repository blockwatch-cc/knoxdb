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
	ErrNoRecord   = errors.New("record not found")
	ErrNoTable    = errors.New("table does not exist")
	ErrNoStore    = errors.New("store does not exist")
	ErrNoIndex    = errors.New("index does not exist")
	ErrNoPkIndex  = errors.New("primary key index does not exist")
	ErrNoEnum     = errors.New("enum does not exist")
	ErrNoMeta     = errors.New("missing row metadata")
	ErrNoKey      = errors.New("key not found")
	ErrNoPk       = errors.New("missing primary key")
	ErrNoTx       = errors.New("missing transaction")

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
	ErrResultOverflow    = errors.New("result overflow")
	ErrInvalidObjectType = errors.New("invalid object type")
	ErrInvalidId         = errors.New("invalid object id")
	ErrTableDropWithRefs = errors.New("table is referenced")
	ErrTableReadOnly     = errors.New("table is read-only")
	ErrTableNotEmpty     = errors.New("table is not empty")

	ErrTxConflict     = errors.New("transaction conflict")
	ErrTxReadonly     = errors.New("transaction is read-only")
	ErrTxClosed       = errors.New("transaction is closed")
	ErrTxTimeout      = errors.New("write transaction wait timed out")
	ErrShortMessage   = errors.New("short message buffer")
	ErrTooManyTasks   = errors.New("too many running tasks")
	ErrNotImplemented = errors.New("feature not implemented")
	ErrAgain          = errors.New("try again")
)
