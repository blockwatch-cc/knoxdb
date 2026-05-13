// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store

import (
	"errors"
)

var (
	ErrDriverRegistered  = errors.New("driver already registered")
	ErrDriverUnknown     = errors.New("driver not found")
	ErrDatabaseNotFound  = errors.New("database not found")
	ErrDatabaseExists    = errors.New("database exists")
	ErrDatabaseClosed    = errors.New("database closed")
	ErrDatabaseOpen      = errors.New("database open")
	ErrDatabaseCorrupt   = errors.New("database corrupt")
	ErrDatabaseAccess    = errors.New("database opened by another process")
	ErrDatabaseFull      = errors.New("max database size reached")
	ErrDatabaseNotEmpty  = errors.New("database is not empty")
	ErrIO                = errors.New("IO error")
	ErrBadAddress        = errors.New("bad memory address")
	ErrTxClosed          = errors.New("tx closed")
	ErrTxReadonly        = errors.New("tx readonly")
	ErrTxManaged         = errors.New("tx is managed")
	ErrTxWouldBlock      = errors.New("concurrent tx is running")
	ErrBucketNotFound    = errors.New("bucket not found")
	ErrBucketExists      = errors.New("bucket exists")
	ErrKeyRequired       = errors.New("key required")
	ErrKeyNotFound       = errors.New("key not found")
	ErrKeyTooLarge       = errors.New("key too large")
	ErrValueTooLarge     = errors.New("value too large")
	ErrNotImplemented    = errors.New("feature not implemented")
	ErrInvalidVersion    = errors.New("invalid version")
	ErrInvalidLabel      = errors.New("invalid manifest label")
	ErrIncompatibleValue = errors.New("incompatible delete (bucket/value mismatch)")
	ErrMoveAcrossDbs     = errors.New("unsupported move across databases")
	ErrInvalidCursor     = errors.New("invalid cursor")
)
