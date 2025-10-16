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
	ErrIO                = errors.New("IO error")
	ErrTxClosed          = errors.New("tx closed")
	ErrTxReadonly        = errors.New("tx readonly")
	ErrBucketNotFound    = errors.New("bucket not found")
	ErrBucketExists      = errors.New("bucket exists")
	ErrKeyRequired       = errors.New("key required")
	ErrKeyTooLarge       = errors.New("key too large")
	ErrValueTooLarge     = errors.New("value too large")
	ErrIncompatibleValue = errors.New("incompatible delete (bucket/value mismatch)")
	ErrMoveAcrossDbs     = errors.New("unsupported move across databases")
	ErrNotImplemented    = errors.New("feature not implemented")
	ErrInvalidCursor     = errors.New("invalid cursor")
	ErrInvalidVersion    = errors.New("invalid version")
)
