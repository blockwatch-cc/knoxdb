// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"blockwatch.cc/knoxdb/pkg/store"
	bolterr "go.etcd.io/bbolt/errors"
)

// wrap converts an internal bolt error into an interface error with an
// equivalent meaning.
func wrap(err error) error {
	if err == nil {
		return nil
	}

	switch err {
	// Database errors.
	case bolterr.ErrInvalid, bolterr.ErrChecksum, bolterr.ErrFreePagesNotLoaded:
		return store.ErrDatabaseCorrupt

	case bolterr.ErrDatabaseNotOpen:
		return store.ErrDatabaseClosed

	case bolterr.ErrVersionMismatch:
		return store.ErrInvalidVersion

	case bolterr.ErrInvalidMapping:
		return store.ErrIO

	case bolterr.ErrTimeout:
		return store.ErrDatabaseAccess

	case bolterr.ErrDifferentDB:
		return store.ErrMoveAcrossDbs

	// Transaction errors.
	case bolterr.ErrTxNotWritable, bolterr.ErrDatabaseReadOnly:
		return store.ErrTxReadonly

	case bolterr.ErrTxClosed:
		return store.ErrTxClosed

	// Bucket errors
	case bolterr.ErrBucketNotFound:
		return store.ErrBucketNotFound

	case bolterr.ErrBucketExists, bolterr.ErrSameBuckets:
		return store.ErrBucketExists

	case bolterr.ErrBucketNameRequired:
		return store.ErrKeyRequired

	// key/value errors
	case bolterr.ErrKeyRequired:
		return store.ErrKeyRequired

	case bolterr.ErrKeyTooLarge:
		return store.ErrKeyTooLarge

	case bolterr.ErrValueTooLarge:
		return store.ErrValueTooLarge

	case bolterr.ErrIncompatibleValue:
		return store.ErrIncompatibleValue

	default:
		return err
	}
}
