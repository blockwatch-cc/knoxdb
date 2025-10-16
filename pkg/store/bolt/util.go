// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"os"

	"blockwatch.cc/knoxdb/pkg/store"
	bolterr "go.etcd.io/bbolt/errors"
)

// convertErr converts the passed bolt error into a database error with an
// equivalent error code  and the passed description.  It also sets the passed
// error as the underlying error.
func wrap(err error) error {
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

// filesExists reports whether the named file or directory exists.
func fileExists(name string) (bool, error) {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func syncDir(name string) error {
	dir, err := os.Open(name)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
