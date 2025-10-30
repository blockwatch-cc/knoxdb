// Copyright (c) 2018-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/pkg/store"
	"blockwatch.cc/knoxdb/pkg/util"
)

// db wraps a boltdb instance and implements the store.DB interface.
// All database access is performed through transactions which are managed.
type db struct {
	store *bolt.DB      // the database
	opts  store.Options // options copy (used during GC which opens a new file)
}

// Enforce db implements the store.DB interface.
var _ store.DB = (*db)(nil)

// Type returns the database's driver name.
func (db *db) Type() string {
	return dbType
}

func (db *db) IsReadOnly() bool {
	return db.store.IsReadOnly()
}

// IsZeroCopyRead returns true if keys and values on Get and from Cursors
// are only valid within the current transaction (or iterator step).
func (db *db) IsZeroCopyRead() bool {
	return true
}

// Path returns the path where the current database is stored.
func (db *db) Path() string {
	return db.store.Path()
}

// Sequence creates a new managed sequence stored in the sequences bucket.
func (db *db) Sequence(key []byte, lease uint64) (store.Sequence, error) {
	return &sequence{
		db:  db,
		key: bytes.Clone(key),
	}, nil
}

// begin starts a boltdb transaction and returns its internal handle
func (db *db) begin(writable bool) (*transaction, error) {
	tx, err := db.store.Begin(writable)
	if err != nil {
		return nil, wrap(err)
	}
	return &transaction{
		db: db,
		tx: tx,
	}, nil
}

// Begin starts a transaction which is either read-only or read-write depending
// on the specified flag.  Multiple read-only transactions can be started
// simultaneously while only a single read-write transaction can be started at a
// time.  The call will block when starting a read-write transaction when one is
// already open.
//
// NOTE: The transaction must be closed by calling Rollback or Commit.
// Failure to do so will result in unclaimed memory.
func (db *db) Begin(writable bool) (store.Tx, error) {
	return db.begin(writable)
}

// View invokes the passed function in the context of a read-only
// transaction with the root bucket for the namespace. Any error
// returned from the user-supplied function will abort the transaction.
func (db *db) View(fn func(store.Tx) error) error {
	err := db.store.View(func(tx *bolt.Tx) error {
		wtx := &transaction{
			db:      db,
			tx:      tx,
			managed: true,
		}
		return fn(wtx)
	})
	return wrap(err)
}

// Update invokes the passed function in the context of a managed read-write
// transaction with the root bucket for the namespace.  Any errors returned from
// the user-supplied function will cause the transaction to be rolled back and
// are returned from this function. On success the transaction is committed.
func (db *db) Update(fn func(store.Tx) error) error {
	err := db.store.Update(func(tx *bolt.Tx) error {
		wtx := &transaction{
			db:      db,
			tx:      tx,
			managed: true,
		}
		return fn(wtx)
	})
	return wrap(err)
}

// Close shuts down the database and syncs all data. It will block
// until all database transactions have been committed or rolled back.
func (db *db) Close() error {
	if err := db.store.Close(); err != nil {
		return wrap(err)
	}
	db.store = nil
	return nil
}

func (db *db) Sync() error {
	return db.store.Sync()
}

// Database maintenance functions

// Exports all database pages to a writer. Use to create a working backup
// of a database.
func (db *db) Dump(w io.Writer) error {
	// backup may run in parallel to any tx and will be using a snapshot copy
	err := db.store.View(func(tx *bolt.Tx) error {
		db.opts.Log.Debugf("Exporting database of size %s (this may take a while)...",
			util.ByteSize(tx.Size()).String())
		n, err := tx.WriteTo(w)
		if err != nil {
			return err
		}
		db.opts.Log.Debugf("Successfully wrote %s of data.", util.ByteSize(n).String())
		return nil
	})
	return wrap(err)
}

// Should be called on a database that is not running any other
// concurrent transactions while it is running.
func (db *db) Restore(r io.Reader) error {
	// not implemented; to do so implement
	// - close bolt db (waiting for any open tx)
	// - restore/overwrite file with reader contents
	// - open bolt db from restored file
	return store.ErrNotImplemented
}

// Should be called on a database that is not running any concurrent tx.
//
// Garbage collect database. This will create a new file, stream all keys into
// that file, replace the existing DB with the new file and reopen the DB.
func (db *db) GC(ctx context.Context, ratio float64) error {
	start := time.Now()
	srcPath := db.store.Path()
	dstPath := srcPath + ".temp"
	log := db.opts.Log

	// sync db file
	if err := db.Sync(); err != nil {
		return err
	}

	// stat
	fi, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	initialSize := fi.Size()

	// Open destination database.
	dopts := db.opts
	dopts.Readonly = false
	dst, err := bolt.Open(dstPath, fi.Mode(), makeBoltOpts(dopts))
	if err != nil {
		return wrap(err)
	}

	defer func(dst *bolt.DB, dstPath string) {
		if err != nil {
			dst.Close()
			os.Remove(dstPath)
		}
	}(dst, dstPath)

	// Run compaction.
	log.Infof("[GC] compacting database %s (%s).", srcPath, util.ByteSize(initialSize))
	if err = compact(ctx, dst, db.store, compactTxSize, ratio); err != nil {
		return wrap(err)
	}

	// sync target db
	if err = dst.Sync(); err != nil {
		return wrap(err)
	}

	// Report stats on new size.
	fi, err = os.Stat(dstPath)
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		return fmt.Errorf("zero size after compaction")
	}
	log.Infof("[GC] backend %s successfully compacted %s -> %s (gain=%.2fx) in %s.",
		srcPath,
		util.ByteSize(initialSize),
		util.ByteSize(fi.Size()),
		float64(initialSize)/float64(fi.Size()),
		time.Since(start))

	// replace db - point of no return
	// also, don't overwrite err to avoid triggering defer
	if err := dst.Close(); err != nil {
		return wrap(err)
	}
	if err := db.store.Close(); err != nil {
		return wrap(err)
	}
	if err := os.Rename(srcPath, srcPath+".backup"); err != nil {
		return err
	}
	if err := os.Rename(dstPath, srcPath); err != nil {
		return err
	}

	// fsync directory here
	if err := syncDir(filepath.Dir(srcPath)); err != nil {
		log.Errorf("sync directory: %v", err)
	}

	// reopen compacted db
	db.store, err = bolt.Open(srcPath, fi.Mode(), makeBoltOpts(db.opts))
	if err != nil {
		return wrap(err)
	}
	log.Debugf("[GC] backend %s reopened successfully.", db.Path())

	// when all is good, remove the backup file, ignoring errors
	_ = os.Remove(srcPath + ".backup")
	log.Info("[GC] using compacted backend from now.")
	return nil
}

func compact(ctx context.Context, dst, src *bolt.DB, txMaxSize int64, fillPercent float64) error {
	// commit regularly, or we'll run out of memory for large datasets if using one transaction.
	var size int64
	tx, err := dst.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := walk(src, func(keys [][]byte, k, v []byte, seq uint64) error {
		// On each key/value, check if we have exceeded tx size.
		sz := int64(len(k) + len(v))
		if size+sz > txMaxSize && txMaxSize != 0 {
			// Commit previous transaction.
			if err := tx.Commit(); err != nil {
				return err
			}

			// check context
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Start new transaction.
			tx, err = dst.Begin(true)
			if err != nil {
				return err
			}
			size = 0
		}
		size += sz

		// Create bucket on the root transaction if this is the first level.
		nk := len(keys)
		if nk == 0 {
			bkt, err := tx.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Create buckets on subsequent levels, if necessary.
		b := tx.Bucket(keys[0])
		if nk > 1 {
			for _, k := range keys[1:] {
				b = b.Bucket(k)
			}
		}

		// Fill the entire page for best compaction.
		b.FillPercent = fillPercent

		// If there is no value then this is a bucket call.
		if v == nil {
			bkt, err := b.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Otherwise treat it as a key/value pair.
		return b.Put(k, v)
	}); err != nil {
		return err
	}

	return tx.Commit()
}

// walkFunc is the type of the function called for keys (buckets and "normal"
// values) discovered by Walk. keys is the list of keys to descend to the bucket
// owning the discovered key/value pair k/v.
type walkFunc func(keys [][]byte, k, v []byte, seq uint64) error

// walk walks recursively the bolt database db, calling walkFn for each key it finds.
func walk(db *bolt.DB, walkFn walkFunc) error {
	return db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return walkBucket(b, nil, name, nil, b.Sequence(), walkFn)
		})
	})
}

func walkBucket(b *bolt.Bucket, keypath [][]byte, k, v []byte, seq uint64, fn walkFunc) error {
	// Execute callback.
	if err := fn(keypath, k, v, seq); err != nil {
		return err
	}

	// If this is not a bucket then stop.
	if v != nil {
		return nil
	}

	// Iterate over each child key/value.
	keypath = append(keypath, k)
	return b.ForEach(func(k, v []byte) error {
		if v == nil {
			bkt := b.Bucket(k)
			return walkBucket(bkt, keypath, k, nil, bkt.Sequence(), fn)
		}
		return walkBucket(b, keypath, k, v, b.Sequence(), fn)
	})
}
