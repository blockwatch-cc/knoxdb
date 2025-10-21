// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/pkg/store"
)

// Bolt Limits
// Max Tx size: 15% of MaxTableSize, default = 0.15*64M = 10M
// Max key update count per Tx: default ~ 100k

const (
	// backend name
	dbType = "bolt"

	// max size of compact transactions
	compactTxSize int64 = 1048576

	// default directory permissions
	permPath = 0700
	permFile = 0600
)

var (
	manifestBucketKey = []byte(".")
	manifestKey       = []byte("_MANIFEST")
)

// ensure types implement store interface
var (
	_ store.Factory  = (*driver)(nil)
	_ store.DB       = (*db)(nil)
	_ store.Tx       = (*transaction)(nil)
	_ store.Bucket   = (*bucket)(nil)
	_ store.Sequence = (*sequence)(nil)
	_ store.Cursor   = (*cursor)(nil)
)

func init() {
	if err := store.RegisterDriver(&driver{}); err != nil {
		panic(fmt.Errorf("failed to register database driver %q: %v", dbType, err))
	}
}

type driver struct{}

func (d *driver) Name() string {
	return dbType
}

func (d *driver) Create(opts store.Options) (store.DB, error) {
	// check file exists, only fail on permissions not of path/file does not exist
	exists, err := fileExists(opts.Path)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, store.ErrDatabaseExists
	}

	// ensure the full path to the database exists
	if err := os.MkdirAll(filepath.Dir(opts.Path), permPath); err != nil {
		return nil, err
	}

	// opts.Log.Debug("Creating database %s", opts.Path)

	// boltdb will create the database file
	b, err := bolt.Open(opts.Path, permFile, makeBoltOpts(opts))
	if err != nil {
		return nil, wrap(err)
	}

	// opts.Log.Debug("Initializing database.")
	m := opts.Manifest
	if m == nil {
		m = store.NewManifestFromOpts(opts)
	}
	err = b.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket(manifestBucketKey)
		if err != nil {
			return err
		}
		return b.Put(manifestKey, m.Bytes())
	})
	if err != nil {
		return nil, wrap(err)
	}

	return &db{store: b, opts: opts}, nil
}

func (d *driver) Open(opts store.Options) (store.DB, error) {
	exists, err := fileExists(opts.Path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrDatabaseNotFound
	}

	// opts.Log.Debug("Opening database %s", opts.Path)
	b, err := bolt.Open(opts.Path, permFile, makeBoltOpts(opts))
	if err != nil {
		return nil, wrap(err)
	}

	// check manifest if set in opts
	var m store.Manifest
	err = b.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(manifestBucketKey)
		if bucket == nil {
			opts.Log.Errorf("missing db manifest")
			return store.ErrDatabaseCorrupt
		}
		buf := bucket.Get(manifestKey)
		if buf == nil {
			opts.Log.Errorf("missing db manifest")
			return store.ErrDatabaseCorrupt
		}
		if err := m.UnmarshalBinary(buf); err != nil {
			opts.Log.Errorf("reading db manifest: %v", err)
			return store.ErrDatabaseCorrupt
		}
		return m.Validate(opts.Manifest)
	})
	if err != nil {
		b.Close()
		return nil, err
	}

	return &db{store: b, opts: opts}, nil
}

func (d *driver) Drop(path string) error {
	return os.Remove(path)
}

func (d *driver) Exists(path string) (bool, error) {
	return fileExists(path)
}

func makeBoltOpts(o store.Options) *bolt.Options {
	return &bolt.Options{
		ReadOnly:        o.Readonly,
		Timeout:         time.Second,
		FreelistType:    bolt.FreelistMapType,
		NoSync:          o.NoSync,
		NoGrowSync:      o.NoGrowSync,
		NoFreelistSync:  o.NoSync,
		PageSize:        o.PageSize,
		MmapFlags:       o.MmapFlags,
		InitialMmapSize: o.InitialMmapSize,
		// Logger:          logger{o.Log},
	}
}

// type logger struct {
// 	log.Logger
// }

// func (l logger) Warning(v ...interface{}) {
// 	l.Logger.Warn(v...)
// }

// func (l logger) Warningf(format string, v ...interface{}) {
// 	l.Logger.Warnf(format, v...)
// }
