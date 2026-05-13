// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"blockwatch.cc/knoxdb/pkg/store"
	"github.com/echa/log"
	bolt "go.etcd.io/bbolt"
)

// Bolt Limits
// Max Tx size: 15% of MaxTableSize, default = 0.15*64M = 10M
// Max key update count per Tx: default ~ 100k

const (
	// backend name
	dbType = "bolt"

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
	_ store.Factory   = (*driver)(nil)
	_ store.DB        = (*db)(nil)
	_ store.DBManager = (*db)(nil)
	_ store.Tx        = (*tx)(nil)
	_ store.Bucket    = (*bucket)(nil)
)

func init() {
	if err := store.RegisterDriver(&driver{}); err != nil {
		panic(fmt.Errorf("failed to register database driver %q: %v", dbType, err))
	}
}

type driver struct{}

func (d *driver) Type() string {
	return dbType
}

func (d *driver) Create(opts store.Options) (store.DBManager, error) {
	// make directory
	if err := store.EnsureDirExists(filepath.Dir(opts.Path)); err != nil {
		return nil, err
	}

	// check file exists, only fail on permissions not if file does not exist
	exists, err := store.CheckFileExists(opts.Path)
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

	// boltdb will create the database file
	b, err := bolt.Open(opts.Path, permFile, makeBoltOpts(opts))
	if err != nil {
		return nil, wrap(err)
	}

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

func (d *driver) Open(opts store.Options) (store.DBManager, error) {
	exists, err := store.CheckFileExists(opts.Path)
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
	return store.CheckFileExists(path)
}

func makeBoltOpts(o store.Options) *bolt.Options {
	return &bolt.Options{
		ReadOnly:       o.Readonly,
		Timeout:        time.Second,
		FreelistType:   bolt.FreelistMapType,
		NoSync:         o.NoSync,
		NoGrowSync:     o.NoSync,
		NoFreelistSync: o.NoSync,
		PageSize:       o.PageSize,
		Logger:         logger{o.Log},
	}
}

type logger struct {
	log log.Logger
}

// redirect info -> debug
func (l logger) Info(v ...any)                 { l.log.Debug(v...) }
func (l logger) Infof(format string, v ...any) { l.log.Debugf(format, v...) }

// redirect debug -> trace
func (l logger) Debug(v ...any)                 { l.log.Trace(v...) }
func (l logger) Debugf(format string, v ...any) { l.log.Tracef(format, v...) }

// rename: bbolt Logger interface has custom name for warning level
func (l logger) Warning(v ...any)                 { l.log.Warn(v...) }
func (l logger) Warningf(format string, v ...any) { l.log.Warnf(format, v...) }

// keep
func (l logger) Error(v ...any)                 { l.log.Error(v...) }
func (l logger) Errorf(format string, v ...any) { l.log.Errorf(format, v...) }
func (l logger) Fatal(v ...any)                 { l.log.Fatal(v...) }
func (l logger) Fatalf(format string, v ...any) { l.log.Fatalf(format, v...) }
func (l logger) Panic(v ...any)                 { l.log.Panic(v...) }
func (l logger) Panicf(format string, v ...any) { l.log.Panicf(format, v...) }
