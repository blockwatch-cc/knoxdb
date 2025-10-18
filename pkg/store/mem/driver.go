// Copyright (c) 2024-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package mem

import (
	"bytes"
	"fmt"

	"blockwatch.cc/knoxdb/pkg/store"
	"github.com/google/btree"
)

const (
	dbType = "mem"

	// bucketIdLen is the length in bytes for bucket ids
	bucketIdLen int = 1
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
	val, ok := registry.Load(opts.Path)
	if ok {
		return nil, store.ErrDatabaseExists
	}
	if val != nil {
		db := val.(*db)
		db.closed = false
		return db, nil
	}

	db := &db{
		store:     btree.NewG[Item](2, func(a, b Item) bool { return bytes.Compare(a.Key, b.Key) < 0 }),
		opts:      opts,
		sequences: make(map[string]*sequence),
		bucketIds: make(map[string][bucketIdLen]byte),
	}

	opts.Log.Debug("Initializing database...")

	// init manifest
	db.manifest = opts.Manifest
	if db.manifest == nil {
		db.manifest = store.NewManifestFromOpts(opts)
	}

	// init buckets
	db.bucketIds["root"] = [bucketIdLen]byte{}

	registry.Store(opts.Path, db)

	return db, nil
}

func (d *driver) Open(opts store.Options) (store.DB, error) {
	val, ok := registry.Load(opts.Path)
	if !ok {
		return nil, store.ErrDatabaseNotFound
	}
	db := val.(*db)
	db.closed = false
	return db, nil
}

func (d *driver) Drop(path string) error {
	val, ok := registry.Load(path)
	if !ok {
		return store.ErrDatabaseNotFound
	}
	if !val.(*db).closed {
		return store.ErrDatabaseOpen
	}
	registry.Delete(path)
	return nil
}

func (d *driver) Exists(path string) (bool, error) {
	_, ok := registry.Load(path)
	return ok, nil
}
