// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package mem

import (
	"bytes"
	"fmt"
	"sync"

	"blockwatch.cc/knoxdb/pkg/store"
	"github.com/RaduBerinde/btreemap"
)

const (
	// type name for this driver
	dbType = "mem"
)

// ensure types implement store interface
var (
	_ store.Factory   = (*driver)(nil)
	_ store.DB        = (*db)(nil)
	_ store.DBManager = (*db)(nil)
	_ store.Tx        = (*tx)(nil)
	_ store.Bucket    = (*bucket)(nil)
)

// Registry stores open memdb instances
var registry sync.Map

func init() {
	if err := store.RegisterDriver(&driver{}); err != nil {
		panic(fmt.Errorf("register database driver %q: %v", dbType, err))
	}
}

type driver struct{}

func (d *driver) Type() string {
	return dbType
}

func (d *driver) Create(opts store.Options) (store.DBManager, error) {
	val, ok := registry.Load(opts.Path)
	if ok {
		return nil, store.ErrDatabaseExists
	}
	if val != nil {
		db := val.(*db)
		db.closed = false
		return db, nil
	}

	// create new memdb instance
	db := &db{
		store:   btreemap.New[[]byte, []byte](32, bytes.Compare),
		buckets: map[string]uint32{root: 0},
		opts:    opts,
		log:     opts.Log,
	}
	registry.Store(opts.Path, db)

	return db, nil
}

func (d *driver) Open(opts store.Options) (store.DBManager, error) {
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
