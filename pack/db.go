// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"blockwatch.cc/knoxdb/store"
)

var (
	ErrNoTable      = errors.New("pack: table does not exist")
	ErrNoIndex      = errors.New("pack: index does not exist")
	ErrNoColumn     = errors.New("pack: column does not exist")
	ErrTypeMismatch = errors.New("pack: type mismatch")
	ErrNoField      = errors.New("pack: field does not exist")
	ErrInvalidType  = errors.New("pack: unsupported block type")

	ErrIndexNotFound  = errors.New("pack: index not found")
	ErrBucketNotFound = errors.New("pack: bucket not found")
	ErrPackNotFound   = errors.New("pack: pack not found")
	ErrPackStripped   = errors.New("pack: pack is stripped")
	ErrIdNotFound     = errors.New("pack: id not found")

	ErrTableExists  = errors.New("pack: table already exists")
	ErrIndexExists  = errors.New("pack: index already exists")
	ErrResultClosed = errors.New("pack: result already closed")

	bigEndian = binary.BigEndian
)

const (
	schemaVersion = 2
	txMaxSize     = 128 // flush boltdb tx after N pending packs
)

type DB struct {
	db     store.DB
	tables map[string]*Table
}

type Tx struct {
	tx      store.Tx
	db      *DB
	pending int
}

func CreateDatabase(path, name, label string, opts interface{}) (*DB, error) {
	db, err := store.Create("bolt", filepath.Join(path, name+".db"), opts)
	if err != nil {
		return nil, fmt.Errorf("pack: creating database: %v", err)
	}
	err = db.SetManifest(store.Manifest{
		Name:    name,
		Label:   label,
		Version: schemaVersion,
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &DB{
		db:     db,
		tables: make(map[string]*Table),
	}, nil
}

func CreateDatabaseIfNotExists(path, name, label string, opts interface{}) (*DB, error) {
	db, err := OpenDatabase(path, name, label, opts)
	if err == nil {
		return db, nil
	}
	if err != nil && !store.IsError(err, store.ErrDbDoesNotExist) {
		return nil, err
	}
	return CreateDatabase(path, name, label, opts)
}

func OpenDatabase(path, name, label string, opts interface{}) (*DB, error) {
	db, err := store.Open("bolt", filepath.Join(path, name+".db"), opts)
	if err != nil {
		return nil, err
	}
	mft, err := db.Manifest()
	if err != nil {
		return nil, fmt.Errorf("pack: reading manifest: %v", err)
	}
	if mft.Version != schemaVersion {
		return nil, fmt.Errorf("pack: invalid DB schema version %d (expected version %d)",
			mft.Version, schemaVersion)
	}
	if mft.Name != name {
		return nil, fmt.Errorf("pack: invalid DB name %s (expected %s)", mft.Name, name)
	}
	if mft.Label != label && label != "*" {
		return nil, fmt.Errorf("pack: invalid DB label %s (expected %s)", mft.Label, label)
	}
	return &DB{
		db:     db,
		tables: make(map[string]*Table),
	}, nil
}

func (d *DB) Manifest() (store.Manifest, error) {
	return d.db.Manifest()
}

func (d *DB) UpdateManifest(name, label string) error {
	mft, err := d.db.Manifest()
	if err != nil {
		return err
	}
	mft.Name = name
	mft.Label = label
	return d.db.SetManifest(mft)
}

func (d *DB) Path() string {
	return d.db.Path()
}

func (d *DB) GC(ctx context.Context, ratio float64) error {
	return d.db.GC(ctx, ratio)
}

func (d *DB) Dump(w io.Writer) error {
	return d.db.Dump(w)
}

func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) View(fn func(store.Tx) error) error {
	return d.db.View(fn)
}

func (d *DB) Update(fn func(store.Tx) error) error {
	return d.db.Update(fn)
}

func (d *DB) Tx(writeable bool) (*Tx, error) {
	tx, err := d.db.Begin(writeable)
	if err != nil {
		return nil, err
	}
	return &Tx{
		tx: tx,
		db: d,
	}, nil
}

func (t *Tx) Pending() int {
	return t.pending
}

func (t *Tx) Commit() error {
	return t.commit(true)
}

func (t *Tx) CommitAndContinue() error {
	return t.commit(false)
}

func (t *Tx) commit(stop bool) error {
	err := t.tx.Commit()
	if err != nil {
		return err
	}
	t.pending = 0
	t.tx = nil
	if !stop {
		t.tx, err = t.db.db.Begin(true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Tx) Rollback() error {
	if t.tx != nil {
		t.tx.Rollback()
	}
	t.pending = 0
	return nil
}

func (db *DB) storePack(name, key []byte, p *Package, fill int) (int, error) {
	tx, err := db.Tx(true)
	if err != nil {
		return 0, err
	}
	n, err := tx.storePack(name, key, p, fill)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	tx.Commit()
	return n, nil
}

func (db *DB) loadPack(name, key []byte, unpack *Package) (*Package, error) {
	tx, err := db.Tx(false)
	if err != nil {
		return nil, err
	}
	pkg, err := tx.loadPack(name, key, unpack)
	tx.Rollback()
	return pkg, err
}

func (tx *Tx) storePack(name, key []byte, p *Package, fill int) (int, error) {
	n, err := storePackTx(tx.tx, name, key, p, fill)
	if err != nil {
		return 0, err
	}
	tx.pending++
	return n, nil
}

func (tx *Tx) deletePack(name, key []byte) error {
	err := deletePackTx(tx.tx, name, key)
	if err != nil {
		return err
	}
	tx.pending++
	return nil
}

func (tx *Tx) loadPack(name, key []byte, unpack *Package) (*Package, error) {
	return loadPackTx(tx.tx, name, key, unpack)
}

func loadPackTx(dbTx store.Tx, name, key []byte, unpack *Package) (*Package, error) {
	if unpack == nil {
		unpack = NewPackage()
	}
	b := dbTx.Bucket(name)
	if b == nil {
		return nil, ErrBucketNotFound
	}
	buf := b.Get(key)
	if buf == nil {
		return nil, ErrPackNotFound
	}
	if err := unpack.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	unpack.key = key
	unpack.dirty = false
	return unpack, nil
}

func storePackTx(dbTx store.Tx, name, key []byte, p *Package, fill int) (int, error) {
	if p.stripped {
		return 0, ErrPackStripped
	}
	buf, err := p.MarshalBinary()
	if err != nil {
		return 0, err
	}
	b := dbTx.Bucket(name)
	if b == nil {
		return 0, ErrBucketNotFound
	}
	b.FillPercent(float64(fill) / 100.0)
	err = b.Put(key, buf)
	if err != nil {
		return 0, err
	}
	p.key = key
	p.dirty = false
	return len(buf), nil
}

func deletePackTx(dbTx store.Tx, name, key []byte) error {
	b := dbTx.Bucket(name)
	if b == nil {
		return ErrBucketNotFound
	}
	return b.Delete(key)
}
