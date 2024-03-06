// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"blockwatch.cc/knoxdb/store"
)

var (
	ErrNoTable          = errors.New("pack: table does not exist")
	ErrNoStore          = errors.New("pack: store does not exist")
	ErrNoIndex          = errors.New("pack: index does not exist")
	ErrNoColumn         = errors.New("pack: column does not exist")
	ErrTypeMismatch     = errors.New("pack: type mismatch")
	ErrNoPk             = errors.New("pack: primary key not defined")
	ErrNoField          = errors.New("pack: field does not exist")
	ErrInvalidType      = errors.New("pack: unsupported block type")
	ErrNilValue         = errors.New("pack: nil value passed")
	ErrReadOnlyDatabase = errors.New("pack: database is read-only")

	ErrIndexNotFound  = errors.New("pack: index not found")
	ErrBucketNotFound = errors.New("pack: bucket not found")
	ErrPackNotFound   = errors.New("pack: pack not found")
	ErrPackStripped   = errors.New("pack: pack is stripped")
	ErrIdNotFound     = errors.New("pack: id not found")
	ErrKeyNotFound    = errors.New("pack: key not found")

	ErrTableExists  = errors.New("pack: table already exists")
	ErrStoreExists  = errors.New("pack: store already exists")
	ErrIndexExists  = errors.New("pack: index already exists")
	ErrResultClosed = errors.New("pack: result already closed")

	EndStream = errors.New("end stream")

	bigEndian    = binary.BigEndian
	littleEndian = binary.LittleEndian
)

const (
	schemaVersion = 2
	txMaxSize     = 128 // flush boltdb tx after N pending packs
)

type DB struct {
	db     store.DB
	tables map[string]*Table
	stores map[string]*Store
}

type Tx struct {
	tx      store.Tx
	db      *DB
	pending int
}

func CreateDatabase(engine, path, name, label string, opts any) (*DB, error) {
	db, err := store.Create(engine, filepath.Join(path, name+".db"), opts)
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
		stores: make(map[string]*Store),
	}, nil
}

func CreateDatabaseIfNotExists(engine, path, name, label string, opts any) (*DB, error) {
	db, err := OpenDatabase(engine, path, name, label, opts)
	if err == nil {
		return db, nil
	}
	if err != nil && !store.IsError(err, store.ErrDbDoesNotExist) {
		return nil, err
	}
	return CreateDatabase(engine, path, name, label, opts)
}

func OpenDatabase(engine, path, name, label string, opts any) (*DB, error) {
	db, err := store.Open(engine, filepath.Join(path, name+".db"), opts)
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
		return nil, fmt.Errorf("pack: invalid DB name %q (expected %s)", mft.Name, name)
	}
	if mft.Label != label && label != "*" {
		return nil, fmt.Errorf("pack: invalid DB label %q (expected %s)", mft.Label, label)
	}
	return &DB{
		db:     db,
		tables: make(map[string]*Table),
		stores: make(map[string]*Store),
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

func (d *DB) Dir() string {
	return filepath.Dir(d.Path())
}

func (d *DB) GC(ctx context.Context, ratio float64) error {
	return d.db.GC(ctx, ratio)
}

func (d *DB) Dump(w io.Writer) error {
	return d.db.Dump(w)
}

func (d *DB) IsReadOnly() bool {
	return d.db.IsReadOnly()
}

func (d *DB) Engine() string {
	return d.db.Type()
}

func (d *DB) IsUsed() bool {
	return len(d.tables)+len(d.stores) > 0
}

func (d *DB) IsClosed() bool {
	if d.db == nil {
		return true
	}
	_, err := d.db.Manifest()
	return store.IsError(err, store.ErrTxClosed)
}

func (d *DB) Close() error {
	// close all remaining open tables
	for n, t := range d.tables {
		if err := t.Close(); err != nil {
			log.Errorf("Closing table %s: %v", t.Name(), err)
		}
		delete(d.tables, n)
	}
	// close all remaining open stores
	for n, s := range d.stores {
		if err := s.Close(); err != nil {
			log.Errorf("Closing store %s: %v", s.Name(), err)
		}
		delete(d.stores, n)
	}
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

func (d *DB) NumOpenTables() int {
	return len(d.tables)
}

func (d *DB) NumOpenStores() int {
	return len(d.stores)
}

func (d *DB) OpenTables() []*Table {
	var list []*Table
	for _, v := range d.tables {
		list = append(list, v)
	}
	return list
}

func (d *DB) OpenStores() []*Store {
	var list []*Store
	for _, v := range d.stores {
		list = append(list, v)
	}
	return list
}

func (d *DB) ListTableNames() ([]string, error) {
	var names []string
	err := d.db.View(func(tx store.Tx) error {
		return tx.Root().ForEachBucket(func(k []byte, _ store.Bucket) error {
			name := string(k)
			if !strings.HasSuffix(name, "_meta") {
				return nil
			}
			name = strings.TrimSuffix(name, "_meta")
			if strings.HasSuffix(name, "_index") {
				return nil
			}
			name = strings.TrimSuffix(name, "_index")
			names = append(names, name)
			return nil
		})
	})
	return names, err
}

func (d *DB) ListIndexNames(table string) ([]string, error) {
	var names []string
	err := d.db.View(func(tx store.Tx) error {
		return tx.Root().ForEachBucket(func(k []byte, _ store.Bucket) error {
			name := string(k)
			if !strings.HasSuffix(name, "_meta") {
				return nil
			}
			name = strings.TrimSuffix(name, "_meta")
			if !strings.HasSuffix(name, "_index") {
				return nil
			}
			names = append(names, name)
			return nil
		})
	})
	return names, err
}

func (d *DB) ListStoreNames() ([]string, error) {
	var names []string
	err := d.db.View(func(tx store.Tx) error {
		return tx.Root().ForEachBucket(func(k []byte, _ store.Bucket) error {
			name := string(k)
			if !strings.HasSuffix(name, "_store") {
				return nil
			}
			names = append(names, strings.TrimSuffix(name, "_store"))
			return nil
		})
	})
	return names, err
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

func (db *DB) loadPack(name, key []byte, unpack *Package, sz int) (*Package, error) {
	tx, err := db.Tx(false)
	if err != nil {
		return nil, err
	}
	pkg, err := tx.loadPack(name, key, unpack, sz)
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

func (tx *Tx) loadPack(name, key []byte, unpack *Package, sz int) (*Package, error) {
	return loadPackTx(tx.tx, name, key, unpack, sz)
}

func loadPackTx(dbTx store.Tx, name, key []byte, unpack *Package, sz int) (*Package, error) {
	if unpack == nil {
		unpack = NewPackage(sz, nil)
	}
	b := dbTx.Bucket(name)
	if b == nil {
		return nil, ErrBucketNotFound
	}
	buf := b.Get(key)
	if buf == nil {
		return nil, ErrPackNotFound
	}
	unpack.SetKey(key)
	if err := unpack.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	unpack.dirty = false
	return unpack, nil
}

func storePackTx(dbTx store.Tx, name, key []byte, p *Package, fill int) (int, error) {
	for _, v := range p.blocks {
		if v == nil {
			return 0, ErrPackStripped
		}
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
