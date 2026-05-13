// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"context"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/schema"
)

type TxFlags = engine.TxFlags

const (
	// Timeouts are controlled via DatabaseOptions.TxWaitTimeout
	TxFlagReadOnly = engine.TxFlagReadOnly // run tx in read-only mode
	TxFlagNoWal    = engine.TxFlagNoWal    // do not write wal
	TxFlagNoSync   = engine.TxFlagNoSync   // write wal but do not fsync
	TxFlagDeferred = engine.TxFlagDeferred // let read tx wait for safe snapshot or timeout
	TxFlagNoWait   = engine.TxFlagNoWait   // don't wait for concurrent writers and back-pressure
)

var _ Database = (*DB)(nil)

type DB struct {
	engine *engine.Engine
}

func IsDatabaseExist(name string, opts ...Option) (bool, error) {
	return engine.IsExist(name, opts...)
}

func DropDatabase(name string, opts ...Option) error {
	return engine.Drop(name, opts...)
}

func WrapEngine(e *engine.Engine) Database {
	return &DB{engine: e}
}

// local
func CreateDatabase(ctx context.Context, name string, opts ...Option) (Database, error) {
	eng, err := engine.Create(ctx, name, opts...)
	if err != nil {
		return nil, err
	}
	db := &DB{engine: eng}
	return db, nil
}

func OpenDatabase(ctx context.Context, name string, opts ...Option) (Database, error) {
	eng, err := engine.Open(ctx, name, opts...)
	if err != nil {
		return nil, err
	}
	db := &DB{engine: eng}
	return db, nil
}

func (d *DB) Close(ctx context.Context) error {
	return d.engine.Close(ctx)
}

func (d *DB) Sync(ctx context.Context) error {
	return d.engine.Sync(ctx)
}

// Transaction
func (d *DB) Begin(ctx context.Context, flags ...TxFlags) (context.Context, func() error, func() error, error) {
	ctx, _, commit, abort, err := d.engine.WithTransaction(ctx, flags...)
	return ctx, commit, abort, err
}

// Table
func (d *DB) ListTables() []string {
	return d.engine.TableNames()
}

func (d *DB) CreateTable(ctx context.Context, s *schema.Schema, opts ...Option) (Table, error) {
	t, err := d.engine.CreateTable(ctx, s, opts...)
	if err != nil {
		return nil, err
	}
	return &TableImpl{d, t, nil}, nil
}

func (d *DB) GetTable(tag uint64) (Table, bool) {
	t, ok := d.engine.GetTable(tag)
	if !ok {
		return nil, false
	}
	return &TableImpl{d, t, nil}, true
}

func (d *DB) FindTable(name string) (Table, error) {
	t, err := d.engine.FindTable(name)
	if err != nil {
		return nil, err
	}
	return &TableImpl{d, t, nil}, nil
}

func (d *DB) DropTable(ctx context.Context, name string) error {
	return d.engine.DropTable(ctx, name)
}

func (d *DB) AlterTable(ctx context.Context, name string, s *schema.Schema) error {
	return d.engine.AlterTable(ctx, name, s)
}

func (d *DB) TruncateTable(ctx context.Context, name string) error {
	return d.engine.TruncateTable(ctx, name)
}

func (d *DB) CompactTable(ctx context.Context, name string) error {
	return d.engine.CompactTable(ctx, name)
}

// Index
func (d *DB) ListIndexes(name string) []string {
	return d.engine.IndexNames(name)
}

func (d *DB) FindIndex(name string) (Index, error) {
	i, err := d.engine.FindIndex(name)
	if err != nil {
		return nil, err
	}
	return &IndexImpl{i, d}, nil
}

func (d *DB) CreateIndex(ctx context.Context, s *schema.IndexSchema, opts ...Option) error {
	_, err := d.engine.CreateIndex(ctx, s, opts...)
	return err
}

func (d *DB) RebuildIndex(ctx context.Context, name string) error {
	return d.engine.RebuildIndex(ctx, name)
}

func (d *DB) DropIndex(ctx context.Context, name string) error {
	return d.engine.DropIndex(ctx, name)
}

// Enum
func (d *DB) ListEnums() []string {
	return d.engine.EnumNames()
}

func (d *DB) FindEnum(name string) (*schema.EnumDictionary, error) {
	enum, err := d.engine.FindEnum(name)
	if err != nil {
		return nil, err
	}
	return enum, nil
}

func (d *DB) CreateEnum(ctx context.Context, name string) (*schema.EnumDictionary, error) {
	return d.engine.CreateEnum(ctx, name)
}

func (d *DB) ExtendEnum(ctx context.Context, name string, vals ...string) error {
	return d.engine.ExtendEnum(ctx, name, vals...)
}

func (d *DB) DropEnum(ctx context.Context, name string) error {
	return d.engine.DropEnum(ctx, name)
}

// // View
// func (d *DB) ListViews(name string) ([]string, error)                {}
// func (d *DB) CreateView(name string, opts ViewOptions) (View, error) {}
// func (d *DB) DropView(name string) error                             {}

// // Stream (change data capture)
// func (d *DB) ListStreams(name string) ([]string, error)                    {}
// func (d *DB) CreateStream(name string, opts StreamOptions) (Stream, error) {}
// func (d *DB) DropStream(name string) error                                 {}

// // Snapshot
// func (d *DB) ListSnapshots(name string) ([]string, error)                         {}
// func (d *DB) CreateSnapshot(name string, opts SnapshotOptions) (*Snapshot, error) {}
// func (d *DB) DropSnapshot(id uint64) error                                        {}
// func (d *DB) BackupSnapshot(id uint64) error                                      {}
// func (d *DB) RestoreSnapshot(id uint64) error                                     {}
