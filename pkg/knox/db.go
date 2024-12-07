// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"context"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

var _ Database = (*DB)(nil)

type DB struct {
	engine *engine.Engine
	log    log.Logger
}

// remote
// func ConnectDatabase(ctx context.Context, uri string, opts DatabaseOptions) (Database, error) {
// 	c := wire.NewClient().WithLogger(opts.Logger)
// 	if err := c.Connect(ctx, uri); err != nil {
// 		return nil, err
// 	}
// 	return c, nil
// }

func WrapEngine(e *engine.Engine) Database {
	return &DB{engine: e, log: log.Log}
}

// local
func CreateDatabase(ctx context.Context, name string, opts DatabaseOptions) (Database, error) {
	eng, err := engine.Create(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	l := opts.Logger
	if l == nil {
		l = log.Log
	}
	db := &DB{engine: eng, log: l}
	return db, nil
}

func OpenDatabase(ctx context.Context, name string, opts DatabaseOptions) (Database, error) {
	eng, err := engine.Open(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	l := opts.Logger
	if l == nil {
		l = log.Log
	}
	db := &DB{engine: eng, log: l}
	return db, nil
}

func (d *DB) Close(ctx context.Context) error {
	return d.engine.Close(ctx)
}

func (d *DB) Sync(ctx context.Context) error {
	return d.engine.Sync(ctx)
}

// Transaction
func (d *DB) Begin(ctx context.Context) (context.Context, func() error, func() error, error) {
	ctx, _, commit, abort, err := d.engine.WithTransaction(ctx)
	return ctx, commit, abort, err
}

// Table
func (d *DB) ListTables() []string {
	return d.engine.TableNames()
}

func (d *DB) CreateTable(ctx context.Context, s *schema.Schema, opts TableOptions) (Table, error) {
	t, err := d.engine.CreateTable(ctx, s, opts)
	if err != nil {
		return nil, err
	}
	return &TableImpl{d, t, nil, d.log}, nil
}

func (d *DB) UseTable(name string) (Table, error) {
	t, err := d.engine.UseTable(name)
	if err != nil {
		return nil, err
	}
	return &TableImpl{d, t, nil, d.log}, nil
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

// Store
func (d *DB) ListStores() []string {
	return d.engine.StoreNames()
}

func (d *DB) CreateStore(ctx context.Context, s *schema.Schema, opts StoreOptions) (Store, error) {
	st, err := d.engine.CreateStore(ctx, s, opts)
	if err != nil {
		return nil, err
	}
	return &StoreImpl{store: st}, nil
}

func (d *DB) UseStore(name string) (Store, error) {
	s, err := d.engine.UseStore(name)
	if err != nil {
		return nil, err
	}
	return &StoreImpl{db: d, store: s}, nil
}

func (d *DB) DropStore(ctx context.Context, name string) error {
	return d.engine.DropStore(ctx, name)
}

// Index
func (d *DB) ListIndexes(name string) []string {
	return d.engine.IndexNames(name)
}

func (d *DB) UseIndex(name string) (Index, error) {
	i, err := d.engine.UseIndex(name)
	if err != nil {
		return nil, err
	}
	return &IndexImpl{i, d, d.log}, nil
}

func (d *DB) CreateIndex(ctx context.Context, name string, table Table, s *schema.Schema, opts IndexOptions) error {
	_, err := d.engine.CreateIndex(ctx, table.Schema().Name(), s, opts)
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

func (d *DB) Enums() schema.EnumRegistry {
	return d.engine.Enums()
}

func (d *DB) UseEnum(name string) (*schema.EnumDictionary, error) {
	enum, err := d.engine.UseEnum(name)
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
