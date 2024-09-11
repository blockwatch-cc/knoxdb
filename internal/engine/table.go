// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"fmt"

	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"golang.org/x/exp/slices"
)

var tableEngineRegistry = make(map[TableKind]TableFactory)

func RegisterTableFactory(n TableKind, fn TableFactory) {
	if _, ok := tableEngineRegistry[n]; ok {
		panic(fmt.Errorf("knox: table engine %s factory already registered", n))
	}
	tableEngineRegistry[n] = fn
}

func (e *Engine) TableNames() []string {
	names := make([]string, 0, len(e.tables))
	for _, v := range e.tables {
		names = append(names, v.Schema().Name())
	}
	return names
}

func (e *Engine) NumTables() int {
	return len(e.tables)
}

func (e *Engine) UseTable(name string) (TableEngine, error) {
	if t, ok := e.tables[types.TaggedHash(types.HashTagTable, name)]; ok {
		return t, nil
	}
	return nil, ErrNoTable
}

func (e *Engine) GetTable(hash uint64) (TableEngine, bool) {
	t, ok := e.tables[hash]
	return t, ok
}

func (e *Engine) CreateTable(ctx context.Context, s *schema.Schema, opts TableOptions) (TableEngine, error) {
	// check name is unique
	tag := s.TaggedHash(types.HashTagTable)
	if _, ok := e.tables[tag]; ok {
		return nil, ErrTableExists
	}

	// check engine and driver
	factory, ok := tableEngineRegistry[opts.Engine]
	if !ok {
		return nil, ErrNoEngine
	}
	if !slices.Contains(store.SupportedDrivers(), opts.Driver) {
		return nil, ErrNoDriver
	}

	// create table engine
	table := factory()

	// ensure logger
	if opts.Logger == nil {
		opts.Logger = e.log
	}

	// start transaction and amend context
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// creata table
	if err := table.Create(ctx, s, opts); err != nil {
		return nil, err
	}

	// add to catalog
	if err := e.cat.AddTable(ctx, tag, s, opts); err != nil {
		return nil, err
	}

	// commit
	if err := commit(); err != nil {
		return nil, err
	}

	// keep reference in engine
	e.tables[tag] = table

	return table, nil
}

func (e *Engine) AlterTable(ctx context.Context, name string, schema *schema.Schema) error {
	// TODO: alter table
	// - check readonly flag
	// permitted changes
	// - change field name
	// - change field compression type (applies to future written packs)
	// - add field
	// - drop field (set deleted flag)
	// - add index (-> use CreateIndex() below)
	// - drop index (-> use DropIndex() below)
	// not permitted changes
	// - field is used in an index
	// problematic changes (need to handle explicitly)
	// - a field before an indexed field is deleted
	//   - new and old row encodings exist in parallel, old encodings may be removed
	//     from index on update
	//   - new row encodings skip deleted fields, so converter order differs, need
	//     multiple converters and store schema version with each value

	// start transaction and amend context
	// ctx = e.WithEngine(ctx)
	// ctx, commit, abort := e.WithTransaction(ctx, true)
	// defer abort()

	return ErrNotImplemented
}

func (e *Engine) DropTable(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.HashTagTable, name)
	t, ok := e.tables[tag]
	if !ok {
		return ErrNoTable
	}

	// start transaction and amend context
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// TODO: wait for open transactions to complete

	// TODO: make table unavailable for new transaction

	// drop indexes and remove them from catalog
	for _, idx := range t.Indexes() {
		if err := e.DropIndex(ctx, idx.Schema().Name()); err != nil {
			return err
		}
	}

	// drop table
	if err := t.Drop(ctx); err != nil {
		e.log.Errorf("Drop table: %v", err)
	}
	if err := t.Close(ctx); err != nil {
		e.log.Errorf("Close table: %v", err)
	}
	delete(e.tables, tag)

	// remove table from catalog
	if err := e.cat.DropTable(ctx, tag); err != nil {
		return err
	}

	return commit()
}

func (e *Engine) TruncateTable(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.HashTagTable, name)
	t, ok := e.tables[tag]
	if !ok {
		return ErrNoTable
	}

	// start transaction and amend context
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// TODO: wait for open transactions to complete

	// TODO: make table unavailable for new transaction

	if err := t.Truncate(ctx); err != nil {
		return err
	}

	return commit()
}

func (e *Engine) CompactTable(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.HashTagTable, name)
	t, ok := e.tables[tag]
	if !ok {
		return ErrNoTable
	}

	// start transaction and amend context
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// TODO: wait for open transactions to complete

	// TODO: make table unavailable for new transaction

	if err := t.Compact(ctx); err != nil {
		return err
	}

	return commit()
}

func (e *Engine) openTables(ctx context.Context) error {
	// iterate catalog
	keys, err := e.cat.ListTables(ctx)
	if err != nil {
		return err
	}

	for _, key := range keys {
		// load schema and options
		s, opts, err := e.cat.GetTable(ctx, key)
		if err != nil {
			return err
		}

		// get table factory
		factory, ok := tableEngineRegistry[opts.Engine]
		if !ok {
			return ErrNoEngine
		}
		if !slices.Contains(store.SupportedDrivers(), opts.Driver) {
			return ErrNoDriver
		}

		table := factory()

		// ensure logger and override flags
		opts.Logger = e.log
		opts.ReadOnly = e.opts.ReadOnly

		// open the store
		if err := table.Open(ctx, s, opts); err != nil {
			return err
		}

		// open indexes
		if err := e.openIndexes(ctx, table); err != nil {
			_ = table.Close(ctx)
			return err
		}

		e.tables[key] = table
	}

	return nil
}