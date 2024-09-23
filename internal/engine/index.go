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

var indexEngineRegistry = make(map[IndexKind]IndexFactory)

func RegisterIndexFactory(n IndexKind, fn IndexFactory) {
	if _, ok := indexEngineRegistry[n]; ok {
		panic(fmt.Errorf("knox: index engine %s factory already registered", n))
	}
	indexEngineRegistry[n] = fn
}

func (e *Engine) IndexNames(tableName string) []string {
	tag := types.TaggedHash(types.ObjectTagTable, tableName)
	table, ok := e.GetTable(tag)
	if !ok {
		return nil
	}
	idxs := table.Indexes()
	names := make([]string, 0, len(idxs))
	for _, v := range idxs {
		names = append(names, v.Schema().Name())
	}
	return names
}

func (e *Engine) NumIndexes() int {
	return len(e.indexes)
}

func (e *Engine) NumTableIndexes(tableName string) int {
	tag := types.TaggedHash(types.ObjectTagTable, tableName)
	table, ok := e.GetTable(tag)
	if !ok {
		return 0
	}
	return len(table.Indexes())
}

func (e *Engine) UseIndex(name string) (IndexEngine, error) {
	if idx, ok := e.indexes[types.TaggedHash(types.ObjectTagIndex, name)]; ok {
		return idx, nil
	}
	return nil, ErrNoIndex
}

func (e *Engine) GetIndex(key uint64) (IndexEngine, bool) {
	index, ok := e.indexes[key]
	return index, ok
}

func (e *Engine) CreateIndex(ctx context.Context, tableName string, s *schema.Schema, opts IndexOptions) (IndexEngine, error) {
	// lookup table
	tableTag := types.TaggedHash(types.ObjectTagTable, tableName)
	table, ok := e.GetTable(tableTag)
	if !ok {
		return nil, ErrNoTable
	}

	// lookup index
	tag := types.TaggedHash(types.ObjectTagIndex, s.Name())
	if _, ok := e.indexes[tag]; ok {
		return nil, ErrIndexExists
	}

	// schema must be a child of table schema
	if err := table.Schema().CanSelect(s); err != nil {
		return nil, err
	}

	// check engine and driver
	factory, ok := indexEngineRegistry[opts.Engine]
	if !ok {
		return nil, ErrNoEngine
	}
	if !slices.Contains(store.SupportedDrivers(), opts.Driver) {
		return nil, ErrNoDriver
	}

	// create index engine
	index := factory()

	// ensure logger
	if opts.Logger == nil {
		opts.Logger = e.log
	}

	// start transaction and amend context
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// creata table
	if err := index.Create(ctx, table, s, opts); err != nil {
		return nil, err
	}

	// add to catalog
	if err := e.cat.AddIndex(ctx, tag, tableTag, s, opts); err != nil {
		return nil, err
	}

	// commit
	if err := commit(); err != nil {
		return nil, err
	}

	// add to table and engine
	table.UseIndex(index)
	e.indexes[tag] = index

	return index, nil
}

func (e *Engine) RebuildIndex(ctx context.Context, name string) error {
	index, err := e.UseIndex(name)
	if err != nil {
		return err
	}
	return index.Rebuild(ctx)
}

func (e *Engine) DropIndex(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.ObjectTagIndex, name)
	index, ok := e.indexes[tag]
	if !ok {
		return ErrNoIndex
	}
	table := index.Table()

	// start transaction and amend context
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// TODO: wait for open transactions to complete

	// TODO: make index unavailable for new transaction

	// remove index from table
	table.UnuseIndex(index)

	// drop index
	if err := index.Drop(ctx); err != nil {
		e.log.Errorf("Drop index: %v", err)
	}
	if err := index.Close(ctx); err != nil {
		e.log.Errorf("Close index: %v", err)
	}
	delete(e.indexes, tag)

	if err := e.cat.DropIndex(ctx, tag); err != nil {
		return err
	}

	return commit()
}

func (e *Engine) openIndexes(ctx context.Context, table TableEngine) error {
	tag := types.TaggedHash(types.ObjectTagTable, table.Schema().Name())

	// filter indexes by table in catalog
	keys, err := e.cat.ListIndexes(ctx, tag)
	if err != nil {
		return err
	}

	for _, key := range keys {
		s, opts, err := e.cat.GetIndex(ctx, key)
		if err != nil {
			return err
		}
		factory, ok := indexEngineRegistry[opts.Engine]
		if !ok {
			return ErrNoEngine
		}
		idx := factory()
		opts.Logger = e.log
		opts.ReadOnly = e.opts.ReadOnly
		if err := idx.Open(ctx, table, s, opts); err != nil {
			return err
		}
		table.UseIndex(idx)
		itag := types.TaggedHash(types.ObjectTagIndex, s.Name())
		e.indexes[itag] = idx
	}

	return nil
}
