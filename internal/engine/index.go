// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"fmt"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/store"
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
	table, ok := e.tables.Get(tag)
	if !ok {
		return nil
	}
	idxs := table.Indexes()
	names := make([]string, 0, len(idxs))
	for _, v := range idxs {
		names = append(names, v.Schema().Name)
	}
	return names
}

func (e *Engine) NumIndexes() int {
	return len(e.indexes.Map())
}

func (e *Engine) NumTableIndexes(tableName string) int {
	tag := types.TaggedHash(types.ObjectTagTable, tableName)
	table, ok := e.tables.Get(tag)
	if !ok {
		return 0
	}
	return len(table.Indexes())
}

func (e *Engine) FindIndex(name string) (IndexEngine, error) {
	if idx, ok := e.indexes.Get(types.TaggedHash(types.ObjectTagIndex, name)); ok {
		return idx, nil
	}
	return nil, ErrNoIndex
}

func (e *Engine) GetIndex(tag uint64) (IndexEngine, bool) {
	return e.indexes.Get(tag)
}

func (e *Engine) CreateIndex(ctx context.Context, s *schema.IndexSchema, opts IndexOptions) (IndexEngine, error) {
	// lookup table
	tableTag := types.TaggedHash(types.ObjectTagTable, s.Base.Name)

	// lookup
	table, ok := e.tables.Get(tableTag)
	if !ok {
		return nil, fmt.Errorf("%s: %v", s.Base.Name, ErrNoTable)
	}

	// schema must be a child of table schema
	if !table.Schema().Equal(s.Base) {
		return nil, schema.ErrSchemaMismatch
	}

	// check engine and driver
	factory, ok := indexEngineRegistry[opts.Engine]
	if !ok {
		return nil, fmt.Errorf("%s: %v", opts.Engine, ErrNoEngine)
	}
	if !store.IsSupported(opts.Driver) {
		return nil, fmt.Errorf("%s: %v", opts.Driver, ErrNoDriver)
	}

	// lookup index
	tag := types.TaggedHash(types.ObjectTagIndex, s.Name)
	_, ok = e.indexes.Get(tag)
	if ok {
		return nil, fmt.Errorf("%s: %v", s.Name, ErrIndexExists)
	}

	// create index engine
	index := factory()

	// ensure logger
	if opts.Logger == nil {
		opts.Logger = e.opts.Logger
	}

	// start (or use) transaction and amend context
	ctx, tx, commit, abort, err := e.WithTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer abort()

	// lock table access
	err = tx.RLock(ctx, tableTag)
	if err != nil {
		return nil, err
	}

	// schedule create
	if err := e.cat.AppendIndexCmd(ctx, CREATE, s, opts); err != nil {
		return nil, err
	}

	// creata index
	if err := index.Create(ctx, table, s, opts); err != nil {
		return nil, err
	}

	// register commit/abort callbacks
	GetTx(ctx).OnCommit(func(ctx context.Context) error {
		// add to table and engine
		table.ConnectIndex(index)

		// register
		e.indexes.Put(tag, index)

		// TODO: rebuild in background
		// index.Rebuild(ctx)

		return nil
	})

	// commit and update catalog
	if err := commit(); err != nil {
		return nil, err
	}

	return index, nil
}

func (e *Engine) RebuildIndex(ctx context.Context, name string) error {
	idx, err := e.FindIndex(name)
	if err != nil {
		return err
	}

	// start read tx (required for table scan and table lock)
	ctx, tx, commit, abort, err := e.WithTransaction(ctx)
	if err != nil {
		return err
	}
	defer abort()

	// get read lock on table (prevent drop or change)
	tableTag := idx.Table().Schema().TaggedHash(types.ObjectTagTable)
	err = tx.RLock(ctx, tableTag)
	if err != nil {
		return err
	}

	// temporarily remove index from table to make it unavailable for queries
	idx.Table().DisconnectIndex(idx)

	// (set index state)

	// schedule index rebuild as background task
	ok := e.tasks.Submit(NewTask(func(ctx context.Context) error {
		// truncate index (will block as long as there are active backend readers)
		if err := idx.Truncate(ctx); err != nil {
			return err
		}

		// rebuild index
		if err := idx.Rebuild(ctx); err != nil {
			return err
		}

		// (set index state)

		// re-add index to table signalling its ready to use again
		idx.Table().ConnectIndex(idx)

		// commit tx (will release table read lock)
		return commit()
	}))
	if !ok {
		return ErrTooManyTasks
	}

	return nil
}

func (e *Engine) DropIndex(ctx context.Context, name string) error {
	// lookup index
	tag := types.TaggedHash(types.ObjectTagIndex, name)
	index, ok := e.indexes.Get(tag)
	if !ok {
		return ErrNoIndex
	}

	// TODO: stop index build if running

	// start transaction and amend context
	ctx, tx, commit, abort, err := e.WithTransaction(ctx)
	if err != nil {
		return err
	}
	defer abort()

	// lock table access
	tableTag := index.Table().Schema().TaggedHash(types.ObjectTagTable)
	err = tx.Lock(ctx, tableTag)
	if err != nil {
		return err
	}

	// write wal and schedule drop on commit
	if err := e.cat.AppendIndexCmd(ctx, DROP, index.IndexSchema(), IndexOptions{}); err != nil {
		return err
	}

	// register commit callback
	GetTx(ctx).OnCommit(func(ctx context.Context) error {
		// remove index from table
		index.Table().DisconnectIndex(index)

		if err := index.Drop(ctx); err != nil {
			e.log.Errorf("Drop index: %v", err)
		}
		if err := index.Close(ctx); err != nil {
			e.log.Errorf("Close index: %v", err)
		}

		e.indexes.Del(tag)

		// clear caches
		for _, k := range e.cache.blocks.Keys() {
			if k[0] != tag {
				continue
			}
			e.cache.blocks.Remove(k)
		}

		return nil
	})

	// write catalog and run post-drop hooks
	return commit()
}

func (e *Engine) openIndexes(ctx context.Context, table TableEngine, ts *schema.Schema) error {
	// tag := types.TaggedHash(types.ObjectTagTable, table.Schema().Name)
	tag := types.TaggedHash(types.ObjectTagTable, ts.Name)

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
		opts.Logger = e.opts.Logger
		opts.ReadOnly = e.opts.ReadOnly
		if err := idx.Open(ctx, table, s, opts); err != nil {
			return err
		}
		table.ConnectIndex(idx)
		itag := types.TaggedHash(types.ObjectTagIndex, s.Name)
		e.indexes.Put(itag, idx)
	}

	return nil
}
