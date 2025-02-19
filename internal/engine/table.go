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
	names := make([]string, 0)
	for _, v := range e.tables.Map() {
		names = append(names, v.Schema().Name())
	}
	return names
}

func (e *Engine) NumTables() int {
	return len(e.tables.Map())
}

func (e *Engine) UseTable(name string) (TableEngine, error) {
	if e.IsShutdown() {
		return nil, ErrDatabaseShutdown
	}
	if t, ok := e.tables.Get(types.TaggedHash(types.ObjectTagTable, name)); ok {
		return t, nil
	}
	return nil, ErrNoTable
}

func (e *Engine) GetTable(tag uint64) (TableEngine, bool) {
	return e.tables.Get(tag)
}

func (e *Engine) CreateTable(ctx context.Context, s *schema.Schema, opts TableOptions) (TableEngine, error) {
	// require primary key
	if s.PkIndex() < 0 {
		return nil, ErrNoPk
	}

	// check name is unique
	tag := s.TaggedHash(types.ObjectTagTable)
	_, ok := e.tables.Get(tag)
	if ok {
		return nil, fmt.Errorf("%s: %v", s.Name(), ErrTableExists)
	}

	// check enums exist and collect
	enums := schema.NewEnumRegistry()
	var err error
	for _, n := range s.EnumFieldNames() {
		enum, ok := e.enums.Lookup(n)
		if !ok {
			err = fmt.Errorf("missing enum %q", n)
			break
		}
		enums.Register(enum)
	}
	if err != nil {
		return nil, err
	}

	// connect schema enums
	s.WithEnums(&enums)

	// check engine and driver
	factory, ok := tableEngineRegistry[opts.Engine]
	if !ok {
		return nil, fmt.Errorf("%s: %v", opts.Engine, ErrNoEngine)
	}
	if !slices.Contains(store.SupportedDrivers(), opts.Driver) {
		return nil, fmt.Errorf("%s: %v", opts.Driver, ErrNoDriver)
	}

	// create table engine
	table := factory()
	var (
		history TableEngine
		htag    uint64
	)

	// ensure logger
	if opts.Logger == nil {
		opts.Logger = e.log
	}

	// start transaction and amend context
	ctx, tx, commit, abort, err := e.WithTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer abort()

	// schedule create
	if err := e.cat.AppendTableCmd(ctx, CREATE, s, opts); err != nil {
		return nil, err
	}

	// lock object access, unlocks on commit/abort
	err = tx.Lock(ctx, tag)
	if err != nil {
		return nil, err
	}

	// create table
	err = table.Create(ctx, s, opts)
	if err != nil {
		return nil, err
	}

	// register abort callbacks
	tx.OnAbort(func(ctx context.Context) error {
		// remove table file(s) on error
		e.tables.Del(tag)
		err := table.Drop(ctx)
		if err != nil {
			return err
		}
		if history != nil {
			e.tables.Del(htag)
			return history.Drop(ctx)
		}
		return nil
	})

	// create history table if configured
	if opts.EnableHistory {
		hopts := TableOptions{
			Engine: TableKindHistory,
			Logger: opts.Logger,
		}
		factory, ok = tableEngineRegistry[hopts.Engine]
		if !ok {
			return nil, fmt.Errorf("%s: %v", hopts.Engine, ErrNoEngine)
		}

		// create history table engine
		history = factory()

		// create history table
		err = history.Create(ctx, s, hopts)
		if err != nil {
			return nil, err
		}

		// register
		htag = history.Schema().TaggedHash(types.ObjectTagTable)
		e.tables.Put(htag, history)
	}

	// commit and update to catalog (may be noop when user controls tx)
	if err := commit(); err != nil {
		return nil, err
	}

	// make available on engine API
	e.tables.Put(tag, table)

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
	// ctx, commit, abort, err := e.WithTransaction(ctx)
	// if err != nil {
	//   return err
	// }
	// defer abort()

	// // lock object access, unlocks on commit/abort
	// _, err = e.lm.Lock(ctx, LockModeExclusive, tag)
	// if err != nil {
	// 	return err
	// }

	// // register commit/abort callbacks
	// tx := GetTransaction(ctx)
	// tx.OnCommit(func(ctx context.Context) error {
	// 	// TODO: change table schema, change journal schema
	// 	return nil
	// })

	// update to catalog
	// if err := e.cat.AppendTableCmd(ctx, ALTER, s, nil); err != nil {
	// 	return nil, err
	// }

	// commit catalog (note: noop when called with outside tx)
	// if err := commit(); err != nil {
	// 	return nil, err
	// }

	return ErrNotImplemented
}

func (e *Engine) DropTable(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.ObjectTagTable, name)
	t, ok := e.tables.Get(tag)
	if !ok {
		return ErrNoTable
	}

	// must drop indexes first
	if len(t.Indexes()) > 0 {
		return ErrTableDropWithRefs
	}

	// start transaction and amend context
	ctx, tx, commit, abort, err := e.WithTransaction(ctx)
	if err != nil {
		return err
	}
	defer abort()

	// lock object access, unlocks on commit/abort, this
	// - wait for open transactions to complete
	// - makes table unavailable for new transaction
	err = tx.Lock(ctx, tag)
	if err != nil {
		return err
	}

	// schedule drop
	if err := e.cat.AppendTableCmd(ctx, DROP, t.Schema(), TableOptions{}); err != nil {
		return err
	}

	// register commit callback
	GetTransaction(ctx).OnCommit(func(ctx context.Context) error {
		if err := t.Drop(ctx); err != nil {
			e.log.Errorf("Drop table: %v", err)
		}
		if err := t.Close(ctx); err != nil {
			e.log.Errorf("Close table: %v", err)
		}
		e.tables.Del(tag)

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

func (e *Engine) TruncateTable(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.ObjectTagTable, name)
	t, ok := e.tables.Get(tag)
	if !ok {
		return ErrNoTable
	}

	// start transaction and amend context
	ctx, tx, commit, abort, err := e.WithTransaction(ctx)
	if err != nil {
		return err
	}
	defer abort()

	// lock object access, unlocks on commit/abort, this
	// - wait for open transactions to complete
	// - makes table unavailable for new transaction
	err = tx.Lock(ctx, tag)
	if err != nil {
		return err
	}

	if err := t.Truncate(ctx); err != nil {
		return err
	}

	// clear caches
	for _, k := range e.cache.blocks.Keys() {
		if k[0] != tag {
			continue
		}
		e.cache.blocks.Remove(k)
	}

	return commit()
}

func (e *Engine) CompactTable(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.ObjectTagTable, name)
	t, ok := e.tables.Get(tag)
	if !ok {
		return ErrNoTable
	}

	// start transaction and amend context
	ctx, tx, commit, abort, err := e.WithTransaction(ctx)
	if err != nil {
		return err
	}
	defer abort()

	// lock object access, unlocks on commit/abort, this
	// - wait for open transactions to complete
	// - make table unavailable for new transaction
	if err := tx.Lock(ctx, tag); err != nil {
		return err
	}

	if err := t.Compact(ctx); err != nil {
		return err
	}

	// clear caches
	for _, k := range e.cache.blocks.Keys() {
		if k[0] != tag {
			continue
		}
		e.cache.blocks.Remove(k)
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

		// lookup schema enums
		s.WithEnums(e.CloneEnums(s.EnumFieldNames()...))

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

		// open the table, load journals, replay wal after crash
		if err := table.Open(ctx, s, opts); err != nil {
			return err
		}
		e.log.Debugf("Loaded table %s", s.Name())

		// open indexes
		if err := e.openIndexes(ctx, table); err != nil {
			_ = table.Close(ctx)
			return err
		}

		e.tables.Put(key, table)
	}

	return nil
}
