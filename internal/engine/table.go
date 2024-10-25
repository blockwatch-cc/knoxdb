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
	e.mu.RLock()
	defer e.mu.RUnlock()
	names := make([]string, 0, len(e.tables))
	for _, v := range e.tables {
		names = append(names, v.Schema().Name())
	}
	return names
}

func (e *Engine) NumTables() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.tables)
}

func (e *Engine) UseTable(name string) (TableEngine, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if t, ok := e.tables[types.TaggedHash(types.ObjectTagTable, name)]; ok {
		return t, nil
	}
	return nil, ErrNoTable
}

func (e *Engine) GetTable(hash uint64) (TableEngine, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	t, ok := e.tables[hash]
	return t, ok
}

func (e *Engine) CreateTable(ctx context.Context, s *schema.Schema, opts TableOptions) (TableEngine, error) {
	// check name is unique
	tag := s.TaggedHash(types.ObjectTagTable)
	e.mu.RLock()
	_, ok := e.tables[tag]
	e.mu.RUnlock()
	if ok {
		return nil, ErrTableExists
	}

	// resolve schema enums
	e.mu.RLock()
	s.WithEnumsFrom(e.enums)
	e.mu.RUnlock()

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
		e.mu.Lock()
		delete(e.tables, tag)
		e.mu.Unlock()
		return table.Drop(ctx)
	})

	// commit and update to catalog (may be noop when user controls tx)
	if err := commit(); err != nil {
		return nil, err
	}

	// make available on engine API
	e.mu.Lock()
	e.tables[tag] = table
	e.mu.Unlock()

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
	e.mu.RLock()
	t, ok := e.tables[tag]
	e.mu.RUnlock()
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
		e.mu.Lock()
		delete(e.tables, tag)
		e.mu.Unlock()
		return nil
	})

	// write catalog and run post-drop hooks
	return commit()
}

func (e *Engine) TruncateTable(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.ObjectTagTable, name)
	e.mu.RLock()
	t, ok := e.tables[tag]
	e.mu.RUnlock()
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

	return commit()
}

func (e *Engine) CompactTable(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.ObjectTagTable, name)
	e.mu.RLock()
	t, ok := e.tables[tag]
	e.mu.RUnlock()
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
	err = tx.Lock(ctx, tag)
	if err != nil {
		return err
	}

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

		// resolve schema enums
		s.WithEnumsFrom(e.enums)

		e.log.Debugf("Table %s", s)
		e.log.Debugf("Resolve enums from %#v", e.enums)

		// open the store
		if err := table.Open(ctx, s, opts); err != nil {
			return err
		}
		e.log.Debugf("Loaded table %s", s.Name())

		// open indexes
		if err := e.openIndexes(ctx, table); err != nil {
			_ = table.Close(ctx)
			return err
		}

		e.tables[key] = table
	}

	return nil
}
