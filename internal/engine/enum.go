// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

func (e *Engine) Enums() schema.EnumRegistry {
	e.mu.RLock()
	defer e.mu.RUnlock()
	clone := make(schema.EnumRegistry)
	for n, v := range e.enums {
		clone[n] = v
	}
	return clone
}

func (e *Engine) EnumNames() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	names := make([]string, 0, len(e.enums))
	for _, v := range e.enums {
		names = append(names, v.Name())
	}
	return names
}

func (e *Engine) NumEnums() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.enums)
}

func (e *Engine) UseEnum(name string) (*schema.EnumDictionary, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	enum, ok := e.enums[types.TaggedHash(types.ObjectTagEnum, name)]
	if !ok {
		return nil, ErrNoEnum
	}
	return enum, nil
}

func (e *Engine) GetEnum(hash uint64) (*schema.EnumDictionary, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	enum, ok := e.enums[hash]
	return enum, ok
}

func (e *Engine) CreateEnum(ctx context.Context, name string) (*schema.EnumDictionary, error) {
	// check name is unique
	tag := types.TaggedHash(types.ObjectTagEnum, name)
	e.mu.RLock()
	_, ok := e.enums[tag]
	e.mu.RUnlock()
	if ok {
		return nil, ErrEnumExists
	}

	// open write transaction
	ctx, tx, commit, abort, err := e.WithTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer abort()

	// create object
	enum := schema.NewEnumDictionary(name)

	// register commit callback
	tx.OnAbort(func(ctx context.Context) error {
		e.mu.Lock()
		delete(e.enums, tag)
		e.mu.Unlock()
		return nil
	})

	// schedule create
	if err := e.cat.AppendEnumCmd(ctx, CREATE, enum); err != nil {
		return nil, err
	}

	// commit and update to catalog (may be noop when user controls tx)
	if err := commit(); err != nil {
		return nil, err
	}

	// make visible
	e.mu.Lock()
	e.enums[tag] = enum
	e.mu.Unlock()

	return enum, nil
}

func (e *Engine) DropEnum(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.ObjectTagEnum, name)
	e.mu.RLock()
	enum, ok := e.enums[tag]
	e.mu.RUnlock()
	if !ok {
		return ErrNoEnum
	}

	// open transaction
	ctx, tx, commit, abort, err := e.WithTransaction(ctx)
	if err != nil {
		return err
	}
	defer abort()

	// lock object access, unlocks on commit/abort
	err = tx.Lock(ctx, tag)
	if err != nil {
		return err
	}

	// register commit callback
	tx.OnCommit(func(ctx context.Context) error {
		e.mu.Lock()
		delete(e.enums, tag)
		e.mu.Unlock()
		return nil
	})

	// schedule drop
	if err := e.cat.AppendEnumCmd(ctx, DROP, enum); err != nil {
		return err
	}

	// commit will remove enum from catalog
	return commit()
}

func (e *Engine) ExtendEnum(ctx context.Context, name string, vals ...string) error {
	tag := types.TaggedHash(types.ObjectTagEnum, name)
	e.mu.RLock()
	enum, ok := e.enums[tag]
	e.mu.RUnlock()
	if !ok {
		return ErrNoEnum
	}

	// open transaction
	ctx, tx, commit, abort, err := e.WithTransaction(ctx)
	if err != nil {
		return err
	}
	defer abort()

	// lock object access, unlocks on commit/abort
	err = tx.Lock(ctx, tag)
	if err != nil {
		return err
	}

	// create a copy in case we rollback
	clone := enum.Clone()

	// tentatively extend enum
	if err := enum.Append(vals...); err != nil {
		return err
	}

	// register abort callback
	tx.OnAbort(func(ctx context.Context) error {
		e.mu.Lock()
		e.enums[tag] = clone
		e.mu.Unlock()
		return nil
	})

	// schedule update
	if err := e.cat.AppendEnumCmd(ctx, ALTER, enum); err != nil {
		return err
	}

	// commit will store updated enum data
	return commit()
}

func (e *Engine) openEnums(ctx context.Context) error {
	// iterate catalog
	keys, err := e.cat.ListEnums(ctx)
	if err != nil {
		return err
	}

	for _, key := range keys {
		enum, err := e.cat.GetEnum(ctx, key)
		if err != nil {
			return err
		}
		e.log.Debugf("Loaded enum %s [0x%016x] [0x%016x]", enum.Name(), key, enum.Tag())
		e.enums[key] = enum
	}

	return nil
}
