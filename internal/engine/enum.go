// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"slices"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

func (e *Engine) CloneEnums(names ...string) *schema.EnumRegistry {
	if len(names) == 0 {
		return nil
	}
	clone := schema.NewEnumRegistry()
	for _, n := range names {
		dict, ok := e.enums.Lookup(n)
		if ok {
			clone.Register(dict)
		}
	}
	return clone
}

func (e *Engine) EnumNames() []string {
	names := make([]string, 0)
	for _, v := range e.enums.Map() {
		names = append(names, v.Name())
	}
	return names
}

func (e *Engine) NumEnums() int {
	return len(e.enums.Map())
}

func (e *Engine) FindEnum(name string) (*schema.EnumDictionary, error) {
	if e.IsShutdown() {
		return nil, ErrDatabaseShutdown
	}
	enum, ok := e.enums.Get(types.TaggedHash(types.ObjectTagEnum, name))
	if !ok {
		return nil, ErrNoEnum
	}
	return enum, nil
}

func (e *Engine) GetEnum(tag uint64) (*schema.EnumDictionary, bool) {
	enum, ok := e.enums.Get(tag)
	return enum, ok
}

func (e *Engine) CreateEnum(ctx context.Context, name string) (*schema.EnumDictionary, error) {
	// check name is unique
	tag := types.TaggedHash(types.ObjectTagEnum, name)
	_, ok := e.enums.Get(tag)
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
		e.enums.Del(tag)
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
	e.enums.Put(tag, enum)

	return enum, nil
}

func (e *Engine) DropEnum(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.ObjectTagEnum, name)
	enum, ok := e.enums.Get(tag)
	if !ok {
		return ErrNoEnum
	}

	// check enum is unused
	for _, t := range e.tables.Map() {
		if slices.Contains(t.Schema().EnumNames(), enum.Name()) {
			return ErrEnumInUse
		}
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
		e.enums.Del(tag)
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
	enum, ok := e.enums.Get(tag)
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
		e.enums.Put(tag, clone)
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
		e.log.Debugf("loaded enum %s key=0x%016x n=%d", enum.Name(), key, enum.Len())
		e.enums.Put(key, enum)
	}

	return nil
}
