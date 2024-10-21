// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

func (e *Engine) Enums() schema.EnumRegistry {
	return e.enums
}

func (e *Engine) EnumNames() []string {
	names := make([]string, 0, len(e.enums))
	for _, v := range e.enums {
		names = append(names, v.Name())
	}
	return names
}

func (e *Engine) NumEnums() int {
	return len(e.enums)
}

func (e *Engine) UseEnum(name string) (*schema.EnumDictionary, error) {
	enum, ok := e.enums[types.TaggedHash(types.ObjectTagEnum, name)]
	if !ok {
		return nil, ErrNoEnum
	}
	return enum, nil
}

func (e *Engine) GetEnum(hash uint64) (*schema.EnumDictionary, bool) {
	enum, ok := e.enums[hash]
	return enum, ok
}

func (e *Engine) CreateEnum(ctx context.Context, name string) (*schema.EnumDictionary, error) {
	// check name is unique
	tag := types.TaggedHash(types.ObjectTagEnum, name)
	if _, ok := e.enums[tag]; ok {
		return nil, ErrEnumExists
	}

	// open write transaction
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// create object
	enum := schema.NewEnumDictionary(name)

	// register commit callback
	GetTransaction(ctx).OnAbort(func(ctx context.Context) error {
		delete(e.enums, tag)
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
	e.enums[tag] = enum

	return enum, nil
}

func (e *Engine) DropEnum(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.ObjectTagEnum, name)
	enum, ok := e.enums[tag]
	if !ok {
		return ErrNoEnum
	}

	// open transaction
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// register commit callback
	GetTransaction(ctx).OnCommit(func(ctx context.Context) error {
		delete(e.enums, tag)
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
	enum, ok := e.enums[tag]
	if !ok {
		return ErrNoEnum
	}

	// create a copy in case we rollback
	clone := enum.Clone()

	// tentatively extend enum
	if err := enum.Append(vals...); err != nil {
		return err
	}

	// open transaction
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// register abort callback
	GetTransaction(ctx).OnAbort(func(ctx context.Context) error {
		e.enums[tag] = clone
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
