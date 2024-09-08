// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

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

func (e *Engine) UseEnum(name string) (schema.EnumLUT, error) {
	enum, ok := e.enums[types.TaggedHash(types.HashTagEnum, name)]
	if !ok {
		return nil, ErrNoEnum
	}
	return enum, nil
}

func (e *Engine) GetEnum(hash uint64) (schema.EnumLUT, bool) {
	enum, ok := e.enums[hash]
	return enum, ok
}

func (e *Engine) CreateEnum(ctx context.Context, name string) (schema.EnumLUT, error) {
	// check name is unique
	tag := types.TaggedHash(types.HashTagEnum, name)
	if _, ok := e.enums[tag]; ok {
		return nil, ErrEnumExists
	}

	// open write transaction
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// create
	enum := schema.NewEnumDictionary(name)

	// store in catalog
	if err := e.cat.AddEnum(ctx, enum); err != nil {
		return nil, err
	}

	// commit
	if err := commit(); err != nil {
		return nil, err
	}

	e.enums[tag] = enum

	return enum, nil
}

func (e *Engine) DropEnum(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.HashTagEnum, name)
	if _, ok := e.enums[tag]; !ok {
		return ErrNoEnum
	}

	// open transaction
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	if err := e.cat.DropEnum(ctx, tag); err != nil {
		return err
	}

	delete(e.enums, tag)

	return commit()
}

func (e *Engine) ExtendEnum(ctx context.Context, name string, vals ...schema.Enum) error {
	tag := types.TaggedHash(types.HashTagEnum, name)
	enum, ok := e.enums[tag]
	if !ok {
		return ErrNoEnum
	}

	// open transaction
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// extend enum
	if err := enum.AddValues(vals...); err != nil {
		return err
	}

	// store enum data
	if err := e.cat.PutEnum(ctx, enum); err != nil {
		return err
	}

	// commit
	if err := commit(); err != nil {
		return err
	}

	return nil
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
		e.enums[key] = enum
		schema.RegisterEnum(enum)
	}

	return nil
}
