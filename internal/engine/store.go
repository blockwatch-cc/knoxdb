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

var storeEngineRegistry = make(map[StoreKind]StoreFactory)

func RegisterStoreFactory(n StoreKind, fn StoreFactory) {
	if _, ok := storeEngineRegistry[n]; ok {
		panic(fmt.Errorf("knox: store engine %s factory already registered", n))
	}
	storeEngineRegistry[n] = fn
}

func (e *Engine) StoreNames() []string {
	names := make([]string, 0, len(e.stores))
	for _, v := range e.stores {
		names = append(names, v.Schema().Name())
	}
	return names
}

func (e *Engine) NumStores() int {
	return len(e.stores)
}

func (e *Engine) UseStore(name string) (StoreEngine, error) {
	if s, ok := e.stores[types.TaggedHash(types.HashTagStore, name)]; ok {
		return s, nil
	}
	return nil, ErrNoStore
}

func (e *Engine) GetStore(hash uint64) (StoreEngine, bool) {
	store, ok := e.stores[hash]
	return store, ok
}

func (e *Engine) CreateStore(ctx context.Context, s *schema.Schema, opts StoreOptions) (StoreEngine, error) {
	// check name is unique
	tag := s.TaggedHash(types.HashTagStore)
	if _, ok := e.stores[tag]; ok {
		return nil, ErrStoreExists
	}

	// check driver
	factory, ok := storeEngineRegistry[StoreKindKV]
	if !ok {
		return nil, ErrNoEngine
	}
	if !slices.Contains(store.SupportedDrivers(), opts.Driver) {
		return nil, ErrNoDriver
	}

	// create store engine
	kvstore := factory()

	// ensure logger
	if opts.Logger == nil {
		opts.Logger = e.log
	}

	// start transaction and amend context
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// add to catalog
	if err := e.cat.AddStore(ctx, tag, s, opts); err != nil {
		return nil, err
	}

	// creata store
	if err := kvstore.Create(ctx, s, opts); err != nil {
		return nil, err
	}

	// commit
	if err := commit(); err != nil {
		return nil, err
	}

	// keep reference in engine
	e.stores[tag] = kvstore

	return kvstore, nil

}

func (e *Engine) DropStore(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.HashTagStore, name)
	s, ok := e.stores[tag]
	if !ok {
		return ErrNoStore
	}

	// start transaction and amend context
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// TODO: wait for open transactions to complete

	// TODO: make store unavailable for new transaction

	// drop table
	if err := s.Drop(ctx); err != nil {
		e.log.Errorf("Drop store: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		e.log.Errorf("Close store: %v", err)
	}
	delete(e.stores, tag)

	// remove from catalog
	if err := e.cat.DropStore(ctx, tag); err != nil {
		return err
	}

	return commit()
}

func (e *Engine) openStores(ctx context.Context) error {
	// iterate catalog
	keys, err := e.cat.ListStores(ctx)
	if err != nil {
		return err
	}

	for _, key := range keys {
		s, opts, err := e.cat.GetStore(ctx, key)
		if err != nil {
			return err
		}

		// get store factory (we currently support a single kind only)
		factory, ok := storeEngineRegistry[StoreKindKV]
		if !ok {
			return ErrNoEngine
		}

		if !slices.Contains(store.SupportedDrivers(), opts.Driver) {
			return ErrNoDriver
		}

		// create store engine
		kvstore := factory()

		// ensure logger
		opts.Logger = e.log
		opts.ReadOnly = e.opts.ReadOnly

		// open the store
		if err := kvstore.Open(ctx, s, opts); err != nil {
			return err
		}

		e.stores[key] = kvstore
	}

	return nil
}
