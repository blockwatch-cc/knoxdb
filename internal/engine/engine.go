// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"path/filepath"
	"sync"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/cache"
	"blockwatch.cc/knoxdb/pkg/cache/rclru"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
	"golang.org/x/sync/errgroup"
)

// Engine is the central instance managing a database
type Engine struct {
	mu       sync.RWMutex
	cat      *Catalog
	cache    CacheManager
	tables   map[uint64]TableEngine
	stores   map[uint64]StoreEngine
	indexes  map[uint64]IndexEngine
	enums    map[uint64]*schema.EnumDictionary
	txs      map[uint64]*Tx
	opts     DatabaseOptions
	nextTxId uint64
	dbId     uint64
	path     string
	log      log.Logger
	// wal      *wal.Wal // one wal for all tables
}

type CacheKeyType [2]uint64

func NewCacheKey(x, y uint64) CacheKeyType {
	return CacheKeyType{x, y}
}

type BlockCacheType cache.Cache[CacheKeyType, *block.Block]

type BufferCacheType cache.Cache[CacheKeyType, *Buffer]

type CacheManager struct {
	// generic block cache
	// - pack engine: 64bit table/index id + 32bit pack id + 16bit block id
	blocks BlockCacheType
	// generic buffer cache
	// - store engine: 64bit store id + 64bit user key
	buffers BufferCacheType
}

func Create(ctx context.Context, name string, opts DatabaseOptions) (*Engine, error) {
	e := &Engine{
		path: filepath.Join(opts.Path, name),
		cache: CacheManager{
			blocks:  rclru.NewNoCache[CacheKeyType, *block.Block](),
			buffers: rclru.NewNoCache[CacheKeyType, *Buffer](),
		},
		tables:   make(map[uint64]TableEngine),
		stores:   make(map[uint64]StoreEngine),
		indexes:  make(map[uint64]IndexEngine),
		enums:    make(map[uint64]*schema.EnumDictionary),
		txs:      make(map[uint64]*Tx),
		nextTxId: 1,
		dbId:     types.TaggedHash(types.HashTagDatabase, name),
		opts:     opts,
		cat:      NewCatalog(name),
		log:      log.Disabled,
	}

	if opts.Logger != nil {
		e.log = opts.Logger
	}

	e.log.Debugf("Creating database %s at %s", name, e.path)

	// init caches
	if opts.CacheSize > 0 {
		e.cache.blocks = rclru.New2Q[CacheKeyType, *block.Block](opts.CacheSize * 90 / 10)
		e.cache.buffers = rclru.New2Q[CacheKeyType, *Buffer](opts.CacheSize / 10)
	}

	// start transaction and amend context (required to store catalog db)
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// init catalog
	if err := e.cat.Create(ctx, opts); err != nil {
		return nil, err
	}

	// write db options to catalog
	if err := e.cat.PutOptions(ctx, e.dbId, &opts); err != nil {
		return nil, err
	}

	// init wal

	// commit open tx
	if err := commit(); err != nil {
		_ = e.Close(ctx)
		return nil, err
	}

	return e, nil
}

func Open(ctx context.Context, name string, opts DatabaseOptions) (*Engine, error) {
	e := &Engine{
		path: filepath.Join(opts.Path, name),
		cache: CacheManager{
			blocks:  rclru.NewNoCache[CacheKeyType, *block.Block](),
			buffers: rclru.NewNoCache[CacheKeyType, *Buffer](),
		},
		tables:  make(map[uint64]TableEngine),
		stores:  make(map[uint64]StoreEngine),
		indexes: make(map[uint64]IndexEngine),
		enums:   make(map[uint64]*schema.EnumDictionary),
		txs:     make(map[uint64]*Tx),
		dbId:    types.TaggedHash(types.HashTagDatabase, name),
		cat:     NewCatalog(name),
		log:     log.Disabled,
	}
	if opts.Logger != nil {
		e.log = opts.Logger
	}

	e.log.Debugf("Opening database %s at %s", name, e.path)

	// start read transaction and amend context (required to load from dbs)
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// load and validate catalog
	if err := e.cat.Open(ctx, opts); err != nil {
		return nil, err
	}

	// load stored database options
	var sopts DatabaseOptions
	err := e.cat.GetOptions(ctx, e.dbId, &sopts)
	if err != nil {
		return nil, err
	}
	// merge options
	e.opts = sopts.Merge(opts)

	// init caches
	if e.opts.CacheSize > 0 {
		e.cache.blocks = rclru.New2Q[CacheKeyType, *block.Block](e.opts.CacheSize * 90 / 10)
		e.cache.buffers = rclru.New2Q[CacheKeyType, *Buffer](e.opts.CacheSize / 10)
	}

	// init tx id
	e.nextTxId, _ = e.Catalog().GetState(e.dbId)

	// open database objects
	if err := e.openTables(ctx); err != nil {
		_ = e.Close(ctx)
		return nil, err
	}

	if err := e.openStores(ctx); err != nil {
		_ = e.Close(ctx)
		return nil, err
	}

	if err := e.openEnums(ctx); err != nil {
		_ = e.Close(ctx)
		return nil, err
	}

	// TODO: load and validate wal

	// TODO: replay wal (post-crash and in normal case to fill table journals)

	// commit open tx
	if err := commit(); err != nil {
		return nil, err
	}

	return e, nil
}

func (e *Engine) Close(ctx context.Context) error {
	// TODO:  set shutdown flag (disallow new transactions)
	e.log.Debugf("Closing database %s at %s", e.cat.name, e.path)

	// abort all pending transactions
	for _, tx := range e.txs {
		tx.Abort()
	}

	// TODO: close wal

	// clear caches
	e.PurgeCache()

	// close all open indexes
	for n, idx := range e.indexes {
		idx.Table().UnuseIndex(idx)
		if err := idx.Close(ctx); err != nil {
			e.log.Errorf("Closing table %s: %v", idx.Schema().Name(), err)
		}
		delete(e.indexes, n)
	}

	// close all open tables
	for n, t := range e.tables {
		if err := t.Close(ctx); err != nil {
			e.log.Errorf("Closing table %s: %v", t.Schema().Name(), err)
		}
		delete(e.tables, n)
	}

	// close all open stores
	for n, s := range e.stores {
		if err := s.Close(ctx); err != nil {
			e.log.Errorf("Closing store %s: %v", s.Schema().Name(), err)
		}
		delete(e.stores, n)
	}

	// close enums
	for n, enum := range e.enums {
		schema.UnregisterEnum(enum)
		delete(e.enums, n)
	}

	// close catalog
	if err := e.cat.Close(ctx); err != nil {
		e.log.Errorf("Closing catalog: %v", err)
	}

	// close catalog
	e.cat.Close(ctx)
	e.cat = nil

	return nil
}

func (e *Engine) PurgeCache() {
	e.cache.blocks.Purge()
	e.cache.buffers.Purge()
}

func (e *Engine) RootPath() string {
	return e.path
}

func (e *Engine) Namespace() string {
	return e.opts.Namespace
}

func (e *Engine) Catalog() *Catalog {
	return e.cat
}

func (e *Engine) BlockCache() BlockCacheType {
	return e.cache.blocks
}

func (e *Engine) BufferCache() BufferCacheType {
	return e.cache.buffers
}

func (e *Engine) Log() log.Logger {
	return e.log
}

func (e *Engine) Sync(ctx context.Context) error {
	// TODO: in wal mode this becomes unnecessary

	// without wal tables write their journal here which requires a tx
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// sync tables
	errg := &errgroup.Group{}
	errg.SetLimit(len(e.tables))
	for _, t := range e.tables {
		errg.Go(func() error { return t.Sync(ctx) })
	}

	// sync stores (unsupported)
	// for _, s := range e.stores {
	// 	errg.Go(func() error { return s.Sync(ctx) })
	// }

	if err := errg.Wait(); err != nil {
		return err
	}

	// commit open tx
	return commit()
}