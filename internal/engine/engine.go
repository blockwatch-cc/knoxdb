// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"errors"
	"path/filepath"
	"sync"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/cache"
	"blockwatch.cc/knoxdb/pkg/cache/rclru"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
	"github.com/gofrs/flock"
	"golang.org/x/sync/errgroup"
)

const (
	ENGINE_LOCK_NAME = "LOCK"
)

// Engine is the central instance managing a database
type Engine struct {
	mu      sync.RWMutex
	lock    *flock.Flock
	cat     *Catalog
	cache   CacheManager
	tables  map[uint64]TableEngine
	stores  map[uint64]StoreEngine
	indexes map[uint64]IndexEngine
	enums   schema.EnumRegistry
	txs     TxList
	opts    DatabaseOptions
	xmin    uint64
	xnext   uint64
	dbId    uint64
	path    string
	log     log.Logger
	merger  *MergerService
	wal     *wal.Wal
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
	opts = DefaultDatabaseOptions.Merge(opts)
	e := &Engine{
		path: filepath.Join(opts.Path, name),
		cache: CacheManager{
			blocks:  rclru.NewNoCache[CacheKeyType, *block.Block](),
			buffers: rclru.NewNoCache[CacheKeyType, *Buffer](),
		},
		tables:  make(map[uint64]TableEngine),
		stores:  make(map[uint64]StoreEngine),
		indexes: make(map[uint64]IndexEngine),
		enums:   make(schema.EnumRegistry),
		txs:     make(TxList, 0),
		xmin:    0,
		xnext:   0,
		dbId:    types.TaggedHash(types.ObjectTagDatabase, name),
		opts:    opts,
		cat:     NewCatalog(name),
		log:     log.Disabled,
		merger:  NewMergerService(),
	}

	if opts.Logger != nil {
		e.log = opts.Logger
		e.merger.WithLogger(opts.Logger)
	}

	e.log.Debugf("Creating database %s at %s", name, e.path)

	// set exclusive directory lock
	lock := flock.New(filepath.Join(opts.Path, ENGINE_LOCK_NAME))
	_, err := lock.TryLock()
	if err != nil && !errors.Is(err, errors.ErrUnsupported) {
		return nil, err
	} else {
		e.lock = lock
	}

	// start transaction and amend context (required to store catalog db)
	ctx, commit, abort := e.WithTransaction(ctx)

	// cleanup on any errors
	defer func() {
		if err == nil {
			return
		}
		abort()
		e.Close(ctx)
	}()

	// init wal
	wopts := wal.WalOptions{
		Seed:           e.dbId,
		Path:           filepath.Join(e.path, "wal"),
		MaxSegmentSize: e.opts.WalSegmentSize,
		RecoveryMode:   e.opts.WalRecoveryMode,
		Logger:         e.log,
	}
	e.wal, err = wal.Create(wopts)
	if err != nil {
		return nil, err
	}
	e.cat.WithWal(e.wal)

	// init caches
	if opts.CacheSize > 0 {
		e.cache.blocks = rclru.New2Q[CacheKeyType, *block.Block](opts.CacheSize * 90 / 10)
		e.cache.buffers = rclru.New2Q[CacheKeyType, *Buffer](opts.CacheSize / 10)
	}

	// init catalog
	if err = e.cat.Create(ctx, opts); err != nil {
		return nil, err
	}

	// write db options to catalog
	if err = e.cat.PutOptions(ctx, e.dbId, &opts); err != nil {
		return nil, err
	}

	// commit open tx
	if err = commit(); err != nil {
		return nil, err
	}

	// start services
	e.merger.Start()

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
		txs:     make(TxList, 0),
		dbId:    types.TaggedHash(types.ObjectTagDatabase, name),
		cat:     NewCatalog(name),
		log:     log.Disabled,
		merger:  NewMergerService(),
	}
	if opts.Logger != nil {
		e.log = opts.Logger
		e.merger.WithLogger(opts.Logger)
	}

	e.log.Debugf("Opening database %s at %s", name, e.path)

	// set exclusive directory lock
	lock := flock.New(filepath.Join(opts.Path, ENGINE_LOCK_NAME))
	_, err := lock.TryLock()
	if err != nil && !errors.Is(err, errors.ErrUnsupported) {
		return nil, err
	} else {
		e.lock = lock
	}

	// start transaction (for wal recovery we may need a write tx)
	ctx, commit, abort := e.WithTransaction(ctx)

	// cleanup on error
	defer func() {
		if err == nil {
			return
		}
		if err := abort(); err != nil {
			e.log.Error(err)
		}
		if err := e.Close(ctx); err != nil {
			e.log.Error(err)
		}
	}()

	// load and validate catalog
	if err := e.cat.Open(ctx, opts); err != nil {
		return nil, err
	}

	// load stored database options
	var sopts DatabaseOptions
	err = e.cat.GetOptions(ctx, e.dbId, &sopts)
	if err != nil {
		return nil, err
	}
	// merge options
	e.opts = sopts.Merge(opts)

	// open and validate wal (recovery happens individually at catalog and table level)
	wopts := wal.WalOptions{
		Seed:           e.dbId,
		Path:           filepath.Join(e.path, "wal"),
		MaxSegmentSize: e.opts.WalSegmentSize,
		RecoveryMode:   e.opts.WalRecoveryMode,
		Logger:         e.log,
	}
	e.log.Debugf("Opening wal at %s", wopts.Path)
	e.wal, err = wal.Open(e.cat.Checkpoint(), wopts)
	if err != nil {
		return nil, err
	}
	e.cat.WithWal(e.wal)

	// recover missing catalog changes from wal, potentially clean up files from
	// failed transactions
	if err = e.cat.Recover(ctx); err != nil {
		return nil, err
	}

	// init caches
	if e.opts.CacheSize > 0 {
		e.cache.blocks = rclru.New2Q[CacheKeyType, *block.Block](e.opts.CacheSize * 90 / 10)
		e.cache.buffers = rclru.New2Q[CacheKeyType, *Buffer](e.opts.CacheSize / 10)
	}

	// open database objects
	if err = e.openEnums(ctx); err != nil {
		return nil, err
	}

	if err = e.openTables(ctx); err != nil {
		return nil, err
	}

	if err = e.openStores(ctx); err != nil {
		return nil, err
	}

	// close tx
	if err = commit(); err != nil {
		return nil, err
	}

	// start services
	e.merger.Start()

	return e, nil
}

func (e *Engine) Close(ctx context.Context) error {
	e.log.Debugf("Closing database %s at %s", e.cat.name, e.path)

	// TODO: set shutdown flag to prevent new transactions

	// abort all pending transactions
	for _, tx := range e.txs {
		tx.Abort(ctx)
	}

	// stop services
	e.merger.Stop()

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

	// close all open tables (set checkpoints)
	for n, t := range e.tables {
		if err := t.Close(ctx); err != nil {
			e.log.Errorf("Closing table %s: %v", t.Schema().Name(), err)
		}
		delete(e.tables, n)
	}

	// close all open stores (set checkpoints)
	for n, s := range e.stores {
		if err := s.Close(ctx); err != nil {
			e.log.Errorf("Closing store %s: %v", s.Schema().Name(), err)
		}
		delete(e.stores, n)
	}

	// close enums
	for n, enum := range e.enums {
		schema.UnregisterEnum(e.dbId, enum)
		delete(e.enums, n)
	}

	// close catalog (set checkpoint)
	if e.cat != nil {
		if err := e.cat.Close(ctx); err != nil {
			e.log.Errorf("Closing catalog: %v", err)
		}
		e.cat = nil
	}

	// close and sync wal
	if e.wal != nil {
		if err := e.wal.Close(); err != nil {
			e.log.Errorf("Closing wal: %v", err)
		}
		e.wal = nil
	}

	// release directory lock
	if e.lock != nil {
		e.lock.Close()
		e.lock = nil
	}

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

func (e *Engine) Wal() *wal.Wal {
	return e.wal
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
	// TODO: in wal mode this becomes unnecessary unless we offer a custom config option

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

func (e *Engine) CommitTx(ctx context.Context, oid, xid uint64) error {
	t, ok := e.tables[oid]
	if ok {
		return t.CommitTx(ctx, xid)
	}
	s, ok := e.stores[oid]
	if ok {
		return s.CommitTx(ctx, xid)
	}
	return nil
}

func (e *Engine) AbortTx(ctx context.Context, oid, xid uint64) error {
	t, ok := e.tables[oid]
	if ok {
		return t.AbortTx(ctx, xid)
	}
	s, ok := e.stores[oid]
	if ok {
		return s.AbortTx(ctx, xid)
	}
	return nil
}
