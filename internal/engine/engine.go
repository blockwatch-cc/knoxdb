// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/gofrs/flock"
	"golang.org/x/sync/errgroup"
)

const (
	ENGINE_LOCK_NAME = "LOCK"
)

// Engine is the central instance managing a database
type Engine struct {
	mu       sync.RWMutex
	shutdown atomic.Value
	flock    *flock.Flock
	cat      *Catalog
	cache    CacheManager
	tables   *util.LockFreeMap[uint64, TableEngine]
	stores   *util.LockFreeMap[uint64, StoreEngine]
	indexes  *util.LockFreeMap[uint64, IndexEngine]
	enums    schema.EnumRegistry
	opts     DatabaseOptions
	txs      TxList
	xact     uint64 // single active write tx (none when 0)
	xmin     uint64 // xid horizon (minimum active xid)
	xnext    uint64 // next txid for read/write tx
	vnext    uint64 // virtual xid for read-only tx
	dbId     uint64 // unique database tag
	path     string // full db base path (from opts + name)
	log      log.Logger
	tasks    *TaskService
	wal      *wal.Wal
	lm       *LockManager
}

type CacheManager struct {
	// generic block cache
	// - pack engine: 64bit table/index id + 32bit pack id + 16bit block id
	blocks block.BlockCache
	// generic buffer cache
	// - store engine: 64bit store id + 64bit user key
	buffers BufferCache
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

func (e *Engine) Options() DatabaseOptions {
	return e.opts
}

func (e *Engine) BlockCache(key uint64) block.BlockCachePartition {
	return e.cache.blocks.Partition(key)
}

func (e *Engine) BufferCache(key uint64) BufferCachePartition {
	return e.cache.buffers.Partition(key)
}

func (e *Engine) Log() log.Logger {
	return e.log
}

func (e *Engine) IsShutdown() bool {
	sd := e.shutdown.Load()
	return sd != nil && sd.(bool)
}

func Create(ctx context.Context, name string, opts DatabaseOptions) (*Engine, error) {
	opts = DefaultDatabaseOptions.Merge(opts)
	e := &Engine{
		path: filepath.Join(opts.Path, name),
		cache: CacheManager{
			blocks:  block.NewCache(0),
			buffers: NewBufferCache(0),
		},
		tables:  util.NewLockFreeMap[uint64, TableEngine](),
		stores:  util.NewLockFreeMap[uint64, StoreEngine](),
		indexes: util.NewLockFreeMap[uint64, IndexEngine](),
		enums:   schema.NewEnumRegistry(),
		txs:     make(TxList, 0),
		xact:    0,
		xmin:    0,
		xnext:   0,
		vnext:   ReadTxOffset,
		dbId:    types.TaggedHash(types.ObjectTagDatabase, name),
		opts:    opts,
		cat:     NewCatalog(name),
		log:     log.Disabled,
		tasks:   NewTaskService().WithLimits(opts.MaxWorkers, opts.MaxTasks),
		lm:      NewLockManager().WithTimeout(opts.LockTimeout),
	}
	e.tasks.WithContext(WithEngine(ctx, e))
	e.shutdown.Store(false)

	// start services
	e.tasks.Start()

	if opts.Logger != nil {
		e.log = opts.Logger
		e.tasks.WithLogger(opts.Logger)
	}

	e.log.Debugf("Creating database %q at %q", name, e.path)

	// set exclusive directory lock
	lock := flock.New(filepath.Join(opts.Path, ENGINE_LOCK_NAME))
	_, err := lock.TryLock()
	if err != nil && !errors.Is(err, errors.ErrUnsupported) {
		return nil, err
	} else {
		e.flock = lock
	}

	// start transaction and amend context (required to store catalog db)
	ctx, _, commit, abort, err := e.WithTransaction(ctx)
	if err != nil {
		return nil, err
	}

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
		e.cache.blocks = block.NewCache(opts.CacheSize * 90 / 10)
		e.cache.buffers = NewBufferCache(opts.CacheSize / 10)
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

	return e, nil
}

func Open(ctx context.Context, name string, opts DatabaseOptions) (*Engine, error) {
	opts = DefaultDatabaseOptions.Merge(opts)
	e := &Engine{
		path: filepath.Join(opts.Path, name),
		cache: CacheManager{
			blocks:  block.NewCache(0),
			buffers: NewBufferCache(0),
		},
		tables:  util.NewLockFreeMap[uint64, TableEngine](),
		stores:  util.NewLockFreeMap[uint64, StoreEngine](),
		indexes: util.NewLockFreeMap[uint64, IndexEngine](),
		enums:   schema.NewEnumRegistry(),
		txs:     make(TxList, 0),
		xact:    0,
		xmin:    0,
		xnext:   0,
		vnext:   ReadTxOffset,
		dbId:    types.TaggedHash(types.ObjectTagDatabase, name),
		cat:     NewCatalog(name),
		log:     log.Disabled,
		tasks:   NewTaskService().WithLimits(opts.MaxWorkers, opts.MaxTasks),
		lm:      NewLockManager(),
	}
	e.tasks.WithContext(WithEngine(ctx, e))
	e.shutdown.Store(false)
	if opts.Logger != nil {
		e.log = opts.Logger
		e.tasks.WithLogger(opts.Logger)
	}

	e.log.Debugf("Opening database %q at %q", name, e.path)

	// set exclusive directory lock
	lock := flock.New(filepath.Join(opts.Path, ENGINE_LOCK_NAME))
	_, err := lock.TryLock()
	if err != nil && !errors.Is(err, errors.ErrUnsupported) {
		return nil, err
	} else {
		e.flock = lock
	}

	// start services before potential recovery
	e.tasks.Start()

	// start transaction (for catalog access and recovery we need a write tx,
	// but we don't want it to write wal records)
	ctx, _, commit, abort, err := e.WithTransaction(ctx, TxFlagsNoWal)
	if err != nil {
		e.Close(ctx)
		return nil, err
	}

	// cleanup on error
	defer func() {
		if err == nil {
			return
		}
		// tx.Fail(err)
		if err := abort(); err != nil {
			e.log.Errorf("abort: %v", err)
		}
		if err := e.Close(ctx); err != nil {
			e.log.Error("close: %v", err)
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
	e.lm.WithTimeout(e.opts.LockTimeout)

	// open and validate wal (recovery happens individually at catalog and table level)
	wopts := wal.WalOptions{
		Seed:           e.dbId,
		Path:           filepath.Join(e.path, "wal"),
		MaxSegmentSize: e.opts.WalSegmentSize,
		RecoveryMode:   e.opts.WalRecoveryMode,
		Logger:         e.log,
	}

	e.log.Debugf("Opening wal at %q lsn 0x%x", wopts.Path, e.cat.Checkpoint())
	e.wal, err = wal.Open(e.cat.Checkpoint(), wopts)
	if err != nil {
		return nil, err
	}
	e.cat.WithWal(e.wal)

	// recover missing catalog changes from wal, potentially clean up files from
	// failed transactions, we can skip this step if we had a clean shutdown, ie.
	// db checkpoint == last wal record
	if e.cat.Checkpoint() < e.wal.Last() {
		// recover catalog object changes
		if err = e.cat.Recover(ctx); err != nil {
			return nil, err
		}

		// sync wal explicitly (because we work without wal support in tx)
		if err := e.wal.Sync(); err != nil {
			return nil, err
		}
	}

	// init caches
	if e.opts.CacheSize > 0 {
		e.cache.blocks = block.NewCache(e.opts.CacheSize * 90 / 10)
		e.cache.buffers = NewBufferCache(e.opts.CacheSize / 10)
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

	// init virtual xid for read-only tx
	e.vnext = e.xnext + ReadTxOffset

	// close tx (we commit here because crash recovery may have rewritten
	// catalog state)
	if err = commit(); err != nil {
		return nil, err
	}

	e.log.Debugf("engine started with xid=%d vxid=%d", e.xnext, e.vnext)

	return e, nil
}

func (e *Engine) Close(ctx context.Context) error {
	e.log.Debugf("Closing database %s at %s", e.cat.name, e.path)

	// set shutdown flag to prevent new transactions
	e.shutdown.Store(true)

	// lock engine
	e.mu.Lock()
	defer e.mu.Unlock()

	// TODO: shutdown user sessions (close wire protocol server)
	// - should cancel contexts

	// cancel pending transaction
	// TODO: find another way, maybe cancel session contexts + define an explicit
	// session for sdk usage
	for _, tx := range e.txs {
		e.log.Tracef("Kill tx id %d", tx.id)
		tx.Kill(ErrDatabaseShutdown)
	}

	// stop services
	e.log.Trace("Stop services")
	if e.tasks != nil {
		e.tasks.Stop()
		e.tasks = nil
	}

	// wait for transactions and services to release all locks
	e.log.Trace("Wait LM")
	e.lm.Wait()
	e.lm = nil

	// clear caches
	e.log.Trace("Purge caches")
	e.PurgeCache()

	// close all open indexes
	e.log.Trace("Close indexes")
	for _, idx := range e.indexes.Map() {
		idx.Table().UnuseIndex(idx)
		name := idx.Schema().Name()
		if err := idx.Sync(ctx); err != nil {
			e.log.Errorf("Syncing index %s: %v", name, err)
		}
		if err := idx.Close(ctx); err != nil {
			e.log.Errorf("Closing index %s: %v", name, err)
		}
	}
	e.indexes.Clear()

	// close all open tables (set checkpoints)
	e.log.Trace("Close tables")
	for _, t := range e.tables.Map() {
		name := t.Schema().Name()
		if err := t.Sync(ctx); err != nil {
			e.log.Errorf("Syncing table %s: %v", name, err)
		}
		if err := t.Close(ctx); err != nil {
			e.log.Errorf("Closing table %s: %v", name, err)
		}
	}
	e.tables.Clear()

	// close all open stores (set checkpoints)
	e.log.Trace("Close stores")
	for _, s := range e.stores.Map() {
		name := s.Schema().Name()
		if err := s.Sync(ctx); err != nil {
			e.log.Errorf("Syncing store %s: %v", name, err)
		}
		if err := s.Close(ctx); err != nil {
			e.log.Errorf("Closing store %s: %v", name, err)
		}
	}
	e.stores.Clear()

	// close enums
	e.log.Trace("Close enums")
	for _, enum := range e.enums.Map() {
		schema.UnregisterEnum(e.dbId, enum)
	}
	e.enums.Clear()

	// close catalog (set checkpoint)
	if e.cat != nil {
		e.log.Trace("Close catalog")
		if err := e.cat.Close(ctx); err != nil {
			e.log.Errorf("Closing catalog: %v", err)
		}
		e.cat = nil
	}

	// close and sync wal
	if e.wal != nil {
		e.log.Trace("Close wal")
		if err := e.wal.Close(); err != nil {
			e.log.Errorf("Closing wal: %v", err)
		}
		e.wal = nil
	}

	// release directory lock
	if e.flock != nil {
		e.log.Trace("Free flock")
		e.flock.Close()
		e.flock = nil
	}

	return nil
}

func (e *Engine) ForceShutdown() error {
	e.log.Debugf("Force shutdown database %s at %s", e.cat.name, e.path)

	// set shutdown flag to prevent new transactions
	e.shutdown.Store(true)

	// lock engine
	e.mu.Lock()
	defer e.mu.Unlock()

	// TODO: shutdown user sessions (close wire protocol server)
	// - should cancel contexts

	// abort all pending transactions
	// TODO: find another way, maybe cancel session contexts + define an explicit
	// session for sdk usage
	// for _, tx := range e.txs {
	// 	e.log.Tracef("Kill tx id %d", tx.id)
	// 	tx.Fail(ErrDatabaseShutdown)
	// }

	// abort storage backend transactions
	for _, tx := range e.txs {
		e.log.Tracef("Kill tx id %d", tx.id)
		tx.Kill(ErrDatabaseShutdown)
	}

	// stop services
	e.log.Trace("Stop services")
	if e.tasks != nil {
		e.tasks.Kill()
		e.tasks = nil
	}

	// release/cleanup locks
	e.log.Trace("Clear LM")
	e.lm.Clear()
	e.lm = nil

	// clear caches
	e.log.Trace("Purge caches")
	e.PurgeCache()

	// close wal without flush&sync
	e.log.Trace("Close wal")
	if err := e.wal.ForceClose(); err != nil {
		e.log.Errorf("close wal: %v", err)
	}
	e.wal = nil

	// close catalog without checkpointing
	e.log.Trace("Close catalog")
	if err := e.cat.ForceClose(); err != nil {
		e.log.Errorf("close catalog: %v", err)
	}
	e.cat = nil

	// close enums
	e.log.Trace("Close enums")
	for _, enum := range e.enums.Map() {
		schema.UnregisterEnum(e.dbId, enum)
	}
	e.enums.Clear()

	ctx := context.Background()

	// close engine storage backend files without journal flush and checkpointing
	e.log.Trace("Close indexes")
	for _, idx := range e.indexes.Map() {
		idx.Table().UnuseIndex(idx)
		name := idx.Schema().Name()
		if err := idx.Close(ctx); err != nil {
			e.log.Errorf("Closing index %s: %v", name, err)
		}
	}
	e.indexes.Clear()

	// close all open tables (without checkpoints)
	e.log.Trace("Close tables")
	for _, t := range e.tables.Map() {
		if err := t.Close(ctx); err != nil {
			e.log.Errorf("Closing table %s: %v", t.Schema().Name(), err)
		}
	}
	e.tables.Clear()

	// close all open stores (without checkpoints)
	e.log.Trace("Close stores")
	for _, s := range e.stores.Map() {
		if err := s.Close(ctx); err != nil {
			e.log.Errorf("Closing store %s: %v", s.Schema().Name(), err)
		}
	}
	e.stores.Clear()

	// release directory lock
	if e.flock != nil {
		e.log.Trace("Free flock")
		e.flock.Close()
		e.flock = nil
	}

	return nil
}

func (e *Engine) Sync(ctx context.Context) error {
	// write explicit checkpoints for all storage backends
	// legacy tables without wal write their journal here
	errg := &errgroup.Group{}
	errg.SetLimit(runtime.NumCPU())

	// sync tables
	for _, t := range e.tables.Map() {
		errg.Go(func() error { return t.Sync(ctx) })
	}

	// sync stores
	for _, s := range e.stores.Map() {
		errg.Go(func() error { return s.Sync(ctx) })
	}

	return errg.Wait()
}

func (e *Engine) CommitTx(ctx context.Context, oid, xid uint64) error {
	var (
		t  TxTracker
		ok bool
	)
	t, ok = e.tables.Get(oid)
	if !ok {
		t, ok = e.stores.Get(oid)
	}
	if !ok {
		return nil
	}
	return t.CommitTx(ctx, xid)
}

func (e *Engine) AbortTx(ctx context.Context, oid, xid uint64) error {
	var (
		t  TxTracker
		ok bool
	)
	t, ok = e.tables.Get(oid)
	if !ok {
		t, ok = e.stores.Get(oid)
	}
	if !ok {
		return nil
	}
	return t.AbortTx(ctx, xid)
}

func (e *Engine) Schedule(t *Task) bool {
	return e.tasks.Submit(t)
}

func (e *Engine) UpdateTxHorizon(xid uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.xmin = max(e.xmin, xid)
	e.xnext = e.xmin + 1
}
