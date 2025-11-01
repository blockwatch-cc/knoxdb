// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/store"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/gofrs/flock"
	"golang.org/x/sync/errgroup"
)

const (
	ENGINE_LOCK_NAME = "LOCK"
)

// Engine is the central instance managing a database
//
// Relevant Subsystems
// - block caches
// - lock manager
// - task manager
// - WAL
// - catalog
//
// Transaction support
// - single writer, multiple reader MVCC
// - readers, writer and background merge processes do not block each other
// - optional tx flags to control tx behavior
//   - enable or disbale no wal write
//   - choose between direct, no or delayed wal fsync
//   - timeout mode for concurrent writers (wait unlimited, limited, don't wait)
//   - readers can wait until writer finished (for more efficient 'safe' MVCC snapshot)
//
// Read-only mode
// - changes no data on disk
// - all backends opened in read-only mode
// - no wal repair/truncate before replay
// - no wal write
// - no journal merge
// - no table gc
// - no write tx
// - no catalog object create/change
// - DDL and DML functions return error ErrDatabaseReadOnly

type Engine struct {
	mu       sync.RWMutex                           // engine mutex
	shutdown atomic.Bool                            // atomic shutdown state
	rungc    atomic.Bool                            // gc task state
	flock    *flock.Flock                           // exclusive directory lock
	dbId     uint64                                 // unique database tag
	path     string                                 // full db base path (from opts + name)
	cat      *Catalog                               // objects, identities, configurations
	cache    CacheManager                           // block and buffer caches
	tables   *util.LockFreeMap[uint64, TableEngine] // table objects
	stores   *util.LockFreeMap[uint64, StoreEngine] // store objects
	indexes  *util.LockFreeMap[uint64, IndexEngine] // index objects
	enums    *schema.EnumRegistry                   // enum objects
	opts     DatabaseOptions                        // engine-wide configuration
	txchan   chan struct{}                          // single writer enforcement
	txs      TxList                                 // active read transactions
	wtx      *Tx                                    // active write transaction (single)
	xmin     XID                                    // xid horizon (minimum active xid)
	xnext    XID                                    // next txid for read/write tx
	vnext    XID                                    // virtual xid for read-only tx
	log      log.Logger                             // engine logger
	tasks    *TaskService                           // async task execution service
	wal      *wal.Wal                               // write ahead log
	lm       *LockManager                           // object lock manager
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
	return e.shutdown.Load()
}

func (e *Engine) IsReadOnly() bool {
	return e.opts.ReadOnly
}

func (e *Engine) Watermark() wal.LSN {
	lsn := e.cat.Checkpoint()
	for _, t := range e.tables.Map() {
		lsn = min(lsn, t.State().Checkpoint)
	}
	return lsn
}

func (e *Engine) NeedsCheckpoint() bool {
	walSize, segSize := e.wal.Next(), wal.LSN(e.opts.WalSegmentSize)
	return e.Watermark() < walSize-5*segSize
}

// checks if catalog backend file exists
func IsExist(ctx context.Context, name string, opts DatabaseOptions) (bool, error) {
	opts = DefaultDatabaseOptions.Merge(opts)
	return store.Exists(opts.Driver, filepath.Join(opts.Path, name, CATALOG_NAME))
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
		txchan:  make(chan struct{}, 1),
		xmin:    1,
		xnext:   1,
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

	e.log.Debugf("create database %q at %q", name, e.path)

	// set exclusive directory lock
	lock := flock.New(filepath.Join(opts.Path, ENGINE_LOCK_NAME))
	_, err := lock.TryLock()
	if err != nil && !errors.Is(err, errors.ErrUnsupported) {
		return nil, err
	} else {
		e.flock = lock
	}

	// start write transaction and amend context (required to store catalog db)
	tx := e.NewTransaction(0)
	ctx = context.WithValue(ctx, TransactionKey{}, tx)
	ctx = WithEngine(ctx, e)

	// cleanup on any errors
	defer func() {
		if err == nil {
			return
		}
		tx.Abort()
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
	if err = tx.Commit(); err != nil {
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
		txchan:  make(chan struct{}, 1),
		xmin:    1,
		xnext:   1,
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

	e.log.Debugf("open database %q at %q", name, e.path)

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
	// but we don't want it to write wal records; in read-only mode we open
	// a read-only tx instead)
	uflags := TxFlagNoWal
	if e.IsReadOnly() {
		uflags |= TxFlagReadOnly
	}
	tx := e.NewTransaction(uflags)

	// link to context
	ctx = context.WithValue(ctx, TransactionKey{}, tx)
	ctx = WithEngine(ctx, e)

	// cleanup on error
	defer func() {
		if err == nil {
			return
		}
		if err := tx.Abort(); err != nil {
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
		ReadOnly:       e.opts.ReadOnly,
		RecoveryMode:   e.opts.WalRecoveryMode,
		Logger:         e.log,
	}

	e.log.Debugf("open wal at %q lsn 0x%x", wopts.Path, e.cat.Checkpoint())
	e.wal, err = wal.Open(e.cat.Checkpoint(), wopts)
	if err != nil {
		return nil, err
	}
	e.cat.WithWal(e.wal)

	// recover missing catalog changes from wal, potentially clean up files from
	// failed transactions, we can skip this step if we had a clean shutdown, ie.
	// db checkpoint == last wal record
	if !e.IsReadOnly() && e.cat.Checkpoint() < e.wal.Last() {
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

	// commit tx, crash recovery may have rewritten catalog state
	if err = tx.Commit(); err != nil {
		return nil, err
	}

	e.log.Debugf("engine started with xid=%d vxid=%d", e.xnext, e.vnext)

	return e, nil
}

func (e *Engine) Close(ctx context.Context) error {
	e.log.Debugf("close database %s at %s", e.cat.name, e.path)

	// export engine
	ctx = WithEngine(ctx, e)

	// set shutdown flag to prevent new transactions
	e.shutdown.Store(true)

	// cancel pending transaction, tx contexts
	for _, tx := range e.txs {
		e.log.Tracef("kill tx id %d", tx.id)
		tx.Kill(ErrDatabaseShutdown)
	}

	// TODO: shutdown user sessions (close wire protocol server)
	// - should cancel session contexts

	// close write token channel, unblocking waiting writers which will cancel
	close(e.txchan)
	e.txchan = nil

	// lock engine
	e.mu.Lock()
	defer e.mu.Unlock()

	// stop services
	e.log.Trace("stop services")
	if e.tasks != nil {
		e.tasks.Stop()
		e.tasks = nil
	}

	// wait for transactions and services to release all locks
	e.log.Trace("wait LM")
	e.lm.Wait()
	e.lm = nil

	// clear caches
	e.log.Trace("purge caches")
	e.PurgeCache()

	// close all open indexes
	e.log.Trace("close indexes")
	for _, idx := range e.indexes.Map() {
		idx.Table().DisconnectIndex(idx)
		name := idx.Schema().Name
		if !e.IsReadOnly() {
			if err := idx.Sync(ctx); err != nil {
				e.log.Errorf("sync index %s: %v", name, err)
			}
		}
		if err := idx.Close(ctx); err != nil {
			e.log.Errorf("close index %s: %v", name, err)
		}
	}
	e.indexes.Clear()

	// close all open tables (set checkpoints)
	e.log.Trace("close tables")
	for _, t := range e.tables.Map() {
		name := t.Schema().Name
		if !e.IsReadOnly() {
			if err := t.Sync(ctx); err != nil {
				e.log.Errorf("sync table %s: %v", name, err)
			}
		}
		if err := t.Close(ctx); err != nil {
			e.log.Errorf("close table %s: %v", name, err)
		}
	}
	e.tables.Clear()

	// close all open stores (set checkpoints)
	e.log.Trace("Close stores")
	for _, s := range e.stores.Map() {
		name := s.Schema().Name
		if !e.IsReadOnly() {
			if err := s.Sync(ctx); err != nil {
				e.log.Errorf("sync store %s: %v", name, err)
			}
		}
		if err := s.Close(ctx); err != nil {
			e.log.Errorf("close store %s: %v", name, err)
		}
	}
	e.stores.Clear()

	// close enums
	e.log.Trace("close enums")
	for _, enum := range e.enums.Map() {
		schema.UnregisterEnum(e.dbId, enum)
	}
	e.enums.Clear()

	// close catalog (set checkpoint)
	if e.cat != nil {
		e.log.Trace("close catalog")
		if err := e.cat.Close(ctx); err != nil {
			e.log.Errorf("close catalog: %v", err)
		}
		e.cat = nil
	}

	// close and sync wal
	if e.wal != nil {
		e.log.Trace("close wal")
		if err := e.wal.Close(); err != nil {
			e.log.Errorf("close wal: %v", err)
		}
		e.wal = nil
	}

	// release directory lock
	if e.flock != nil {
		e.log.Trace("free flock")
		e.flock.Close()
		e.flock = nil
	}

	return nil
}

func (e *Engine) ForceShutdown() error {
	e.log.Debugf("force shutdown database %s at %s", e.cat.name, e.path)

	// set shutdown flag to prevent new transactions
	e.shutdown.Store(true)

	// lock engine
	e.mu.Lock()
	defer e.mu.Unlock()

	// close write token channel, unblocking waiting writers which will cancel
	close(e.txchan)
	e.txchan = nil

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
		e.log.Tracef("kill tx id %d", tx.id)
		tx.Kill(ErrDatabaseShutdown)
	}

	// stop services
	e.log.Trace("stop services")
	if e.tasks != nil {
		e.tasks.Kill()
		e.tasks = nil
	}

	// release/cleanup locks
	e.log.Trace("clear LM")
	e.lm.Clear()
	e.lm = nil

	// clear caches
	e.log.Trace("purge caches")
	e.PurgeCache()

	// close wal without flush&sync
	e.log.Trace("close wal")
	if err := e.wal.ForceClose(); err != nil {
		e.log.Errorf("close wal: %v", err)
	}
	e.wal = nil

	// close catalog without checkpointing
	e.log.Trace("close catalog")
	if err := e.cat.ForceClose(); err != nil {
		e.log.Errorf("close catalog: %v", err)
	}
	e.cat = nil

	// close enums
	e.log.Trace("close enums")
	for _, enum := range e.enums.Map() {
		schema.UnregisterEnum(e.dbId, enum)
	}
	e.enums.Clear()

	ctx := context.Background()

	// close engine storage backend files without journal flush and checkpointing
	e.log.Trace("close indexes")
	for _, idx := range e.indexes.Map() {
		idx.Table().DisconnectIndex(idx)
		name := idx.Schema().Name
		if err := idx.Close(ctx); err != nil {
			e.log.Errorf("close index %s: %v", name, err)
		}
	}
	e.indexes.Clear()

	// close all open tables (without checkpoints)
	e.log.Trace("close tables")
	for _, t := range e.tables.Map() {
		if err := t.Close(ctx); err != nil {
			e.log.Errorf("close table %s: %v", t.Schema().Name, err)
		}
	}
	e.tables.Clear()

	// close all open stores (without checkpoints)
	e.log.Trace("close stores")
	for _, s := range e.stores.Map() {
		if err := s.Close(ctx); err != nil {
			e.log.Errorf("close store %s: %v", s.Schema().Name, err)
		}
	}
	e.stores.Clear()

	// release directory lock
	if e.flock != nil {
		e.log.Trace("free flock")
		e.flock.Close()
		e.flock = nil
	}

	return nil
}

func (e *Engine) Sync(ctx context.Context) error {
	// skip in read-only mode
	if e.IsReadOnly() {
		return nil
	}

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

func (e *Engine) CommitTx(ctx context.Context, oid uint64, xid types.XID) error {
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

func (e *Engine) AbortTx(ctx context.Context, oid uint64, xid types.XID) error {
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

// TryGC is triggered after table engines have merged new journal checkpoints.
// Its aim is to reduce the number of WAL files we need to keep around for crash
// recovery by writing new table and catalog checkpoints at the end of the WAL.
// TryGC will only measure the distance of the oldest active checkpoint and
// schedule a GC task. It makes sure only a single GC task runs in the system.
func (e *Engine) TryGC(ctx context.Context) error {
	// skip during shutdown
	if e.IsShutdown() {
		e.log.Debug("GC skipped on shutdown")
		return nil
	}

	// skip when not required
	if !e.NeedsCheckpoint() {
		e.log.Debug("GC starting")
		return e.wal.GC(e.Watermark())
	}

	// schedule GC task atomically
	if ok := e.rungc.CompareAndSwap(false, true); !ok {
		e.log.Debug("GC already running")
		return nil
	}

	// schedule task
	e.log.Debug("WAL watermark too old, scheduling checkpointing task")
	if !e.Schedule(NewTask(e.RunGC)) {
		// reset atomic state
		e.rungc.Store(false)
		e.log.Warn("task queue full")
	}

	return nil
}

func (e *Engine) RunGC(ctx context.Context) error {
	// ensure GC is enabled (prevents from accidentally calling RunGC
	// directly without a task)
	if !e.rungc.Load() {
		return nil
	}

	// reset state on exit
	defer e.rungc.Store(false)

	// skip on shutdown
	if e.IsShutdown() {
		return nil
	}

	// skip on canceled context
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	e.log.Debug("gc: running table checkpointing task")

	// read current wal size and min watermark
	startLsn := e.Watermark()
	startSize := e.wal.Next()
	segSize := wal.LSN(e.opts.WalSegmentSize)

	// write catalog checkpoint when older than one segment, this also
	// syncs the wal
	if e.cat.Checkpoint() < startSize-segSize {
		if err := e.cat.doCheckpoint(ctx); err != nil {
			return fmt.Errorf("checkpoint catalog: %v", err)
		}
	}

	// write table checkpoints when older than 5 segments and schedule
	// table merge tasks (note: only after merge completes we will update
	// the new table checkpoint in table state. this means we must defer
	// wal gc until all tables have finalized the next merge.
	for _, t := range e.tables.Map() {
		if t.State().Checkpoint < startSize-5*segSize {
			if err := t.Checkpoint(ctx); err != nil {
				return fmt.Errorf("checkpoint table %s: %v", t.Schema().Name, err)
			}
		}
	}

	// sync WAL again for table checkpoints to be safe
	if err := e.wal.Sync(); err != nil {
		return fmt.Errorf("sync wal: %v", err)
	}

	// wait for tables to merge
	//
	// Two issues are to consider here:
	//
	// - There is no definitive signal about when all merges have finished.
	//   We could use task completion status, but it seems easier to just poll
	//   from time to time to see if tables have updated their state.
	// - High througput tables will merge often and may merge faster than this
	//   wait loop. In this case we will again end up with a large gap between
	//   low and high throughput table checkpoints in the WAL which means
	//   slower startup/crash-recovery and more on-disk space used.
	//
	// To balance both situations we wait only as long as either
	// - all tables have merged
	// - the WAL has grown more than one segment
	//
	e.log.Debug("gc: checkpoints synced, waiting for merge tasks to finish")
	var lsn wal.LSN
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}

		// read watermark and wal size again
		lsn = e.Watermark()
		sz := e.wal.Next()

		// stop wait when the WAL grew more than one segment
		if sz-startSize > segSize {
			e.log.Debug("gc: WAL grows fast, stop wait before all tables have merged")
			break
		}

		// stop when current watermark has crossed the start watermark
		if lsn >= startLsn {
			e.log.Debug("gc: stop wait, all tables have merged")
			break
		}
	}

	// run WAL GC
	e.log.Debugf("gc: drop wal segments before LSN 0x%016x", lsn)
	if err := e.wal.GC(lsn); err != nil {
		e.log.Errorf("gc: %v", err)
		return err
	}
	return nil
}
