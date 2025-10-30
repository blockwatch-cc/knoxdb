// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack/journal"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/store"
	"github.com/echa/log"
)

var _ engine.TableEngine = (*Table)(nil)

func init() {
	engine.RegisterTableFactory(engine.TableKindPack, NewTable)
	engine.RegisterTableFactory(engine.TableKindHistory, NewTable)
}

var (
	DefaultTableOptions = engine.TableOptions{
		Driver:          "bolt",
		NoSync:          true,
		ReadOnly:        false,
		PageSize:        1 << 16, // 64kB
		PageFill:        1.0,     // append only
		PackSize:        1 << 14, // 16k
		JournalSize:     1 << 15, // 32k
		JournalSegments: 16,      // max 256 (unused)
		TxMaxSize:       1 << 24, // 16 MB,
		Logger:          log.Disabled,
	}

	DefaultHistoryOptions = engine.TableOptions{
		Driver:    "bolt",
		NoSync:    true,
		ReadOnly:  false,
		PageSize:  1 << 16, // 64kB
		PageFill:  1.0,     // append only
		PackSize:  1 << 14, // 16k
		TxMaxSize: 1 << 24, // 16 MB,
		Logger:    log.Disabled,
	}
)

type Table struct {
	mu      sync.RWMutex            // global table lock (syncs r/w access, single writer)
	engine  *engine.Engine          // engine access
	schema  *schema.Schema          // ordered list of table fields as central type info
	opts    engine.TableOptions     // copy of config options
	id      uint64                  // unique table id (tagged name hash)
	px      int                     // field index for primary key (required)
	db      store.DB                // lower-level storage (e.g. boltdb wrapper)
	state   engine.ObjectState      // volatile state
	indexes []engine.QueryableIndex // list of indexes
	stats   *stats.AtomicPointer    // in-memory metadata
	journal *journal.Journal        // in-memory data not yet written to packs
	metrics engine.TableMetrics     // usage statistics
	log     log.Logger
}

func NewTable() engine.TableEngine {
	return &Table{}
}

func (t *Table) Schema() *schema.Schema {
	return t.schema
}

func (t *Table) State() engine.ObjectState {
	return t.journal.State()
}

func (t *Table) IsReadOnly() bool {
	return t.opts.ReadOnly
}

func (t *Table) Indexes() []engine.QueryableIndex {
	return t.indexes
}

func (t *Table) PkIndex() (engine.QueryableIndex, bool) {
	for _, idx := range t.indexes {
		if idx.IsPk() {
			return idx, true
		}
	}
	return nil, false
}

// main and history tables use different setups
func mergeDefaultOptions(opts engine.TableOptions) engine.TableOptions {
	if opts.Engine == engine.TableKindHistory {
		opts = DefaultHistoryOptions.Merge(opts)
	} else {
		opts = DefaultTableOptions.Merge(opts)
	}
	return opts
}

func (t *Table) Create(ctx context.Context, s *schema.Schema, opts engine.TableOptions) error {
	// setup table
	t.engine = engine.GetEngine(ctx)
	t.schema = s
	t.id = s.TaggedHash(types.ObjectTagTable)
	t.px = s.PkIndex()
	t.opts = mergeDefaultOptions(opts)
	t.state = engine.NewObjectState(s.Name)
	t.metrics = engine.NewTableMetrics(s.Name)
	t.log = t.opts.Logger.WithTag(fmt.Sprintf("table[%s]:", s.Name))

	// write initial checkpoint
	lsn, err := t.engine.Wal().Write(&wal.Record{
		Type:   wal.RecordTypeCheckpoint,
		Tag:    types.ObjectTagTable,
		Entity: t.id,
	})
	if err != nil {
		return err
	}
	t.state.Checkpoint = lsn

	// create db backend and store initial state
	if err := t.createBackend(ctx); err != nil {
		return err
	}

	t.log.Debug("backend successfully created")
	return nil
}

func (t *Table) createBackend(ctx context.Context) error {
	// setup backend db file
	name := t.schema.Name
	path := filepath.Join(t.engine.RootPath(), name)
	t.log.Debugf("creating backend=%s path=%s opts=%#v", t.opts.Engine, path, t.opts)

	opts := append(
		t.opts.StoreOptions(),
		store.WithLogger(t.log),
		store.WithPath(path),
		store.WithManifest(
			store.NewManifest(
				name,
				t.engine.Namespace()+"."+t.schema.Label(),
			),
		),
	)
	db, err := store.Create(opts...)
	if err != nil {
		return fmt.Errorf("creating table %s: %v", name, err)
	}
	t.db = db

	// init table storage
	tx, err := t.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, v := range [][]byte{
		engine.DataKeySuffix,
		engine.StateKeySuffix,
	} {
		key := append([]byte(name), v...)
		if _, err := tx.Root().CreateBucket(key); err != nil {
			if errors.Is(err, store.ErrBucketExists) {
				return engine.ErrTableExists
			}
			return err
		}
	}

	// init statistics index and storage
	sx := stats.NewIndex().
		WithDB(t.db).
		WithTable(t).
		WithEpoch(uint32(t.state.Epoch)).
		WithSchema(t.schema).
		WithMaxSize(t.opts.PackSize).
		WithLogger(t.log)
	if err := sx.Store(ctx, tx); err != nil {
		return err
	}
	t.stats = sx.AtomicPtr()

	// init and store table state
	if err := t.state.Store(ctx, tx); err != nil {
		return err
	}

	// setup journal (note: history tables have no journal)
	if t.opts.JournalSize*t.opts.JournalSegments > 0 {
		t.journal = journal.NewJournal(t.schema, t.opts.JournalSize, t.opts.JournalSegments).
			WithState(t.state).
			WithWal(t.engine.Wal()).
			WithLogger(t.log)
	}

	// commit backend tx
	return tx.Commit()
}

func (t *Table) Open(ctx context.Context, s *schema.Schema, opts engine.TableOptions) error {
	// setup table
	t.engine = engine.GetEngine(ctx)
	t.schema = s
	t.id = s.TaggedHash(types.ObjectTagTable)
	t.px = s.PkIndex()
	t.opts = mergeDefaultOptions(opts)
	t.state = engine.NewObjectState(s.Name)
	t.metrics = engine.NewTableMetrics(s.Name)
	t.log = t.opts.Logger.WithTag(fmt.Sprintf("table[%s]:", s.Name))

	// open db backend and load latest state
	if err := t.openBackend(ctx); err != nil {
		t.log.Errorf("open: %v", err)
		_ = t.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}

	// cleanup after crash
	if !t.IsReadOnly() && !t.stats.Get().IsClean() {
		t.db.Update(func(tx store.Tx) error {
			return t.stats.Get().CleanupEpochs(tx)
		})
	}

	// setup journal and replay wal
	if t.opts.JournalSize*t.opts.JournalSegments > 0 {
		t.journal = journal.NewJournal(s, t.opts.JournalSize, t.opts.JournalSegments).
			WithWal(t.engine.Wal()).
			WithState(t.state).
			WithLogger(t.log)

		// replay wal from latest checkpoint
		if err := t.ReplayWal(ctx); err != nil {
			// t.log.Errorf("replay wal: %v", err)
			// if err2 := t.Close(ctx); err2 != nil {
			// 	t.log.Errorf("close: %v", err2)
			// }
			return fmt.Errorf("replay wal: %v", err)
		}
		state := t.journal.State()
		t.log.Debugf("opened with rows=%d, %d/%d journal entries, rid=%d pk=%d",
			state.NRows, t.journal.NumTuples(), t.journal.NumTombstones(),
			state.NextRid, state.NextPk)
	} else {
		t.log.Debugf("opened with rows=%d rid=%d pk=%d",
			t.state.NRows, t.state.NextRid, t.state.NextPk)
	}

	return nil
}

func (t *Table) openBackend(ctx context.Context) error {
	name := t.schema.Name
	path := filepath.Join(t.engine.RootPath(), name)
	t.log.Debugf("open backend=%s path=%s opts=%#v", t.opts.Engine, path, t.opts)
	opts := append(
		t.opts.StoreOptions(),
		store.WithLogger(t.log),
		store.WithPath(path),
		store.WithManifest(
			store.NewManifest(
				name,
				t.engine.Namespace()+"."+t.schema.Label(),
			),
		),
	)
	db, err := store.Open(opts...)
	if err != nil {
		return err
	}
	t.db = db

	// load table state
	err = t.db.View(func(tx store.Tx) error {
		// check table storage
		for _, v := range [][]byte{
			engine.DataKeySuffix,
			engine.StateKeySuffix,
		} {
			key := append([]byte(name), v...)
			if tx.Bucket(key) == nil {
				return fmt.Errorf("bucket %s: %v", string(key), store.ErrBucketNotFound)
			}
		}

		// load state
		if err := t.state.Load(ctx, tx); err != nil {
			return fmt.Errorf("loading state: %v", err)
		}

		t.log.Debugf("state pk=%d rid=%d nrows=%d epoch=%d lsn=0x%x",
			t.state.NextPk, t.state.NextRid, t.state.NRows,
			t.state.Epoch, t.state.Checkpoint)

		// init statistics index
		sx := stats.NewIndex().
			WithDB(t.db).
			WithTable(t).
			WithEpoch(uint32(t.state.Epoch)).
			WithSchema(t.schema).
			WithMaxSize(t.opts.PackSize).
			WithLogger(t.log)

		// load statistics
		if err := sx.Load(ctx, tx); err != nil {
			return fmt.Errorf("loading statistics: %v", err)
		}

		// wrap as atomic pointer
		t.stats = sx.AtomicPtr()

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (t *Table) Close(ctx context.Context) (err error) {
	if t.db != nil {
		t.log.Debug("closing")
		err = t.db.Close()
		t.db = nil
	}
	if t.journal != nil {
		t.journal.Close()
		t.journal = nil
	}
	t.engine = nil
	t.schema = nil
	t.id = 0
	t.px = 0
	t.opts = engine.TableOptions{}
	t.metrics = engine.TableMetrics{}
	t.state.Reset()
	t.indexes = nil
	if t.stats != nil {
		t.stats.Get().Close()
		t.stats = nil
	}
	return
}

func (t *Table) Metrics() engine.TableMetrics {
	m := t.metrics
	s := t.stats.Retain()
	m.PacksCount = int64(s.Len())
	m.MetaSize = int64(s.HeapSize())
	m.TotalSize = int64(s.TableSize())
	m.MetaBytesRead, m.MetaBytesWritten = s.Metrics()
	s.Release(false)

	m.TupleCount = int64(t.journal.Tip().State().NRows)
	m.JournalSize = int64(t.journal.Size())
	m.JournalSegments = int64(t.journal.NumSegments())
	m.JournalCapacity = int64(t.opts.JournalSize)
	m.JournalTuples = int64(t.journal.NumTuples())
	m.JournalTombstones = int64(t.journal.NumTombstones())
	return m
}

func (t *Table) Drop(ctx context.Context) error {
	drv, path := t.opts.Driver, t.db.Path()
	if err := t.Close(ctx); err != nil {
		return err
	}
	t.log.Debugf("dropping path=%s", path)
	return store.Drop(drv, path)
}

func (t *Table) Sync(ctx context.Context) error {
	return t.db.Sync()
}

func (t *Table) Truncate(ctx context.Context) error {
	// lock journal access
	t.mu.Lock()
	defer t.mu.Unlock()

	// write storage
	err := t.db.Update(func(tx store.Tx) error {
		if err := t.stats.Get().Drop(ctx, tx); err != nil {
			return err
		}
		t.journal.Reset()
		for _, v := range [][]byte{
			engine.DataKeySuffix,
			engine.StateKeySuffix,
		} {
			key := append([]byte(t.schema.Name), v...)
			if err := tx.Root().DeleteBucket(key); err != nil {
				return err
			}
			if _, err := tx.Root().CreateBucket(key); err != nil {
				return err
			}
		}

		// write wal checkpoint
		lsn, err := t.engine.Wal().Write(&wal.Record{
			Type:   wal.RecordTypeCheckpoint,
			Tag:    types.ObjectTagTable,
			Entity: t.id,
		})
		if err != nil {
			return err
		}

		// reset state
		t.state.Reset()
		t.state.Checkpoint = lsn
		t.journal.WithState(t.state)
		return t.state.Store(ctx, tx)
	})
	if err != nil {
		return err
	}

	// update metrics
	atomic.AddInt64(&t.metrics.DeletedTuples, int64(t.state.NRows))
	atomic.StoreInt64(&t.metrics.TupleCount, 0)
	atomic.StoreInt64(&t.metrics.MetaSize, 0)
	atomic.StoreInt64(&t.metrics.PacksCount, 0)
	return nil
}

func (t *Table) ConnectIndex(idx engine.QueryableIndex) {
	t.indexes = append(t.indexes, idx)
}

func (t *Table) DisconnectIndex(idx engine.QueryableIndex) {
	idxId := idx.Schema().TaggedHash(types.ObjectTagIndex)
	t.indexes = slices.DeleteFunc(t.indexes, func(v engine.QueryableIndex) bool {
		return v.Schema().TaggedHash(types.ObjectTagIndex) == idxId
	})
}

func (t *Table) CommitTx(ctx context.Context, xid types.XID) error {
	// lock journal access
	t.mu.Lock()
	defer t.mu.Unlock()
	canMerge := t.journal.CommitTx(xid)
	if canMerge {
		// cascading merge calls on high tx volume are scheduled, but may
		// bail out when segment merge takes too long
		t.log.Debug("scheduling merge task")
		ok := t.engine.Schedule(engine.NewTask(t.Merge))
		if !ok {
			t.log.Warn("merge task queue full")
		}
	}
	return nil
}

func (t *Table) AbortTx(ctx context.Context, xid types.XID) error {
	// lock journal access
	t.mu.Lock()
	defer t.mu.Unlock()
	canMerge := t.journal.AbortTx(xid)
	if canMerge {
		// cascading merge calls on high tx volume are scheduled, but may
		// bail out when segment merge takes too long
		t.log.Debug("scheduling merge task")
		ok := t.engine.Schedule(engine.NewTask(t.Merge))
		if !ok {
			t.log.Warn("merge task queue full")
		}
	}
	return nil
}

// Checkpoint journal. Rotates the active segment and writes
// new table checkpoint to WAL. This may be called concurrently to
// queries and writer calls by a background worker to advance WAL LSNs
// across tables. After writing a new WAL checkpoint this function
// schedules a merge call which is required to push the new table
// checkpoint to disk.
func (t *Table) Checkpoint(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.journal.Checkpoint(ctx); err != nil {
		return err
	}
	// schedule merge task to make new checkpoint durable
	t.engine.Schedule(engine.NewTask(t.Merge))
	return nil
}

func (t *Table) dataBucket(tx store.Tx) store.Bucket {
	key := append([]byte(t.schema.Name), engine.DataKeySuffix...)
	b := tx.Bucket(key)
	if b != nil {
		b.FillPercent(t.opts.PageFill)
	}
	return b
}
