// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack/journal"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

var _ engine.TableEngine = (*Table)(nil)

func init() {
	engine.RegisterTableFactory(engine.TableKindPack, NewTable)
}

var (
	DefaultTableOptions = engine.TableOptions{
		Driver:      "bolt",
		PackSize:    1 << 16, // 64k
		JournalSize: 1 << 17, // 128k
		PageSize:    1 << 16, // 64kB
		PageFill:    0.9,
		TxMaxSize:   1 << 24, // 16 MB,
		ReadOnly:    false,
		NoSync:      false,
		NoGrowSync:  false,
		Logger:      log.Disabled,
	}
)

type Table struct {
	mu      sync.RWMutex            // global table lock (syncs r/w access, single writer)
	engine  *engine.Engine          // engine access
	schema  *schema.Schema          // ordered list of table fields as central type info
	id      uint64                  // unique tagged name hash
	px      int                     // field index for primary key (if any)
	opts    engine.TableOptions     // copy of config options
	db      store.DB                // lower-level storage (e.g. boltdb wrapper)
	state   engine.ObjectState      // volatile state
	indexes []engine.QueryableIndex // list of indexes
	stats   *stats.Index            // in-memory table statistics
	journal *journal.Journal        // in-memory data not yet written to packs
	metrics engine.TableMetrics     // metrics statistics
	log     log.Logger
}

func NewTable() engine.TableEngine {
	return &Table{}
}

func (t *Table) Create(ctx context.Context, s *schema.Schema, opts engine.TableOptions) error {
	e := engine.GetTransaction(ctx).Engine()

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// setup store
	t.engine = e
	t.schema = s
	t.id = s.TaggedHash(types.ObjectTagTable)
	t.px = s.PkIndex()
	t.opts = DefaultTableOptions.Merge(opts)
	t.state = engine.NewObjectState([]byte(name))
	t.metrics = engine.NewTableMetrics(name)
	t.journal = journal.NewJournal(s, t.opts.JournalSize)
	t.db = opts.DB
	t.log = opts.Logger

	// create db if not passed in options
	if t.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		t.log.Debugf("Creating pack table %q with opts %#v", path, t.opts)
		db, err := store.Create(t.opts.Driver, path, t.opts.ToDriverOpts())
		if err != nil {
			return fmt.Errorf("creating table %s: %v", typ, err)
		}
		err = db.SetManifest(store.Manifest{
			Name:    name,
			Schema:  typ,
			Version: int(s.Version()),
		})
		if err != nil {
			db.Close()
			return err
		}
		t.db = db
	}

	// init statistics
	t.stats = stats.NewIndex(t.db, t.schema, t.opts.PackSize)

	// init table storage
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}
	for _, v := range [][]byte{
		engine.DataKeySuffix,
		engine.StateKeySuffix,
	} {
		key := append([]byte(name), v...)
		if _, err := store.CreateBucket(tx, key, engine.ErrTableExists); err != nil {
			return err
		}
	}

	// create stats
	if err := t.stats.Store(ctx, tx); err != nil {
		return err
	}

	// TODO: replace with WAL stream
	jsz, tsz, err := t.journal.StoreLegacy(ctx, tx, t.schema.Name())
	if err != nil {
		return err
	}
	t.metrics.JournalDiskSize = int64(jsz)
	t.metrics.TombstoneDiskSize = int64(tsz)
	t.metrics.JournalTuplesThreshold = int64(opts.JournalSize)
	t.metrics.TombstoneTuplesThreshold = int64(opts.JournalSize)

	// init state storage
	if err := t.state.Store(ctx, tx); err != nil {
		return err
	}

	t.log.Debugf("Created table %s", typ)
	return nil
}

func (t *Table) Open(ctx context.Context, s *schema.Schema, opts engine.TableOptions) error {
	e := engine.GetEngine(ctx)

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// setup table
	t.engine = e
	t.schema = s
	t.id = s.TaggedHash(types.ObjectTagTable)
	t.px = s.PkIndex()
	t.opts = DefaultTableOptions.Merge(opts)
	t.state = engine.NewObjectState([]byte(name))
	t.metrics = engine.NewTableMetrics(name)
	t.journal = journal.NewJournal(s, t.opts.JournalSize)
	t.db = opts.DB
	t.log = opts.Logger

	// open db if not passed in options
	if t.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		t.log.Debugf("Opening pack table %q with opts %#v", path, t.opts)
		db, err := store.Open(t.opts.Driver, path, t.opts.ToDriverOpts())
		if err != nil {
			t.log.Errorf("open table %s: %v", typ, err)
			return engine.ErrNoTable
		}
		t.db = db

		// check manifest matches
		mft, err := t.db.Manifest()
		if err != nil {
			t.log.Errorf("missing manifest: %v", err)
			_ = t.Close(ctx)
			return engine.ErrDatabaseCorrupt
		}
		err = mft.Validate(name, "*", typ, -1)
		if err != nil {
			t.log.Errorf("schema mismatch: %v", err)
			_ = t.Close(ctx)
			return schema.ErrSchemaMismatch
		}
	}

	// init statistics
	t.stats = stats.NewIndex(t.db, t.schema, t.opts.PackSize)

	// check table storage
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return err
	}
	for _, v := range [][]byte{
		engine.DataKeySuffix,
		engine.StateKeySuffix,
	} {
		key := append([]byte(name), v...)
		if tx.Bucket(key) == nil {
			t.log.Errorf("open %s: %v", engine.ErrNoBucket, string(key))
			tx.Rollback()
			t.Close(ctx)
			return engine.ErrDatabaseCorrupt
		}
	}

	// TODO: maybe refactor

	// load state
	if err := t.state.Load(ctx, tx); err != nil {
		t.log.Errorf("open state: %v", err)
		tx.Rollback()
		t.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}
	t.metrics.TupleCount = int64(t.state.NRows)

	// load stats
	t.log.Debugf("Loading statistics for %s", typ)
	if err := t.stats.Load(ctx, tx); err != nil {
		// TODO: rebuild corrupt stats here instead of failing
		tx.Rollback()
		t.Close(ctx)
		return fmt.Errorf("open stats: %v", err)
	}

	// FIXME: reconstruct journal from WAL instead of load in legacy mode
	err = t.journal.Open(ctx, tx, t.schema.Name())
	if err != nil {
		tx.Rollback()
		t.Close(ctx)
		return fmt.Errorf("open journal: %v", err)
	}

	t.log.Debugf("Table %s opened with %d rows, %d journal rows, seq=%d",
		typ, t.state.NRows, t.journal.Len(), t.state.Sequence)

	return nil
}

func (t *Table) Close(ctx context.Context) (err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.db != nil {
		t.log.Debugf("Closing table %s", t.schema.TypeLabel(t.engine.Namespace()))
		err = t.db.Close()
		t.db = nil
	}
	t.engine = nil
	t.schema = nil
	t.id = 0
	t.px = 0
	t.opts = engine.TableOptions{}
	t.metrics = engine.TableMetrics{}
	t.state.Reset()
	clear(t.indexes)
	t.indexes = t.indexes[:0]
	t.indexes = nil
	t.stats.Close()
	t.stats = nil
	t.journal.Close()
	t.journal = nil
	return
}

func (t *Table) Schema() *schema.Schema {
	return t.schema
}

func (t *Table) State() engine.ObjectState {
	return t.state
}

func (t *Table) Indexes() []engine.QueryableIndex {
	return t.indexes
}

func (t *Table) Metrics() engine.TableMetrics {
	m := t.metrics
	s := t.stats
	m.TupleCount = int64(t.state.NRows)
	m.PacksCount = int64(s.Len())
	m.MetaSize = int64(s.HeapSize())
	m.TotalSize = int64(s.TableSize())
	m.MetaBytesRead, m.MetaBytesWritten = s.Metrics()
	return m
}

func (t *Table) Drop(ctx context.Context) error {
	typ := t.schema.TypeLabel(t.engine.Namespace())
	path := t.db.Path()
	clear(t.indexes)
	t.indexes = t.indexes[:0]
	t.journal.Close()
	t.stats.Close()
	t.db.Close()
	t.db = nil
	t.log.Debugf("dropping table %s with path %s", typ, path)
	return store.Drop(t.opts.Driver, path)
}

func (t *Table) Sync(ctx context.Context) error {
	// FIXME: refactor legacy journal

	// lock table journal
	t.mu.Lock()
	defer t.mu.Unlock()

	// use db write transaction
	return t.db.Update(func(tx store.Tx) error {
		// store journal
		if err := t.storeJournal(ctx, tx); err != nil {
			return err
		}

		// store state
		if err := t.state.Store(ctx, tx); err != nil {
			return err
		}

		// store stats
		if err := t.stats.Store(ctx, tx); err != nil {
			return err
		}

		return nil
	})
}

func (t *Table) Truncate(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}
	if err := t.stats.Delete(ctx, tx); err != nil {
		return err
	}
	t.journal.Reset()
	for _, v := range [][]byte{
		engine.DataKeySuffix,
		engine.StateKeySuffix,
	} {
		key := append([]byte(t.schema.Name()), v...)
		if err := tx.Root().DeleteBucket(key); err != nil {
			return err
		}
		if _, err := tx.Root().CreateBucket(key); err != nil {
			return err
		}
	}
	nDel := t.state.NRows
	t.state.Reset()
	if err := t.state.Store(ctx, tx); err != nil {
		return err
	}
	atomic.AddInt64(&t.metrics.DeletedTuples, int64(nDel))
	atomic.StoreInt64(&t.metrics.TupleCount, 0)
	atomic.StoreInt64(&t.metrics.MetaSize, 0)
	atomic.StoreInt64(&t.metrics.JournalSize, 0)
	atomic.StoreInt64(&t.metrics.TombstoneDiskSize, 0)
	atomic.StoreInt64(&t.metrics.PacksCount, 0)
	return nil
}

func (t *Table) UseIndex(idx engine.QueryableIndex) {
	t.indexes = append(t.indexes, idx)
}

func (t *Table) UnuseIndex(idx engine.QueryableIndex) {
	idxId := idx.Schema().TaggedHash(types.ObjectTagIndex)
	t.indexes = slices.DeleteFunc(t.indexes, func(v engine.QueryableIndex) bool {
		return v.Schema().TaggedHash(types.ObjectTagIndex) == idxId
	})
}

func (t *Table) CommitTx(_ context.Context, _ uint64) error {
	return nil
}

func (t *Table) AbortTx(_ context.Context, _ uint64) error {
	return nil
}
