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
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
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
		PackSize:        1 << 14, // 16k
		JournalSize:     1 << 15, // 32k
		JournalSegments: 16,      // max 256 (unused)
		PageSize:        1 << 16, // 64kB
		PageFill:        1.0,     // append only
		TxMaxSize:       1 << 24, // 16 MB,
		ReadOnly:        false,
		NoSync:          false,
		NoGrowSync:      false,
		Logger:          log.Disabled,
	}

	DefaultHistoryOptions = engine.TableOptions{
		Driver:     "bolt",
		PackSize:   1 << 14, // 16k
		PageSize:   1 << 16, // 64kB
		PageFill:   1.0,     // append only
		TxMaxSize:  1 << 24, // 16 MB,
		ReadOnly:   false,
		NoSync:     false,
		NoGrowSync: false,
		Logger:     log.Disabled,
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
	stats   atomic.Value            // in-memory list of pack and block info
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
	return t.state
}

func (t *Table) Indexes() []engine.QueryableIndex {
	return t.indexes
}

// main and history tables use different setups
func validateSchemaAndOptions(s *schema.Schema, opts engine.TableOptions) (*schema.Schema, engine.TableOptions, error) {
	if opts.Engine == engine.TableKindHistory {
		// ensure options
		opts = DefaultHistoryOptions.Merge(opts)

		// check history schema (schema must have meta columns enabled and RID must be PK)
		if !s.HasMeta() {
			s, _ = s.WithMeta().ResetPk(schema.MetaRid)
		}
		if s.Pk().Id() != schema.MetaRid {
			return nil, opts, fmt.Errorf("invalid pk %q on history table %q", s.Pk().Name(), s.Name())
		}

	} else {
		// ensure options
		opts = DefaultTableOptions.Merge(opts)

		// check history schema (schema must have meta columns enabled, must have PK)
		if !s.HasMeta() {
			s = s.WithMeta()
		}

		// ensure we have a pk field, use RID when missing
		if s.PkId() == 0 {
			s.ResetPk(schema.MetaRid)
		}
	}

	return s, opts, nil
}

func (t *Table) Create(ctx context.Context, s *schema.Schema, opts engine.TableOptions) error {
	// validate schema and options
	var err error
	s, opts, err = validateSchemaAndOptions(s, opts)
	if err != nil {
		return err
	}
	name := s.Name()

	// setup table
	t.engine = engine.GetEngine(ctx)
	t.schema = s
	t.id = s.TaggedHash(types.ObjectTagTable)
	t.px = s.PkIndex()
	t.opts = opts
	t.state = engine.NewObjectState(name)
	t.metrics = engine.NewTableMetrics(name)
	t.log = opts.Logger

	// init journal (note: history tables have no journal)
	if t.opts.JournalSize*t.opts.JournalSegments > 0 {
		t.journal = journal.NewJournal(t.schema, t.opts.JournalSize, t.opts.JournalSegments)
	}

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

	t.log.Debugf("Created table %s", name)
	return nil
}

func (t *Table) createBackend(ctx context.Context) error {
	// setup backend db file
	typ := t.schema.TypeLabel(t.engine.Namespace())
	path := filepath.Join(t.engine.RootPath(), t.schema.Name()+".db")
	t.log.Debugf("Creating %s table %q at %q with opts %#v",
		t.opts.Engine, t.schema.Name(), path, t.opts)

	db, err := store.Create(t.opts.Driver, path, t.opts.ToDriverOpts())
	if err != nil {
		return fmt.Errorf("creating table %s: %v", typ, err)
	}
	err = db.SetManifest(store.Manifest{
		Name:    t.schema.Name(),
		Schema:  typ,
		Version: int(t.schema.Version()),
	})
	if err != nil {
		db.Close()
		return err
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
		engine.JournalKeySuffix,
		engine.StateKeySuffix,
	} {
		key := append([]byte(t.schema.Name()), v...)
		if _, err := store.CreateBucket(tx, key, engine.ErrTableExists); err != nil {
			return err
		}
	}

	// init statistics index and storage
	st := stats.NewIndex(t.db, t.schema, t.opts.PackSize)
	if err := st.Store(ctx, tx); err != nil {
		return err
	}
	t.stats.Store(st)

	// init and store table state
	if err := t.state.Store(ctx, tx); err != nil {
		return err
	}

	// commit backend tx
	return tx.Commit()
}

func (t *Table) Open(ctx context.Context, s *schema.Schema, opts engine.TableOptions) error {
	// validate schema and options
	var err error
	s, opts, err = validateSchemaAndOptions(s, opts)
	if err != nil {
		return err
	}
	name := s.Name()

	// setup table
	t.engine = engine.GetEngine(ctx)
	t.schema = s
	t.id = s.TaggedHash(types.ObjectTagTable)
	t.px = s.PkIndex()
	t.opts = opts
	t.state = engine.NewObjectState(name)
	t.metrics = engine.NewTableMetrics(name)
	t.log = opts.Logger.WithTag(name)

	// init journal (note: history tables have no journal)
	if t.opts.JournalSize*t.opts.JournalSegments > 0 {
		t.journal = journal.NewJournal(s, t.opts.JournalSize, t.opts.JournalSegments)
	}

	// open db backend and load latest state
	if err := t.openBackend(ctx); err != nil {
		t.log.Errorf("%s: open table: %v", name, err)
		_ = t.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}

	// replay wal into journal
	if t.journal != nil {
		if err := t.ReplayWal(ctx); err != nil {
			t.log.Errorf("%s: replay wal: %v", name, err)
			_ = t.Close(ctx)
			return engine.ErrDatabaseCorrupt
		}
	}

	t.log.Debugf("Opened table %s with %d rows, %d journal rows, seq=%d",
		name, t.state.NRows, t.journal.Len(), t.state.NextPk)

	return nil
}

func (t *Table) openBackend(ctx context.Context) error {
	// open db
	name := t.schema.Name()
	typ := t.schema.TypeLabel(t.engine.Namespace())
	path := filepath.Join(t.engine.RootPath(), name+".db")
	t.log.Debugf("Opening %s table %q at %q with opts %#v",
		t.opts.Engine, name, path, t.opts)

	db, err := store.Open(t.opts.Driver, path, t.opts.ToDriverOpts())
	if err != nil {
		return fmt.Errorf("open: %v: %v", err, engine.ErrNoTable)
	}
	t.db = db

	// check manifest matches
	mft, err := t.db.Manifest()
	if err != nil {
		return fmt.Errorf("loading manifest: %v", err)
	}
	if err := mft.Validate(name, "*", typ, -1); err != nil {
		return schema.ErrSchemaMismatch
	}

	// load table state
	err = t.db.View(func(tx store.Tx) error {
		// check table storage
		for _, v := range [][]byte{
			engine.DataKeySuffix,
			engine.JournalKeySuffix,
			engine.StateKeySuffix,
		} {
			key := append([]byte(name), v...)
			if tx.Bucket(key) == nil {
				return fmt.Errorf("%q: %v", string(key), store.ErrNoBucket)
			}
		}

		if err := t.state.Load(ctx, tx); err != nil {
			return fmt.Errorf("loading state: %v", err)
		}
		t.metrics.TupleCount = int64(t.state.NRows)

		// init statistics
		t.stats.Store(stats.NewIndex(t.db, t.schema, t.opts.PackSize))

		// load statistics
		if err := t.stats.Load().(*stats.Index).Load(ctx, tx); err != nil {
			return fmt.Errorf("loading statistics: %v", err)
		}

		// load immutable journal segments
		if t.journal != nil {
			if err := t.journal.Load(ctx, tx); err != nil {
				return fmt.Errorf("loading journal: %v", err)
			}
		}

		return nil
	})
	return err
}

func (t *Table) Close(ctx context.Context) (err error) {
	if t.db != nil {
		t.log.Debugf("Closing table %s", t.schema.Name())
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
	t.indexes = nil
	t.stats.Load().(*stats.Index).Close()
	t.stats.Store(&stats.Index{})
	if t.journal != nil {
		t.journal.Close()
		t.journal = nil
	}
	return
}

func (t *Table) Metrics() engine.TableMetrics {
	m := t.metrics
	s := t.stats.Load().(*stats.Index)
	m.TupleCount = int64(t.state.NRows)
	m.PacksCount = int64(s.Len())
	m.MetaSize = int64(s.HeapSize())
	m.TotalSize = int64(s.TableSize())
	m.MetaBytesRead, m.MetaBytesWritten = s.Metrics()
	return m
}

func (t *Table) Drop(ctx context.Context) error {
	path := t.db.Path()
	clear(t.indexes)
	t.indexes = t.indexes[:0]
	if t.journal != nil {
		t.journal.Close()
	}
	t.stats.Load().(*stats.Index).Close()
	t.state.Reset()
	t.db.Close()
	t.db = nil
	t.log.Debugf("Dropping table %s with path %s", t.schema.Name(), path)
	return store.Drop(t.opts.Driver, path)
}

func (t *Table) Sync(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// flush journal data, write wal checkpoint and state
	if err := t.flushJournal(ctx); err != nil {
		return err
	}

	// sync db file
	if err := t.db.Sync(); err != nil {
		return err
	}

	// atomic.AddInt64(&t.stats.MetaBytesWritten, int64(n))
	// atomic.StoreInt64(&t.metrics.PacksCount, int64(t.stats.Len()))
	// atomic.StoreInt64(&t.metrics.MetaSize, int64(t.stats.HeapSize()))

	return nil
}

func (t *Table) Truncate(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}
	if err := t.stats.Load().(*stats.Index).Delete(ctx, tx); err != nil {
		return err
	}
	t.journal.Reset()
	for _, v := range [][]byte{
		engine.DataKeySuffix,
		engine.JournalKeySuffix,
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
	err = t.state.Store(ctx, tx)
	if err != nil {
		return err
	}

	// update metrics
	atomic.AddInt64(&t.metrics.DeletedTuples, int64(t.state.NRows))
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

func (t *Table) CommitTx(ctx context.Context, xid uint64) error {
	// lock journal access
	t.mu.Lock()
	defer t.mu.Unlock()
	t.journal.CommitTx(xid)
	return nil
}

func (t *Table) AbortTx(ctx context.Context, xid uint64) error {
	// lock journal access
	t.mu.Lock()
	defer t.mu.Unlock()
	t.journal.AbortTx(xid)
	return nil
}

func (t *Table) flushJournal(ctx context.Context) error {
	// flush journal contents
	err := t.db.Update(func(tx store.Tx) error {
		return t.journal.Flush(ctx, tx)
	})
	if err != nil {
		return err
	}

	// ensure data is written to disk
	if t.opts.NoSync {
		if err := t.db.Sync(); err != nil {
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

	// update table state
	t.state.Checkpoint = lsn
	err = t.db.Update(func(tx store.Tx) error {
		return t.state.Store(ctx, tx)
	})
	if err != nil {
		return fmt.Errorf("storing state: %v", err)
	}

	return nil
}

func (t *Table) dataBucket(tx store.Tx) store.Bucket {
	key := append([]byte(t.schema.Name()), engine.DataKeySuffix...)
	b := tx.Bucket(key)
	if b != nil {
		b.FillPercent(t.opts.PageFill)
	}
	return b
}
