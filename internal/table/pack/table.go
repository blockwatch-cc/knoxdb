// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/journal"
	"blockwatch.cc/knoxdb/internal/metadata"
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

type TableState struct {
	Sequence uint64 // next free sequence
	Rows     uint64 // total non-deleted rows
}

var (
	metaKeySuffix = []byte("_meta")
	dataKeySuffix = []byte("_data")
)

type Table struct {
	mu      sync.RWMutex            // global table lock (syncs r/w access, single writer)
	engine  *engine.Engine          // engine access
	schema  *schema.Schema          // ordered list of table fields as central type info
	tableId uint64                  // unique tagged name hash
	pkindex int                     // field index for primary key (if any)
	opts    engine.TableOptions     // copy of config options
	db      store.DB                // lower-level storage (e.g. boltdb wrapper)
	datakey []byte                  // name of table data bucket
	metakey []byte                  // name of table metadata bucket
	state   TableState              // volatile state, synced with catalog
	indexes []engine.IndexEngine    // list of indexes
	meta    *metadata.MetadataIndex // in-memory list of pack and block info
	journal *journal.Journal        // in-memory data not yet written to packs
	stats   engine.TableStats       // usage statistics
	log     log.Logger
}

func NewTable() engine.TableEngine {
	return &Table{}
}

func (t *Table) Create(ctx context.Context, s *schema.Schema, opts engine.TableOptions) error {
	// require primary key
	pki := s.PkIndex()
	if pki < 0 {
		return engine.ErrNoPk
	}

	e := engine.GetTransaction(ctx).Engine()

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// setup store
	t.engine = e
	t.schema = s
	t.tableId = s.TaggedHash(types.HashTagTable)
	t.pkindex = pki
	t.opts = DefaultTableOptions.Merge(opts)
	t.datakey = append([]byte(name), dataKeySuffix...)
	t.metakey = append([]byte(name), metaKeySuffix...)
	t.state.Sequence = 1
	t.stats.Name = name
	t.meta = metadata.NewMetadataIndex(pki, opts.PackSize)
	t.journal = journal.NewJournal(s, opts.JournalSize)
	t.db = opts.DB
	t.log = opts.Logger

	// create db if not passed in options
	if t.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		t.log.Debugf("Creating pack table %q with opts %#v", path, opts)
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

	// init table storage
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}
	if _, err := store.CreateBucket(tx, t.datakey, engine.ErrTableExists); err != nil {
		return err
	}
	if _, err := store.CreateBucket(tx, t.metakey, engine.ErrTableExists); err != nil {
		return err
	}
	jsz, tsz, err := t.journal.StoreLegacy(ctx, tx, t.datakey)
	if err != nil {
		return err
	}
	t.stats.JournalDiskSize = int64(jsz)
	t.stats.TombstoneDiskSize = int64(tsz)
	t.stats.JournalTuplesThreshold = int64(opts.JournalSize)
	t.stats.TombstoneTuplesThreshold = int64(opts.JournalSize)

	// init catalog state
	t.engine.Catalog().SetState(t.tableId, 1, 0)

	t.log.Debugf("Created table %s", typ)
	return nil
}

func (t *Table) Open(ctx context.Context, s *schema.Schema, opts engine.TableOptions) error {
	e := engine.GetTransaction(ctx).Engine()

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// setup table
	t.engine = e
	t.schema = s
	t.tableId = s.TaggedHash(types.HashTagTable)
	t.pkindex = s.PkIndex()
	t.opts = DefaultTableOptions.Merge(opts)
	t.datakey = append([]byte(name), dataKeySuffix...)
	t.metakey = append([]byte(name), metaKeySuffix...)
	t.state.Sequence, t.state.Rows = e.Catalog().GetState(t.tableId)
	t.stats.Name = name
	t.stats.TupleCount = int64(t.state.Rows)
	t.meta = metadata.NewMetadataIndex(s.PkIndex(), opts.PackSize)
	t.journal = journal.NewJournal(s, opts.JournalSize)
	t.db = opts.DB
	t.log = opts.Logger

	// open db if not passed in options
	if t.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		t.log.Debugf("Opening pack table %q with opts %#v", path, opts)
		db, err := store.Open(t.opts.Driver, path, t.opts.ToDriverOpts())
		if err != nil {
			t.log.Errorf("opening table %s: %v", typ, err)
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

	// check table storage
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return err
	}
	if tx.Bucket(t.datakey) == nil || tx.Bucket(t.metakey) == nil {
		t.log.Error("missing table data: %v", engine.ErrNoBucket)
		tx.Rollback()
		t.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}

	// TODO: maybe refactor

	// load metadata
	t.log.Debugf("Loading package metadata for %s", typ)
	n, err := t.meta.Load(ctx, tx, t.metakey)
	if err != nil {
		// TODO: rebuild corrupt metadata here
		return err
	}
	t.stats.MetaBytesRead += int64(n)
	t.stats.TupleCount = int64(t.state.Rows)
	t.stats.PacksCount = int64(t.meta.Len())
	t.stats.MetaSize = int64(t.meta.HeapSize())
	t.stats.TotalSize = int64(t.meta.TableSize())

	// FIXME: reconstruct journal from WAL instead of load in legacy mode
	err = t.journal.Open(ctx, tx, t.datakey)
	if err != nil {
		return fmt.Errorf("Open journal for table %s: %v", typ, err)
	}

	t.log.Debugf("Table %s opened with %d rows, %d journal rows, seq=%d",
		typ, t.state.Rows, t.journal.Len(), t.state.Sequence)

	// t.DumpType(os.Stdout)
	// t.DumpMetadata(os.Stdout, types.DumpModeHex)
	// t.DumpMetadataDetail(os.Stdout, types.DumpModeHex)

	return nil
}

func (t *Table) Close(ctx context.Context) (err error) {
	if t.db != nil {
		t.log.Debugf("Closing table %s", t.schema.TypeLabel(t.engine.Namespace()))
		err = t.db.Close()
		t.db = nil
	}
	t.engine = nil
	t.schema = nil
	t.tableId = 0
	t.pkindex = 0
	t.datakey = nil
	t.metakey = nil
	t.opts = engine.TableOptions{}
	t.stats = engine.TableStats{}
	t.state = TableState{}
	t.indexes = nil
	t.meta.Reset()
	t.meta = nil
	t.journal.Close()
	t.journal = nil
	return
}

func (t *Table) Schema() *schema.Schema {
	return t.schema
}

func (t *Table) Indexes() []engine.IndexEngine {
	return t.indexes
}

func (t *Table) name() string {
	return t.schema.Name()
}

func (t *Table) Stats() engine.TableStats {
	stats := t.stats
	stats.TupleCount = int64(t.state.Rows)
	return stats
}

func (t *Table) Drop(ctx context.Context) error {
	typ := t.schema.TypeLabel(t.engine.Namespace())
	path := t.db.Path()
	t.journal.Close()
	t.db.Close()
	t.db = nil
	t.meta.Reset()
	t.log.Debugf("dropping table %s with path %s", typ, path)
	if err := os.Remove(path); err != nil {
		return err
	}
	return nil
}

func (t *Table) Sync(ctx context.Context) error {
	// FIXME: refactor legacy
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}

	// store journal
	if err := t.storeJournal(ctx); err != nil {
		return err
	}

	// store metadata
	n, err := t.meta.Store(ctx, tx, t.metakey, t.opts.PageFill)
	if err != nil {
		return err
	}
	atomic.AddInt64(&t.stats.MetaBytesWritten, int64(n))
	atomic.StoreInt64(&t.stats.PacksCount, int64(t.meta.Len()))
	atomic.StoreInt64(&t.stats.MetaSize, int64(t.meta.HeapSize()))

	return nil
}

func (t *Table) Truncate(ctx context.Context) error {
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}
	t.journal.Reset()
	t.meta.Reset()
	for _, key := range [][]byte{t.datakey, t.metakey} {
		if err := tx.Root().DeleteBucket(key); err != nil {
			return err
		}
		if _, err := tx.Root().CreateBucket(key); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	t.engine.Catalog().SetState(t.tableId, 1, 0)
	t.state.Rows = 0
	t.state.Sequence = 1
	atomic.AddInt64(&t.stats.DeletedTuples, int64(t.state.Rows))
	atomic.StoreInt64(&t.stats.TupleCount, 0)
	atomic.StoreInt64(&t.stats.MetaSize, 0)
	atomic.StoreInt64(&t.stats.JournalSize, 0)
	atomic.StoreInt64(&t.stats.TombstoneDiskSize, 0)
	atomic.StoreInt64(&t.stats.PacksCount, 0)
	return nil
}

func (t *Table) UseIndex(idx engine.IndexEngine) {
	t.indexes = append(t.indexes, idx)
}

func (t *Table) UnuseIndex(idx engine.IndexEngine) {
	idxId := idx.Schema().TaggedHash(types.HashTagIndex)
	t.indexes = slices.DeleteFunc(t.indexes, func(v engine.IndexEngine) bool {
		return v.Schema().TaggedHash(types.HashTagIndex) == idxId
	})
}
