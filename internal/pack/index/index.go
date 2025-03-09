// Copyright (c) 2024-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

// TODO
// - 'include' extra fields support (load/store/merge more columns)
// - tomb should not have to store extra fields

// This index supports the following condition types on lookup.
// - hash: EQ, IN (single or composite EQ)
// - int:  EQ, LT, LE GT, GE, RG (single condition)

var _ engine.IndexEngine = (*Index)(nil)

// key extraction from wire format is little endian
var LE = binary.LittleEndian

func init() {
	engine.RegisterIndexFactory(engine.IndexKindPack, NewIndex)
}

var (
	DefaultIndexOptions = engine.IndexOptions{
		Driver:      "bolt",
		PackSize:    1 << 11, // 2k
		JournalSize: 1 << 17, // 128k
		PageSize:    1 << 16,
		PageFill:    1.0,
		TxMaxSize:   1 << 20, // 1 MB
		ReadOnly:    false,
		NoSync:      false,
		NoGrowSync:  false,
		Logger:      log.Disabled,
	}
)

type Index struct {
	engine  *engine.Engine      // engine access
	ischema *schema.Schema      // index storage schema [u64, u64, ... extra]
	schema  *schema.Schema      // index source schema [n index cols, rid, extra]
	id      uint64              // unique tagged name hash
	opts    engine.IndexOptions // copy of config options
	table   engine.TableEngine  // related table
	state   engine.ObjectState  // volatile state
	db      store.DB            // storage backend
	journal *pack.Package       // in-memory updates
	tomb    *pack.Package       // in-memory deletes
	convert Converter           // table to index schema converter
	metrics engine.IndexMetrics // usage statistics
	log     log.Logger          // log instance
}

func NewIndex() engine.IndexEngine {
	return &Index{}
}

func (idx *Index) Create(ctx context.Context, t engine.TableEngine, s *schema.Schema, opts engine.IndexOptions) error {
	// require primary key
	if !s.HasMeta() {
		return engine.ErrNoMeta
	}

	// init names
	name := s.Name()
	opts = DefaultIndexOptions.Merge(opts)

	// storage schema depends on index type
	indexSchema, convert, err := convertSchema(s, t.Schema(), opts.Type)
	if err != nil {
		return err
	}

	// setup index
	idx.engine = engine.GetEngine(ctx)
	idx.ischema = indexSchema
	idx.schema = s
	idx.id = s.TaggedHash(types.ObjectTagIndex)
	idx.opts = opts
	idx.table = t
	idx.state = engine.NewObjectState(name)
	idx.journal = pack.New().
		WithMaxRows(opts.JournalSize).
		WithSchema(indexSchema).
		Alloc()
	idx.tomb = pack.New().
		WithMaxRows(opts.JournalSize).
		WithSchema(indexSchema).
		Alloc()
	idx.convert = convert
	idx.metrics = engine.NewIndexMetrics(name)
	idx.log = opts.Logger

	// create backend and store initial state
	if err := idx.createBackend(ctx); err != nil {
		return err
	}

	idx.log.Debugf("Created index %s", name)
	return nil
}

func (idx *Index) createBackend(ctx context.Context) error {
	// setup backend db file
	name := idx.schema.Name()
	typ := idx.schema.TypeLabel(idx.engine.Namespace())
	path := filepath.Join(idx.engine.RootPath(), name+".db")
	idx.log.Debugf("Creating %s index %q on %q at %q with opts %#v",
		idx.opts.Engine, name, idx.table.Schema().Name(), path, idx.opts)

	db, err := store.Create(idx.opts.Driver, path, idx.opts.ToDriverOpts())
	if err != nil {
		return fmt.Errorf("creating index %s: %v", name, err)
	}
	err = db.SetManifest(store.Manifest{
		Name:    name,
		Schema:  typ,
		Version: int(idx.schema.Version()),
	})
	if err != nil {
		db.Close()
		return err
	}
	idx.db = db

	// init table storage
	tx, err := idx.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, v := range [][]byte{
		engine.DataKeySuffix,
		engine.StateKeySuffix,
	} {
		key := append([]byte(name), v...)
		if _, err := store.CreateBucket(tx, key, engine.ErrIndexExists); err != nil {
			return err
		}
	}

	// init and store state
	if err := idx.state.Store(ctx, tx); err != nil {
		return err
	}

	// commit backend tx
	return tx.Commit()
}

func (idx *Index) Open(ctx context.Context, t engine.TableEngine, s *schema.Schema, opts engine.IndexOptions) error {
	// storage schema depends on index type
	indexSchema, convert, err := convertSchema(s, t.Schema(), opts.Type)
	if err != nil {
		return err
	}
	name := s.Name()

	// setup index
	idx.engine = engine.GetEngine(ctx)
	idx.ischema = indexSchema
	idx.schema = s
	idx.id = s.TaggedHash(types.ObjectTagIndex)
	idx.opts = DefaultIndexOptions.Merge(opts)
	idx.table = t
	idx.state = engine.NewObjectState(name)
	idx.journal = pack.New().
		WithMaxRows(opts.JournalSize).
		WithSchema(indexSchema).
		Alloc()
	idx.tomb = pack.New().
		WithMaxRows(opts.JournalSize).
		WithSchema(indexSchema).
		Alloc()
	idx.convert = convert
	idx.metrics = engine.NewIndexMetrics(name)
	idx.log = opts.Logger

	// open db backend and load latest state
	if err := idx.openBackend(ctx); err != nil {
		idx.log.Errorf("%s: open index: %v", name, err)
		_ = idx.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}

	idx.log.Debugf("Index %s opened with %d rows", name, idx.state.NRows)

	return nil
}

func (idx *Index) openBackend(ctx context.Context) error {
	// open db
	name := idx.schema.Name()
	typ := idx.schema.TypeLabel(idx.engine.Namespace())
	path := filepath.Join(idx.engine.RootPath(), name+".db")
	idx.log.Debugf("Opening %s index %q at %q with opts %#v",
		idx.opts.Engine, name, path, idx.opts)

	db, err := store.Open(idx.opts.Driver, path, idx.opts.ToDriverOpts())
	if err != nil {
		return fmt.Errorf("open: %v: %v", err, engine.ErrNoIndex)
	}
	idx.db = db

	// check manifest matches
	mft, err := idx.db.Manifest()
	if err != nil {
		return fmt.Errorf("loading manifest: %v", err)
	}
	if err := mft.Validate(name, "*", typ, -1); err != nil {
		return schema.ErrSchemaMismatch
	}

	// load table state
	err = idx.db.View(func(tx store.Tx) error {
		// check table storage
		for _, v := range [][]byte{
			engine.DataKeySuffix,
			engine.StateKeySuffix,
		} {
			key := append([]byte(name), v...)
			if tx.Bucket(key) == nil {
				return fmt.Errorf("%q: %v", string(key), store.ErrNoBucket)
			}
		}

		if err := idx.state.Load(ctx, tx); err != nil {
			return fmt.Errorf("loading state: %v", err)
		}
		idx.metrics.TupleCount = int64(idx.state.NRows)

		return nil
	})
	return err
}

func (idx *Index) Close(ctx context.Context) (err error) {
	if idx.db != nil {
		idx.log.Debugf("Closing index %s", idx.schema.TypeLabel(idx.engine.Namespace()))
		err = idx.db.Close()
		idx.db = nil
	}
	idx.engine = nil
	idx.ischema = nil
	idx.schema = nil
	idx.table = nil
	idx.id = 0
	idx.db = nil
	idx.opts = engine.IndexOptions{}
	idx.metrics = engine.IndexMetrics{}
	idx.convert = nil
	idx.state.Reset()
	idx.journal.Release()
	idx.tomb.Release()
	idx.journal = nil
	idx.tomb = nil
	return
}

func (idx *Index) Schema() *schema.Schema {
	return idx.schema
}

func (idx *Index) Table() engine.TableEngine {
	return idx.table
}

func (idx *Index) IsComposite() bool {
	return idx.opts.Type == types.IndexTypeComposite
}

func (idx *Index) Sync(ctx context.Context) error {
	return idx.merge(ctx)
}

func (idx *Index) Metrics() engine.IndexMetrics {
	m := idx.metrics
	m.TupleCount = int64(idx.state.NRows)
	m.TotalSize = int64(idx.state.Size)
	m.PacksCount = int64(idx.state.Count)
	return m
}

func (idx *Index) Drop(ctx context.Context) error {
	path := idx.db.Path()
	idx.db.Close()
	idx.db = nil
	idx.log.Debugf("Dropping index %s files at path %s", idx.schema.Name(), path)
	return store.Drop(idx.opts.Driver, path)
}

func (idx *Index) Truncate(ctx context.Context) error {
	// start direct backend write tx (assumes index and table are
	// not stored in the same backend db file)
	tx, err := idx.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, v := range [][]byte{
		engine.DataKeySuffix,
		engine.StateKeySuffix,
	} {
		key := append([]byte(idx.schema.Name()), v...)
		if err := tx.Root().DeleteBucket(key); err != nil {
			return err
		}
		if _, err := tx.Root().CreateBucket(key); err != nil {
			return err
		}
	}

	// reset state
	nDel := idx.state.NRows
	idx.state.Reset()
	if err := idx.state.Store(ctx, tx); err != nil {
		return err
	}

	// commit storage tx
	if err := tx.Commit(); err != nil {
		return err
	}

	// clear data
	idx.journal.Clear()
	idx.tomb.Clear()

	// update metrics
	idx.metrics.DeletedTuples += int64(nDel)
	idx.metrics.TupleCount = 0

	return nil
}

func (idx *Index) Rebuild(ctx context.Context) error {
	// walk all table packs
	rd := idx.table.NewReader().WithFields(idx.schema.AllFieldIds())
	defer rd.Close()

	for {
		// read next table pack
		pkg, err := rd.Next(ctx)
		if err != nil {
			return err
		}

		// stop when we reached the end of the table
		if pkg == nil {
			break
		}

		// add pack contents to index
		if err := idx.AddPack(ctx, pkg, pack.WriteModeAll); err != nil {
			return err
		}
	}

	// final index flush
	return idx.merge(ctx)
}

func (idx *Index) AddPack(ctx context.Context, pkg *pack.Package, mode pack.WriteMode) error {
	// build new index pack, relink columns and produce hash column hash)
	ipkg := idx.convert.ConvertPack(pkg, mode)
	defer ipkg.Release()

	var state pack.AppendState
	for {
		// append next chunk of data to journal: max(cap(journal), len(src))
		state = idx.journal.AppendSelected(ipkg, mode, state)

		// store when journal is full
		if idx.journal.IsFull() {
			if err := idx.merge(ctx); err != nil {
				return err
			}
		}

		// stop when src is exhausted
		if !state.More() {
			break
		}
	}
	return nil
}

func (idx *Index) DelPack(ctx context.Context, pkg *pack.Package, mode pack.WriteMode) error {
	// build new index pack, relink columns and produce hash column hash)
	ipkg := idx.convert.ConvertPack(pkg, mode)
	defer ipkg.Release()

	var state pack.AppendState
	for {
		// append next chunk of data to tomb: max(cap(tomb), len(src))
		state = idx.tomb.AppendSelected(ipkg, mode, state)

		// store when tomb is full
		if idx.tomb.IsFull() {
			if err := idx.merge(ctx); err != nil {
				return err
			}
		}

		// stop when src is exhausted
		if !state.More() {
			break
		}
	}
	return nil
}
