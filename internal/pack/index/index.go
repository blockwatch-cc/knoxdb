// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

// This index supports the following condition types on lookup.
// - hash: EQ, IN, NI (single or composite EQ)
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
		PageFill:    0.95,
		TxMaxSize:   1 << 20, // 1 MB
		ReadOnly:    false,
		NoSync:      false,
		NoGrowSync:  false,
		Logger:      log.Disabled,
	}
)

type Index struct {
	engine  *engine.Engine      // engine access
	schema  *schema.Schema      // table schema
	id      uint64              // unique tagged name hash
	opts    engine.IndexOptions // copy of config options
	table   engine.TableEngine  // related table
	state   engine.ObjectState  // volatile state
	db      store.DB            // lower-level KV store (e.g. boltdb or badger)
	journal *pack.Package       // 2-column in-memory data not yet written to packs
	tomb    *pack.Package       // 2-column in-memory data not yet written to packs
	convert *schema.Converter   // table to index schema converter
	metrics engine.IndexMetrics // usage statistics
	genkey  hashFunc            // key generator function
	log     log.Logger          // log instance
	noClose bool                // don't close underlying store db on Close
}

func NewIndex() engine.IndexEngine {
	return &Index{}
}

func (idx *Index) Create(ctx context.Context, t engine.TableEngine, s *schema.Schema, opts engine.IndexOptions) error {
	// require primary key
	pki := s.PkIndex()
	if pki < 0 {
		return engine.ErrNoPk
	}

	e := engine.GetTransaction(ctx).Engine()

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())
	opts = DefaultIndexOptions.Merge(opts)

	// storage schema depends on index type
	indexSchema, keyFn, err := convertSchema(s, opts.Type)
	if err != nil {
		return err
	}

	// setup index
	idx.engine = e
	idx.schema = indexSchema
	idx.id = s.TaggedHash(types.ObjectTagIndex)
	idx.opts = opts
	idx.table = t
	idx.state = engine.NewObjectState()
	idx.db = opts.DB
	idx.journal = pack.New().
		WithMaxRows(opts.JournalSize).
		WithKey(pack.JournalKeyId).
		WithSchema(indexSchema).
		Alloc()
	idx.tomb = pack.New().
		WithMaxRows(opts.JournalSize).
		WithKey(pack.TombstoneKeyId).
		WithSchema(indexSchema).
		Alloc()
	idx.convert = schema.NewConverter(t.Schema(), s, LE).WithSkipLen()
	idx.metrics = engine.NewIndexMetrics(name)
	idx.genkey = keyFn
	idx.log = opts.Logger
	idx.noClose = true

	idx.log.Debugf("Creating pack index %s on %s with driver %s", name, t.Schema().Name(), idx.opts.Driver)

	// create db if not passed in options
	if idx.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		idx.log.Debugf("Creating pack index %q with opts %#v", path, idx.opts)
		db, err := store.Create(idx.opts.Driver, path, idx.opts.ToDriverOpts())
		if err != nil {
			return fmt.Errorf("creating database for index %s: %v", typ, err)
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
		idx.db = db
		idx.noClose = false
	}

	// init index storage
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, true)
	if err != nil {
		return err
	}
	for _, v := range [][]byte{
		engine.DataKeySuffix,
		engine.StateKeySuffix,
	} {
		key := append([]byte(name), v...)
		if _, err := store.CreateBucket(tx, key, engine.ErrIndexExists); err != nil {
			return err
		}
	}

	// init state storage
	if err := idx.state.Store(ctx, tx, name); err != nil {
		return err
	}

	idx.log.Debugf("Created index %s", typ)
	return nil
}

func (idx *Index) Open(ctx context.Context, t engine.TableEngine, s *schema.Schema, opts engine.IndexOptions) error {
	e := engine.GetTransaction(ctx).Engine()

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// storage schema depends on index type
	indexSchema, keyFn, err := convertSchema(s, opts.Type)
	if err != nil {
		return err
	}

	// setup index
	idx.engine = e
	idx.schema = indexSchema
	idx.id = s.TaggedHash(types.ObjectTagIndex)
	idx.opts = DefaultIndexOptions.Merge(opts)
	idx.table = t
	idx.state = engine.NewObjectState()
	idx.db = opts.DB
	idx.journal = pack.New().
		WithMaxRows(opts.JournalSize).
		WithKey(pack.JournalKeyId).
		WithSchema(indexSchema).
		Alloc()
	idx.tomb = pack.New().
		WithMaxRows(opts.JournalSize).
		WithKey(pack.TombstoneKeyId).
		WithSchema(indexSchema).
		Alloc()
	idx.convert = schema.NewConverter(t.Schema(), s, LE).WithSkipLen()
	idx.metrics = engine.NewIndexMetrics(name)
	idx.genkey = keyFn
	idx.log = opts.Logger
	idx.noClose = true

	idx.log.Debugf("Opening pack index %s on %s with driver %s",
		name, t.Schema().Name(), idx.opts.Driver)

	// open db if not passed in options
	if idx.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		idx.log.Debugf("Opening pack index %q with opts %#v", path, idx.opts)
		db, err := store.Open(idx.opts.Driver, path, idx.opts.ToDriverOpts())
		if err != nil {
			idx.log.Errorf("open index %s: %v", typ, err)
			return engine.ErrNoIndex
		}
		idx.db = db
		idx.noClose = false

		// check manifest matches
		mft, err := idx.db.Manifest()
		if err != nil {
			idx.log.Errorf("missing manifest: %v", err)
			_ = t.Close(ctx)
			return engine.ErrDatabaseCorrupt
		}
		err = mft.Validate(name, "*", typ, -1)
		if err != nil {
			idx.log.Errorf("schema mismatch: %v", err)
			_ = idx.Close(ctx)
			return schema.ErrSchemaMismatch
		}
	}

	// check index storage
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, false)
	if err != nil {
		return err
	}
	for _, v := range [][]byte{
		engine.DataKeySuffix,
		engine.StateKeySuffix,
	} {
		key := append([]byte(name), v...)
		if tx.Bucket(key) == nil {
			idx.log.Errorf("open %s: %v", string(key), engine.ErrNoBucket)
			tx.Rollback()
			_ = idx.Close(ctx)
			return engine.ErrDatabaseCorrupt
		}
	}

	// load state
	if err := idx.state.Load(ctx, tx, idx.schema.Name()); err != nil {
		idx.log.Error("open state: %v", err)
		tx.Rollback()
		t.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}

	idx.log.Debugf("Index %s opened with %d rows", typ, idx.state.NRows)

	return nil
}

func (idx *Index) Close(ctx context.Context) (err error) {
	if !idx.noClose && idx.db != nil {
		idx.log.Debugf("Closing index %s", idx.schema.TypeLabel(idx.engine.Namespace()))
		err = idx.db.Close()
		idx.db = nil
	}
	idx.db = nil
	idx.engine = nil
	idx.schema = nil
	idx.table = nil
	idx.id = 0
	idx.noClose = false
	idx.opts = engine.IndexOptions{}
	idx.metrics = engine.IndexMetrics{}
	idx.convert = nil
	idx.genkey = nil
	idx.state = engine.ObjectState{}
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
	typ := idx.schema.TypeLabel(idx.engine.Namespace())
	if idx.noClose {
		tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, true)
		if err != nil {
			return err
		}
		idx.log.Debugf("Dropping index %s", typ)
		for _, v := range [][]byte{
			engine.DataKeySuffix,
			engine.StateKeySuffix,
		} {
			key := append([]byte(idx.schema.Name()), v...)
			if err := tx.Root().DeleteBucket(key); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}
	path := idx.db.Path()
	idx.db.Close()
	idx.db = nil
	idx.log.Debugf("Dropping index %s with path %s", typ, path)
	if err := os.Remove(path); err != nil {
		return err
	}
	return nil
}

func (idx *Index) Truncate(ctx context.Context) error {
	// unlink index from table to prevent use
	idx.table.UnuseIndex(idx)
	defer idx.table.UseIndex(idx)

	// start backend write tx
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
	if err := idx.state.Store(ctx, tx, idx.schema.Name()); err != nil {
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
	// truncate index first
	if err := idx.Truncate(ctx); err != nil {
		return err
	}

	// unlink index from table to prevent use
	idx.table.UnuseIndex(idx)
	defer idx.table.UseIndex(idx)

	// build a query plan to walk all table data and only fetch
	// columns we need for indexing, since idx.schema is storage
	// schema (columns replaced by hash column) we use converter
	// child schema for this query which extracts what we need
	// in the order in which we construct index keys
	plan := query.NewQueryPlan().
		WithTable(idx.table).
		WithSchema(idx.convert.Schema()).
		WithFlags(query.QueryFlagNoIndex).
		WithLogger(idx.log)

	err := idx.table.Stream(ctx, plan, func(row engine.QueryRow) error {
		// create wire encoding compatible with index, potentially hashing data
		buf := idx.convert.Extract(row.Bytes())
		key := idx.genkey(buf)

		// append to journal
		idx.journal.AppendWire(key, nil)

		// flush journal when full
		var err error
		if idx.journal.IsFull() {
			err = idx.merge(ctx)
		}
		return err
	})
	if err != nil {
		return err
	}

	// final index flush
	return idx.merge(ctx)
}

func (idx *Index) Add(ctx context.Context, prev, val []byte) error {
	pkey := idx.convert.Extract(prev)
	vkey := idx.convert.Extract(val)
	sameKey := bytes.Equal(pkey, vkey)
	if pkey != nil && !sameKey {
		pkey = idx.genkey(pkey)
		idx.tomb.AppendWire(pkey, nil)
	}
	if vkey != nil && !sameKey {
		vkey = idx.genkey(vkey)
		idx.journal.AppendWire(vkey, nil)
	}
	if idx.journal.IsFull() || idx.tomb.IsFull() {
		return idx.merge(ctx)
	}
	return nil
}

func (idx *Index) Del(ctx context.Context, prev []byte) error {
	pkey := idx.convert.Extract(prev)
	if pkey == nil {
		return nil
	}
	pkey = idx.genkey(pkey)
	idx.tomb.AppendWire(pkey, nil)
	if idx.journal.IsFull() || idx.tomb.IsFull() {
		return idx.merge(ctx)
	}
	return nil
}
