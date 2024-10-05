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
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

var _ engine.IndexEngine = (*Index)(nil)

func init() {
	engine.RegisterIndexFactory(engine.IndexKindLSM, NewIndex)
}

var (
	BE = binary.BigEndian    // byte order for keys
	NE = binary.NativeEndian // byte order for values (LE)
)

var (
	DefaultIndexOptions = engine.IndexOptions{
		Driver:     "badger",
		Type:       types.IndexTypeComposite,
		PageSize:   1 << 16,
		PageFill:   0.9,
		TxMaxSize:  1 << 20, // 1 MB
		ReadOnly:   false,
		NoSync:     false,
		NoGrowSync: false,
		Logger:     log.Disabled,
	}
)

type Index struct {
	engine     *engine.Engine      // engine access
	schema     *schema.Schema      // table schema
	indexId    uint64              // unique tagged name hash
	opts       engine.IndexOptions // copy of config options
	db         store.DB            // lower-level KV store (e.g. boltdb or badger)
	key        []byte              // name of the data bucket
	isZeroCopy bool                // storage reads are zero copy (copy to safe references)
	noClose    bool                // don't close underlying store db on Close
	table      engine.TableEngine  // related table
	convert    *schema.Converter   // table to index schema converter
	metrics    engine.IndexMetrics // usage statistics
	log        log.Logger          // log instance
	// state      engine.ObjectState              // number of live entries
}

func NewIndex() engine.IndexEngine {
	return &Index{}
}

func (idx *Index) Create(ctx context.Context, t engine.TableEngine, s *schema.Schema, opts engine.IndexOptions) error {
	e := engine.GetTransaction(ctx).Engine()

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// setup index
	idx.engine = e
	idx.schema = s
	idx.indexId = s.TaggedHash(types.ObjectTagIndex)
	idx.opts = DefaultIndexOptions.Merge(opts)
	idx.key = []byte(name)
	idx.metrics = engine.NewIndexMetrics(name)
	idx.db = opts.DB
	idx.noClose = true
	idx.table = t
	idx.convert = schema.NewConverter(t.Schema(), s, BE).WithSkipLen()
	idx.log = opts.Logger

	if idx.opts.Type != types.IndexTypeComposite {
		return fmt.Errorf("lsm index: unsupported index type %q", idx.opts.Type)
	}

	idx.log.Debugf("Creating LSM index %s on %s with driver %s", name, t.Schema().Name(), idx.opts.Driver)

	// create db if not passed in options
	if idx.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		idx.log.Debugf("Creating LSM index %q with opts %#v", path, idx.opts)
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
			_ = db.Close()
			return err
		}
		idx.db = db
		idx.noClose = false
	}
	idx.isZeroCopy = idx.db.IsZeroCopyRead()

	// init index storage
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, true)
	if err != nil {
		return err
	}
	if _, err := store.CreateBucket(tx, idx.key, engine.ErrIndexExists); err != nil {
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

	// setup index
	idx.engine = e
	idx.schema = s
	idx.indexId = s.TaggedHash(types.ObjectTagIndex)
	idx.opts = DefaultIndexOptions.Merge(opts)
	idx.key = []byte(name)
	idx.metrics = engine.NewIndexMetrics(name)
	idx.db = opts.DB
	idx.noClose = true
	idx.table = t
	idx.convert = schema.NewConverter(t.Schema(), s, BE).WithSkipLen()
	idx.log = opts.Logger

	idx.log.Debugf("Opening LSM index %s on %s with driver %s", name, t.Schema().Name(), idx.opts.Driver)

	// open db if not passed in options
	if idx.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		idx.log.Debugf("Opening LSM index %q with opts %#v", path, idx.opts)
		db, err := store.Open(idx.opts.Driver, path, idx.opts.ToDriverOpts())
		if err != nil {
			idx.log.Errorf("opening index %s: %v", typ, err)
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
	idx.isZeroCopy = idx.db.IsZeroCopyRead()

	// check table storage
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, false)
	if err != nil {
		return err
	}
	b := tx.Bucket(idx.key)
	if b == nil {
		idx.log.Error("reading table stats: %v", err)
		tx.Rollback()
		_ = idx.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}
	stats := b.Stats()
	idx.metrics.TotalSize = int64(stats.Size) // estimate only

	idx.log.Debugf("Index %s opened", typ)

	return nil
}

func (idx *Index) Close(ctx context.Context) (err error) {
	if !idx.noClose && idx.db != nil {
		idx.log.Debugf("Closing index %s", idx.schema.TypeLabel(idx.engine.Namespace()))
		err = idx.db.Close()
		idx.db = nil
	}
	idx.engine = nil
	idx.schema = nil
	idx.table = nil
	idx.indexId = 0
	idx.key = nil
	idx.noClose = false
	idx.isZeroCopy = false
	idx.opts = engine.IndexOptions{}
	idx.metrics = engine.IndexMetrics{}
	idx.convert = nil
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

func (idx *Index) Sync(_ context.Context) error {
	return nil
}

func (idx *Index) Metrics() engine.IndexMetrics {
	m := idx.metrics
	// m.TupleCount = int64(idx.nrows)
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
		if err := tx.Root().DeleteBucket(idx.key); err != nil {
			return err
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
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, true)
	if err != nil {
		return err
	}
	if err := tx.Root().DeleteBucket(idx.key); err != nil {
		return err
	}
	if _, err := tx.Root().CreateBucket(idx.key); err != nil {
		return err
	}
	// idx.metrics.DeletedTuples += int64(idx.nrows)
	idx.metrics.TupleCount = 0
	// idx.nrows = 0
	return nil
}

func (idx *Index) Rebuild(ctx context.Context) error {
	// run inside a storage transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, true)
	if err != nil {
		return err
	}

	if err := idx.Truncate(ctx); err != nil {
		return err
	}

	// GC/commit storage tx
	tx, err = store.CommitAndContinue(tx)
	if err != nil {
		return err
	}

	// reference the data bucket
	bucket := tx.Bucket(idx.key)
	if bucket == nil {
		return engine.ErrNoIndex
	}

	// build a query plan that walk all table data
	plan := query.NewQueryPlan().
		WithTable(idx.table).
		WithSchema(idx.schema).
		WithLogger(idx.log)

	// table data is encoded little endian wire format containing
	// only the fields our index requires, but we still need
	// to convert LE ints to BE (in particular the primary key)
	conv := schema.NewConverter(idx.schema, idx.schema, BE).WithSkipLen()

	var nBytes int
	err = idx.table.Stream(ctx, plan, func(row engine.QueryRow) error {
		// row is a row-encoded idx schema in little endian order
		err := bucket.Put(conv.Extract(row.Bytes()), nil)
		if err != nil {
			return err
		}

		// batch commit storage transactions
		nBytes += len(row.Bytes())
		if nBytes >= idx.opts.TxMaxSize {
			tx, err = store.CommitAndContinue(tx)
			if err != nil {
				return err
			}
			bucket = tx.Bucket(idx.key)
			nBytes = 0
		}
		return nil
	})
	if err != nil {
		return err
	}

	// final commit
	return tx.Commit()
}

func (idx *Index) Add(ctx context.Context, prev, val []byte) error {
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, true)
	if err != nil {
		return err
	}
	pkey := idx.convert.Extract(prev)
	vkey := idx.convert.Extract(val)
	sameKey := bytes.Equal(pkey, vkey)
	if pkey != nil && !sameKey {
		_ = tx.Bucket(idx.key).Delete(pkey)
	}
	if vkey != nil && !sameKey {
		return tx.Bucket(idx.key).Put(vkey, nil)
	}
	return nil
}

func (idx *Index) Del(ctx context.Context, prev []byte) error {
	pkey := idx.convert.Extract(prev)
	if pkey == nil {
		return nil
	}
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, true)
	if err != nil {
		return err
	}
	return tx.Bucket(idx.key).Delete(pkey)
}
