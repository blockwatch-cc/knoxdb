// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

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
	id         uint64              // unique tagged name hash
	opts       engine.IndexOptions // copy of config options
	table      engine.TableEngine  // related table
	state      engine.ObjectState  // volatile state
	db         store.DB            // lower-level KV store (e.g. boltdb or badger)
	key        []byte              // name of the data bucket
	isZeroCopy bool                // storage reads are zero copy (copy to safe references)
	noClose    bool                // don't close underlying store db on Close
	convert    *schema.Converter   // table to index schema converter
	metrics    engine.IndexMetrics // usage statistics
	log        log.Logger          // log instance
	// state      engine.ObjectState              // number of live entries
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

	e := engine.GetEngine(ctx)

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())
	opts = DefaultIndexOptions.Merge(opts)

	// setup index
	idx.engine = e
	idx.schema = s
	idx.id = s.TaggedHash(types.ObjectTagIndex)
	idx.opts = opts
	idx.table = t
	idx.state = engine.NewObjectState([]byte(name))
	idx.db = opts.DB
	idx.key = append([]byte(name), engine.DataKeySuffix...)
	idx.convert = schema.NewConverter(t.Schema(), s, BE).WithSkipLen()
	idx.metrics = engine.NewIndexMetrics(name)
	idx.log = opts.Logger
	idx.noClose = true

	idx.log.Debugf("Creating LSM index %s on %s with driver %s", name, t.Schema().Name(), idx.opts.Driver)

	// create db if not passed in options
	if idx.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		idx.log.Debugf("Creating LSM index %q with opts %#v", path, idx.opts)
		db, err := store.Create(idx.opts.Driver, path, idx.opts.ToDriverOpts())
		if err != nil {
			return fmt.Errorf("creating database for index %s: %v", name, err)
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
	if err := idx.state.Store(ctx, tx); err != nil {
		return err
	}

	idx.log.Debugf("Created index %s", name)
	return nil
}

func (idx *Index) Open(ctx context.Context, t engine.TableEngine, s *schema.Schema, opts engine.IndexOptions) error {
	e := engine.GetEngine(ctx)

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// setup index
	idx.engine = e
	idx.schema = s
	idx.id = s.TaggedHash(types.ObjectTagIndex)
	idx.opts = DefaultIndexOptions.Merge(opts)
	idx.table = t
	idx.state = engine.NewObjectState([]byte(name))
	idx.db = opts.DB
	idx.key = []byte(name)
	idx.convert = schema.NewConverter(t.Schema(), s, BE).WithSkipLen()
	idx.metrics = engine.NewIndexMetrics(name)
	idx.log = opts.Logger
	idx.noClose = true

	idx.log.Debugf("Opening LSM index %s on %s with driver %s", name, t.Schema().Name(), idx.opts.Driver)

	// open db if not passed in options
	if idx.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		idx.log.Debugf("Opening LSM index %q with opts %#v", path, idx.opts)
		db, err := store.Open(idx.opts.Driver, path, idx.opts.ToDriverOpts())
		if err != nil {
			idx.log.Errorf("open index %s: %v", name, err)
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
	if err := idx.state.Load(ctx, tx); err != nil {
		idx.log.Error("open state: %v", err)
		tx.Rollback()
		t.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}

	idx.log.Debugf("Index %s opened with %d rows", name, idx.state.NRows)

	return nil
}

func (idx *Index) Close(ctx context.Context) (err error) {
	if !idx.noClose && idx.db != nil {
		idx.log.Debugf("Closing index %s", idx.schema.Name())
		err = idx.db.Close()
		idx.db = nil
	}
	idx.engine = nil
	idx.schema = nil
	idx.table = nil
	idx.id = 0
	idx.db = nil
	idx.key = nil
	idx.noClose = false
	idx.isZeroCopy = false
	idx.opts = engine.IndexOptions{}
	idx.metrics = engine.IndexMetrics{}
	idx.state = engine.ObjectState{}
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
	return idx.db.Sync()
}

func (idx *Index) Metrics() engine.IndexMetrics {
	m := idx.metrics
	m.TupleCount = int64(idx.state.NRows)
	m.TotalSize = int64(idx.state.Size)
	return m
}

func (idx *Index) Drop(ctx context.Context) error {
	if idx.noClose {
		tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, true)
		if err != nil {
			return err
		}
		idx.log.Debugf("Dropping index %s", idx.schema.Name())
		for _, v := range [][]byte{
			engine.DataKeySuffix,
			engine.StateKeySuffix,
		} {
			key := append([]byte(idx.schema.Name()), v...)
			if err := tx.Root().DeleteBucket(key); err != nil {
				return err
			}
		}
		// commit and continue
		_, err = engine.GetTransaction(ctx).Continue(tx)
		return err
	}
	path := idx.db.Path()
	idx.db.Close()
	idx.db = nil
	idx.log.Debugf("Dropping index %s with path %s", idx.schema.Name(), path)
	return store.Drop(idx.opts.Driver, path)
}

func (idx *Index) Truncate(ctx context.Context) error {
	idx.log.Debugf("Truncate index %s", idx.schema.Name())

	// get shared backend write tx
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, true)
	if err != nil {
		return err
	}

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

	// commit and continue backend tx
	if _, err := engine.GetTransaction(ctx).Continue(tx); err != nil {
		return err
	}

	// update metrics
	idx.metrics.DeletedTuples += int64(nDel)
	idx.metrics.TupleCount = 0

	return nil
}

func (idx *Index) Rebuild(ctx context.Context) error {
	// get shared backend write tx (when backed by badger LSM backend
	// we use a single badger db instance for all indexes and tables)
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, true)
	if err != nil {
		return err
	}

	// reference the data bucket
	bucket := tx.Bucket(idx.key)
	if bucket == nil {
		return engine.ErrNoIndex
	}

	// build a query plan to walk all table data and only fetch
	// columns we need for indexing, since idx.schema is storage
	// schema (columns replaced by hash column) we use converter
	// child schema for this query which extracts what we need
	// in the order in which we construct index keys
	plan := query.NewQueryPlan().
		WithTable(idx.table).
		WithSchema(idx.schema).
		WithFlags(query.QueryFlagNoIndex).
		WithLogger(idx.log)

	// table data is encoded little endian wire format containing
	// only the fields our index requires, but we still need
	// to convert LE ints to BE (in particular the primary key)
	conv := schema.NewConverter(idx.schema, idx.schema, BE).WithSkipLen()
	start := time.Now()

	var nBytes, nIns, nWrite int
	err = idx.table.Stream(ctx, plan, func(row engine.QueryRow) error {
		// row is a row-encoded idx schema in little endian order
		key := conv.Extract(row.Bytes())
		err := bucket.Put(key, nil)
		if err != nil {
			return err
		}

		// batch commit storage transactions
		nBytes += len(key)
		nWrite += len(key)
		nIns++
		if nBytes >= idx.opts.TxMaxSize {
			tx, err = engine.GetTransaction(ctx).Continue(tx)
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

	// update state
	idx.state.Size = uint64(nWrite)
	idx.state.NRows = uint64(nIns)
	if err := idx.state.Store(ctx, tx); err != nil {
		return err
	}

	// final commit
	_, err = engine.GetTransaction(ctx).Continue(tx)
	if err != nil {
		return err
	}

	// update metrics
	atomic.StoreInt64(&idx.metrics.LastFlushTime, start.UnixNano())
	atomic.StoreInt64(&idx.metrics.LastFlushDuration, int64(time.Since(start)))
	atomic.AddInt64(&idx.metrics.InsertedTuples, int64(nIns))
	atomic.AddInt64(&idx.metrics.BytesWritten, int64(nWrite))
	atomic.StoreInt64(&idx.metrics.TotalSize, int64(idx.state.Size))

	return nil
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
		idx.state.Size -= uint64(len(pkey))
		_ = tx.Bucket(idx.key).Delete(pkey)
	}
	if vkey != nil && !sameKey {
		idx.state.Size += uint64(len(vkey))
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
	idx.state.Size -= uint64(len(pkey))
	return tx.Bucket(idx.key).Delete(pkey)
}
