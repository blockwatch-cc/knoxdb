// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package kvstore

import (
	"context"
	"os"
	"path/filepath"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

var _ engine.StoreEngine = (*KVStore)(nil)

func init() {
	engine.RegisterStoreFactory(engine.StoreKindKV, NewKVStore)
}

var DefaultOptions = engine.StoreOptions{
	Driver:     "bolt",
	PageSize:   1 << 16,
	PageFill:   0.9,
	TxMaxSize:  128,
	ReadOnly:   false,
	NoSync:     false,
	NoGrowSync: false,
	Logger:     log.Disabled,
}

type KVStore struct {
	engine     *engine.Engine      // engine access
	schema     *schema.Schema      // store schema
	storeId    uint64              // tagged hash
	opts       engine.StoreOptions // copy of config options
	db         store.DB            // lower-level KV store (e.g. boltdb)
	key        []byte              // name of store's data bucket in the db
	state      engine.ObjectState  // volatile state
	isZeroCopy bool                // storage reads are zero copy (copy to safe references)
	noClose    bool                // don't close underlying store db on Close
	metrics    engine.StoreMetrics // usage statistics
	log        log.Logger
}

func NewKVStore() engine.StoreEngine {
	return &KVStore{}
}

func (kv *KVStore) Create(ctx context.Context, s *schema.Schema, opts engine.StoreOptions) error {
	e := engine.GetTransaction(ctx).Engine()

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// setup store
	kv.engine = e
	kv.schema = s
	kv.state = engine.NewObjectState(name)
	kv.storeId = s.TaggedHash(types.ObjectTagStore)
	kv.opts = DefaultOptions.Merge(opts)
	kv.key = []byte(name)
	kv.metrics = engine.NewStoreMetrics(name)
	kv.noClose = true
	kv.log = opts.Logger

	// create db if not passed in options
	if kv.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		kv.log.Debugf("Creating KV store %s with opts %#v", path, opts)
		db, err := store.Create(kv.opts.Driver, path, kv.opts.ToDriverOpts())
		if err != nil {
			kv.log.Errorf("creating store %s: %v", typ, err)
			return engine.ErrNoStore
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
		kv.db = db
		kv.noClose = false
	}
	kv.isZeroCopy = kv.db.IsZeroCopyRead()

	// init store
	tx, err := kv.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	bucket := tx.Bucket(kv.key)
	if bucket != nil {
		return engine.ErrStoreExists
	}
	_, err = tx.Root().CreateBucketIfNotExists(kv.key)
	if err != nil {
		return err
	}

	// init state storage
	if err := kv.state.Store(ctx, tx); err != nil {
		return err
	}

	kv.log.Debugf("Created store %s", typ)
	return tx.Commit()
}

func (kv *KVStore) Open(ctx context.Context, s *schema.Schema, opts engine.StoreOptions) error {
	e := engine.GetTransaction(ctx).Engine()

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// setup store
	kv.engine = e
	kv.schema = s
	kv.storeId = s.TaggedHash(types.ObjectTagStore)
	kv.opts = DefaultOptions.Merge(opts)
	kv.key = []byte(name)
	kv.metrics = engine.NewStoreMetrics(name)
	kv.noClose = true
	kv.log = opts.Logger

	// open db if not passed in options
	if kv.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		kv.log.Debugf("Opening KV store %q with opts %#v", path, opts)
		db, err := store.Open(kv.opts.Driver, path, kv.opts.ToDriverOpts())
		if err != nil {
			kv.log.Errorf("opening store %s: %v", typ, err)
			return engine.ErrNoDatabase
		}
		kv.db = db
		kv.noClose = false

		// check manifest matches
		mft, err := kv.db.Manifest()
		if err != nil {
			kv.log.Errorf("missing manifest: %v", err)
			_ = kv.Close(ctx)
			return engine.ErrDatabaseCorrupt
		}
		err = mft.Validate(name, "*", typ, -1)
		if err != nil {
			kv.log.Errorf("schema mismatch: %v", err)
			_ = kv.Close(ctx)
			return schema.ErrSchemaMismatch
		}
	}
	kv.isZeroCopy = kv.db.IsZeroCopyRead()

	tx, err := kv.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// load state
	if err := kv.state.Load(ctx, tx); err != nil {
		kv.log.Error("missing table state: %v", err)
		kv.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}

	// init metrics
	bucket := tx.Bucket(kv.key)
	if bucket == nil {
		kv.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}
	stats := bucket.Stats()
	kv.metrics.TotalSize = int64(stats.Size) // estimate only
	kv.metrics.NumKeys = int64(stats.KeyN)

	kv.log.Debugf("store %s opened with %d entries", kv.schema.Name(), kv.metrics.NumKeys)
	return nil
}

func (kv *KVStore) Close(ctx context.Context) (err error) {
	if !kv.noClose && kv.db != nil {
		kv.log.Debugf("closing store %s", kv.schema.TypeLabel(kv.engine.Namespace()))
		err = kv.db.Close()
		kv.db = nil
	}
	kv.engine = nil
	kv.schema = nil
	kv.storeId = 0
	kv.key = nil
	kv.opts = engine.StoreOptions{}
	kv.metrics = engine.StoreMetrics{}
	kv.state.Reset()
	kv.noClose = false
	kv.isZeroCopy = false
	return
}

func (kv *KVStore) Sync(_ context.Context) error {
	return kv.db.Sync()
}

func (kv *KVStore) Schema() *schema.Schema {
	return kv.schema
}

func (kv *KVStore) State() engine.ObjectState {
	return kv.state
}

func (kv *KVStore) Metrics() engine.StoreMetrics {
	// copy store stats
	m := kv.metrics

	// copy cache stats
	// cs := s.cache.Stats()
	// m.CacheHits = cs.Hits
	// m.CacheMisses = cs.Misses
	// m.CacheInserts = cs.Inserts
	// m.CacheEvictions = cs.Evictions
	// m.CacheCount = cs.Count
	// m.CacheSize = cs.Size

	return m
}

func (kv *KVStore) Drop(ctx context.Context) error {
	typ := kv.schema.TypeLabel(kv.engine.Namespace())
	if kv.noClose {
		kv.log.Debugf("dropping store %s", typ)
		tx, err := kv.db.Begin(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := tx.Root().DeleteBucket(kv.key); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}
	path := kv.db.Path()
	_ = kv.db.Close()
	kv.db = nil
	kv.log.Debugf("dropping store %s with path %s", typ, path)
	if err := os.Remove(path); err != nil {
		return err
	}
	return nil
}

func (kv *KVStore) CommitTx(_ context.Context, _ types.XID) error {
	return nil
}

func (kv *KVStore) AbortTx(_ context.Context, _ types.XID) error {
	return nil
}
