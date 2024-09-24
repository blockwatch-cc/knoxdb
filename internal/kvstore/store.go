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
	db         store.DB            // lower-level KV store (e.g. boltdb or badger)
	key        []byte              // name of store's data bucket in the db
	state      KVState             // volatile state, synced with catalog
	isZeroCopy bool                // storage reads are zero copy (copy to safe references)
	noClose    bool                // don't close underlying store db on Close
	stats      engine.StoreStats   // usage statistics
	log        log.Logger
}

func NewKVStore() engine.StoreEngine {
	return &KVStore{}
}

type KVState struct {
	Checkpoint uint64 // latest wal checkpoint LSN
}

func (s *KVState) Init() {
	s.Checkpoint = 0
}

func (s *KVState) FromObjectState(o engine.ObjectState) {
	s.Checkpoint = o[2]
}

func (s KVState) ToObjectState() engine.ObjectState {
	return engine.ObjectState{0, 0, s.Checkpoint}
}

func (kv *KVStore) Create(ctx context.Context, s *schema.Schema, opts engine.StoreOptions) error {
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
	kv.state.Init()
	kv.stats.Name = name
	kv.db = opts.DB
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
	tx, err := engine.GetTransaction(ctx).StoreTx(kv.db, true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(kv.key)
	if bucket != nil {
		return engine.ErrStoreExists
	}
	_, err = tx.Root().CreateBucketIfNotExists(kv.key)
	if err != nil {
		return err
	}
	kv.engine.Catalog().SetState(kv.storeId, kv.state.ToObjectState())

	kv.log.Debugf("Created store %s", typ)
	return nil
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
	kv.state.FromObjectState(e.Catalog().GetState(kv.storeId))
	kv.stats.Name = name
	kv.db = opts.DB
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

	tx, err := engine.GetTransaction(ctx).StoreTx(kv.db, false)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(kv.key)
	if bucket == nil {
		return engine.ErrNoBucket
	}
	stats := bucket.Stats()
	kv.stats.TotalSize = int64(stats.Size) // estimate only
	kv.stats.NumKeys = int64(stats.KeyN)

	if err != nil {
		kv.log.Error("reading store stats: %v", err)
		tx.Rollback()
		_ = kv.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}

	kv.log.Debugf("store %s opened with %d entries", kv.schema.Name(), kv.stats.NumKeys)
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
	kv.stats = engine.StoreStats{}
	kv.noClose = false
	kv.isZeroCopy = false
	return
}

func (kv *KVStore) Schema() *schema.Schema {
	return kv.schema
}

func (kv *KVStore) Stats() engine.StoreStats {
	// copy store stats
	stats := kv.stats

	// copy cache stats
	// cs := s.cache.Stats()
	// stats.CacheHits = cs.Hits
	// stats.CacheMisses = cs.Misses
	// stats.CacheInserts = cs.Inserts
	// stats.CacheEvictions = cs.Evictions
	// stats.CacheCount = cs.Count
	// stats.CacheSize = cs.Size

	return stats
}

func (kv *KVStore) Drop(ctx context.Context) error {
	typ := kv.schema.TypeLabel(kv.engine.Namespace())
	if kv.noClose {
		kv.log.Debugf("dropping store %s", typ)
		tx, err := engine.GetTransaction(ctx).StoreTx(kv.db, true)
		if err != nil {
			return err
		}
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
