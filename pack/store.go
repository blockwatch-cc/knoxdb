// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
    "context"
    "encoding"
    "encoding/json"
    "fmt"
    "sync/atomic"

    "blockwatch.cc/knoxdb/cache/rclru"
    "blockwatch.cc/knoxdb/store"
)

var (
    storeKey = []byte("_store")
)

type Buffer struct {
    ref int64
    buf []byte
}

func (b *Buffer) IncRef() int64 {
    return atomic.AddInt64(&b.ref, 1)
}

func (b *Buffer) DecRef() int64 {
    return atomic.AddInt64(&b.ref, -1)
}

func (b *Buffer) HeapSize() int {
    return 8 + 24 + len(b.buf)
}

func (b *Buffer) Bytes() []byte {
    return b.buf
}

func NewBuffer(b []byte) *Buffer {
    return &Buffer{buf: b}
}

type Store struct {
    name    string                       // printable pool name
    opts    Options                      // runtime configuration options
    db      *DB                          // lower-level storage (e.g. boltdb wrapper)
    cache   rclru.Cache[uint64, *Buffer] // cache for improving data loads
    key     []byte                       // name of store's data bucket
    metakey []byte                       // name of store's metadata bucket
    stats   TableStats                   // usage statistics
}

func (d *DB) CreateStore(name string, opts Options) (*Store, error) {
    opts = DefaultOptions.Merge(opts)
    if err := opts.Check(); err != nil {
        return nil, err
    }
    s := &Store{
        name:    name,
        opts:    opts,
        db:      d,
        key:     []byte(name + "_store"),
        metakey: []byte(name + "_store_meta"),
    }
    s.stats.TableName = name
    err := d.db.Update(func(dbTx store.Tx) error {
        b := dbTx.Bucket(s.key)
        if b != nil {
            return ErrStoreExists
        }
        _, err := dbTx.Root().CreateBucketIfNotExists(s.key)
        if err != nil {
            return err
        }
        meta, err := dbTx.Root().CreateBucketIfNotExists(s.metakey)
        if err != nil {
            return err
        }
        buf, err := json.Marshal(s.opts)
        if err != nil {
            return err
        }
        err = meta.Put(optsKey, buf)
        if err != nil {
            return err
        }
        return nil
    })
    if err != nil {
        return nil, err
    }
    if s.opts.CacheSize > 0 {
        s.cache, err = rclru.New2Q[uint64, *Buffer](int(s.opts.CacheSizeMBytes()))
        if err != nil {
            return nil, err
        }
        s.stats.PackCacheCapacity = int64(s.opts.CacheSizeMBytes())
    } else {
        s.cache = rclru.NewNoCache[uint64, *Buffer]()
    }
    log.Debugf("Created bitpool %s", name)
    d.stores[name] = s
    return s, nil
}

func (d *DB) CreateStoreIfNotExists(name string, opts Options) (*Store, error) {
    s, err := d.CreateStore(name, opts)
    if err != nil {
        if err != ErrStoreExists {
            return nil, err
        }
        s, err = d.Store(name, opts)
        if err != nil {
            return nil, err
        }
    }
    return s, nil
}

func (d *DB) DropStore(name string) error {
    s, err := d.Store(name)
    if err != nil {
        return err
    }
    s.cache.Purge()
    err = d.db.Update(func(dbTx store.Tx) error {
        err = dbTx.Root().DeleteBucket([]byte(name + "_store"))
        if err != nil {
            return err
        }
        return dbTx.Root().DeleteBucket([]byte(name + "store__meta"))
    })
    if err != nil {
        return err
    }
    delete(d.stores, s.name)
    s = nil
    return nil
}

func (d *DB) Store(name string, opts ...Options) (*Store, error) {
    if s, ok := d.stores[name]; ok {
        return s, nil
    }
    if len(opts) > 0 {
        log.Debugf("Opening store %s with opts %#v", name, opts[0])
    } else {
        log.Debugf("Opening store %s with default opts", name)
    }
    s := &Store{
        name:    name,
        db:      d,
        key:     []byte(name + "_store"),
        metakey: []byte(name + "_store_meta"),
    }
    s.stats.TableName = name
    err := d.db.View(func(dbTx store.Tx) error {
        d := dbTx.Bucket(s.key)
        b := dbTx.Bucket(s.metakey)
        if d == nil || b == nil {
            return ErrNoStore
        }
        buf := b.Get(optsKey)
        if buf == nil {
            return fmt.Errorf("pack: missing options for store %s", name)
        }
        err := json.Unmarshal(buf, &s.opts)
        if err != nil {
            return err
        }
        if len(opts) > 0 {
            s.opts = s.opts.Merge(opts[0])
        }
        stats := d.Stats()
        atomic.StoreInt64(&s.stats.TupleCount, int64(stats.KeyN))
        log.Debugf("pack: %s store opened with %d entries", name, s.stats.TupleCount)
        return nil
    })
    if err != nil {
        return nil, err
    }
    if s.opts.CacheSize > 0 {
        s.cache, err = rclru.New2Q[uint64, *Buffer](int(s.opts.CacheSizeMBytes()))
        if err != nil {
            return nil, err
        }
        s.stats.PackCacheCapacity = int64(s.opts.CacheSizeMBytes())
    } else {
        s.cache = rclru.NewNoCache[uint64, *Buffer]()
    }
    d.stores[name] = s
    return s, nil
}

func (s *Store) Name() string {
    return s.name
}

func (s *Store) Database() *DB {
    return s.db
}

func (s *Store) Options() Options {
    return s.opts
}

func (s *Store) Stats() []TableStats {
    // copy store stats
    stats := s.stats

    // copy cache stats
    cs := s.cache.Stats()
    stats.PackCacheHits = cs.Hits
    stats.PackCacheMisses = cs.Misses
    stats.PackCacheInserts = cs.Inserts
    stats.PackCacheEvictions = cs.Evictions
    stats.PackCacheCount = cs.Count
    stats.PackCacheSize = cs.Size

    return []TableStats{stats}
}

func (s *Store) PurgeCache() {
    s.cache.Purge()
}

func (s *Store) Put(ctx context.Context, key uint64, val interface{}) (int, error) {
    var (
        buf []byte
        err error
    )
    if val == nil {
        return 0, ErrNilValue
    }
    if b, ok := val.([]byte); ok {
        buf = b
    } else if m, ok := val.(encoding.BinaryMarshaler); ok {
        buf, err = m.MarshalBinary()
    } else if m, ok := val.(json.Marshaler); ok {
        buf, err = m.MarshalJSON()
    } else if m, ok := val.(encoding.TextMarshaler); ok {
        buf, err = m.MarshalText()
    } else {
        err = fmt.Errorf("no compatible marshaler interface on type %T", val)
    }
    if err != nil {
        return 0, err
    }
    var bkey [8]byte
    bigEndian.PutUint64(bkey[:], key)
    n, err := s.PutBytes(ctx, bkey[:], buf)
    if err == nil {
        s.cache.Add(key, NewBuffer(buf))
    }
    return n, err
}

func (s *Store) PutBytes(ctx context.Context, key, val []byte) (int, error) {
    err := s.db.Update(func(tx store.Tx) error {
        b := tx.Bucket(s.key)
        if b == nil {
            return ErrBucketNotFound
        }
        b.FillPercent(float64(s.opts.FillLevel) / 100.0)
        return b.Put(key, val)
    })
    if err != nil {
        return 0, err
    }
    atomic.AddInt64(&s.stats.PacksBytesWritten, 8+int64(len(val)))
    return len(val), nil
}

func (s *Store) Get(ctx context.Context, key uint64, val interface{}) error {
    if val == nil {
        return ErrNilValue
    }
    buf, ok := s.cache.Get(key)
    if !ok {
        var bkey [8]byte
        bigEndian.PutUint64(bkey[:], key)
        b, err := s.GetBytes(ctx, bkey[:])
        if err != nil {
            return err
        }
        buf = NewBuffer(b)
        buf.IncRef()
        // cache result
        s.cache.Add(key, buf)
    }

    // decode if a decoder interface is found
    var err error
    if _, ok := val.([]byte); ok {
        val = buf.Bytes()
    } else if m, ok := val.(encoding.BinaryUnmarshaler); ok {
        err = m.UnmarshalBinary(buf.Bytes())
    } else if m, ok := val.(json.Unmarshaler); ok {
        err = m.UnmarshalJSON(buf.Bytes())
    } else if m, ok := val.(encoding.TextUnmarshaler); ok {
        err = m.UnmarshalText(buf.Bytes())
    } else {
        err = fmt.Errorf("no compatible unmarshaler interface on type %T", val)
    }
    buf.DecRef()
    return err
}

func (s *Store) GetBytes(ctx context.Context, key []byte) ([]byte, error) {
    var ret []byte
    err := s.db.View(func(tx store.Tx) error {
        b := tx.Bucket(s.key)
        if b == nil {
            return ErrBucketNotFound
        }
        buf := b.Get(key)
        if buf == nil {
            return ErrKeyNotFound
        }
        ret = make([]byte, len(buf))
        copy(ret, buf)
        return nil
    })
    if err == nil {
        atomic.AddInt64(&s.stats.PacksBytesRead, int64(len(ret)))
    }
    return ret, err
}

func (s *Store) Delete(ctx context.Context, key uint64) error {
    s.cache.Remove(key)
    var bkey [8]byte
    bigEndian.PutUint64(bkey[:], key)
    err := s.db.Update(func(tx store.Tx) error {
        b := tx.Bucket(s.key)
        if b == nil {
            return ErrBucketNotFound
        }
        return b.Delete(bkey[:])
    })
    return err
}

func (s *Store) IsClosed() bool {
    return s.db == nil
}

func (s *Store) Close() error {
    // unregister from db
    log.Debugf("pack: closing %s store", s.name)
    delete(s.db.stores, s.name)
    s.db = nil
    return nil
}
