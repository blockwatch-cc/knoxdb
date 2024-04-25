// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"blockwatch.cc/knoxdb/cache/rclru"
	"blockwatch.cc/knoxdb/store"
)

var (
	storeKey     = []byte("_store")
	storeMetaKey = []byte("_store_meta")
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

type GenericStore struct {
	name       string                       // printable store name
	opts       Options                      // runtime configuration options
	db         *DB                          // lower-level KV store (e.g. boltdb or badger)
	cache      rclru.Cache[uint64, *Buffer] // cache for improving data loads
	key        []byte                       // name of store's data bucket
	stats      TableStats                   // usage statistics
	isZeroCopy bool                         // read is zero copy (requires copy to reference safely)
}

func CreateGenericStore(d *DB, name string, opts Options) (*GenericStore, error) {
	opts = DefaultOptions.Merge(opts)
	if err := opts.Check(); err != nil {
		return nil, err
	}
	s := &GenericStore{
		name:       name,
		opts:       opts,
		db:         d,
		key:        append([]byte(name), storeKey...),
		isZeroCopy: d.db.IsZeroCopyRead(),
	}
	s.stats.TableName = name
	err := d.db.Update(func(dbTx store.Tx) error {
		data := dbTx.Bucket(s.key)
		if data != nil {
			return ErrStoreExists
		}
		_, err := dbTx.Root().CreateBucketIfNotExists(s.key)
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
		s.stats.CacheCapacity = int64(s.opts.CacheSizeMBytes())
	} else {
		s.cache = rclru.NewNoCache[uint64, *Buffer]()
	}
	log.Debugf("Created store %s", name)

	return s, nil
}

func OpenGenericStore(d *DB, name string, opts Options) (*GenericStore, error) {
	if opts.IsValid() {
		log.Debugf("Opening store %q with opts %#v", name, opts)
	} else {
		log.Debugf("Opening store %q with default opts", name)
	}
	s := &GenericStore{
		name:       name,
		db:         d,
		key:        append([]byte(name), storeKey...),
		isZeroCopy: d.db.IsZeroCopyRead(),
	}
	s.stats.TableName = name
	err := d.db.View(func(dbTx store.Tx) error {
		data := dbTx.Bucket(s.key)
		stats := data.Stats()
		s.stats.TotalSize = int64(stats.Size) // estimate only
		log.Debugf("pack: %s store opened with %d entries", name, stats.KeyN)
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
		s.stats.CacheCapacity = int64(s.opts.CacheSizeMBytes())
	} else {
		s.cache = rclru.NewNoCache[uint64, *Buffer]()
	}

	return s, nil
}

func (s *GenericStore) Name() string {
	return s.name
}

func (s *GenericStore) Engine() string {
	return s.db.Engine()
}

func (s *GenericStore) DB() *DB {
	return s.db
}

func (s *GenericStore) Options() Options {
	return s.opts
}

func (s *GenericStore) PurgeCache() {
	s.cache.Purge()
}

func (s *GenericStore) IsClosed() bool {
	return s.db == nil
}

func (s *GenericStore) Close() error {
	log.Debugf("pack: closing %s store", s.name)

	// unregister from db
	delete(s.db.stores, s.name)
	s.db = nil
	return nil
}

func (s *GenericStore) Stats() []TableStats {
	// copy store stats
	stats := s.stats
	// stats.TupleCount = s.meta.Rows

	// copy cache stats
	cs := s.cache.Stats()
	stats.CacheHits = cs.Hits
	stats.CacheMisses = cs.Misses
	stats.CacheInserts = cs.Inserts
	stats.CacheEvictions = cs.Evictions
	stats.CacheCount = cs.Count
	stats.CacheSize = cs.Size

	return []TableStats{stats}
}

func (s *GenericStore) Drop() error {
	s.cache.Purge()
	err := s.db.Update(func(tx *Tx) error {
		_ = tx.Root().DeleteBucket(append([]byte(s.name), storeKey...))
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// compat interface for bitset storage
func (s *GenericStore) GetValue64(key uint64, val any) error {
	if val == nil {
		return ErrNilValue
	}
	buf, ok := s.cache.Get(key)
	if !ok {
		var bkey [8]byte
		bigEndian.PutUint64(bkey[:], key)
		b, err := s.Get(bkey[:])
		if err != nil {
			return err
		}
		// cache (copy of) result
		if s.isZeroCopy {
			b = bytes.Clone(b)
		}
		buf = NewBuffer(b)
		buf.IncRef()
		s.cache.Add(key, buf)
	}

	atomic.AddInt64(&s.stats.QueriedTuples, 1)
	atomic.AddInt64(&s.stats.QueryCalls, 1)

	// decode if a decoder interface is found
	var err error
	if m, ok := val.(KeyValueUnmarshaler); ok {
		err = m.UnmarshalValue(buf.Bytes())
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

// compat interface for bitset storage
func (s *GenericStore) PutValue64(key uint64, val any) error {
	var (
		buf []byte
		err error
	)
	if val == nil {
		return ErrNilValue
	}
	if b, ok := val.([]byte); ok {
		buf = b
	} else if m, ok := val.(KeyValueMarshaler); ok {
		buf, err = m.MarshalValue()
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
		return err
	}
	var bkey [8]byte
	bigEndian.PutUint64(bkey[:], key)
	err = s.Put(bkey[:], buf)
	if err == nil {
		s.cache.Add(key, NewBuffer(buf))
	}
	return err
}

// compat interface for bitset storage
func (s *GenericStore) DeleteValue64(key uint64) error {
	s.cache.Remove(key)
	var bkey [8]byte
	bigEndian.PutUint64(bkey[:], key)
	return s.Del(bkey[:])
}

// low-level interface for direct KV storage access
func (s *GenericStore) Get(key []byte) ([]byte, error) {
	var ret []byte
	err := s.db.View(func(tx *Tx) error {
		b := tx.Bucket(s.key)
		if b == nil {
			return ErrBucketNotFound
		}
		buf := b.Get(key)
		if buf == nil {
			return ErrKeyNotFound
		}
		ret = buf
		return nil
	})
	if err == nil {
		atomic.AddInt64(&s.stats.BytesRead, int64(len(ret)))
	}
	return ret, err
}

func (s *GenericStore) Get64(key uint64) ([]byte, error) {
	var bkey [8]byte
	bigEndian.PutUint64(bkey[:], key)
	return s.Get(bkey[:])
}

func (s *GenericStore) Put(key, val []byte) error {
	prevSize := -1
	err := s.db.Update(func(tx *Tx) error {
		b := tx.Bucket(s.key)
		if b == nil {
			return ErrBucketNotFound
		}
		b.FillPercent(float64(s.opts.FillLevel) / 100.0)
		buf := b.Get(key)
		if buf != nil {
			prevSize = len(buf)
		} else {
			// s.meta.Rows++
		}
		return b.Put(key, val)
	})
	if err != nil {
		return err
	}
	sz := int64(len(key) + len(val))
	if prevSize >= 0 {
		// update
		atomic.AddInt64(&s.stats.UpdatedTuples, 1)
		atomic.AddInt64(&s.stats.UpdateCalls, 1)
		atomic.AddInt64(&s.stats.TotalSize, sz-int64(prevSize))
	} else {
		// insert
		atomic.AddInt64(&s.stats.InsertedTuples, 1)
		atomic.AddInt64(&s.stats.InsertCalls, 1)
		atomic.AddInt64(&s.stats.TupleCount, 1)
		atomic.AddInt64(&s.stats.TotalSize, sz)
	}
	atomic.AddInt64(&s.stats.BytesWritten, sz)
	return nil
}

func (s *GenericStore) Put64(key uint64, val []byte) error {
	var bkey [8]byte
	bigEndian.PutUint64(bkey[:], key)
	return s.Put(bkey[:], val)
}

func (s *GenericStore) Del(key []byte) error {
	prevSize := -1
	err := s.db.Update(func(tx *Tx) error {
		b := tx.Bucket(s.key)
		if b == nil {
			return ErrBucketNotFound
		}
		buf := b.Get(key)
		if buf != nil {
			prevSize = len(buf)
			// s.meta.Rows--
		}
		return b.Delete(key)
	})
	if err == nil && prevSize >= 0 {
		atomic.AddInt64(&s.stats.TupleCount, -1)
		atomic.AddInt64(&s.stats.DeletedTuples, 1)
		atomic.AddInt64(&s.stats.DeleteCalls, 1)
		atomic.AddInt64(&s.stats.TotalSize, -int64(prevSize))
	}
	return err
}

func (s *GenericStore) Del64(key uint64) error {
	var bkey [8]byte
	bigEndian.PutUint64(bkey[:], key)
	return s.Del(bkey[:])
}

func (s *GenericStore) GetTx(tx *Tx, key []byte) []byte {
	var ret []byte
	data := tx.tx.Bucket(s.key)
	if data == nil {
		return nil
	}
	buf := data.Get(key)
	if buf != nil {
		if s.isZeroCopy {
			ret = make([]byte, len(buf))
			copy(ret, buf)
		} else {
			ret = buf
		}
	}
	atomic.AddInt64(&s.stats.BytesRead, int64(len(ret)))
	return ret
}

func (s *GenericStore) PutTx(tx *Tx, key, val []byte) ([]byte, error) {
	prevSize, sz := -1, len(key)+len(val)
	data := tx.tx.Bucket(s.key)
	if data == nil {
		return nil, ErrBucketNotFound
	}
	buf := s.GetTx(tx, key)
	if buf != nil {
		prevSize = len(buf) + len(key)
	} else {
		// s.meta.Rows++
	}
	err := data.Put(key, val)
	if err != nil {
		return nil, err
	}
	if prevSize >= 0 {
		// update
		atomic.AddInt64(&s.stats.UpdatedTuples, 1)
		atomic.AddInt64(&s.stats.UpdateCalls, 1)
		atomic.AddInt64(&s.stats.TotalSize, int64(sz-prevSize))
	} else {
		// insert
		atomic.AddInt64(&s.stats.InsertedTuples, 1)
		atomic.AddInt64(&s.stats.TupleCount, 1)
		atomic.AddInt64(&s.stats.TotalSize, int64(sz))
	}
	atomic.AddInt64(&s.stats.BytesWritten, int64(sz))
	return buf, nil
}

func (s *GenericStore) DelTx(tx *Tx, key []byte) ([]byte, error) {
	prevSize := -1
	data := tx.tx.Bucket(s.key)
	if data == nil {
		return nil, ErrBucketNotFound
	}
	buf := data.Get(key)
	if buf != nil {
		prevSize = len(buf)
		// s.meta.Rows--
	}
	err := data.Delete(key)
	if err == nil && prevSize >= 0 {
		atomic.AddInt64(&s.stats.TupleCount, -1)
		atomic.AddInt64(&s.stats.DeletedTuples, 1)
		atomic.AddInt64(&s.stats.TotalSize, -int64(prevSize))
	}
	return buf, err
}

// poor man's query interface where user generates key range
// and decodes key + value
func (s *GenericStore) PrefixRange(prefix []byte, fn func(k, v []byte) error) error {
	err := s.db.View(func(tx *Tx) error {
		b := tx.Bucket(s.key)
		if b == nil {
			return ErrBucketNotFound
		}
		c := b.Range(prefix)
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			err := fn(c.Key(), c.Value())
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil || err == EndStream {
		return nil
	}
	return err
}

func (s *GenericStore) Range(from, to []byte, fn func(k, v []byte) error) error {
	err := s.db.View(func(tx *Tx) error {
		b := tx.Bucket(s.key)
		if b == nil {
			return ErrBucketNotFound
		}
		c := b.Cursor()
		defer c.Close()
		for ok := c.Seek(from); ok && bytes.Compare(c.Key(), to) <= 0; ok = c.Next() {
			err := fn(c.Key(), c.Value())
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil || err == EndStream {
		return nil
	}
	return err
}
