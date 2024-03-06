// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"context"
	"encoding"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"

	"blockwatch.cc/knoxdb/cache/rclru"
	"blockwatch.cc/knoxdb/encoding/bitmap"
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

type Store struct {
	name       string                       // printable store name
	fields     FieldList                    // list of field names and types
	meta       TableMeta                    // authoritative metadata
	opts       Options                      // runtime configuration options
	db         *DB                          // lower-level KV store (e.g. boltdb or badger)
	cache      rclru.Cache[uint64, *Buffer] // cache for improving data loads
	key        []byte                       // name of store's data bucket
	metakey    []byte                       // name of store's metadata bucket
	indexes    []*StoreIndex                // list of indexes
	stats      TableStats                   // usage statistics
	pkindex    int                          // field index for primary key (if any)
	isZeroCopy bool                         // read is zero copy (requires copy to reference safely)
}

type StoreIndex struct {
	name   string    // printable index name
	key    []byte    // name of index data bucket
	fields FieldList // list of field names and types
	idxs   []int     // list of field index positions in original data
	store  *Store    // lower-level KV store (e.g. boltdb or badger)
}

func (i StoreIndex) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.fields)
}

func (i *StoreIndex) UnmarshalJSON(buf []byte) error {
	return json.Unmarshal(buf, &i.fields)
}

func (s *Store) Name() string {
	return s.name
}

func (s *Store) Fields() FieldList {
	return s.fields
}

func (s *Store) Database() *DB {
	return s.db
}

func (s *Store) Options() Options {
	return s.opts
}

func (s *Store) NextSequence() uint64 {
	// todo: maybe use bucket sequence for better crash safety
	s.meta.Sequence++
	return s.meta.Sequence
}

func (s *Store) IsClosed() bool {
	return s.db == nil
}

func (s *Store) Close() error {
	log.Debugf("pack: closing %s store", s.name)

	// write metadata
	err := s.db.Update(func(tx store.Tx) error {
		meta := tx.Bucket(s.metakey)
		if meta == nil {
			return ErrBucketNotFound
		}
		buf, err := json.Marshal(s.meta)
		if err != nil {
			return err
		}
		return meta.Put(metaKey, buf)
	})
	if err != nil {
		return err
	}

	// unregister from db
	delete(s.db.stores, s.name)
	s.db = nil
	return nil
}

func (s *Store) Stats() []TableStats {
	// copy store stats
	stats := s.stats
	stats.TupleCount = s.meta.Rows

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

func (s *Store) PurgeCache() {
	s.cache.Purge()
}

func (d *DB) CreateStore(name string, fields FieldList, opts Options) (*Store, error) {
	// if !fields.HasPk() {
	// 	return nil, ErrNoPk
	// }
	opts = DefaultOptions.Merge(opts)
	if err := opts.Check(); err != nil {
		return nil, err
	}
	s := &Store{
		name:       name,
		fields:     fields,
		opts:       opts,
		db:         d,
		key:        append([]byte(name), storeKey...),
		metakey:    append([]byte(name), storeMetaKey...),
		indexes:    make([]*StoreIndex, 0),
		isZeroCopy: d.db.IsZeroCopyRead(),
	}
	if !fields.HasCompositePk() {
		s.pkindex = fields.PkIndex()
	}
	s.stats.TableName = name
	err := d.db.Update(func(dbTx store.Tx) error {
		data := dbTx.Bucket(s.key)
		if data != nil {
			return ErrStoreExists
		}
		meta, err := dbTx.Root().CreateBucketIfNotExists(s.metakey)
		if err != nil {
			return err
		}
		_, err = dbTx.Root().CreateBucketIfNotExists(s.key)
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
		buf, err = json.Marshal(s.fields)
		if err != nil {
			return err
		}
		err = meta.Put(fieldsKey, buf)
		if err != nil {
			return err
		}
		buf, err = json.Marshal(s.meta)
		if err != nil {
			return err
		}
		err = meta.Put(metaKey, buf)
		if err != nil {
			return err
		}
		buf, err = json.Marshal(s.indexes)
		if err != nil {
			return err
		}
		err = meta.Put(indexesKey, buf)
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
	log.Debugf("Created bitpool %s", name)
	d.stores[name] = s
	return s, nil
}

func (d *DB) CreateStoreIfNotExists(name string, fields FieldList, opts Options) (*Store, error) {
	s, err := d.CreateStore(name, fields, opts)
	if err != nil {
		if err != ErrStoreExists {
			return nil, err
		}
		s, err = d.OpenStore(name, opts)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (d *DB) DropStore(name string) error {
	s, err := d.OpenStore(name, NoOptions)
	if err != nil {
		return err
	}
	s.cache.Purge()
	err = d.db.Update(func(dbTx store.Tx) error {
		for _, idx := range s.indexes {
			_ = dbTx.Root().DeleteBucket(idx.key)
		}
		_ = dbTx.Root().DeleteBucket(append([]byte(name), storeKey...))
		_ = dbTx.Root().DeleteBucket(append([]byte(name), storeMetaKey...))
		return nil
	})
	if err != nil {
		return err
	}
	delete(d.stores, s.name)
	s = nil
	return nil
}

func (d *DB) OpenStore(name string, opts Options) (*Store, error) {
	if s, ok := d.stores[name]; ok {
		return s, nil
	}
	if opts.IsValid() {
		log.Debugf("Opening store %q with opts %#v", name, opts)
	} else {
		log.Debugf("Opening store %q with default opts", name)
	}
	s := &Store{
		name:       name,
		db:         d,
		key:        append([]byte(name), storeKey...),
		metakey:    append([]byte(name), storeMetaKey...),
		indexes:    make([]*StoreIndex, 0),
		isZeroCopy: d.db.IsZeroCopyRead(),
	}
	s.stats.TableName = name
	err := d.db.View(func(dbTx store.Tx) error {
		data := dbTx.Bucket(s.key)
		meta := dbTx.Bucket(s.metakey)
		if data == nil || meta == nil {
			return ErrNoStore
		}
		s.stats.TotalSize = int64(data.Stats().Size) // estimate only
		buf := meta.Get(optsKey)
		if buf == nil {
			return fmt.Errorf("pack: missing options for store %q", name)
		}
		err := json.Unmarshal(buf, &s.opts)
		if err != nil {
			return err
		}
		if opts.IsValid() {
			s.opts = s.opts.Merge(opts)
		}
		buf = meta.Get(fieldsKey)
		if buf == nil {
			return fmt.Errorf("pack: missing fields for store %q", name)
		}
		err = json.Unmarshal(buf, &s.fields)
		if err != nil {
			return fmt.Errorf("pack: cannot read fields for store %q: %v", name, err)
		}
		if !s.fields.HasCompositePk() {
			s.pkindex = s.fields.PkIndex()
		}
		buf = meta.Get(metaKey)
		if buf == nil {
			return fmt.Errorf("pack: missing metadata for store %q", name)
		}
		err = json.Unmarshal(buf, &s.meta)
		if err != nil {
			return fmt.Errorf("pack: cannot read metadata for store %q: %v", name, err)
		}
		buf = meta.Get(indexesKey)
		if buf == nil {
			return fmt.Errorf("pack: missing indexes for store %q", name)
		}
		err = json.Unmarshal(buf, &s.indexes)
		if err != nil {
			return fmt.Errorf("pack: cannot read indexes for store %q: %v", name, err)
		}
		log.Debugf("pack: %s store opened with %d entries and seq %d", name, s.meta.Rows, s.meta.Sequence)
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

	for _, idx := range s.indexes {
		idx.store = s
		idx.name = idx.fields.String()
		idx.key = []byte(fmt.Sprintf("%s_index_%016x", s.name, idx.fields.Hash()))
		idx.idxs = make([]int, len(idx.fields))
		for i, f := range idx.fields {
			orig := s.fields.Find(f.Name)
			if orig == nil {
				return nil, fmt.Errorf("pack: %s missing field %q referenced by index %s", s.name, f.Name, idx.fields)
			}
			idx.idxs[i] = orig.Index
		}
	}

	d.stores[name] = s
	return s, nil
}

func (s *Store) CreateIndex(fields FieldList, _ Options) error {
	// opts = DefaultOptions.Merge(opts)
	// if err := opts.Check(); err != nil {
	// 	return nil, err
	// }

	// generate unique index name
	idx := &StoreIndex{
		name:   fields.String(),
		key:    []byte(fmt.Sprintf("%s_index_%016x", s.name, fields.Hash())),
		fields: fields,
		idxs:   make([]int, len(fields)),
		store:  s,
	}
	for i, f := range fields {
		orig := s.fields.Find(f.Name)
		if orig == nil {
			return fmt.Errorf("pack: %s missing field %q referenced by index %s", s.name, f.Name, fields)
		}
		idx.idxs[i] = orig.Index
	}

	err := s.db.Update(func(dbTx store.Tx) error {
		// check if index exists
		data := dbTx.Bucket(idx.key)
		if data != nil {
			return ErrIndexExists
		}

		// create index bucket
		_, err := dbTx.Root().CreateBucketIfNotExists(idx.key)
		if err != nil {
			return err
		}

		// store index metadata
		indexes := append(s.indexes, idx)
		meta := dbTx.Bucket(s.metakey)
		if meta == nil {
			return fmt.Errorf("pack: missing metadata for store %q", s.name)
		}
		buf, err := json.Marshal(indexes)
		if err != nil {
			return err
		}
		if err := meta.Put(indexesKey, buf); err != nil {
			return err
		}
		s.indexes = indexes
		return nil
	})
	if err != nil {
		return err
	}

	// populate index (when data exists)
	// FIXME: make this exclusive to concurrent write transactions
	if s.meta.Sequence > 0 {
		return idx.Rebuild()
	}
	return nil
}

func (s *Store) CreateIndexIfNotExists(fields FieldList, opts Options) error {
	err := s.CreateIndex(fields, opts)
	if err != nil && err != ErrIndexExists {
		return err
	}
	return nil
}

func (s *Store) DropIndex(fields FieldList) error {
	key := []byte(fmt.Sprintf("%s_index_%016x", s.name, fields.Hash()))
	for i, idx := range s.indexes {
		if !bytes.Equal(idx.key, key) {
			continue
		}
		err := s.db.Update(func(dbTx store.Tx) error {
			_ = dbTx.Root().DeleteBucket(idx.key)

			// remove from list
			s.indexes = append(s.indexes[:i], s.indexes[i+1:]...)

			// store index metadata
			meta := dbTx.Bucket(s.metakey)
			if meta == nil {
				return fmt.Errorf("pack: missing metadata for store %q", s.name)
			}
			buf, err := json.Marshal(s.indexes)
			if err != nil {
				return err
			}
			return meta.Put(indexesKey, buf)
		})
		if err != nil {
			return err
		}
		break
	}
	return nil
}

func (idx *StoreIndex) Rebuild() error {
	// clear index data bucket
	err := idx.store.db.Update(func(dbTx store.Tx) error {
		_ = dbTx.Root().DeleteBucket(idx.key)
		_, err := dbTx.Root().CreateBucketIfNotExists(idx.key)
		return err
	})
	if err != nil {
		return err
	}

	// walk data bucket and insert into index
	dbTx, err := idx.store.db.db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		dbTx.Rollback()
	}()
	index := dbTx.Bucket(idx.key)
	if index == nil {
		return ErrIndexNotFound
	}
	data := dbTx.Bucket(idx.store.key)
	if data == nil {
		return ErrIndexNotFound
	}
	c := data.Cursor()
	var count int
	for ok := c.First(); ok; ok = c.Next() {
		ikey := idx.store.fields.CopyData(idx.idxs, c.Value())
		// TODO: this could benefit from a batch write interface
		if err := index.Put(ikey, nil); err != nil {
			return err
		}
		count++
		if count >= txMaxSize {
			lastKey := c.Key()
			// commit data
			if err := dbTx.Commit(); err != nil {
				return err
			}
			// refresh references with a new write tx
			dbTx, err = idx.store.db.db.Begin(true)
			if err != nil {
				return err
			}
			data = dbTx.Bucket(idx.store.key)
			index = dbTx.Bucket(idx.key)
			c = data.Cursor()
			c.Seek(lastKey)
		}
	}
	return dbTx.Commit()
}

func (idx *StoreIndex) AddTx(tx *Tx, prev, val []byte) error {
	pkey := idx.store.fields.CopyData(idx.idxs, prev)
	vkey := idx.store.fields.CopyData(idx.idxs, val)
	sameKey := bytes.Equal(pkey, vkey)
	if pkey != nil && !sameKey {
		// log.Infof("Idx %s DEL %s", idx.name, hex.EncodeToString(pkey))
		_ = idx.delTx(tx, pkey)
	}
	if vkey != nil && !sameKey {
		// log.Infof("Idx %s ADD %s", idx.name, hex.EncodeToString(vkey))
		return idx.putTx(tx, vkey)
	}
	return nil
}

func (idx *StoreIndex) DelTx(tx *Tx, prev []byte) error {
	pkey := idx.store.fields.CopyData(idx.idxs, prev)
	if pkey != nil {
		// log.Infof("Idx %s DEL %s", idx.name, hex.EncodeToString(pkey))
		return idx.delTx(tx, pkey)
	}
	return nil
}

func (idx *StoreIndex) putTx(tx *Tx, key []byte) error {
	data := tx.tx.Bucket(idx.key)
	return data.Put(key, nil)
}

func (idx *StoreIndex) delTx(tx *Tx, key []byte) error {
	data := tx.tx.Bucket(idx.key)
	return data.Delete(key)
}

func (idx *StoreIndex) Scan(prefix []byte) (bitmap.Bitmap, error) {
	bits := bitmap.New()
	err := idx.store.db.View(func(tx store.Tx) error {
		b := tx.Bucket(idx.key)
		if b == nil {
			return ErrBucketNotFound
		}
		// log.Infof("Idx %s SCAN %s", idx.name, hex.EncodeToString(prefix))
		c := b.Range(prefix, store.IndexCursor)
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			// assumes pk is last 8 bytes of key
			key := c.Key()
			u64 := binary.BigEndian.Uint64(key[len(key)-8:])
			bits.Set(u64)
			// log.Infof("> found PK %016x", u64)
		}
		return nil
	})
	return bits, err
}

func (idx *StoreIndex) ScanTx(tx *Tx, prefix []byte) (bitmap.Bitmap, error) {
	bits := bitmap.New()
	b := tx.tx.Bucket(idx.key)
	if b == nil {
		return bits, ErrBucketNotFound
	}
	// log.Infof("Idx %s SCAN %s", idx.name, hex.EncodeToString(prefix))
	c := b.Range(prefix, store.IndexCursor)
	defer c.Close()
	for ok := c.First(); ok; ok = c.Next() {
		// assumes pk is last 8 bytes of key
		key := c.Key()
		u64 := binary.BigEndian.Uint64(key[len(key)-8:])
		bits.Set(u64)
		// log.Infof("> found PK %016x", u64)
	}
	return bits, nil
}

func (idx *StoreIndex) RangeTx(tx *Tx, from, to []byte) (bitmap.Bitmap, error) {
	bits := bitmap.New()
	b := tx.tx.Bucket(idx.key)
	if b == nil {
		return bits, ErrBucketNotFound
	}
	c := b.Cursor(store.IndexCursor)
	defer c.Close()
	for ok := c.Seek(from); ok && bytes.Compare(c.Key(), to) < 0; ok = c.Next() {
		// assumes pk is last 8 bytes of key
		key := c.Key()
		u64 := binary.BigEndian.Uint64(key[len(key)-8:])
		bits.Set(u64)
	}
	return bits, nil
}

// compat interface for bitset storage
func (s *Store) GetValue64(key uint64, val any) error {
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
		buf = NewBuffer(b)
		buf.IncRef()
		// cache result
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
func (s *Store) PutValue64(key uint64, val any) error {
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
func (s *Store) DeleteValue64(key uint64) error {
	s.cache.Remove(key)
	var bkey [8]byte
	bigEndian.PutUint64(bkey[:], key)
	return s.Del(bkey[:])
}

// low-level interface for direct KV storage access
func (s *Store) Get(key []byte) ([]byte, error) {
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
		if s.isZeroCopy {
			ret = make([]byte, len(buf))
			copy(ret, buf)
		} else {
			ret = buf
		}
		return nil
	})
	if err == nil {
		atomic.AddInt64(&s.stats.BytesRead, int64(len(ret)))
	}
	return ret, err
}

func (s *Store) Get64(key uint64) ([]byte, error) {
	var bkey [8]byte
	bigEndian.PutUint64(bkey[:], key)
	return s.Get(bkey[:])
}

func (s *Store) Put(key, val []byte) error {
	prevSize := -1
	err := s.db.Update(func(tx store.Tx) error {
		b := tx.Bucket(s.key)
		if b == nil {
			return ErrBucketNotFound
		}
		b.FillPercent(float64(s.opts.FillLevel) / 100.0)
		buf := b.Get(key)
		if buf != nil {
			prevSize = len(buf)
		} else {
			s.meta.Rows++
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

func (s *Store) Put64(key uint64, val []byte) error {
	var bkey [8]byte
	bigEndian.PutUint64(bkey[:], key)
	return s.Put(bkey[:], val)
}

func (s *Store) Del(key []byte) error {
	prevSize := -1
	err := s.db.Update(func(tx store.Tx) error {
		b := tx.Bucket(s.key)
		if b == nil {
			return ErrBucketNotFound
		}
		buf := b.Get(key)
		if buf != nil {
			prevSize = len(buf)
			s.meta.Rows--
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

func (s *Store) Del64(key uint64) error {
	var bkey [8]byte
	bigEndian.PutUint64(bkey[:], key)
	return s.Del(bkey[:])
}

// Marshaler interface, a more performant alternative to type based
// {Get|Put|Delete}Value methods, but incompatible with secondary indexes.
type KeyValueMarshaler interface {
	MarshalKey() ([]byte, error)
	MarshalValue() ([]byte, error)
}

type KeyValueUnmarshaler interface {
	UnmarshalKey([]byte) error
	UnmarshalValue([]byte) error
}

func (s *Store) GetValue(val any) error {
	// use PK based indexing when struct defines a PK
	if s.pkindex >= 0 {
		pkv := reflect.Indirect(reflect.ValueOf(val)).Field(s.pkindex)
		return s.GetValue64(pkv.Uint(), val)
	}

	// try marshaler interface
	enc, ok := val.(KeyValueMarshaler)
	dec, ok2 := val.(KeyValueUnmarshaler)
	if ok && ok2 {
		key, err := enc.MarshalKey()
		if err != nil {
			return err
		}
		buf, err := s.Get(key)
		if err != nil {
			return err
		}
		return dec.UnmarshalValue(buf)
	}

	return ErrNoPk
}

func (s *Store) GetTx(tx *Tx, key []byte) []byte {
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

func (s *Store) PutValue(val any) error {
	// use PK based indexing when struct defines a PK
	if s.pkindex >= 0 {
		var pk uint64
		pkv := reflect.Indirect(reflect.ValueOf(val)).Field(s.pkindex)
		pk = pkv.Uint()
		if pk == 0 {
			// set next pk value if zero
			pk = s.NextSequence()
			pkv.SetUint(pk)
		}
	}

	// try marshaler interface (note: not compatible with indexes)
	var (
		key, buf, prev []byte
		err            error
	)
	if enc, ok := val.(KeyValueMarshaler); ok {
		key, err = enc.MarshalKey()
		if err != nil {
			return err
		}
		buf, err = enc.MarshalValue()
		if err != nil {
			return err
		}
	} else {
		// construct uint64 key or composite key for non-pk structs
		key, err = s.fields.CompositePk().Encode(val)
		if err != nil {
			return err
		}

		// encode all fields into value (required for indexes)
		buf, err = s.fields.Encode(val)
		if err != nil {
			return err
		}
	}

	// open write transaction
	tx, err := s.db.Tx(true)
	if err != nil {
		return err
	}

	// cleanup on exit
	defer func() {
		tx.Rollback()
	}()

	// write value
	// log.Infof("PUT %s", hex.EncodeToString(key))
	prev, err = s.PutTx(tx, key, buf)
	if err != nil {
		return err
	}

	// update indexes
	for _, idx := range s.indexes {
		idx.AddTx(tx, prev, buf)
	}

	return tx.Commit()
}

func (s *Store) PutTx(tx *Tx, key, val []byte) ([]byte, error) {
	prevSize, sz := -1, len(key)+len(val)
	data := tx.tx.Bucket(s.key)
	if data == nil {
		return nil, ErrBucketNotFound
	}
	buf := s.GetTx(tx, key)
	if buf != nil {
		prevSize = len(buf) + len(key)
	} else {
		s.meta.Rows++
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
		atomic.AddInt64(&s.stats.InsertCalls, 1)
		atomic.AddInt64(&s.stats.TupleCount, 1)
		atomic.AddInt64(&s.stats.TotalSize, int64(sz))
	}
	atomic.AddInt64(&s.stats.BytesWritten, int64(sz))
	return buf, nil
}

func (s *Store) DeleteValue(val any) error {
	// use PK based indexing when struct defines a PK
	if s.pkindex >= 0 {
		var pk uint64
		pkv := reflect.Indirect(reflect.ValueOf(val)).Field(s.pkindex)
		pk = pkv.Uint()
		if pk == 0 {
			return nil
		}
	}

	// try custom interface (note: not compatible with indexes)
	var (
		key, prev []byte
		err       error
	)
	if enc, ok := val.(KeyValueMarshaler); ok {
		key, err = enc.MarshalKey()
		if err != nil {
			return err
		}
	} else {
		// for non-pk structs construct composite key
		key, err = s.fields.CompositePk().Encode(val)
		if err != nil {
			return err
		}
	}

	// open write transaction
	tx, err := s.db.Tx(true)
	if err != nil {
		return err
	}

	// cleanup on exit
	defer func() {
		tx.Rollback()
	}()

	// remove key
	// log.Infof("DEL %s", hex.EncodeToString(key))
	prev, err = s.DelTx(tx, key)
	if err != nil {
		return err
	}

	// update indexes
	for _, idx := range s.indexes {
		idx.DelTx(tx, prev)
	}
	return tx.Commit()
}

func (s *Store) DelTx(tx *Tx, key []byte) ([]byte, error) {
	prevSize := -1
	data := tx.tx.Bucket(s.key)
	if data == nil {
		return nil, ErrBucketNotFound
	}
	buf := data.Get(key)
	if buf != nil {
		prevSize = len(buf)
		s.meta.Rows--
	}
	err := data.Delete(key)
	if err == nil && prevSize >= 0 {
		atomic.AddInt64(&s.stats.TupleCount, -1)
		atomic.AddInt64(&s.stats.DeletedTuples, 1)
		atomic.AddInt64(&s.stats.DeleteCalls, 1)
		atomic.AddInt64(&s.stats.TotalSize, -int64(prevSize))
	}
	return buf, err
}

// poor man's query interface where user generates key range
// and decodes key + value
func (s *Store) PrefixRange(prefix []byte, fn func(k, v []byte) error) error {
	err := s.db.View(func(tx store.Tx) error {
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

func (s *Store) Range(from, to []byte, fn func(k, v []byte) error) error {
	err := s.db.View(func(tx store.Tx) error {
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

// Table Query Interface
// - requires main data bucket to be indexed by pk (uint64)
// - generate index scan ranges from query conditions
// - run index scans -> bitsets
// - merge bitsets along condition tree
// - resolve result from value bucket via final bitset
// - append row data to Result
// - result decoder can skip unused fields

func (s *Store) Query(ctx context.Context, q Query) (*Result, error) {
	var (
		bits bitmap.Bitmap
		key  [8]byte
	)
	atomic.AddInt64(&s.stats.QueryCalls, 1)

	// check conditions match schema
	err := q.Compile()
	if err != nil {
		return nil, err
	}

	// open read transaction
	tx, err := s.db.Tx(false)
	if err != nil {
		return nil, err
	}

	// cleanup on exit
	defer func() {
		atomic.AddInt64(&s.stats.QueriedTuples, int64(q.stats.RowsMatched))
		tx.Rollback()
		q.Close()
		bits.Free()
	}()

	data := tx.tx.Bucket(s.key)
	if data == nil {
		return nil, ErrBucketNotFound
	}

	// prepare result
	res := &Result{
		fields:  s.fields,
		offsets: make([]int, 0, q.Limit),
		values:  make([]byte, 0, q.Limit*64),
	}

	// run index queries
	// q.stats.IndexLookups = ??
	err = q.QueryIndexes(ctx, tx)
	if err != nil {
		return nil, err
	}

	// handle cases
	switch {
	case q.conds.IsProcessed():
		// 1: full index query -> everything is resolved, walk bitset
		it := q.conds.Bits.Bitmap.NewIterator()
		for id := it.Next(); id > 0; id = it.Next() {
			// skip offset
			if q.Offset > 0 {
				q.Offset--
				continue
			}
			bigEndian.PutUint64(key[:], id)
			val := data.Get(key[:])
			if val == nil {
				// warn on indexed but missing pks
				log.Warnf("Missing PK %d from index scan in query %s", id, q.Name)
				continue
			}
			res.offsets = append(res.offsets, len(res.values))
			res.values = append(res.values, val...)

			// q.stats.RowsScanned++ // TODO: add to stats

			// apply limit
			q.stats.RowsMatched++
			if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
				break
			}
		}
	case !q.conds.OrKind:
		// 2: partial index query & root = AND: walk bitset but check each value
		it := q.conds.Bits.Bitmap.NewIterator()
		val := NewValue(s.fields)
		for id := it.Next(); id > 0; id = it.Next() {
			bigEndian.PutUint64(key[:], id)
			buf := data.Get(key[:])
			if buf == nil {
				// warn on indexed but missing pks
				log.Warnf("Missing PK %d from index scan in query %s", id, q.Name)
				continue
			}

			// check conditions
			if !q.conds.MatchValue(val.Reset(buf)) {
				continue
			}

			// skip offset
			if q.Offset > 0 {
				q.Offset--
				continue
			}

			res.offsets = append(res.offsets, len(res.values))
			res.values = append(res.values, buf...)

			// apply limit
			q.stats.RowsMatched++
			if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
				break
			}
		}
	default:
		// 3: partial index query & root = OR: walk full table and check each value
		// 4: no index query: walk full table and check each value
		c := data.Cursor(store.ForwardCursor)
		val := NewValue(s.fields)
		for ok := c.First(); ok; ok = c.Next() {
			buf := c.Value()

			// check conditions
			if !q.conds.MatchValue(val.Reset(buf)) {
				continue
			}

			// skip offset
			if q.Offset > 0 {
				q.Offset--
				continue
			}

			res.offsets = append(res.offsets, len(res.values))
			res.values = append(res.values, buf...)

			// apply limit
			q.stats.RowsMatched++
			if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
				break
			}
		}
	}
	q.stats.ScanTime = q.Tick()

	return res, nil
}

func (s *Store) Stream(ctx context.Context, q Query, fn func(Row) error) error {
	var (
		bits bitmap.Bitmap
		key  [8]byte
	)
	atomic.AddInt64(&s.stats.StreamCalls, 1)

	// check conditions match schema
	err := q.Compile()
	if err != nil {
		return err
	}

	// open read transaction
	tx, err := s.db.Tx(false)
	if err != nil {
		return err
	}

	// cleanup on exit
	defer func() {
		q.stats.ScanTime = q.Tick()
		atomic.AddInt64(&s.stats.QueriedTuples, int64(q.stats.RowsMatched))
		tx.Rollback()
		q.Close()
		bits.Free()
	}()

	data := tx.tx.Bucket(s.key)
	if data == nil {
		return ErrBucketNotFound
	}

	// prepare result
	res := &Result{
		fields:  s.fields,
		offsets: make([]int, 1),
	}

	// run index queries
	// q.stats.IndexLookups = ??
	err = q.QueryIndexes(ctx, tx)
	if err != nil {
		return err
	}

	// handle cases
	switch {
	case q.conds.IsProcessed():
		// 1: full index query -> everything is resolved, walk bitset
		it := q.conds.Bits.Bitmap.NewIterator()
		for id := it.Next(); id > 0; id = it.Next() {
			// skip offset
			if q.Offset > 0 {
				q.Offset--
				continue
			}
			bigEndian.PutUint64(key[:], id)
			val := data.Get(key[:])
			if val == nil {
				// warn on indexed but missing pks
				log.Warnf("Missing PK %d from index scan in query %s", id, q.Name)
				continue
			}
			q.stats.RowsMatched++
			// q.stats.RowsScanned++ // TODO: add to stats

			res.values = val
			if err := fn(Row{res: res, n: 0}); err != nil {
				if err != EndStream {
					return err
				}
				return nil
			}

			// apply limit
			if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
				break
			}
		}
	case !q.conds.OrKind:
		// 2: partial index query & root = AND: walk bitset but check each value
		it := q.conds.Bits.Bitmap.NewIterator()
		val := NewValue(s.fields)
		for id := it.Next(); id > 0; id = it.Next() {
			bigEndian.PutUint64(key[:], id)
			buf := data.Get(key[:])
			if buf == nil {
				// warn on indexed but missing pks
				log.Warnf("Missing PK %d from index scan in query %s", id, q.Name)
				continue
			}

			// check conditions
			if !q.conds.MatchValue(val.Reset(buf)) {
				continue
			}

			// skip offset
			if q.Offset > 0 {
				q.Offset--
				continue
			}

			res.values = buf
			if err := fn(Row{res: res, n: 0}); err != nil {
				if err != EndStream {
					return err
				}
				return nil
			}

			// apply limit
			q.stats.RowsMatched++
			if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
				break
			}
		}
	default:
		// 3: partial index query & root = OR: walk full table and check each value
		// 4: no index query: walk full table and check each value
		c := data.Cursor(store.ForwardCursor)
		val := NewValue(s.fields)
		for ok := c.First(); ok; ok = c.Next() {
			buf := c.Value()

			// check conditions
			if !q.conds.MatchValue(val.Reset(buf)) {
				continue
			}

			// skip offset
			if q.Offset > 0 {
				q.Offset--
				continue
			}

			res.values = buf
			if err := fn(Row{res: res, n: 0}); err != nil {
				if err != EndStream {
					return err
				}
				return nil
			}

			// apply limit
			q.stats.RowsMatched++
			if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
				break
			}
		}
	}

	return nil
}

func (s *Store) Delete(ctx context.Context, q Query) (int64, error) {
	// TODO
	return 0, nil
}

func (s *Store) Count(ctx context.Context, q Query) (int64, error) {
	// TODO
	return 0, nil
}
