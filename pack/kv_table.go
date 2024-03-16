// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"

	"blockwatch.cc/knoxdb/encoding/bitmap"
	"blockwatch.cc/knoxdb/store"
)

var _ Table = (*KeyValueTable)(nil)

type KeyValueTable struct {
	name       string           // printable store name
	fields     FieldList        // list of field names and types
	meta       TableMeta        // authoritative metadata
	opts       Options          // runtime configuration options
	db         *DB              // lower-level KV store (e.g. boltdb or badger)
	key        []byte           // name of store's data bucket
	metakey    []byte           // name of store's metadata bucket
	indexes    []*KeyValueIndex // list of indexes
	stats      TableStats       // usage statistics
	pkindex    int              // field index for primary key (if any)
	isZeroCopy bool             // read is zero copy (requires copy to reference safely)
}

func CreateKeyValueTable(d *DB, name string, fields FieldList, opts Options) (*KeyValueTable, error) {
	// if !fields.HasPk() {
	//  return nil, ErrNoPk
	// }
	opts = DefaultOptions.Merge(opts)
	if err := opts.Check(); err != nil {
		return nil, err
	}
	t := &KeyValueTable{
		name:       name,
		fields:     fields,
		opts:       opts,
		db:         d,
		key:        append([]byte(name), storeKey...),
		metakey:    append([]byte(name), storeMetaKey...),
		indexes:    make([]*KeyValueIndex, 0),
		isZeroCopy: d.db.IsZeroCopyRead(),
	}
	if !fields.HasCompositePk() {
		t.pkindex = fields.PkIndex()
	}
	t.stats.TableName = name
	err := d.Update(func(tx *Tx) error {
		data := tx.Bucket(t.key)
		if data != nil {
			return ErrTableExists
		}
		meta, err := tx.Root().CreateBucketIfNotExists(t.metakey)
		if err != nil {
			return err
		}
		_, err = tx.Root().CreateBucketIfNotExists(t.key)
		if err != nil {
			return err
		}
		buf, err := json.Marshal(t.opts)
		if err != nil {
			return err
		}
		err = meta.Put(optsKey, buf)
		if err != nil {
			return err
		}
		buf, err = json.Marshal(t.fields)
		if err != nil {
			return err
		}
		err = meta.Put(fieldsKey, buf)
		if err != nil {
			return err
		}
		buf, err = json.Marshal(t.meta)
		if err != nil {
			return err
		}
		err = meta.Put(metaKey, buf)
		if err != nil {
			return err
		}
		err = meta.Put(indexesKey, []byte(`[]`))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	log.Debugf("Created table %s", name)
	return t, nil
}

func OpenKeyValueTable(d *DB, name string, opts Options) (*KeyValueTable, error) {
	if opts.IsValid() {
		log.Debugf("Opening kv table %q with opts %#v", name, opts)
	} else {
		log.Debugf("Opening kv table %q with default opts", name)
	}
	t := &KeyValueTable{
		name:       name,
		db:         d,
		key:        append([]byte(name), storeKey...),
		metakey:    append([]byte(name), storeMetaKey...),
		indexes:    make([]*KeyValueIndex, 0),
		isZeroCopy: d.db.IsZeroCopyRead(),
	}
	t.stats.TableName = name
	var indexes []IndexData
	err := t.db.View(func(tx *Tx) error {
		data := tx.Bucket(t.key)
		meta := tx.Bucket(t.metakey)
		if data == nil || meta == nil {
			return ErrNoTable
		}
		t.stats.TotalSize = int64(data.Stats().Size) // estimate only
		buf := meta.Get(optsKey)
		if buf == nil {
			return fmt.Errorf("pack: missing options for store %q", name)
		}
		err := json.Unmarshal(buf, &t.opts)
		if err != nil {
			return err
		}
		if opts.IsValid() {
			t.opts = t.opts.Merge(opts)
		}
		buf = meta.Get(fieldsKey)
		if buf == nil {
			return fmt.Errorf("pack: missing fields for store %q", name)
		}
		err = json.Unmarshal(buf, &t.fields)
		if err != nil {
			return fmt.Errorf("pack: cannot read fields for store %q: %v", name, err)
		}
		if !t.fields.HasCompositePk() {
			t.pkindex = t.fields.PkIndex()
		}
		buf = meta.Get(metaKey)
		if buf == nil {
			return fmt.Errorf("pack: missing metadata for store %q", name)
		}
		err = json.Unmarshal(buf, &t.meta)
		if err != nil {
			return fmt.Errorf("pack: cannot read metadata for store %q: %v", name, err)
		}
		buf = meta.Get(indexesKey)
		if buf == nil {
			return fmt.Errorf("pack: missing indexes for store %q", name)
		}
		err = json.Unmarshal(buf, &indexes)
		if err != nil {
			return fmt.Errorf("pack: cannot read indexes for store %q: %v", name, err)
		}
		log.Debugf("pack: %s store opened with %d entries and seq %d", name, t.meta.Rows, t.meta.Sequence)
		return nil
	})
	if err != nil {
		return nil, err
	}

	for _, v := range indexes {
		idx, err := OpenKeyValueIndex(t, v.Kind, v.Fields, opts)
		if err != nil {
			return nil, err
		}
		t.indexes = append(t.indexes, idx)
	}

	return t, nil
}

func (t *KeyValueTable) CreateIndex(kind IndexKind, fields FieldList, opts Options) error {
	// TODO: add pk column name if missing from fields
	idx, err := CreateKeyValueIndex(t, kind, fields, opts)
	if err != nil {
		return err
	}

	// add index to table's list of indexes and store the list
	t.indexes = append(t.indexes, idx)

	data := make([]IndexData, len(t.indexes))
	for i, v := range t.indexes {
		data[i].Kind = v.Kind()
		data[i].Fields = v.Fields()
	}

	err = t.db.Update(func(tx *Tx) error {
		meta := tx.Bucket(t.metakey)
		if meta == nil {
			return fmt.Errorf("pack: table %s: missing metadata bucket", t.name)
		}
		buf, err := json.Marshal(data)
		if err != nil {
			return err
		}
		return meta.Put(indexesKey, buf)
	})
	if err != nil {
		return err
	}

	// populate index (when data exists)
	// FIXME: make this concurrent and use context for shutdown
	if t.meta.Sequence > 0 {
		return t.RebuildIndex(idx)
	}
	return nil
}

func (t *KeyValueTable) CreateIndexIfNotExists(kind IndexKind, fields FieldList, opts Options) error {
	err := t.CreateIndex(kind, fields, opts)
	if err != nil && err != ErrIndexExists {
		return err
	}
	return nil
}

func (t *KeyValueTable) DropIndex(fields FieldList) error {
	name := fields.String()
	for i, idx := range t.indexes {
		if idx.Name() != name {
			continue
		}

		// delete index buckets
		if err := idx.Drop(); err != nil {
			return err
		}

		// store table metadata
		t.indexes = append(t.indexes[:i], t.indexes[i+1:]...)

		data := make([]IndexData, len(t.indexes))
		for i, v := range t.indexes {
			data[i].Kind = v.Kind()
			data[i].Fields = v.Fields()
		}

		return t.DB().Update(func(tx *Tx) error {
			meta := tx.Bucket(t.metakey)
			if meta == nil {
				return fmt.Errorf("pack: table %s: missing metadata bucket", t.name)
			}
			buf, err := json.Marshal(data)
			if err != nil {
				return err
			}
			return meta.Put(indexesKey, buf)
		})
	}
	return nil
}

func (t *KeyValueTable) Name() string {
	return t.name
}

func (_ *KeyValueTable) Engine() TableEngine {
	return TableEngineKV
}

func (t *KeyValueTable) Fields() FieldList {
	return t.fields
}

func (t *KeyValueTable) DB() *DB {
	return t.db
}

func (t *KeyValueTable) Options() Options {
	return t.opts
}

func (t *KeyValueTable) Sync(_ context.Context) error {
	return nil
}

func (t *KeyValueTable) Compact(_ context.Context) error {
	return nil
}

func (t *KeyValueTable) NextSequence() uint64 {
	// todo: maybe use bucket sequence for better crash safety
	t.meta.Sequence++
	return t.meta.Sequence
}

func (t *KeyValueTable) IsClosed() bool {
	return t.db == nil
}

func (t *KeyValueTable) Close() error {
	if t.db == nil {
		return nil
	}
	log.Debugf("pack: closing %s table", t.name)

	// write metadata
	err := t.db.Update(func(tx *Tx) error {
		meta := tx.Bucket(t.metakey)
		if meta == nil {
			return ErrBucketNotFound
		}
		buf, err := json.Marshal(t.meta)
		if err != nil {
			return err
		}
		return meta.Put(metaKey, buf)
	})
	if err != nil {
		return err
	}

	// unregister from db
	delete(t.db.stores, t.name)
	t.db = nil
	return nil
}

func (t *KeyValueTable) Drop() error {
	// drop indexes
	for _, idx := range t.indexes {
		if err := idx.Drop(); err != nil {
			log.Errorf("pack: drop index %s: %v", idx.Name(), err)
		}
	}

	// drop data
	err := t.db.Update(func(tx *Tx) error {
		_ = tx.Root().DeleteBucket(append([]byte(t.name), storeKey...))
		_ = tx.Root().DeleteBucket(append([]byte(t.name), storeMetaKey...))
		return nil
	})
	if err != nil {
		return err
	}

	// unregister from db
	delete(t.db.stores, t.name)
	t.db = nil
	return nil
}

func (t *KeyValueTable) Stats() []TableStats {
	// copy store stats
	stats := t.stats
	stats.TupleCount = t.meta.Rows
	return []TableStats{stats}
}

func (_ *KeyValueTable) PurgeCache() {
	// empty
}

// FIXME: this accesses private index fields, better to hide rebuild behind interface
// and stream table data
func (t *KeyValueTable) RebuildIndex(idx *KeyValueIndex) error {
	// clear index data bucket
	err := t.db.Update(func(tx *Tx) error {
		_ = tx.Root().DeleteBucket(idx.key)
		_, err := tx.Root().CreateBucketIfNotExists(idx.key)
		return err
	})
	if err != nil {
		return err
	}

	// walk data bucket and insert into index
	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}
	defer func() {
		tx.Rollback()
	}()
	index := tx.Bucket(idx.key)
	if index == nil {
		return ErrIndexNotFound
	}
	data := tx.Bucket(t.key)
	if data == nil {
		return ErrIndexNotFound
	}
	c := data.Cursor()
	var count int
	for ok := c.First(); ok; ok = c.Next() {
		ikey := idx.table.Fields().CopyData(idx.idxs, c.Value())
		// TODO: this could benefit from a batch write interface
		if err := index.Put(ikey, nil); err != nil {
			return err
		}
		count++
		if count >= txMaxSize {
			lastKey := c.Key()
			// commit data
			if err := tx.CommitAndContinue(); err != nil {
				return err
			}
			// refresh references with a new write tx
			data = tx.Bucket(t.key)
			index = tx.Bucket(idx.key)
			c = data.Cursor()
			c.Seek(lastKey)
		}
	}
	return tx.Commit()
}

// low-level interface for direct KV storage access
func (t *KeyValueTable) Get(key []byte) ([]byte, error) {
	var ret []byte
	err := t.db.View(func(tx *Tx) error {
		b := tx.Bucket(t.key)
		if b == nil {
			return ErrBucketNotFound
		}
		buf := b.Get(key)
		if buf == nil {
			return ErrKeyNotFound
		}
		if t.isZeroCopy {
			ret = make([]byte, len(buf))
			copy(ret, buf)
		} else {
			ret = buf
		}
		return nil
	})
	if err == nil {
		atomic.AddInt64(&t.stats.BytesRead, int64(len(ret)))
	}
	return ret, err
}

func (t *KeyValueTable) Get64(key uint64) ([]byte, error) {
	var bkey [8]byte
	bigEndian.PutUint64(bkey[:], key)
	return t.Get(bkey[:])
}

func (t *KeyValueTable) Put(key, val []byte) error {
	prevSize := -1
	err := t.db.Update(func(tx *Tx) error {
		b := tx.Bucket(t.key)
		if b == nil {
			return ErrBucketNotFound
		}
		b.FillPercent(float64(t.opts.FillLevel) / 100.0)
		buf := b.Get(key)
		if buf != nil {
			prevSize = len(buf)
		} else {
			t.meta.Rows++
		}
		return b.Put(key, val)
	})
	if err != nil {
		return err
	}
	sz := int64(len(key) + len(val))
	if prevSize >= 0 {
		// update
		atomic.AddInt64(&t.stats.UpdatedTuples, 1)
		atomic.AddInt64(&t.stats.UpdateCalls, 1)
		atomic.AddInt64(&t.stats.TotalSize, sz-int64(prevSize))
	} else {
		// insert
		atomic.AddInt64(&t.stats.InsertedTuples, 1)
		atomic.AddInt64(&t.stats.InsertCalls, 1)
		atomic.AddInt64(&t.stats.TupleCount, 1)
		atomic.AddInt64(&t.stats.TotalSize, sz)
	}
	atomic.AddInt64(&t.stats.BytesWritten, sz)
	return nil
}

func (t *KeyValueTable) Put64(key uint64, val []byte) error {
	var bkey [8]byte
	bigEndian.PutUint64(bkey[:], key)
	return t.Put(bkey[:], val)
}

func (t *KeyValueTable) Del(key []byte) error {
	prevSize := -1
	err := t.db.Update(func(tx *Tx) error {
		b := tx.Bucket(t.key)
		if b == nil {
			return ErrBucketNotFound
		}
		buf := b.Get(key)
		if buf != nil {
			prevSize = len(buf)
			t.meta.Rows--
		}
		return b.Delete(key)
	})
	if err == nil && prevSize >= 0 {
		atomic.AddInt64(&t.stats.TupleCount, -1)
		atomic.AddInt64(&t.stats.DeletedTuples, 1)
		atomic.AddInt64(&t.stats.DeleteCalls, 1)
		atomic.AddInt64(&t.stats.TotalSize, -int64(prevSize))
	}
	return err
}

func (t *KeyValueTable) Del64(key uint64) error {
	var bkey [8]byte
	bigEndian.PutUint64(bkey[:], key)
	return t.Del(bkey[:])
}

func (t *KeyValueTable) GetTx(tx *Tx, key []byte) []byte {
	var ret []byte
	data := tx.Bucket(t.key)
	if data == nil {
		return nil
	}
	buf := data.Get(key)
	if buf != nil {
		if t.isZeroCopy {
			ret = make([]byte, len(buf))
			copy(ret, buf)
		} else {
			ret = buf
		}
	}
	atomic.AddInt64(&t.stats.BytesRead, int64(len(ret)))
	return ret
}

func (t *KeyValueTable) PutValue(val any) error {
	// use PK based indexing when struct defines a PK
	if t.pkindex >= 0 {
		var pk uint64
		pkv := reflect.Indirect(reflect.ValueOf(val)).Field(t.pkindex)
		pk = pkv.Uint()
		if pk == 0 {
			// set next pk value if zero
			pk = t.NextSequence()
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
		key, err = t.fields.CompositePk().Encode(val)
		if err != nil {
			return err
		}

		// encode all fields into value (required for indexes)
		buf, err = t.fields.Encode(val)
		if err != nil {
			return err
		}
	}

	// open write transaction
	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}

	// cleanup on exit
	defer func() {
		tx.Rollback()
	}()

	// write value
	// log.Infof("PUT %s", hex.EncodeToString(key))
	prev, err = t.PutTx(tx, key, buf)
	if err != nil {
		return err
	}

	// update indexes
	for _, idx := range t.indexes {
		idx.AddTx(tx, prev, buf)
	}

	return tx.Commit()
}

func (t *KeyValueTable) PutTx(tx *Tx, key, val []byte) ([]byte, error) {
	prevSize, sz := -1, len(key)+len(val)
	data := tx.Bucket(t.key)
	if data == nil {
		return nil, ErrBucketNotFound
	}
	buf := t.GetTx(tx, key)
	if buf != nil {
		prevSize = len(buf) + len(key)
	} else {
		t.meta.Rows++
	}
	err := data.Put(key, val)
	if err != nil {
		return nil, err
	}
	if prevSize >= 0 {
		// update
		atomic.AddInt64(&t.stats.UpdatedTuples, 1)
		atomic.AddInt64(&t.stats.UpdateCalls, 1)
		atomic.AddInt64(&t.stats.TotalSize, int64(sz-prevSize))
	} else {
		// insert
		atomic.AddInt64(&t.stats.InsertedTuples, 1)
		atomic.AddInt64(&t.stats.TupleCount, 1)
		atomic.AddInt64(&t.stats.TotalSize, int64(sz))
	}
	atomic.AddInt64(&t.stats.BytesWritten, int64(sz))
	return buf, nil
}

func (t *KeyValueTable) DeleteValue(val any) error {
	atomic.AddInt64(&t.stats.DeleteCalls, 1)

	// use PK based indexing when struct defines a PK
	if t.pkindex >= 0 {
		var pk uint64
		pkv := reflect.Indirect(reflect.ValueOf(val)).Field(t.pkindex)
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
		key, err = t.fields.CompositePk().Encode(val)
		if err != nil {
			return err
		}
	}

	// open write transaction
	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}

	// cleanup on exit
	defer func() {
		tx.Rollback()
	}()

	// remove key
	// log.Infof("DEL %s", hex.EncodeToString(key))
	prev, err = t.DelTx(tx, key)
	if err != nil {
		return err
	}

	// update indexes
	for _, idx := range t.indexes {
		idx.DelTx(tx, prev)
	}
	return tx.Commit()
}

func (t *KeyValueTable) DelTx(tx *Tx, key []byte) ([]byte, error) {
	prevSize := -1
	data := tx.Bucket(t.key)
	if data == nil {
		return nil, ErrBucketNotFound
	}
	buf := data.Get(key)
	if buf != nil {
		prevSize = len(buf)
		t.meta.Rows--
	}
	err := data.Delete(key)
	if err == nil && prevSize >= 0 {
		atomic.AddInt64(&t.stats.TupleCount, -1)
		atomic.AddInt64(&t.stats.DeletedTuples, 1)
		atomic.AddInt64(&t.stats.TotalSize, -int64(prevSize))
	}
	return buf, err
}

// Table Compat interface
func (t *KeyValueTable) Insert(_ context.Context, val any) error {
	atomic.AddInt64(&t.stats.InsertCalls, 1)
	switch rval := reflect.Indirect(reflect.ValueOf(val)); rval.Kind() {
	case reflect.Slice, reflect.Array:
		for i, l := 0, rval.Len(); i < l; i++ {
			if err := t.PutValue(rval.Index(i).Interface()); err != nil {
				return err
			}
		}
		return nil
	default:
		return t.PutValue(val)
	}
}

func (t *KeyValueTable) Update(ctx context.Context, val any) error {
	atomic.AddInt64(&t.stats.UpdateCalls, 1)
	return t.Insert(ctx, val)
}

// Table Query Interface
// - requires main data bucket to be indexed by pk (uint64)
// - generate index scan ranges from query conditions
// - run index scans -> bitsets
// - merge bitsets along condition tree
// - resolve result from value bucket via final bitset
// - append row data to Result
// - result decoder can skip unused fields

func (t *KeyValueTable) Query(ctx context.Context, q Query) (*Result, error) {
	var (
		bits bitmap.Bitmap
		key  [8]byte
	)
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	// check conditions match schema
	err := q.Compile()
	if err != nil {
		return nil, err
	}

	// open read transaction
	tx, err := t.db.Tx(false)
	if err != nil {
		return nil, err
	}

	// cleanup on exit
	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.stats.RowsMatched))
		tx.Rollback()
		q.Close()
		bits.Free()
	}()

	data := tx.Bucket(t.key)
	if data == nil {
		return nil, ErrBucketNotFound
	}

	// prepare result
	res := &Result{
		fields:  t.fields,
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
	case q.conds.Empty():
		// No conds: walk entire table
		c := data.Cursor(store.ForwardCursor)
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			// skip offset
			if q.Offset > 0 {
				q.Offset--
				continue
			}

			res.offsets = append(res.offsets, len(res.values))
			res.values = append(res.values, c.Value()...)

			// apply limit
			q.stats.RowsMatched++
			if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
				break
			}
		}

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

			// apply limit
			q.stats.RowsScanned++
			q.stats.RowsMatched++
			if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
				break
			}
		}
	case !q.conds.OrKind && q.conds.Bits.IsValid():
		// 2: partial index query & root = AND: walk bitset but check each value
		it := q.conds.Bits.Bitmap.NewIterator()
		val := NewValue(t.fields)
		for id := it.Next(); id > 0; id = it.Next() {
			bigEndian.PutUint64(key[:], id)
			buf := data.Get(key[:])
			if buf == nil {
				// warn on indexed but missing pks
				log.Warnf("Missing PK %d from index scan in query %s", id, q.Name)
				continue
			}

			// check conditions
			q.stats.RowsScanned++
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
		// TODO: construct prefix scan from unprocessed pk condition
		// 3: partial index query & root = OR: walk full table and check each value
		// 4: no index query: walk full table and check each value
		c := data.Cursor(store.ForwardCursor)
		val := NewValue(t.fields)
		for ok := c.First(); ok; ok = c.Next() {
			buf := c.Value()

			// check conditions
			q.stats.RowsScanned++
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

func (t *KeyValueTable) Stream(ctx context.Context, q Query, fn func(Row) error) error {
	var (
		bits bitmap.Bitmap
		key  [8]byte
	)
	atomic.AddInt64(&t.stats.StreamCalls, 1)

	// check conditions match schema
	err := q.Compile()
	if err != nil {
		return err
	}

	// open read transaction
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}

	// cleanup on exit
	defer func() {
		q.stats.ScanTime = q.Tick()
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.stats.RowsMatched))
		tx.Rollback()
		q.Close()
		bits.Free()
	}()

	data := tx.Bucket(t.key)
	if data == nil {
		return ErrBucketNotFound
	}

	// prepare result
	var ofs [1]int
	row := Row{
		res: &Result{
			fields:  t.fields,
			offsets: ofs[:],
		},
		n: 0,
	}

	// run index queries
	// q.stats.IndexLookups = ??
	err = q.QueryIndexes(ctx, tx)
	if err != nil {
		return err
	}

	// handle cases
	switch {
	case q.conds.Empty():
		// No conds: walk entire table
		c := data.Cursor(store.ForwardCursor)
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			// skip offset
			if q.Offset > 0 {
				q.Offset--
				continue
			}

			row.res.values = c.Value()
			if err := fn(row); err != nil {
				if err != EndStream {
					return err
				}
				return nil
			}

			// apply limit
			q.stats.RowsScanned++
			q.stats.RowsMatched++
			if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
				break
			}
		}

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
			q.stats.RowsScanned++
			q.stats.RowsMatched++

			row.res.values = val
			if err := fn(row); err != nil {
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
	case !q.conds.OrKind && q.conds.Bits.IsValid():
		// 2: partial index query & root = AND: walk bitset but check each value
		it := q.conds.Bits.Bitmap.NewIterator()
		val := NewValue(t.fields)
		for id := it.Next(); id > 0; id = it.Next() {
			bigEndian.PutUint64(key[:], id)
			buf := data.Get(key[:])
			if buf == nil {
				// warn on indexed but missing pks
				log.Warnf("Missing PK %d from index scan in query %s", id, q.Name)
				continue
			}

			// check conditions
			q.stats.RowsScanned++
			if !q.conds.MatchValue(val.Reset(buf)) {
				continue
			}

			// skip offset
			if q.Offset > 0 {
				q.Offset--
				continue
			}

			row.res.values = buf
			if err := fn(row); err != nil {
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
		// TODO: construct prefix scan from unprocessed pk condition
		// 3: partial index query & root = OR: walk full table and check each value
		// 4: no index query: walk full table and check each value
		c := data.Cursor(store.ForwardCursor)
		defer c.Close()
		val := NewValue(t.fields)
		for ok := c.First(); ok; ok = c.Next() {
			buf := c.Value()

			// check conditions
			q.stats.RowsScanned++
			if !q.conds.MatchValue(val.Reset(buf)) {
				continue
			}

			// skip offset
			if q.Offset > 0 {
				q.Offset--
				continue
			}

			row.res.values = buf
			if err := fn(row); err != nil {
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

func (t *KeyValueTable) Delete(ctx context.Context, q Query) (int64, error) {
	var key [8]byte
	atomic.AddInt64(&t.stats.DeleteCalls, 1)

	// check conditions match schema
	err := q.Compile()
	if err != nil {
		return 0, err
	}

	// open read transaction
	tx, err := t.db.Tx(false)
	if err != nil {
		return 0, err
	}

	// cleanup on exit
	defer func() {
		atomic.AddInt64(&t.stats.DeletedTuples, int64(q.stats.RowsMatched))
		tx.Rollback()
		q.Close()
	}()

	data := tx.Bucket(t.key)
	if data == nil {
		return 0, ErrBucketNotFound
	}

	// run index queries
	// q.stats.IndexLookups = ??
	err = q.QueryIndexes(ctx, tx)
	if err != nil {
		return 0, err
	}

	// handle cases
	switch {
	case q.conds.Empty():
		// nothing to delete
		return 0, nil

	case q.conds.IsProcessed():
		// 1: full index query -> everything is resolved, walk bitset
		it := q.conds.Bits.Bitmap.NewIterator()
		for pk := it.Next(); pk > 0; pk = it.Next() {
			bigEndian.PutUint64(key[:], pk)
			prev, err := t.DelTx(tx, key[:])
			if err != nil {
				return 0, err
			}
			if prev == nil {
				continue
			}
			q.stats.RowsMatched++

			// update indexes
			for _, idx := range t.indexes {
				idx.DelTx(tx, prev)
			}
		}

	case !q.conds.OrKind:
		// 2: partial index query & root = AND: walk bitset but check each value
		it := q.conds.Bits.Bitmap.NewIterator()
		val := NewValue(t.fields)
		for id := it.Next(); id > 0; id = it.Next() {
			bigEndian.PutUint64(key[:], id)
			buf := data.Get(key[:])
			if buf == nil {
				continue
			}

			// check conditions
			q.stats.RowsScanned++
			if !q.conds.MatchValue(val.Reset(buf)) {
				continue
			}

			// delete
			prev, err := t.DelTx(tx, key[:])
			if err != nil {
				return 0, err
			}
			q.stats.RowsMatched++

			// update indexes
			for _, idx := range t.indexes {
				idx.DelTx(tx, prev)
			}
		}
	default:
		// 3: partial index query & root = OR: walk full table and check each value
		// 4: no index query: walk full table and check each value
		c := data.Cursor(store.ForwardCursor)
		val := NewValue(t.fields)
		for ok := c.First(); ok; ok = c.Next() {
			buf := c.Value()

			// check conditions
			q.stats.RowsScanned++
			if !q.conds.MatchValue(val.Reset(buf)) {
				continue
			}

			// delete
			prev, err := t.DelTx(tx, key[:])
			if err != nil {
				return 0, err
			}
			q.stats.RowsMatched++

			// update indexes
			for _, idx := range t.indexes {
				idx.DelTx(tx, prev)
			}
		}
	}
	q.stats.ScanTime = q.Tick()

	return int64(q.stats.RowsMatched), nil
}

func (t *KeyValueTable) Count(ctx context.Context, q Query) (int64, error) {
	var (
		bits bitmap.Bitmap
		key  [8]byte
	)
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	// check conditions match schema
	err := q.Compile()
	if err != nil {
		return 0, err
	}

	// open read transaction
	tx, err := t.db.Tx(false)
	if err != nil {
		return 0, err
	}

	// cleanup on exit
	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.stats.RowsMatched))
		tx.Rollback()
		q.Close()
		bits.Free()
	}()

	data := tx.Bucket(t.key)
	if data == nil {
		return 0, ErrBucketNotFound
	}

	// run index queries
	// q.stats.IndexLookups = ??
	err = q.QueryIndexes(ctx, tx)
	if err != nil {
		return 0, err
	}

	// handle cases
	switch {
	case q.conds.Empty():
		// No conds: walk entire table
		c := data.Cursor(store.IndexCursor)
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			q.stats.RowsMatched++
		}

	case q.conds.IsProcessed():
		// 1: full index query -> everything is resolved, count bitset
		q.stats.RowsMatched = q.conds.Bits.Count()

	case !q.conds.OrKind:
		// 2: partial index query & root = AND: walk bitset but check each value
		it := q.conds.Bits.Bitmap.NewIterator()
		val := NewValue(t.fields)
		for id := it.Next(); id > 0; id = it.Next() {
			bigEndian.PutUint64(key[:], id)
			buf := data.Get(key[:])
			if buf == nil {
				// warn on indexed but missing pks
				log.Warnf("Missing PK %d from index scan in query %s", id, q.Name)
				continue
			}

			// check conditions
			q.stats.RowsScanned++
			if !q.conds.MatchValue(val.Reset(buf)) {
				continue
			}

			q.stats.RowsMatched++
		}

	default:
		// 3: partial index query & root = OR: walk full table and check each value
		// 4: no index query: walk full table and check each value
		c := data.Cursor(store.ForwardCursor)
		val := NewValue(t.fields)
		for ok := c.First(); ok; ok = c.Next() {
			buf := c.Value()

			// check conditions
			q.stats.RowsScanned++
			if !q.conds.MatchValue(val.Reset(buf)) {
				continue
			}

			q.stats.RowsMatched++
		}
	}
	q.stats.ScanTime = q.Tick()

	return int64(q.stats.RowsMatched), nil
}

func (t *KeyValueTable) LookupPks(ctx context.Context, pks []uint64) (*Result, error) {
	var (
		key [8]byte
		cnt int64
	)
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	// open read transaction
	tx, err := t.db.Tx(false)
	if err != nil {
		return nil, err
	}

	// cleanup on exit
	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, cnt)
		tx.Rollback()
	}()

	data := tx.Bucket(t.key)
	if data == nil {
		return nil, ErrBucketNotFound
	}

	// prepare result
	res := &Result{
		fields:  t.fields,
		offsets: make([]int, 0, len(pks)),
		values:  make([]byte, 0, len(pks)),
	}
	for _, pk := range pks {
		if pk == 0 {
			continue
		}

		bigEndian.PutUint64(key[:], pk)
		buf := data.Get(key[:])
		if buf == nil {
			continue
		}

		cnt++
		res.offsets = append(res.offsets, len(res.values))
		res.values = append(res.values, buf...)
	}
	return res, nil
}

func (t *KeyValueTable) StreamPks(ctx context.Context, pks []uint64, fn func(r Row) error) error {
	var (
		key [8]byte
		cnt int64
	)
	atomic.AddInt64(&t.stats.StreamCalls, 1)

	// open read transaction
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}

	// cleanup on exit
	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, cnt)
		tx.Rollback()
	}()

	data := tx.Bucket(t.key)
	if data == nil {
		return ErrBucketNotFound
	}

	// prepare result
	var ofs [1]int
	res := &Result{
		fields:  t.fields,
		offsets: ofs[:],
	}
	for _, pk := range pks {
		if pk == 0 {
			continue
		}

		bigEndian.PutUint64(key[:], pk)
		buf := data.Get(key[:])
		if buf == nil {
			continue
		}

		cnt++
		res.values = buf
		if err := fn(Row{res: res, n: 0}); err != nil {
			if err != EndStream {
				return err
			}
			return nil
		}
	}
	return nil
}

func (t *KeyValueTable) DeletePks(ctx context.Context, pks []uint64) (int64, error) {
	atomic.AddInt64(&t.stats.DeleteCalls, 1)
	if len(pks) == 0 {
		return 0, nil
	}

	// open write transaction
	tx, err := t.db.Tx(true)
	if err != nil {
		return 0, err
	}

	// cleanup on exit
	defer func() {
		tx.Rollback()
	}()

	// remove each key
	var (
		key [8]byte
		cnt int64
	)
	for _, pk := range pks {
		if pk == 0 {
			continue
		}

		// remove value (check if exists to allow index removal)
		binary.BigEndian.PutUint64(key[:], pk)
		prev, err := t.DelTx(tx, key[:])
		if err != nil {
			return 0, err
		}
		if prev == nil {
			continue
		}
		cnt++

		// update indexes
		for _, idx := range t.indexes {
			idx.DelTx(tx, prev)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	atomic.AddInt64(&t.stats.DeletedTuples, cnt)
	return cnt, nil
}

var (
	ZERO = []byte{}
	FF   = []byte{0xff}
)

func (t *KeyValueTable) QueryIndexesTx(ctx context.Context, tx *Tx, node *ConditionTreeNode) (int, error) {
	// Process
	// - Pre-condition invariants
	//   - root node is empty or not leaf
	//   - AND nodes are flattened
	// - NON-LEAF nodes
	//   - recurse
	// - AND nodes
	//   - foreach indexes check if we can produce a prefix scan from any condition combi
	//   - calculate prefix and run scan -> bitset
	//   - mark conditions as processed
	//   - append bitset as new condition
	//   - continue until no more indexes or no more conditions are left
	// - OR nodes
	//   - handle each child separately
	//
	// Limitations
	// - IN, NI, NE, RE mode conditions cannot use range scans
	// - index scans do not consider offset and limit (full index scans are costly)
	//
	// Cases
	// A - AND(C,C) with full index
	//   > AND(c,c,IN) -> merge bitsets -> scan bitset only
	// B - AND(C,C) with partial index
	//   > AND(c,C,IN) -> scan bitset, apply cond tree to each val
	// C - AND(C,C) no index (or no index matched)
	//   > AND(C,C) -> full scan, apply cond tree to each val
	//
	// D - OR(C,C) with full index
	//   > OR(IN,IN) -> merge bitsets -> scan bitset only
	// E - OR(C,C) with partial index
	//   > OR(IN,C) -> full scan, apply cond tree to each val
	// F - OR(C,C) with no index
	//   > OR(C,C) -> full scan, apply cond tree to each val
	//
	// G - OR(AND(C,C),AND(C)) with full index
	//   > OR(AND(c,c,IN),AND(c,IN)) -> merge bitsets -> scan bitset only
	// H - OR(AND(C,C),AND(C)) with partial index
	//   > OR(AND(C,c,IN),AND(C)) -> full scan, apply cond tree to each val
	// I - OR(AND(C,C),C) with no index
	//   > OR(AND(C,C),C) -> full scan, apply cond tree to each val
	//
	// TODO
	// - run index scans in blocks & forward results through operator tree,
	//   then consume final aggregate with offset/limit
	pkfield := t.fields.Pk()
	var hits int

	if node.OrKind {
		// visit OR nodes individually
		if node.Leaf() {
			// convert primary key query to bitset
			if pkfield != nil && pkfield.Equal(*node.Cond.Field) {
				var bits bitmap.Bitmap
				switch node.Cond.Mode {
				case FilterModeEqual:
					bits = bitmap.New()
					bits.Set(node.Cond.Value.(uint64))
				case FilterModeIn:
					bits = bitmap.New()
					for _, pk := range node.Cond.Value.([]uint64) {
						bits.Set(pk)
					}
				}
				if bits.IsValid() {
					node.Bits = bits
					return bits.Count(), nil
				}
			}
			// run index scan
			for _, idx := range t.indexes {
				if !node.Cond.Field.Equal(*idx.fields[0]) {
					continue
				}
				val := node.Cond.Value
				if val == nil {
					val = node.Cond.From
				}
				prefix, err := node.Cond.Field.Encode(val)
				if err != nil {
					return hits, err
				}
				switch node.Cond.Mode {
				case FilterModeEqual:
					node.Bits, err = idx.ScanTx(tx, prefix)
				case FilterModeLt:
					// LT    => scan(0x, to)
					// EQ+LT => scan(prefix, prefix+to)
					node.Bits, err = idx.RangeTx(tx, ZERO, prefix)
				case FilterModeLte:
					// LE    => scan(0x, to)
					// EQ+LE => scan(prefix, prefix+to)
					node.Bits, err = idx.RangeTx(tx, ZERO, store.BytesPrefix(prefix).Limit)
				case FilterModeGt:
					// GT    => scan(from, FF)
					// EQ+GT => scan(prefix+from, prefix+FF)
					node.Bits, err = idx.RangeTx(tx, store.BytesPrefix(prefix).Limit, bytes.Repeat(FF, len(prefix)))
				case FilterModeGte:
					// GE    => scan(from, FF)
					// EQ+GE => scan(prefix+from, prefix+FF)
					node.Bits, err = idx.RangeTx(tx, prefix, bytes.Repeat(FF, len(prefix)))
				case FilterModeRange:
					// RG    => scan(from, to)
					// EQ+RG => scan(prefix+from, prefix+to)
					var to []byte
					to, err = node.Cond.Field.Encode(node.Cond.To)
					if err != nil {
						return hits, err
					}
					node.Bits, err = idx.RangeTx(tx, prefix, store.BytesPrefix(to).Limit)
					hits += node.Bits.Count()
				default:
					log.Warnf("Unsupported filter mode %s for field %s in store index query",
						node.Cond.Mode, node.Cond.Field.Alias)
				}
				if err != nil {
					return hits, err
				}
				if node.Bits.IsValid() {
					node.Cond.processed = true
				}
				break
			}
		} else {
			// recurse into children one by one
			var agg bitmap.Bitmap
			for i := range node.Children {
				n, err := t.QueryIndexesTx(ctx, tx, &node.Children[i])
				if err != nil {
					return hits, err
				}
				hits += n
			}
			// collect nested child bitmap results
			for _, v := range node.Children {
				if !v.Bits.IsValid() {
					continue
				}
				if agg.IsValid() {
					agg.Or(v.Bits)
				} else {
					agg = v.Bits.Clone()
				}
			}
			node.Bits = agg
		}
		return hits, nil
	}

	// AND nodes may contain leafs and nested OR nodes which we need to visit separately
	var agg bitmap.Bitmap
	eq := make(map[string]*Condition) // all equal child conditions
	ex := make(map[string]*Condition) // all eligible extra child conditions
	for i := range node.Children {
		if node.Children[i].IsNested() {
			n, err := t.QueryIndexesTx(ctx, tx, &node.Children[i])
			if err != nil {
				return hits, err
			}
			hits += n
		}

		// identify eligible conditions for constructing range scans
		if node.Children[i].Leaf() {
			c := node.Children[i].Cond

			// convert primary key query to bitset
			if pkfield != nil && pkfield.Equal(*c.Field) {
				var bits bitmap.Bitmap
				switch c.Mode {
				case FilterModeEqual:
					bits = bitmap.New()
					bits.Set(c.Value.(uint64))
				case FilterModeIn:
					bits = bitmap.New()
					for _, pk := range c.Value.([]uint64) {
						bits.Set(pk)
					}
				}
				if bits.IsValid() {
					node.Children[i].Bits = bits
					continue
				}
			}

			// keep range-scan compatible conditions
			switch c.Mode {
			case FilterModeEqual:
				eq[c.Field.Name] = c
			case FilterModeLt, FilterModeLte, FilterModeGt, FilterModeGte, FilterModeRange:
				ex[c.Field.Name] = c
			default:
				log.Warnf("Unsupported filter mode %s for field %s in store index query",
					c.Mode, c.Field.Alias)
			}
		}
	}

	// collect nested child bitmap results
	for _, v := range node.Children {
		if !v.Bits.IsValid() {
			continue
		}
		if agg.IsValid() {
			agg.And(v.Bits)
		} else {
			agg = v.Bits.Clone()
		}
	}

	// try combine AND node leaf conditions for index scans
	for _, idx := range t.indexes {
		// see if we can produce an ordered prefix from existing conditions
		var (
			prefix []byte
			extra  *Condition
		)
		for _, field := range idx.fields {
			c, ok := eq[field.Name]
			if !ok {
				// before stopping, check if we can append an extra range condition
				extra, _ = ex[field.Name]
				break
			}
			buf, err := c.Field.Encode(c.Value)
			if err != nil {
				return hits, err
			}
			prefix = append(prefix, buf...)
			c.processed = true
			delete(eq, field.Name)
		}

		if len(prefix) == 0 && extra == nil {
			continue
		}

		var (
			bits bitmap.Bitmap
			err  error
		)
		if extra != nil {
			// equal plus extra range condition
			extra.processed = true
			val := extra.Value
			if val == nil {
				val = extra.From
			}
			var buf []byte
			buf, err = extra.Field.Encode(val)
			if err != nil {
				return hits, err
			}
			switch extra.Mode {
			case FilterModeLt:
				// LT    => scan(0x, to)
				// EQ+LT => scan(prefix, prefix+to)
				bits, err = idx.RangeTx(tx, prefix, append(prefix, buf...))
			case FilterModeLte:
				// LE    => scan(0x, to)
				// EQ+LE => scan(prefix, prefix+to)
				bits, err = idx.RangeTx(tx, prefix, store.BytesPrefix(append(prefix, buf...)).Limit)
			case FilterModeGt:
				// GT    => scan(from, FF)
				// EQ+GT => scan(prefix+from, prefix+FF)
				bits, err = idx.RangeTx(tx, store.BytesPrefix(append(prefix, buf...)).Limit, bytes.Repeat(FF, len(prefix)+len(buf)))
			case FilterModeGte:
				// GE    => scan(from, FF)
				// EQ+GE => scan(prefix+from, prefix+FF)
				bits, err = idx.RangeTx(tx, append(prefix, buf...), bytes.Repeat(FF, len(prefix)+len(buf)))
			case FilterModeRange:
				// RG    => scan(from, to)
				// EQ+RG => scan(prefix+from, prefix+to)
				var to []byte
				to, err = extra.Field.Encode(extra.To)
				if err != nil {
					return hits, err
				}
				bits, err = idx.RangeTx(tx, append(prefix, buf...), store.BytesPrefix(append(prefix, to...)).Limit)
			}
		} else {
			// equal condition(s) only
			bits, err = idx.ScanTx(tx, prefix)
		}
		if err != nil {
			return hits, err
		}
		if bits.IsValid() {
			hits += bits.Count()
		}
		if agg.IsValid() {
			agg.And(bits)
			bits.Free()
		} else {
			agg = bits
		}
		if len(eq) == 0 {
			break
		}
	}

	// store aggregate bitmap in node
	if agg.IsValid() {
		node.Bits = agg
	}

	return hits, nil
}
