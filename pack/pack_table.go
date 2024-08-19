// Copyright (c) 2018-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// Design concepts
// - columnar design with type-specific multi-level compression
// - column groups (i.e. packs with equal size vectors across all columns)
// - statistics (zonemaps) with min/max values per column per pack
// - buffer pools for packs and slices
// - pack caches and cache-sensitive pack query scheduler

// TODO Query features
// - complex conditions using AND/OR and brackets
// - GROUP BY and HAVING (special condition to filter groups after aggregation)
// - aggregate functions sum, mean, median, std,
// - selectors (first, last, min, max, topN, bottomN)
// - arithmetic expressions (simple math)
// - PARTITION BY analytics (keep rows unlike GROUP BY which aggregates)

// TODO Performance and Safety
// - WAL for durable journal insert/update/delete
// - concurrent/background pack compaction/storage
// - concurrent index build

// TODO Other
// - materialized views for storing expensive query results
// - auto-create indexes when index keyword is used in struct tag for CreateTable
// - other indexes (b-tree?)

package pack

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/assert"
	"blockwatch.cc/knoxdb/cache/rclru"
	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/util"
	"blockwatch.cc/knoxdb/vec"
)

var _ Table = (*PackTable)(nil)

var (
	optsKey             = []byte("_options")
	fieldsKey           = []byte("_fields")
	metaKey             = []byte("_meta")
	infoKey             = []byte("_packinfo")
	indexesKey          = []byte("_indexes")
	journalKey   uint32 = 0xFFFFFFFF
	tombstoneKey uint32 = 0xFFFFFFFE
	resultKey    uint32 = 0xFFFFFFFD
)

type Tombstone struct {
	Id uint64 `knox:"I,pk,snappy"`
}

type TableMeta struct {
	Sequence uint64 `json:"sequence"`
	Rows     int64  `json:"rows"`
	dirty    bool   `json:"-"`
}

type PackTable struct {
	name     string                            // printable table name
	opts     Options                           // runtime configuration options
	fields   FieldList                         // ordered list of table fields as central type info
	indexes  PackIndexList                     // pack indexes only
	meta     TableMeta                         // authoritative metadata
	db       *DB                               // lower-level storage (e.g. boltdb wrapper)
	bcache   rclru.Cache[uint64, *block.Block] // keep decoded packs for query/updates
	journal  *Journal                          // in-memory data not yet written to packs
	packidx  *PackHeader                       // in-memory list of pack and block info
	key      []byte                            // name of table data bucket
	metakey  []byte                            // name of table metadata bucket
	packPool *sync.Pool                        // buffer pool for new packages
	u64Pool  *sync.Pool                        // buffer pool for uint64 slices (used by indexes)
	u32Pool  *sync.Pool                        // buffer pool for uint32 slices (used by match algos)
	stats    TableStats                        // usage statistics
	mu       sync.RWMutex                      // global table lock
}

func CreatePackTable(d *DB, name string, fields FieldList, opts Options) (*PackTable, error) {
	opts = DefaultOptions.Merge(opts)
	if err := opts.Check(); err != nil {
		return nil, err
	}
	maxPackSize := opts.PackSize()
	maxJournalSize := opts.JournalSize()
	t := &PackTable{
		name:   name,
		opts:   opts,
		fields: fields,
		meta: TableMeta{
			Sequence: 0,
		},
		db:      d,
		indexes: make(PackIndexList, 0),
		packidx: NewPackHeader(nil, fields.PkIndex(), maxPackSize),
		key:     []byte(name),
		metakey: append([]byte(name), metaKey...),
		u64Pool: &sync.Pool{
			New: func() interface{} { return make([]uint64, 0, maxPackSize) },
		},
		u32Pool: &sync.Pool{
			New: func() interface{} { return make([]uint32, 0, maxPackSize) },
		},
	}
	t.stats.TableName = name
	t.stats.JournalTuplesThreshold = int64(maxJournalSize)
	t.stats.TombstoneTuplesThreshold = int64(maxJournalSize)
	t.packPool = &sync.Pool{
		New: t.makePackage,
	}
	err := d.Update(func(tx *Tx) error {
		b := tx.Bucket(t.key)
		if b != nil {
			return ErrTableExists
		}
		_, err := tx.Root().CreateBucketIfNotExists(t.key)
		if err != nil {
			return err
		}
		meta, err := tx.Root().CreateBucketIfNotExists(t.metakey)
		if err != nil {
			return err
		}
		_, err = meta.CreateBucketIfNotExists(infoKey)
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
		err = meta.Put(indexesKey, []byte(`[]`))
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
		t.journal = NewJournal(0, maxJournalSize, t.name)
		if err := t.journal.InitFields(fields); err != nil {
			return err
		}
		jsz, tsz, err := t.journal.StoreLegacy(tx, t.metakey)
		if err != nil {
			return err
		}
		t.stats.JournalDiskSize = int64(jsz)
		t.stats.TombstoneDiskSize = int64(tsz)
		// TODO: switch to WAL for durability
		// err = t.journal.Open(d.Dir())
		// if err != nil {
		// 	return err
		// }
		return nil
	})
	if err != nil {
		return nil, err
	}
	if t.opts.CacheSize > 0 {
		t.bcache, err = rclru.New2Q[uint64, *block.Block](t.opts.CacheSizeMBytes())
		if err != nil {
			return nil, err
		}
		t.stats.CacheCapacity = int64(t.opts.CacheSizeMBytes())
	} else {
		t.bcache = rclru.NewNoCache[uint64, *block.Block]()
	}
	log.Debugf("Created table %s", name)
	return t, nil
}

func OpenPackTable(d *DB, name string, opts ...Options) (*PackTable, error) {
	if len(opts) > 0 {
		log.Debugf("Opening table %s with opts %#v", name, opts[0])
	} else {
		log.Debugf("Opening table %s with default opts", name)
	}
	t := &PackTable{
		name:    name,
		db:      d,
		key:     []byte(name),
		metakey: append([]byte(name), metaKey...),
	}
	t.stats.TableName = name
	t.packPool = &sync.Pool{
		New: t.makePackage,
	}
	var indexes []IndexData
	err := d.View(func(tx *Tx) error {
		b := tx.Bucket(t.metakey)
		if b == nil {
			return ErrNoTable
		}
		buf := b.Get(optsKey)
		if buf == nil {
			return fmt.Errorf("pack: missing options for table %s", name)
		}
		err := json.Unmarshal(buf, &t.opts)
		if err != nil {
			return err
		}
		if len(opts) > 0 {
			if opts[0].PackSizeLog2 > 0 && t.opts.PackSizeLog2 != opts[0].PackSizeLog2 {
				return fmt.Errorf("pack: %s pack size change not allowed", name)
			}
			t.opts = t.opts.Merge(opts[0])
		}
		maxJournalSize := t.opts.JournalSize()
		maxPackSize := t.opts.PackSize()
		t.stats.JournalTuplesThreshold = int64(maxJournalSize)
		t.stats.TombstoneTuplesThreshold = int64(maxJournalSize)
		t.u64Pool = &sync.Pool{
			New: func() interface{} { return make([]uint64, 0, maxPackSize) },
		}
		t.u32Pool = &sync.Pool{
			New: func() interface{} { return make([]uint32, 0, maxPackSize) },
		}
		buf = b.Get(fieldsKey)
		if buf == nil {
			return fmt.Errorf("pack: missing fields for table %s", name)
		}
		err = json.Unmarshal(buf, &t.fields)
		if err != nil {
			return fmt.Errorf("pack: cannot read fields for table %s: %v", name, err)
		}
		buf = b.Get(indexesKey)
		if buf == nil {
			return fmt.Errorf("pack: missing indexes for table %s", name)
		}
		err = json.Unmarshal(buf, &indexes)
		if err != nil {
			return fmt.Errorf("pack: cannot read indexes for table %s: %v", name, err)
		}
		buf = b.Get(metaKey)
		if buf == nil {
			return fmt.Errorf("pack: missing metadata for table %s", name)
		}
		err = json.Unmarshal(buf, &t.meta)
		if err != nil {
			return fmt.Errorf("pack: cannot read metadata for table %s: %v", name, err)
		}
		atomic.StoreInt64(&t.stats.TupleCount, t.meta.Rows)
		t.journal = NewJournal(t.meta.Sequence, maxJournalSize, t.name)
		t.journal.InitFields(t.fields)
		err = t.journal.LoadLegacy(tx, t.metakey)
		if err != nil {
			return fmt.Errorf("pack: cannot open journal for table %s: %v", name, err)
		}
		// TODO: switch to WAL
		// err = t.journal.Open(d.Dir())
		// if err != nil {
		// 	return err
		// }
		// log.Debugf("pack: %s table opened WAL with %d entries", name, t.journal.Len())
		log.Debugf("pack: %s table opened journal with %d entries", name, t.journal.Len())
		return t.loadPackInfo(tx)
	})
	if err != nil {
		return nil, err
	}
	if t.opts.CacheSize > 0 {
		t.bcache, err = rclru.New2Q[uint64, *block.Block](t.opts.CacheSizeMBytes())
		if err != nil {
			return nil, err
		}
		t.stats.CacheCapacity = int64(t.opts.CacheSizeMBytes())
	} else {
		t.bcache = rclru.NewNoCache[uint64, *block.Block]()
	}

	// open existing indexes
	iopts := DefaultOptions
	if len(opts) > 1 {
		iopts = iopts.Merge(opts[1])
	}
	for _, v := range indexes {
		idx, err := OpenPackIndex(t, v.Kind, v.Fields, iopts)
		if err != nil {
			return nil, err
		}
		t.indexes = append(t.indexes, idx)
	}

	return t, nil
}

func (t *PackTable) Drop() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// clear cache
	t.bcache.Purge()

	// drop indexes
	for _, idx := range t.indexes {
		if err := idx.Drop(); err != nil {
			log.Errorf("pack: drop index %s: %v", idx.Name(), err)
		}
	}

	// drop data
	return t.db.Update(func(dbTx *Tx) error {
		err := dbTx.Root().DeleteBucket([]byte(t.name))
		if err != nil {
			return err
		}
		return dbTx.Root().DeleteBucket(append([]byte(t.name), metaKey...))
	})
}

func (t *PackTable) CreateIndex(kind IndexKind, fields FieldList, opts Options) error {
	idx, err := CreatePackIndex(t, kind, fields, opts)
	if err != nil {
		return err
	}

	// add index to table's list of indexes and store the list
	t.indexes = append(t.indexes, idx)

	data := make([]IndexData, len(t.indexes))
	for i, v := range t.indexes {
		data[i].Kind = v.kind
		data[i].Fields = FieldList{v.field}
	}

	return t.db.Update(func(tx *Tx) error {
		meta := tx.Bucket(t.metakey)
		if meta == nil {
			return fmt.Errorf("pack: table %s: missing metadata bucket", t.name)
		}
		buf, err := json.Marshal(data)
		if err != nil {
			return err
		}
		err = meta.Put(indexesKey, buf)
		return err
	})
}

func (t *PackTable) CreateIndexIfNotExists(kind IndexKind, fields FieldList, opts Options) error {
	err := t.CreateIndex(kind, fields, opts)
	if err != nil && err != ErrIndexExists {
		return err
	}
	return nil
}

func (t *PackTable) DropIndex(fields FieldList) error {
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
			data[i].Kind = v.kind
			data[i].Fields = FieldList{v.field}
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

func (t *PackTable) loadPackInfo(tx *Tx) error {
	meta := tx.Bucket(t.metakey)
	if meta == nil {
		return ErrNoTable
	}
	maxPackSize := t.opts.PackSize()
	packs := make(PackInfoList, 0)
	bi := meta.Bucket(infoKey)
	if bi != nil {
		log.Debugf("pack: %s table loading package info from bucket", t.name)
		c := bi.Cursor()
		var err error
		for ok := c.First(); ok; ok = c.Next() {
			info := PackInfo{}
			err = info.UnmarshalBinary(c.Value())
			if err != nil {
				break
			}
			packs = append(packs, info)
			atomic.AddInt64(&t.stats.MetaBytesRead, int64(len(c.Value())))
		}
		if err != nil {
			packs = packs[:0]
			log.Errorf("pack: info decode for table %s pack %x: %v", t.name, c.Key(), err)
		} else {
			t.packidx = NewPackHeader(packs, t.fields.PkIndex(), maxPackSize)
			atomic.StoreInt64(&t.stats.PacksCount, int64(t.packidx.Len()))
			atomic.StoreInt64(&t.stats.MetaSize, int64(t.packidx.HeapSize()))
			atomic.StoreInt64(&t.stats.TotalSize, int64(t.packidx.TableSize()))
			log.Debugf("pack: %s table loaded index data for %d packs", t.name, t.packidx.Len())
			return nil
		}
	}
	log.Warnf("pack: %s table has corrupt or missing statistics! Re-scanning table. This may take some time...", t.name)
	c := tx.Bucket(t.key).Cursor()
	pkg := NewPackage(maxPackSize, nil)
	if err := pkg.InitFieldsFrom(t.journal.DataPack()); err != nil {
		return err
	}
	for ok := c.First(); ok; ok = c.Next() {
		err := pkg.UnmarshalBinary(c.Value())
		if err != nil {
			return fmt.Errorf("pack: cannot read %s/%x: %v", t.name, c.Key(), err)
		}
		pkg.SetKey(c.Key())
		if pkg.IsJournal() || pkg.IsTomb() {
			pkg.Clear()
			continue
		}
		info := pkg.Info()
		_ = info.UpdateStats(pkg)
		packs = append(packs, info)
		atomic.AddInt64(&t.stats.MetaBytesRead, int64(len(c.Value())))
		pkg.Clear()
	}
	t.packidx = NewPackHeader(packs, t.fields.PkIndex(), maxPackSize)
	atomic.StoreInt64(&t.stats.PacksCount, int64(t.packidx.Len()))
	atomic.StoreInt64(&t.stats.MetaSize, int64(t.packidx.HeapSize()))
	atomic.StoreInt64(&t.stats.TotalSize, int64(t.packidx.TableSize()))
	log.Debugf("pack: %s table scanned %d packages", t.name, t.packidx.Len())
	return nil
}

func (t *PackTable) storePackInfo(tx *Tx) error {
	meta := tx.Bucket(t.metakey)
	if meta == nil {
		return ErrNoTable
	}
	hb := meta.Bucket(infoKey)
	// create statistics bucket when missing
	if hb == nil {
		var err error
		hb, err = meta.CreateBucketIfNotExists(infoKey)
		if err != nil {
			return err
		}
	}
	// remove headers for deleted packs, if any
	for _, v := range t.packidx.removed {
		log.Debugf("pack: %s table removing pack info %x", t.name, v)
		hb.Delete(encodePackKey(v))
	}
	t.packidx.removed = t.packidx.removed[:0]

	// store headers for new/updated packs
	for i := range t.packidx.packs {
		if !t.packidx.packs[i].dirty {
			continue
		}
		buf, err := t.packidx.packs[i].MarshalBinary()
		if err != nil {
			return err
		}
		if err := hb.Put(t.packidx.packs[i].KeyBytes(), buf); err != nil {
			return err
		}
		t.packidx.packs[i].dirty = false
		atomic.AddInt64(&t.stats.MetaBytesWritten, int64(len(buf)))
	}
	return nil
}

func (t *PackTable) Fields() FieldList {
	return t.fields
}

func (t *PackTable) Schema() *schema.Schema {
	return fieldsToSchema(t.name, t.fields, t.journal.data.tinfo)
}

func (t *PackTable) Name() string {
	return t.name
}

func (t *PackTable) DB() *DB {
	return t.db
}

func (t *PackTable) Engine() TableEngine {
	return TableEnginePack
}

func (t *PackTable) Options() Options {
	return t.opts
}

func (t *PackTable) Indexes() PackIndexList {
	return t.indexes
}

func (t *PackTable) Lock() {
	t.mu.Lock()
}

func (t *PackTable) Unlock() {
	t.mu.Unlock()
}

func (t *PackTable) Stats() []TableStats {
	s := t.stats.Clone()

	// update from journal and tomb (reading here may be more efficient than
	// update on change, but creates a data race)
	s.JournalTuplesCount = int64(t.journal.data.Len())
	s.JournalTuplesCapacity = int64(t.journal.data.Cap())
	s.JournalSize = int64(t.journal.data.HeapSize())

	s.TombstoneTuplesCount = int64(len(t.journal.tomb))
	s.TombstoneTuplesCapacity = int64(cap(t.journal.tomb))
	s.TombstoneSize = s.TombstoneTuplesCount * 8

	// copy cache stats
	cs := t.bcache.Stats()
	s.CacheHits = cs.Hits
	s.CacheMisses = cs.Misses
	s.CacheInserts = cs.Inserts
	s.CacheEvictions = cs.Evictions
	s.CacheCount = cs.Count
	s.CacheSize = cs.Size

	resp := []TableStats{s}
	for _, idx := range t.indexes {
		resp = append(resp, idx.Stats())
	}
	return resp
}

func (t *PackTable) PurgeCache() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.bcache.Purge()
	for _, idx := range t.indexes {
		idx.PurgeCache()
	}
}

func (t *PackTable) Sequence() uint64 {
	return t.meta.Sequence
}

func (t *PackTable) NextSequence() uint64 {
	t.meta.Sequence++
	t.meta.dirty = true
	return t.meta.Sequence
}

func (t *PackTable) Insert(ctx context.Context, val any) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := t.insertJournal(val); err != nil {
		return err
	}

	if t.journal.ShouldFlush() {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}
		return tx.Commit()
	}
	return nil
}

// unsafe when used concurrently, need to obtain lock _before_ starting bolt tx
func (t *PackTable) InsertTx(ctx context.Context, tx *Tx, val any) error {
	if err := t.insertJournal(val); err != nil {
		return err
	}

	if t.journal.ShouldFlush() {
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}
	}

	return nil
}

func (t *PackTable) insertJournal(val any) error {
	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}
	if t.IsClosed() {
		return ErrDatabaseClosed
	}
	atomic.AddInt64(&t.stats.InsertCalls, 1)

	var (
		count int
		err   error
	)

	switch rval := reflect.Indirect(reflect.ValueOf(val)); rval.Kind() {
	case reflect.Slice, reflect.Array:
		count, err = t.journal.InsertBatch(rval)
	default:
		count, err = 1, t.journal.Insert(val)
	}

	if err != nil {
		return err
	}
	t.meta.Sequence = max(t.meta.Sequence, t.journal.MaxId())
	t.meta.Rows += int64(count)
	t.meta.dirty = true
	atomic.AddInt64(&t.stats.InsertedTuples, int64(count))
	atomic.StoreInt64(&t.stats.TupleCount, t.meta.Rows)
	return nil
}

func (t *PackTable) InsertRow(ctx context.Context, row Row) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := t.appendPackIntoJournal(ctx, row.res.pkg, row.n, 1); err != nil {
		return err
	}

	if t.journal.ShouldFlush() {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}

		return tx.Commit()
	}
	return nil
}

func (t *PackTable) InsertResult(ctx context.Context, res *Result) error {
	if res == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := t.appendPackIntoJournal(ctx, res.pkg, 0, res.pkg.Len()); err != nil {
		return err
	}

	if t.journal.ShouldFlush() {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}

		return tx.Commit()
	}
	return nil
}

// FIXME: only works for same table schema, requires pkg to be sorted by pk
func (t *PackTable) appendPackIntoJournal(ctx context.Context, pkg *Package, pos, n int) error {
	if pkg.Len() == 0 {
		return nil
	}
	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}
	if t.IsClosed() {
		return ErrDatabaseClosed
	}

	atomic.AddInt64(&t.stats.InsertCalls, 1)

	count, err := t.journal.InsertPack(pkg, pos, n)
	if err != nil {
		return err
	}

	t.meta.Sequence = max(t.meta.Sequence, t.journal.MaxId())
	t.meta.Rows += int64(count)
	t.meta.dirty = true
	atomic.AddInt64(&t.stats.InsertedTuples, int64(count))
	atomic.StoreInt64(&t.stats.TupleCount, t.meta.Rows)
	return nil
}

func (t *PackTable) Update(ctx context.Context, val any) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := t.updateJournal(val); err != nil {
		return err
	}

	if t.journal.ShouldFlush() {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}

		return tx.Commit()
	}

	return nil
}

func (t *PackTable) UpdateTx(ctx context.Context, tx *Tx, val any) error {
	if err := t.updateJournal(val); err != nil {
		return err
	}

	if t.journal.ShouldFlush() {
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}
	}

	return nil
}

func (t *PackTable) updateJournal(val any) error {
	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}
	if t.IsClosed() {
		return ErrDatabaseClosed
	}
	atomic.AddInt64(&t.stats.UpdateCalls, 1)

	var (
		count int
		err   error
	)

	switch rval := reflect.Indirect(reflect.ValueOf(val)); rval.Kind() {
	case reflect.Slice, reflect.Array:
		count, err = t.journal.UpdateBatch(rval)
	default:
		count, err = 1, t.journal.Update(val)
	}
	if err != nil {
		return err
	}

	t.meta.Sequence = max(t.meta.Sequence, t.journal.MaxId())
	t.meta.dirty = true
	atomic.AddInt64(&t.stats.UpdatedTuples, int64(count))
	return nil
}

func (t *PackTable) Delete(ctx context.Context, q Query) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	tx, err := t.db.Tx(true)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	q.Fields = []string{t.fields.Pk().Name}
	res, err := t.QueryTx(ctx, tx, q)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	n, err := t.DeletePksTx(ctx, tx, res.PkColumn())
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return n, nil
}

func (t *PackTable) DeletePks(ctx context.Context, val []uint64) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if err := t.deleteJournal(val); err != nil {
		return 0, err
	}

	if t.journal.ShouldFlush() {
		tx, err := t.db.Tx(true)
		if err != nil {
			return 0, err
		}

		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return 0, err
		}

		if err := tx.Commit(); err != nil {
			return 0, err
		}
	}

	return int64(len(val)), nil
}

func (t *PackTable) DeletePksTx(ctx context.Context, tx *Tx, val []uint64) (int64, error) {
	if err := t.deleteJournal(val); err != nil {
		return 0, err
	}

	if t.journal.ShouldFlush() {
		if err := t.flushTx(ctx, tx); err != nil {
			return 0, err
		}
	}

	return int64(len(val)), nil
}

func (t *PackTable) deleteJournal(ids []uint64) error {
	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}
	if t.IsClosed() {
		return ErrDatabaseClosed
	}

	atomic.AddInt64(&t.stats.DeleteCalls, 1)
	count, err := t.journal.DeleteBatch(ids)
	if err != nil {
		return err
	}

	// Note: we don't check if ids actually exist, so row counter may be off
	// until journal/tombstone are flushed
	t.meta.Rows -= int64(count)
	t.meta.dirty = true
	atomic.AddInt64(&t.stats.DeletedTuples, int64(count))
	atomic.StoreInt64(&t.stats.TupleCount, t.meta.Rows)
	return nil
}

func (t *PackTable) IsClosed() bool {
	return t.db == nil
}

func (t *PackTable) Close() error {
	if t.db == nil {
		return nil
	}
	log.Debugf("pack: closing %s table with %d journal records", t.name, t.journal.Len())
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.db == nil {
		return nil
	}

	if !t.db.IsReadOnly() {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()

		// store table metadata
		if t.meta.dirty {
			buf, err := json.Marshal(t.meta)
			if err != nil {
				return err
			}
			err = tx.tx.Bucket(t.metakey).Put(metaKey, buf)
			if err != nil {
				return err
			}
			t.meta.dirty = false
		}

		// save journal and tombstone
		if jsz, tsz, err := t.journal.StoreLegacy(tx, t.metakey); err != nil {
			return err
		} else {
			t.stats.JournalDiskSize = int64(jsz)
			t.stats.TombstoneDiskSize = int64(tsz)
		}

		// store pack headers
		if err := t.storePackInfo(tx); err != nil {
			return err
		}

		// close indexes
		for _, idx := range t.indexes {
			if err := idx.CloseTx(tx); err != nil {
				return err
			}
		}
		t.indexes = t.indexes[:0]

		// commit storage transaction
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	// close journal
	t.journal.Close()
	t.db = nil

	return nil
}

func (t *PackTable) Sync(ctx context.Context) error {
	return t.FlushJournal(ctx)
}

func (t *PackTable) FlushJournal(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	if err := t.flushJournalTx(ctx, tx); err != nil {
		return err
	}

	// store table metadata
	if t.meta.dirty {
		buf, err := json.Marshal(t.meta)
		if err != nil {
			return err
		}
		err = tx.tx.Bucket(t.metakey).Put(metaKey, buf)
		if err != nil {
			return err
		}
		t.meta.dirty = false
	}

	return tx.Commit()
}

func (t *PackTable) flushJournalTx(ctx context.Context, tx *Tx) error {
	nTuples, nTomb := t.journal.Len(), t.journal.TombLen()
	nJournalBytes, nTombBytes, err := t.journal.StoreLegacy(tx, t.metakey)
	if err != nil {
		return err
	}
	atomic.AddInt64(&t.stats.JournalTuplesFlushed, int64(nTuples))
	atomic.AddInt64(&t.stats.JournalPacksStored, 1)
	atomic.AddInt64(&t.stats.JournalBytesWritten, int64(nJournalBytes))
	atomic.AddInt64(&t.stats.TombstoneTuplesFlushed, int64(nTomb))
	atomic.AddInt64(&t.stats.TombstonePacksStored, 1)
	atomic.AddInt64(&t.stats.TombstoneBytesWritten, int64(nTombBytes))
	atomic.StoreInt64(&t.stats.JournalDiskSize, int64(nJournalBytes))
	atomic.StoreInt64(&t.stats.TombstoneDiskSize, int64(nTombBytes))
	return nil
}

func (t *PackTable) Flush(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	if err := t.flushTx(ctx, tx); err != nil {
		return err
	}

	return tx.Commit()
}

// TODO
// - make concurrency safe to be called from background writer
// - allow step-wise execution (flush x number of journal entries per call)
// - support context cancellation
//
// merge journal entries into data partitions, repack, store, and update all indexes
func (t *PackTable) flushTx(ctx context.Context, tx *Tx) error {
	var (
		nParts, nBytes, nUpd, nAdd, nDel, n int                          // total stats counters
		pUpd, pAdd, pDel                    int                          // per-pack stats counters
		start                               time.Time = time.Now().UTC() // logging
		err                                 error
	)

	atomic.AddInt64(&t.stats.FlushCalls, 1)
	atomic.AddInt64(&t.stats.FlushedTuples, int64(t.journal.Len()+t.journal.TombLen()))
	atomic.StoreInt64(&t.stats.LastFlushTime, start.UnixNano())

	// use internal journal data slices for faster lookups
	live := t.journal.keys
	dead := t.journal.tomb
	jpack := t.journal.data
	dbits := t.journal.deleted

	// walk journal/tombstone updates and group updates by pack
	var (
		pkg                                  *Package // current target pack
		packsz                               int      // target pack size
		jpos, tpos, jlen, tlen               int      // journal/tomb slice offsets & lengths
		nextpack, lastpack                   int      // pack list positions (not keys)
		packmin, packmax, nextmin, globalmax uint64   // data placement hints
		loop, maxloop                        int      // circuit breaker
	)

	// on error roll back table metadata to last valid value on storage
	defer func() {
		if e := recover(); e != nil || err != nil {
			log.Debugf("pack: %s table restore metadata on error", t.name)
			if err := t.loadPackInfo(tx); err != nil {
				log.Errorf("pack: %s table metadata rollback on error failed: %v", t.name, err)
			}
		}
	}()

	// init global max
	packsz = t.opts.PackSize()
	jlen, tlen = len(live), len(dead)
	_, globalmax = t.packidx.GlobalMinMax()
	maxloop = 2*t.packidx.Len() + 2*(tlen+jlen)/packsz + 2

	// This algorithm works like a merge-sort over a sequence of sorted packs.
	for {
		// stop when all journal and tombstone entries have been processed
		if jpos >= jlen && tpos >= tlen {
			break
		}

		// skip deleted journal entries
		for ; jpos < jlen && dbits.IsSet(live[jpos].idx); jpos++ {
			// log.Debugf("%s: skipping deleted journal entry %d/%d gmax=%d", t.name, jpos, jlen, globalmax)
		}

		// skip processed tombstone entries
		for ; tpos < tlen && dead[tpos] == 0; tpos++ {
			// log.Debugf("%s: skipping processed tomb entry %d/%d gmax=%d", t.name, tpos, tlen, globalmax)
		}

		// skip trailing tombstone entries (for unwritten journal entries)
		for ; tpos < tlen && dead[tpos] > globalmax; tpos++ {
			// log.Debugf("%s: skipping trailing tomb entry %d at %d/%d gmax=%d", t.name, dead[tpos], tpos, tlen, globalmax)
		}

		// init on each iteration, either from journal or tombstone
		var nextid uint64
		switch true {
		case jpos < jlen && tpos < tlen:
			nextid = min(live[jpos].pk, dead[tpos])
			// if nextid == live[jpos].pk {
			// 	log.Debugf("%s: next id %d from journal %d/%d, gmax=%d", t.name, nextid, jpos, jlen, globalmax)
			// } else {
			// 	log.Debugf("%s: next id %d from tomb %d/%d, gmax=%d", t.name, nextid, tpos, tlen, globalmax)
			// }
		case jpos < jlen && tpos >= tlen:
			nextid = live[jpos].pk
			// log.Debugf("%s: next id %d from journal %d/%d, gmax=%d", t.name, nextid, jpos, jlen, globalmax)
		case jpos >= jlen && tpos < tlen:
			nextid = dead[tpos]
			// log.Debugf("%s: next id %d from tomb %d/%d, gmax=%d", t.name, nextid, tpos, tlen, globalmax)
		default:
			// stop in case remaining journal/tombstone entries were skipped
			break
		}

		// find best pack for insert/update/delete
		// skip when we're already appending to a new pack
		if lastpack < t.packidx.Len() {
			nextpack, packmin, packmax, nextmin = t.findBestPack(nextid)
			// log.Debugf("%s: selecting next pack %d with range [%d:%d] for next pkid=%d last-pack=%d/%d next-min=%d",
			// 	t.name, nextpack, packmin, packmax, nextid, lastpack, t.packidx.Len(), nextmin)
		}

		// store last pack when nextpack changes
		if lastpack != nextpack && pkg != nil {
			// saving a pack also deletes empty packs from storage!
			if pkg.IsDirty() {
				// log.Debugf("Storing pack %d with key %d with %d records", lastpack, pkg.key, pkg.Len())
				n, err = t.storePack(tx, pkg)
				if err != nil {
					return err
				}
				nParts++
				nBytes += n
				// commit storage tx after each N written packs
				if tx.Pending() >= txMaxSize {
					if err = t.storePackInfo(tx); err != nil {
						return err
					}
					if err = tx.CommitAndContinue(); err != nil {
						return err
					}
					// TODO: for a safe return we must also
					// - clear written journal/tombstone entries
					// - flush index (or implement index journal lookup)
					// - write table metadata and pack headers
					//
					// // check context before next turn
					// if err := ctx.Err(); err != nil {
					// 	return err
					// }
				}
				// update next values after pack index has changed
				nextpack, _, packmax, nextmin = t.findBestPack(nextid)
				// log.Debugf("%s: post-store next pack %d max=%d nextmin=%d", t.name, nextpack, packmax, nextmin)
			}
			// prepare for next pack
			pkg.Release()
			pkg = nil
		}

		// load or create the next pack
		if pkg == nil {
			if nextpack < t.packidx.Len() {
				// log.Debugf("%s: loading pack %d/%d key=%d len=%d", t.name, nextpack, t.packidx.Len(), t.packidx.packs[nextpack].Key, t.packidx.packs[nextpack].NValues)
				pkg, err = t.loadWritablePack(tx, t.packidx.packs[nextpack].Key)
				if err != nil && err != ErrPackNotFound {
					return err
				}
			}
			// start new pack
			if pkg == nil {
				nextpack = t.packidx.Len()
				packmin = 0
				packmax = 0
				nextmin = 0
				pkg = t.newPackage().PopulateFields(nil).WithKey(t.packidx.NextKey())
				// log.Debugf("%s: starting new pack %d/%d with key %d", t.name, nextpack, t.packidx.Len(), pkg.key)
			}
			lastpack = nextpack
			pAdd = 0
			pDel = 0
			pUpd = 0
		}

		// log.Debugf("Loop %d: tomb=%d/%d journal=%d/%d", loop, tpos, tlen, jpos, jlen)
		loop++
		if loop > 2*maxloop {
			log.Errorf("pack: %s stopping infinite flush loop %d: tomb-flush-pos=%d/%d journal-flush-pos=%d/%d pack=%d/%d nextid=%d",
				t.name, loop, tpos, tlen, jpos, jlen, lastpack, t.packidx.Len(), nextid,
			)
			err = fmt.Errorf("pack: %s infinite flush loop detected. Database is likely corrupted.", t.name)
			return err
		} else if loop == maxloop {
			lvl := log.Level()
			log.SetLevel(levelDebug)
			defer log.SetLevel(lvl)
			log.Debugf("pack: %s circuit breaker activated at loop %d tomb-flush-pos=%d/%d journal-flush-pos=%d/%d pack=%d/%d nextid=%d",
				t.name, loop, tpos, tlen, jpos, jlen, lastpack, t.packidx.Len(), nextid,
			)
		}

		// process tombstone records for this pack (skip for empty packs)
		if tpos < tlen && packmax > 0 && dead[tpos] <= packmax {
			// load current state of pack slices (will change after delete)
			pkcol := pkg.PkColumn()

			for ppos := 0; tpos < tlen; tpos++ {
				// next pk to delete
				pkid := dead[tpos]

				// skip already processed tombstone entries
				if pkid == 0 {
					continue
				}

				// stop on pack boundary
				if pkid > packmax {
					// log.Debugf("Tomb key %d does not match pack %d [%d:%d]", pkid, lastpack, packmin, packmax)
					break
				}

				// find the next matching pkid to clear
				ppos += sort.Search(len(pkcol)-ppos, func(i int) bool { return pkcol[i+ppos] >= pkid })
				if ppos == len(pkcol) || pkcol[ppos] != pkid {
					// clear from tombstone if not found
					dead[tpos] = 0
					continue
				}

				// count consecutive matches
				n := 1
				for tpos+n < tlen &&
					ppos+n < len(pkcol) &&
					pkcol[ppos+n] == dead[tpos+n] {
					n++
				}

				// remove records from all indexes
				for _, idx := range t.indexes {
					if err = idx.RemoveTx(tx, pkg, ppos, n); err != nil {
						return err
					}
				}

				// remove records from pack, changes pkcol (!)
				pkg.Delete(ppos, n)

				// mark as processed
				for i := 0; i < n; i++ {
					dead[tpos+i] = 0
				}
				nDel += n
				pDel += n

				// reload current state of pack slices
				pkcol = pkg.PkColumn()

				// update pack min/max
				packmin, packmax = 0, 0
				if l := len(pkcol); l > 0 {
					packmin, packmax = pkcol[0], pkcol[l-1]
				}

				// advance tomb pointer by one less (for-loop adds +1)
				tpos += n - 1
				// log.Debugf("Deleted %d tombstones from pack %d/%d with key %d", n, lastpack, t.packidx.Len(), pkg.key)
			}
		} else {
			// process journal entries for this pack

			// TODO: can we optimize for bulk-insert/append, e.g. when pk > packmax?
			// journal order matters since we walk indirect
			//
			// implement a reverse-merge-sort like algorithm similar
			// to how we handle journal data, bulk update/insert/append
			// when journal data is consecutive

			for last, offs := 0, 0; jpos < jlen; jpos++ {
				// next journal key for insert/update
				key := live[jpos]

				// skip deleted journal records
				if dbits.IsSet(key.idx) {
					continue
				}

				// stop on pack boundary
				if nextmin > 0 && key.pk >= nextmin {
					// best, min, max, _ := t.findBestPack(key.pk)
					// log.Debugf("Key %d does not fit into pack %d [%d:%d], suggested %d/%d [%d:%d] nextmin=%d",
					// 	key.pk, lastpack, packmin, packmax, best, t.packidx.Len(), min, max, nextmin)
					break
				}

				// check if record exists: packs are sorted by pk, so we can
				// safely skip ahead using the last offset, if the pk does
				// not exist we know the insert position right away; insert
				// will have to move all block slices by +1 so it is highly
				// inefficient for massive amounts of out-of-order inserts
				offs, last = pkg.PkIndex(key.pk, last)
				var isOOInsert bool

				if offs > -1 {
					// update existing record

					// replace index records when data has changed
					for _, idx := range t.indexes {
						if !idx.field.Type.EqualPacksAt(
							pkg, idx.field.Index, offs,
							jpack, idx.field.Index, key.idx,
						) {
							// remove index for original data
							if err = idx.RemoveTx(tx, pkg, offs, 1); err != nil {
								return err
							}
							// add new index record
							if err = idx.AddTx(tx, jpack, key.idx, 1); err != nil {
								return err
							}
						}
					}

					// overwrite original
					if err = pkg.ReplaceFrom(jpack, offs, key.idx, 1); err != nil {
						return err
					}
					nUpd++
					pUpd++

					// next journal record
					continue

				} else {
					// detect out of order inserts
					isOOInsert = key.pk < packmax

					// split on out-of-order inserts into a full pack
					if isOOInsert && pkg.IsFull() {
						log.Warnf("flush: split %s table pack %d [%d:%d] at out-of-order insert key %d ",
							t.name, pkg.key, packmin, packmax, key.pk)

						// split pack
						n, err = t.splitPack(tx, pkg)
						if err != nil {
							return err
						}
						nParts++
						nBytes += n

						// leave journal for-loop to trigger new pack selection
						loop = 0      // reset circuit breaker check
						lastpack = -1 // force pack load in next round
						pkg.Release()
						pkg = nil
						break
					}

					// Don't insert when pack is full to prevent buffer overflows. This may
					// happen when the current full pack was selected for a prior update,
					// but no re-selection happened before this insert.
					//
					// Reason is that the above boundary check does not always work, in
					// particular for the edge case of the very last pack because
					// nextmin = 0 in this case.
					//
					if pkg.IsFull() {
						break
					}

					// insert new record
					if isOOInsert {
						// insert in-place (EXPENSIVE!)
						// log.Debugf("Insert key %d to pack %d", key.pk, lastpack)
						if err = pkg.InsertFrom(jpack, last, key.idx, 1); err != nil {
							return err
						}
						packmin = util.NonZeroMin(packmin, key.pk)
					} else {
						// append new records
						// log.Debugf("Append key %d to pack %d", key.pk, lastpack)
						if err = pkg.AppendFrom(jpack, key.idx, 1); err != nil {
							return err
						}
						packmax = max(packmax, key.pk)
						globalmax = max(globalmax, key.pk)
					}

					// add to indexes
					for _, idx := range t.indexes {
						if err = idx.AddTx(tx, jpack, key.idx, 1); err != nil {
							return err
						}
					}
				}
				nAdd++
				pAdd++

				// save when full
				if pkg.Len() >= packsz {
					// store pack, will update t.packidx
					// log.Debugf("%s: storing pack %d with %d records at key %d", t.name, lastpack, pkg.Len(), pkg.key)
					n, err = t.storePack(tx, pkg)
					if err != nil {
						return err
					}
					nParts++
					nBytes += n

					// commit tx after each N written packs
					if tx.Pending() >= txMaxSize {
						if err = t.storePackInfo(tx); err != nil {
							return err
						}
						if err = tx.CommitAndContinue(); err != nil {
							return err
						}
						// TODO: for a safe return we must also
						// - clear written journal/tombstone entries
						// - flush index (or implement index journal lookup)
						// - write table metadata and pack headers
						//
						// // check context before next turn
						// if err:=ctx.Err(); err != nil {
						// 	return err
						// }
					}

					// after store, leave journal for-loop to trigger pack selection
					jpos++
					lastpack = -1 // force pack load in next round
					pkg.Release()
					pkg = nil
					break
				}
			}
		}
	}

	// store last processed pack
	if pkg != nil && pkg.IsDirty() {
		// log.Debugf("Storing final pack %d with %d records at key %d", lastpack, pkg.Len(), pkg.key)
		n, err = t.storePack(tx, pkg)
		if err != nil {
			return err
		}
		nParts++
		nBytes += n
		pkg.Release()
		pkg = nil
	}

	dur := time.Since(start)
	atomic.StoreInt64(&t.stats.LastFlushDuration, int64(dur))
	log.Debugf("flush: %s table %d packs add=%d del=%d total_size=%s in %s",
		t.name, nParts, nAdd, nDel, util.ByteSize(nBytes), dur)

	// flush indexes
	for _, idx := range t.indexes {
		if err = idx.FlushTx(ctx, tx); err != nil {
			return err
		}
	}

	// fix row count which may be wrong when
	// - non-existing pks were deleted
	// - updates are actually inserts
	t.meta.Rows = int64(t.packidx.Count())
	t.meta.dirty = true
	atomic.StoreInt64(&t.stats.TupleCount, t.meta.Rows)

	// store table metadata
	if t.meta.dirty {
		var buf []byte
		buf, err = json.Marshal(t.meta)
		if err != nil {
			return err
		}
		err = tx.tx.Bucket(t.metakey).Put(metaKey, buf)
		if err != nil {
			return err
		}
		t.meta.dirty = false
	}

	// store pack headers
	if err = t.storePackInfo(tx); err != nil {
		return err
	}

	// clear journal and tombstone
	t.journal.Reset()

	// save (now empty) journal and tombstone
	return t.flushJournalTx(ctx, tx)
}

// Use pack index to find closest match for placing pkval based on min/max of the
// pk column. Handles gaps in the pk sequence inside packs and gaps between packs.
// Note that pk values are user-defined, so they may contain gaps and insert/update/
// delete may happen anywhere in a pack.
//
// Attention!
//
// Placement does not support clean out-of-order pk inserts or deletion+reinsert
// of the same keys. This will lead to pack fragmentation. See flushTx for more
// details.
//
// The placement algorithm works as follows:
// - keep lastpack when no pack exists (effectively == 0)
// - choose pack with pack.min <= val <= pack.max
// - choose pack with closest max < val
// - when val < min of first pack, choose first pack
func (t *PackTable) findBestPack(pkval uint64) (int, uint64, uint64, uint64) {
	// returns 0 when list is empty, this ensures we initially stick
	// to the first pack until it's full; returns last pack for values
	// > global max
	bestpack, min, max, nextmin, isFull := t.packidx.Best(pkval)

	// insert/update placement into an exsting pack's range always stays with this pack

	// hacker's delight trick for unsigned range checks
	// see https://stackoverflow.com/questions/17095324/fastest-way-to-determine-if-an-integer-is-between-two-integers-inclusive-with
	// pkval >= min && pkval <= max
	if !isFull || pkval-min <= max-min {
		// log.Debugf("%s: %d is full=%t or pk %d is in range [%d:%d]", t.name, bestpack, isFull, pkval, min, max)
		return bestpack, min, max, nextmin
	}

	// if pack is full check if there is room in the next pack, but protect
	// invariant by checking pkval against next pack's min value
	if isFull && nextmin > 0 && pkval < nextmin {
		nextbest, min, max, nextmin, isFull := t.packidx.Next(bestpack)
		if min+max > 0 && !isFull {
			// log.Debugf("%s: %d is full, but next pack %d exists and is not", t.name, bestpack, nextbest)
			return nextbest, min, max, nextmin
		}
	}

	// trigger new pack creation
	// log.Debugf("%s: Should create new pack for key=%d: isfull=%t min=%d, max=%d nextmin=%d", t.name, pkval, isFull, min, max, nextmin)
	return t.packidx.Len(), 0, 0, 0

}

func (t *PackTable) QueryIndexesTx(ctx context.Context, tx *Tx, node *ConditionTreeNode) (int, error) {
	// - visit all leafs, run index scan when field is indexed and condition allowed
	// - if collission-free, mark condition as processed (don't execute again)
	// - add IN cond to front of current tree branch level
	//   -> leaf-roots do not exist (invariant)
	var hits int
	ins := make([]ConditionTreeNode, 0)
	for i, v := range node.Children {
		if v.Leaf() {
			// if !q.fidx.Contains(v.Cond.Field.Name) {
			// 	// q.Debugf("query: %s table non-indexed field %s for cond %s, fallback to table scan",
			// 	// 	q.Name, v.Cond.Field.Name, v.Cond.String())
			// 	continue
			// }
			idx := t.Indexes().Find(v.Cond.Field.Name)
			if idx == nil {
				// q.Debugf("query: %s table missing index on field %s for cond %d, fallback to table scan",
				// 	q.Name, v.Cond.Field.Name, v.Cond.String())
				continue
			}
			if !idx.CanMatch(*v.Cond) {
				// q.Debugf("query: %s index %s cannot match cond %s, fallback to table scan",
				// 	q.Name, idx.Name, v.Cond.String())
				continue
			}

			// q.Debugf("query: %s index scan for %s", idx.stats.IndexName, v.Cond.String())

			// lookup matching primary keys from index (result is sorted)
			pkmatch, err := idx.LookupTx(ctx, tx, *v.Cond)
			if err != nil {
				return hits, fmt.Errorf("index scan: %v", err)
			}
			hits += len(pkmatch)

			// mark condition as processed (exclude hash indexes because they may
			// have collisions; to protect against this, we continue matching this
			// condition against the full result set, which should be much smaller
			// now)
			if !idx.kind.MayHaveCollisions() {
				v.Cond.processed = true
			}
			// q.Debugf("query: %s index scan found %d matches", q.Name, len(pkmatch))

			if len(pkmatch) == 0 {
				v.Cond.nomatch = true
				continue
			}

			// create new leaf node
			c := &Condition{
				Field:    t.Fields().Pk(), // primary key
				Mode:     FilterModeIn,    // IN
				Value:    pkmatch,         // list
				IsSorted: true,            // already sorted by index lookup
				Raw:      v.Cond.Raw + "/index_lookup",
			}

			// compile to build internal maps
			if err := c.Compile(); err != nil {
				return 0, fmt.Errorf("compile index cond: %v", err)
			}

			// keep for later append
			ins = append(ins, ConditionTreeNode{Cond: c})
		} else {
			// recurse into child (use ptr to slice element)
			n, err := t.QueryIndexesTx(ctx, tx, &node.Children[i])
			if err != nil {
				return hits, err
			}
			hits += n
		}
	}

	// add new leafs to front of child list; this assumes the new indexed
	// condition (a list of primary keys) has lower execution cost than
	// other conditions in the same sub-tree
	//
	// FIXME: ideally we would keep processed conditions around and just skip
	// them in MaybeMatchPack() and MatchPack(); then we could just prepend
	// node.Children = append(ins, node.Children...)
	if len(ins) > 0 {
		for _, v := range node.Children {
			// skip processed source conditions unless they led to an empty result
			// because we need them to check for nomatch later
			if v.Leaf() && v.Cond.processed && !v.Cond.nomatch {
				// q.Debugf("query: %s replacing condition %s", q.Name, v.Cond.String())
				continue
			}
			ins = append(ins, v)
		}
		node.Children = ins
		// q.Debugf("Updated query: %v", logpkg.NewClosure(func() string {
		// 	return q.Dump()
		// }))
	}

	return hits, nil
}

func (t *PackTable) LookupPks(ctx context.Context, ids []uint64) (*Result, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return t.LookupPksTx(ctx, tx, ids)
}

// unsafe when called concurrently! lock table _before_ starting bolt tx!
func (t *PackTable) LookupPksTx(ctx context.Context, tx *Tx, ids []uint64) (*Result, error) {
	q := NewQuery(t.name + ".lookup").WithTable(t)
	if err := q.Compile(); err != nil {
		return nil, err
	}
	res := &Result{
		fields: t.Fields(), // we return all fields
		pkg: t.newPackage().
			WithKey(resultKey).
			WithCap(q.Limit).
			PopulateFields(nil),
	}
	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.stats.RowsMatched))
		q.Close()
	}()
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	// make sorted and unique copy of ids and strip any zero (i.e. illegal) ids
	ids = vec.Uint64.RemoveZeros(ids)
	ids = vec.Uint64.Unique(ids)

	// since journal can contain deleted entries, remove them from lookup
	if t.journal.TombLen() > 0 {
		var (
			ok   bool
			last int
		)
		for i, v := range ids {
			ok, last = t.journal.IsDeleted(v, last)
			if ok {
				ids[i] = 0
			}
			if last == t.journal.TombLen() {
				break
			}
		}
		// remove zeros again
		ids = vec.Uint64.RemoveZeros(ids)
	}

	// early return if all lookup ids are deleted or out of range
	if len(ids) == 0 || ids[0] > t.meta.Sequence {
		return res, nil
	}

	// keep max lookup id
	maxRows := len(ids)
	maxNonZeroId := ids[maxRows-1]

	// lookup journal first (Note: its sorted by pk)
	var (
		idx, last  int
		needUpdate bool
	)
	for i, v := range ids {
		// no more matches in journal?
		if last == t.journal.Len() {
			break
		}

		idx, last = t.journal.PkIndex(v, last)
		// not in journal?
		if idx < 0 {
			continue
		}

		// on match, copy result from journal
		if err := res.pkg.AppendFrom(t.journal.DataPack(), idx, 1); err != nil {
			res.Close()
			return nil, err
		}
		q.stats.RowsMatched++

		// mark id as processed (set 0)
		ids[i] = 0
		needUpdate = true
	}
	if needUpdate {
		// remove processed ids
		ids = vec.Uint64.RemoveZeros(ids)
	}

	q.stats.JournalTime = q.Tick()

	// everything found in journal?, return early
	if len(ids) == 0 {
		return res, nil
	}

	// PACK SCAN, iterator uses range checks
	var nextid int
	it := NewLookupIterator(&q, ids)
	defer it.Close()

	for {
		// stop when all inputs are matched
		if maxRows == q.stats.RowsMatched {
			break
		}

		if err := ctx.Err(); err != nil {
			res.Close()
			return nil, err
		}

		// load next pack with potential matches, use pack max pk to break early
		pack, maxPk, err := it.Next(tx)
		if err != nil {
			res.Close()
			return nil, err
		}

		// finish when no more packs are found
		if pack == nil {
			break
		}

		pk := pack.PkColumn()

		// loop over the remaining (unresolved) list of pks
		// packs are sorted by pk, ids does not contain zero values
		last := 0
		for _, v := range ids[nextid:] {
			// no more matches in this pack?
			if maxPk < v || pk[last] > maxNonZeroId {
				break
			}

			// not in pack
			j, _ := pack.PkIndex(v, last)
			if j < 0 {
				nextid++
				continue
			}

			// on match, copy result from package
			if err := res.pkg.AppendFrom(pack, j, 1); err != nil {
				res.Close()
				return nil, err
			}
			nextid++
			q.stats.RowsMatched++
			last = j
		}
	}
	q.stats.ScanTime = q.Tick()
	return res, nil
}

func (t *PackTable) Query(ctx context.Context, q Query) (*Result, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()
	if q.Order == OrderAsc {
		return t.QueryTx(ctx, tx, q)
	} else {
		return t.QueryTxDesc(ctx, tx, q)
	}
}

// NOTE: not concurrency safe lock table _before_ starting bolt tx!
func (t *PackTable) QueryTx(ctx context.Context, tx *Tx, q Query) (*Result, error) {
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	// check conditions match table
	q = q.WithTable(t)
	if err := q.Compile(); err != nil {
		return nil, err
	}

	// prepare journal match
	var jbits *vec.Bitset
	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.stats.RowsMatched))
		q.Close()
		if jbits != nil {
			jbits.Close()
		}
	}()

	// run journal query before index query to avoid side-effects of
	// added pk lookup condition (otherwise only indexed pks are found,
	// but not new pks that are only in journal)
	jbits = q.conds.MatchPack(t.journal.DataPack(), PackInfo{})
	q.stats.JournalTime = q.Tick()
	// log.Debugf("Table %s: %d journal results", t.name, jbits.Count())

	// maybe run index query
	if err := q.QueryIndexes(ctx, tx); err != nil {
		return nil, err
	}

	// prepare result package
	res := &Result{
		fields: q.freq,
		pkg: t.newPackage().
			WithKey(resultKey).
			WithCap(q.Limit).
			PopulateFields(q.freq).
			UpdateAliasesFrom(q.freq),
	}

	// early return
	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return res, nil
	}

	// PACK SCAN (either using found pk ids or non-indexed conditions)
	// scan packs only if (a) index match returned any results or (b) no index exists
	if !q.IsEmptyMatch() {
		it := NewForwardIterator(&q)
		defer it.Close()

	packloop:
		for {
			// check context
			if err := ctx.Err(); err != nil {
				res.Close()
				return nil, err
			}

			// load next pack with real matches
			pack, hits, err := it.Next(tx)
			if err != nil {
				res.Close()
				return nil, err
			}

			// finish when no more packs are found
			if pack == nil {
				break
			}

			for _, idx := range hits {
				i := int(idx)

				// skip broken entries
				pkid, err := pack.Uint64At(pack.pkindex, i)
				if err != nil {
					continue
				}

				// skip deleted entries
				if ok, _ := t.journal.IsDeleted(pkid, 0); ok {
					continue
				}

				src := pack
				index := i

				// when exists, use row version found in journal
				if j, _ := t.journal.PkIndex(pkid, 0); j >= 0 {
					// cross-check the journal row actually matches the cond
					if !jbits.IsSet(j) {
						continue
					}

					// remove match bit
					jbits.Clear(j)
					src = t.journal.DataPack()
					index = j
				}

				// skip offset
				if q.Offset > 0 {
					q.Offset--
					continue
				}

				if err := res.pkg.AppendFrom(src, index, 1); err != nil {
					res.Close()
					return nil, err
				}
				q.stats.RowsMatched++

				if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
					break packloop
				}
			}
		}
		q.stats.ScanTime = q.Tick()
	}

	// finalize on limit
	if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
		return res, nil
	}

	// after all packs have been scanned, add remaining rows from journal, if any
	idxs, _ := t.journal.SortedIndexes(jbits)
	jpack := t.journal.DataPack()
	// log.Debugf("Table %s: %d remaining journal rows", t.name, len(idxs))
	for _, idx := range idxs {
		// skip offset
		if q.Offset > 0 {
			q.Offset--
			continue
		}

		// Note: deleted entries are already removed from index list!
		if err := res.pkg.AppendFrom(jpack, idx, 1); err != nil {
			res.Close()
			return nil, err
		}
		q.stats.RowsMatched++

		if q.Limit > 0 && q.stats.RowsMatched == q.Limit {
			break
		}
	}
	q.stats.JournalTime += q.Tick()

	return res, nil
}

// DESCENDING pk order algorithm
func (t *PackTable) QueryTxDesc(ctx context.Context, tx *Tx, q Query) (*Result, error) {
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	// check conditions match table
	q = q.WithTable(t)
	if err := q.Compile(); err != nil {
		return nil, err
	}

	// prepare journal query
	var jbits *vec.Bitset
	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.stats.RowsMatched))
		q.Close()
		if jbits != nil {
			jbits.Close()
		}
	}()

	// run journal query before index query to avoid side-effects of
	// added pk lookup condition (otherwise only indexed pks are found,
	// but not new pks that are only in journal)
	// reverse the bitfield order for descending walk
	jbits = q.conds.MatchPack(t.journal.DataPack(), PackInfo{})
	q.stats.JournalTime = q.Tick()

	// maybe run index query
	if err := q.QueryIndexes(ctx, tx); err != nil {
		return nil, err
	}

	// prepare result package
	res := &Result{
		fields: q.freq,
		pkg: t.newPackage().
			WithKey(resultKey).
			WithCap(q.Limit).
			PopulateFields(q.freq).
			UpdateAliasesFrom(q.freq),
	}

	// early return
	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return res, nil
	}

	// find max pk across all saved packs (we assume any journal entry greater than this max
	// is new and hasn't been saved before; this assumption breaks when user-defined pk
	// values are smaller, so a user must flush the journal before query)
	_, maxPackedPk := t.packidx.GlobalMinMax()

	// before scanning packs, add 'new' rows from journal (i.e. pk > maxPackedPk),
	// walk in descending order
	idxs, pks := t.journal.SortedIndexesReversed(jbits)
	jpack := t.journal.DataPack()
	for i, idx := range idxs {
		// Note: deleted indexes are already removed from list

		// skip entries that are already inside packs (will be processed later)
		if pks[i] <= maxPackedPk {
			continue
		}

		// skip offset
		if q.Offset > 0 {
			q.Offset--
			continue
		}

		if err := res.pkg.AppendFrom(jpack, idx, 1); err != nil {
			res.Close()
			return nil, err
		}
		q.stats.RowsMatched++
		jbits.Clear(idx)

		if q.Limit > 0 && q.stats.RowsMatched == q.Limit {
			break
		}
	}
	q.stats.JournalTime = q.Tick()

	// finalize on limit
	if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
		return res, nil
	}

	// reverse-scan packs only if
	// (a) index match returned any results or
	// (b) no index exists
	if q.IsEmptyMatch() {
		return res, nil
	}

	// REVERSE PACK SCAN (either using found pk ids or non-indexed conditions)
	it := NewReverseIterator(&q)
	defer it.Close()

packloop:
	for {
		// check context
		if err := ctx.Err(); err != nil {
			res.Close()
			return nil, err
		}

		// load next pack with real matches
		pack, hits, err := it.Next(tx)
		if err != nil {
			res.Close()
			return nil, err
		}

		// finish when no more packs are found
		if pack == nil {
			break
		}

		// walk hits in reverse pk order
		for k := len(hits) - 1; k >= 0; k-- {
			i := int(hits[k])

			// skip broken entries
			pkid, err := pack.Uint64At(pack.pkindex, i)
			if err != nil {
				continue
			}

			// skip deleted entries
			if ok, _ := t.journal.IsDeleted(pkid, 0); ok {
				continue
			}

			src := pack
			index := i

			// when exists, use row from journal
			if j, _ := t.journal.PkIndex(pkid, 0); j >= 0 {
				// cross-check if the journal row actually matches the cond
				if !jbits.IsSet(j) {
					continue
				}
				jbits.Clear(j)
				src = t.journal.DataPack()
				index = j
			}

			// skip offset
			if q.Offset > 0 {
				q.Offset--
				continue
			}

			if err := res.pkg.AppendFrom(src, index, 1); err != nil {
				res.Close()
				return nil, err
			}
			q.stats.RowsMatched++

			if q.Limit > 0 && q.stats.RowsMatched == q.Limit {
				break packloop
			}
		}
	}

	q.stats.ScanTime = q.Tick()
	return res, nil
}

func (t *PackTable) Count(ctx context.Context, q Query) (int64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return 0, err
	}

	defer tx.Rollback()
	return t.CountTx(ctx, tx, q)
}

func (t *PackTable) CountTx(ctx context.Context, tx *Tx, q Query) (int64, error) {
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	q = q.WithTable(t)
	if err := q.Compile(); err != nil {
		return 0, err
	}

	var jbits *vec.Bitset

	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.stats.RowsMatched))
		jbits.Close()
		q.Close()
	}()

	// run journal query before index query to avoid side-effects of
	// added pk lookup condition (otherwise only indexed pks are found,
	// but not new pks that are only in journal)
	jbits = q.conds.MatchPack(t.journal.DataPack(), PackInfo{})
	q.stats.JournalTime = q.Tick()

	// maybe run index query
	if err := q.QueryIndexes(ctx, tx); err != nil {
		return 0, err
	}

	// early return
	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return 0, nil
	}

	// PACK SCAN (either using found pk ids or non-indexed conditions)
	// scan packs only when index match returned any results of when no index exists
	if !q.IsEmptyMatch() {
		it := NewForwardIterator(&q)
		defer it.Close()

	packloop:
		for {
			// check context
			if err := ctx.Err(); err != nil {
				return int64(q.stats.RowsMatched), err
			}

			// load next pack with real matches
			pack, hits, err := it.Next(tx)
			if err != nil {
				return int64(q.stats.RowsMatched), err
			}

			// finish when no more packs are found
			if pack == nil {
				break
			}

			for _, idx := range hits {
				i := int(idx)

				// skip broken entries
				pkid, err := pack.Uint64At(pack.pkindex, i)
				if err != nil {
					continue
				}

				// skip deleted entries
				if ok, _ := t.journal.IsDeleted(pkid, 0); ok {
					continue
				}

				// when exists, clear from journal bitmask
				if j, _ := t.journal.PkIndex(pkid, 0); j >= 0 {
					// cross-check if journal row actually matches the cond
					if !jbits.IsSet(j) {
						continue
					}
					jbits.Clear(j)
				}

				// skip offset
				if q.Offset > 0 {
					q.Offset--
					continue
				}
				q.stats.RowsMatched++

				if q.Limit > 0 && q.stats.RowsMatched == q.Limit {
					break packloop
				}
			}
		}
		q.stats.ScanTime = q.Tick()
	}

	// after all packs have been scanned, add remaining rows from journal, if any
	// use SortedIndexes to mask deleted rows that are only in journal
	// subtract offset and clamp to [0, limit]
	ids, _ := t.journal.SortedIndexes(jbits)
	q.stats.RowsMatched += max(len(ids)-q.Offset, 0)
	if q.Limit > 0 {
		q.stats.RowsMatched = min(q.stats.RowsMatched, q.Limit)
	}

	return int64(q.stats.RowsMatched), nil
}

func (t *PackTable) Stream(ctx context.Context, q Query, fn func(r Row) error) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	if q.Order == OrderAsc {
		err = t.StreamTx(ctx, tx, q, fn)
	} else {
		err = t.StreamTxDesc(ctx, tx, q, fn)
	}
	if err == EndStream {
		return nil
	}
	return err
}

// Similar to QueryTx but returns each match via callback function to allow stream
// processing at low memory overheads.
func (t *PackTable) StreamTx(ctx context.Context, tx *Tx, q Query, fn func(r Row) error) error {
	atomic.AddInt64(&t.stats.StreamCalls, 1)

	q = q.WithTable(t)
	if err := q.Compile(); err != nil {
		return err
	}

	// prepare journal query
	var jbits *vec.Bitset
	defer func() {
		atomic.AddInt64(&t.stats.StreamedTuples, int64(q.stats.RowsMatched))
		if jbits != nil {
			jbits.Close()
		}
		q.Close()
	}()

	// run journal query before index query to avoid side-effects of
	// added pk lookup condition (otherwise only indexed pks are found,
	// but not new pks that are only in journal)
	jbits = q.conds.MatchPack(t.journal.DataPack(), PackInfo{})
	q.stats.JournalTime = q.Tick()
	// q.Debugf("Table %s: %d journal results", t.name, jbits.Count())

	// maybe run index query
	if err := q.QueryIndexes(ctx, tx); err != nil {
		return err
	}

	// early return
	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return nil
	}

	// prepare result
	res := &Result{fields: q.freq}

	// PACK SCAN (either using found pk ids or non-indexed conditions)
	// scan packs only when
	// (a) index match returned any results or
	// (b) when no index exists
	if !q.IsEmptyMatch() {
		it := NewForwardIterator(&q)
		defer it.Close()
	packloop:
		for {
			// check context
			if err := ctx.Err(); err != nil {
				return err
			}

			// load next pack with real matches
			pack, hits, err := it.Next(tx)
			if err != nil {
				return err
			}

			// finish when no more packs are found
			if pack == nil {
				break
			}

			for _, idx := range hits {
				i := int(idx)

				// skip broken entries
				pkid, err := pack.Uint64At(pack.pkindex, i)
				if err != nil {
					continue
				}

				// skip deleted entries
				if ok, _ := t.journal.IsDeleted(pkid, 0); ok {
					continue
				}

				res.pkg = pack
				index := i

				// when exists, use row version found in journal
				if j, _ := t.journal.PkIndex(pkid, 0); j >= 0 {
					// cross-check the journal row actually matches the cond
					if !jbits.IsSet(j) {
						continue
					}

					// remove match bit
					jbits.Clear(j)
					res.pkg = t.journal.DataPack()
					index = j
				}

				// skip offset
				if q.Offset > 0 {
					q.Offset--
					continue
				}

				// forward match
				// q.Debugf("Table %s: using result at pack=%d index=%d pkid=%d", t.name, pack.key, index, pkid)
				if err := fn(Row{res: res, n: index}); err != nil {
					return err
				}
				res.pkg = nil
				q.stats.RowsMatched++

				if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
					break packloop
				}
			}
		}
		q.stats.ScanTime = q.Tick()
	}

	if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
		return nil
	}

	// after all packs have been scanned, add remaining rows from journal, if any
	res.pkg = t.journal.DataPack()
	idxs, _ := t.journal.SortedIndexes(jbits)
	// q.Debugf("Table %s: %d remaining journal rows", t.name, len(idxs))
	for _, idx := range idxs {
		// Note: deleted indexes are already removed from list

		// skip offset
		if q.Offset > 0 {
			q.Offset--
			continue
		}

		// forward match
		if err := fn(Row{res: res, n: idx}); err != nil {
			return err
		}
		q.stats.RowsMatched++

		if q.Limit > 0 && q.stats.RowsMatched == q.Limit {
			return nil
		}
	}
	q.stats.JournalTime += q.Tick()
	// q.Debugf("%s", q.PrintTiming())

	return nil
}

// DESCENDING order stream
func (t *PackTable) StreamTxDesc(ctx context.Context, tx *Tx, q Query, fn func(r Row) error) error {
	atomic.AddInt64(&t.stats.StreamCalls, 1)

	q = q.WithTable(t)
	if err := q.Compile(); err != nil {
		return err
	}

	// prepare journal query
	var jbits *vec.Bitset
	defer func() {
		atomic.AddInt64(&t.stats.StreamedTuples, int64(q.stats.RowsMatched))
		if jbits != nil {
			jbits.Close()
		}
		q.Close()
	}()

	// run journal query before index query to avoid side-effects of
	// added pk lookup condition (otherwise only indexed pks are found,
	// but not new pks that are only in journal)
	// reverse the bitfield order for descending walk
	jbits = q.conds.MatchPack(t.journal.DataPack(), PackInfo{})
	q.stats.JournalTime = q.Tick()
	// log.Debugf("Table %s: %d journal results", t.name, jbits.Count())

	// maybe run index query
	if err := q.QueryIndexes(ctx, tx); err != nil {
		return err
	}

	// early return
	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return nil
	}

	// find max pk across all saved packs (we assume any journal entry greater than this max
	// is new and hasn't been saved before; this assumption breaks when user-defined pk
	// values are smaller, so a user must flush the journal before query)
	_, maxPackedPk := t.packidx.GlobalMinMax()

	// prepare result
	res := &Result{fields: q.freq}

	// before scanning packs, add 'new' rows from journal (i.e. pk > maxPackedPk),
	// walk in descending order
	res.pkg = t.journal.DataPack()
	idxs, pks := t.journal.SortedIndexesReversed(jbits)
	// log.Debugf("Table %s: %d processing journal rows first", t.name, len(idxs))
	for i, idx := range idxs {
		// Note: deleted indexes are already removed from list

		// skip previously stored entries (will be processed later)
		if pks[i] <= maxPackedPk {
			continue
		}

		// clear matching bit
		jbits.Clear(idx)

		// skip offset
		if q.Offset > 0 {
			q.Offset--
			continue
		}

		// forward match
		if err := fn(Row{res: res, n: idx}); err != nil {
			return err
		}
		q.stats.RowsMatched++

		if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
			return nil
		}
	}
	q.stats.JournalTime += q.Tick()

	// reverse-scan packs only when
	// (a) index match returned any results or
	// (b) when no index exists
	if q.IsEmptyMatch() {
		return nil
	}

	it := NewReverseIterator(&q)
	defer it.Close()

packloop:
	for {
		// check context
		if err := ctx.Err(); err != nil {
			return err
		}

		// load next pack with real matches
		pack, hits, err := it.Next(tx)
		if err != nil {
			return err
		}

		// finish when no more packs are found
		if pack == nil {
			break
		}
		// log.Debugf("Table %s: %d results in pack %d", t.name, len(hits), pkg.key)

		// walk hits in reverse pk order
		for k := len(hits) - 1; k >= 0; k-- {
			i := int(hits[k])

			// skip broken entries
			pkid, err := pack.Uint64At(pack.pkindex, i)
			if err != nil {
				continue
			}

			// skip deleted entries
			if ok, _ := t.journal.IsDeleted(pkid, 0); ok {
				continue
			}

			res.pkg = pack
			index := i

			// when exists, use row from journal
			if j, _ := t.journal.PkIndex(pkid, 0); j >= 0 {
				if !jbits.IsSet(j) {
					continue
				}
				res.pkg = t.journal.DataPack()
				index = j
				jbits.Clear(j)
			}

			// skip offset
			if q.Offset > 0 {
				q.Offset--
				continue
			}

			// forward match
			if err := fn(Row{res: res, n: index}); err != nil {
				return err
			}
			res.pkg = nil
			q.stats.RowsMatched++

			if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
				break packloop
			}
		}
	}

	q.stats.ScanTime = q.Tick()
	return nil
}

func (t *PackTable) StreamPks(ctx context.Context, ids []uint64, fn func(r Row) error) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = t.StreamPksTx(ctx, tx, ids, fn)
	if err == EndStream {
		return nil
	}
	return err
}

func (t *PackTable) StreamPksTx(ctx context.Context, tx *Tx, ids []uint64, fn func(r Row) error) error {
	atomic.AddInt64(&t.stats.StreamCalls, 1)
	q := NewQuery(t.name + ".stream-lookup").WithTable(t)
	if err := q.Compile(); err != nil {
		return err
	}

	defer func() {
		atomic.AddInt64(&t.stats.StreamedTuples, int64(q.stats.RowsMatched))
		q.Close()
	}()

	// make sorted and unique copy of ids and strip any zero (i.e. illegal) ids
	ids = vec.Uint64.RemoveZeros(ids)
	ids = vec.Uint64.Unique(ids)

	// since journal can contain deleted entries, remove them from lookup
	if t.journal.TombLen() > 0 {
		var (
			ok   bool
			last int
		)
		for i, v := range ids {
			ok, last = t.journal.IsDeleted(v, last)
			if ok {
				ids[i] = 0
			}
			if last == t.journal.TombLen() {
				break
			}
		}
		// sort and remove zeros again
		ids = vec.Uint64.RemoveZeros(ids)
	}

	// early return if all lookup ids are deleted or out of range
	if len(ids) == 0 || ids[0] > t.meta.Sequence {
		return nil
	}

	// keep max lookup id
	maxRows := len(ids)
	maxNonZeroId := ids[maxRows-1]

	res := &Result{
		fields: t.Fields(),
		pkg:    t.journal.DataPack(),
	}

	// lookup journal first (Note: its sorted by pk)
	var (
		idx, last  int
		needUpdate bool
	)
	for i, v := range ids {
		// no more matches in journal?
		if last == t.journal.Len() {
			break
		}

		// not in journal
		idx, last = t.journal.PkIndex(v, last)
		if idx < 0 {
			continue
		}

		// on match, forward result from journal
		if err := fn(Row{res: res, n: idx}); err != nil {
			return err
		}
		q.stats.RowsMatched++

		// mark id as processed (set 0)
		ids[i] = 0
		needUpdate = true
	}
	if needUpdate {
		// remove processed ids
		ids = vec.Uint64.RemoveZeros(ids)
	}
	q.stats.JournalTime = q.Tick()

	// everything found in journal?, return early
	if len(ids) == 0 {
		return nil
	}

	// PACK SCAN, iterator uses range checks
	var nextid int
	it := NewLookupIterator(&q, ids)
	defer it.Close()

	for {
		// stop when all inputs are matched
		if maxRows == q.stats.RowsMatched {
			break
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		// load next pack with potential matches, use pack max pk to break early
		pack, maxPk, err := it.Next(tx)
		if err != nil {
			return err
		}

		// finish when no more packs are found
		if pack == nil {
			break
		}

		res.pkg = pack
		pk := pack.PkColumn()

		// loop over the remaining (unresolved) list of pks
		last := 0
		for _, v := range ids[nextid:] {
			// no more matches in this pack?
			if maxPk < v || pk[last] > maxNonZeroId {
				break
			}

			// not in pack == not in table, skip this id
			j, _ := pack.PkIndex(v, last)
			if j < 0 {
				nextid++
				continue
			}

			// forward match
			if err := fn(Row{res: res, n: j}); err != nil {
				return err
			}

			nextid++
			q.stats.RowsMatched++
			last = j
		}
	}
	q.stats.ScanTime = q.Tick()
	return nil
}

// merges non-full packs to minimize total pack count, also re-establishes a
// sequential/gapless pack key order when packs have been deleted
func (t *PackTable) Compact(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	start := time.Now()

	if err := ctx.Err(); err != nil {
		return err
	}

	// check if compaction is possible
	if t.packidx.Len() <= 1 {
		return nil
	}

	// check if compaction is required, either because packs are non-sequential
	// or not full (except the last)
	var (
		maxsz                 int = t.opts.PackSize()
		srcSize               int64
		nextpack              uint32
		needCompact           bool
		srcPacks              int = t.packidx.Len()
		total, moved, written int64
	)
	for i, v := range t.packidx.packs {
		needCompact = needCompact || v.Key > nextpack                      // sequence gap
		needCompact = needCompact || (i < srcPacks-1 && v.NValues < maxsz) // non-full pack (except the last)
		nextpack++
		total += int64(v.NValues)
		srcSize += int64(v.Packsize)
	}
	if !needCompact {
		log.Debugf("pack: %s table %d packs / %d rows already compact", t.name, srcPacks, total)
		return nil
	}

	// check if compaction precondition is satisfied
	// - no out-of-order min/max ranges across sorted pack keys exist

	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var (
		dstPack, srcPack *Package
		dstSize          int64
		dstIndex         int
		lastMaxPk        uint64
		isNewPack        bool
	)

	log.Debugf("pack: %s table compacting %d packs / %d rows", t.name, srcPacks, total)
	// t.DumpPackInfoDetail(os.Stdout, DumpModeDec, false)

	// This algorithm walks the table's pack list in pack key order and
	// collects/compacts contents in row id (pk) order. Note that pk order may
	// differ from pack order if out-of-order inserts ever happened. In such case
	// this algorithm may abort or skip such packs to preserve the invariant
	// of non-overlapping pk ranges between packs.
	//
	// Gaps in pack key sequence are filled with new packs created on the fly.
	// When source packs are emptied during the process, they are immediatly removed
	// from KV storage and header list, but may be re-added subsequently.
	//
	for {
		// stop when no more dst packs are found
		if dstIndex == t.packidx.Len() {
			break
		}

		// load next dst pack
		if dstPack == nil {
			dstKey := uint32(dstIndex)

			// handle existing pack keys
			if dstKey == t.packidx.packs[dstIndex].Key {
				// skip full packs
				if t.packidx.packs[dstIndex].NValues == maxsz {
					// log.Debugf("pack: skipping full dst pack key=%x", dstKey)
					dstIndex++
					continue
				}
				// skip out of order packs
				pmin, pmax := t.packidx.MinMax(dstIndex)
				if pmin < lastMaxPk {
					// log.Debugf("pack: skipping out-of-order dst pack key=%x", dstKey)
					dstIndex++
					continue
				}

				// log.Debugf("pack: loading dst pack %d key=%x", dstIndex, dstKey)
				dstPack, err = t.loadWritablePack(tx, dstKey)
				if err != nil {
					return err
				}
				lastMaxPk = pmax
				isNewPack = false
			} else {
				// handle gaps in key sequence
				// clone new pack from journal
				// log.Debugf("pack: creating new dst pack %d key=%x", dstIndex, dstKey)
				dstPack = t.newPackage().PopulateFields(nil).WithKey(dstKey)
				isNewPack = true
			}
		}

		// search for the next src pack that
		// - has a larger key than the current destination pack AND
		// - has the smallest min pk higher than the current destination's max pk
		if srcPack == nil {
			minSlice, _ := t.packidx.MinMaxSlices()
			var startIndex, srcIndex int = dstIndex, -1
			var lastmin uint64 = math.MaxUint64
			if isNewPack && startIndex > 0 {
				startIndex--
			}
			for i := startIndex; i < len(minSlice); i++ {
				if t.packidx.packs[i].Key < dstPack.key {
					continue
				}
				currmin := minSlice[i]
				if currmin <= lastMaxPk {
					continue
				}
				if lastmin > currmin {
					lastmin = currmin
					srcIndex = i
				}
			}

			// stop when no more source pack was found
			if srcIndex < 0 {
				break
			}

			ph := t.packidx.packs[srcIndex]
			// log.Debugf("pack: loading src pack %d key=%x", srcIndex, ph.Key)
			srcPack, err = t.loadWritablePack(tx, ph.Key)
			if err != nil {
				return err
			}
		}

		// Guarantees at this point:
		// - dstPack has free space
		// - srcPack is not empty

		// determine free space in destination
		free := maxsz - dstPack.Len()
		cp := min(free, srcPack.Len())
		moved += int64(cp)

		// move data from src to dst
		// log.Debugf("pack: moving %d/%d rows from pack %x to %x", cp, srcPack.Len(),
		// 	srcPack.key, dstPack.key)
		if err := dstPack.AppendFrom(srcPack, 0, cp); err != nil {
			return err
		}
		if err := srcPack.Delete(0, cp); err != nil {
			return err
		}
		total += int64(cp)
		lastMaxPk, err = dstPack.Uint64At(dstPack.pkindex, dstPack.Len()-1)
		if err != nil {
			return err
		}

		// write dst when full
		if dstPack.Len() == maxsz {
			// this may extend the pack header list when dstPack is new
			// log.Debugf("pack: storing full dst pack %x", dstPack.key)
			n, err := t.storePack(tx, dstPack)
			if err != nil {
				return err
			}
			dstSize += int64(n)
			dstIndex++
			written += int64(maxsz)

			// will load or create another output pack in next iteration
			dstPack.Release()
			dstPack = nil
		}

		// if srcPack.Len() == 0 {
		// 	log.Debugf("pack: deleting empty src pack %x", srcPack.key)
		// }

		// store or delete source pack
		if _, err := t.storePack(tx, srcPack); err != nil {
			return err
		}

		// load new src in next iteration (or stop there)
		srcPack.Release()
		srcPack = nil

		// commit tx after each N written packs
		if tx.Pending() >= txMaxSize {
			if err := t.storePackInfo(tx); err != nil {
				return err
			}
			if err := tx.CommitAndContinue(); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}

	// store the last dstPack
	if dstPack != nil {
		// log.Debugf("pack: storing last dst pack %x", dstPack.key)
		n, err := t.storePack(tx, dstPack)
		if err != nil {
			return err
		}
		dstSize += int64(n)
		written += int64(dstPack.Len())
		dstPack.Release()
	}

	log.Debugf("pack: %s table compacted %d(+%d) rows into %d(%d) packs (%s ->> %s) in %s",
		t.name, moved, written-moved,
		t.packidx.Len(), srcPacks-t.packidx.Len(),
		util.ByteSize(srcSize), util.ByteSize(dstSize),
		time.Since(start),
	)
	// t.DumpPackInfoDetail(os.Stdout, DumpModeDec, false)

	// store pack headers
	if err := t.storePackInfo(tx); err != nil {
		return err
	}

	return tx.Commit()
}

func (t *PackTable) cachekey(key []byte) string {
	return t.name + "/" + hex.EncodeToString(key)
}

func (t *PackTable) loadSharedPack(tx *Tx, id uint32, touch bool, fields FieldList) (*Package, error) {
	if len(fields) == 0 {
		fields = t.fields
	}
	key := encodePackKey(id)

	// try cache lookup for existing blocks first
	cachefn := t.bcache.Peek
	if touch {
		cachefn = t.bcache.Get
	}
	// Get PackInfo and fill metadata
	pi := t.packidx.GetByKey(id)

	// fetch pack from pool or create new pack, has nil in block slice
	pkg := t.newPackage().WithKey(pi.Key)
	pkg.nValues = pi.NValues
	pkg.size = pi.Packsize

	// identify uncached blocks
	var loadField FieldList
	for i, v := range pkg.fields {
		if !fields.Contains(v.Name) {
			continue
		}
		cachekey := encodeBlockKey(id, i)

		if b, ok := cachefn(cachekey); ok {
			pkg.blocks[i] = b
		} else {
			loadField = loadField.Add(v)
		}
	}

	// all blocks found in cache
	if len(loadField) == 0 {
		return pkg, nil
	}

	// load missing blocks from storage using a temp pack as buffer
	var err error
	pkg2 := t.newPackage().PopulateFields(loadField)
	pkg2, err = loadPackTx(tx, t.key, key, pkg2, t.opts.PackSize())
	if err != nil {
		pkg2.Release()
		pkg.Release()
		return nil, err
	}

	// log.Debugf("%s: loaded shared pack %d col=%d row=%d", t.name, pkg.key, pkg.nFields, pkg.nValues)
	atomic.AddInt64(&t.stats.PacksLoaded, 1)
	atomic.AddInt64(&t.stats.BytesRead, int64(pkg.size))

	// store in cache
	if touch {
		for i, v := range pkg2.blocks {
			if v != nil {
				t.bcache.Add(encodeBlockKey(id, i), v)
			}
		}
	}

	// merge blocks into target pack
	err = pkg.Merge(pkg2)
	pkg2.Release()
	if err != nil {
		pkg.Release()
		return nil, err
	}

	return pkg, nil
}

// loads a private copy of a pack for writing
func (t *PackTable) loadWritablePack(tx *Tx, id uint32) (*Package, error) {
	key := encodePackKey(id)

	// Get PackInfo and fill metadata
	pi := t.packidx.GetByKey(id)

	// fetch pack from pool or create new pack, blocks are nil at this point
	pkg := t.newPackage().WithKey(pi.Key)
	pkg.nValues = pi.NValues
	pkg.size = pi.Packsize

	var loadField FieldList
	for i, v := range pkg.fields {
		cachekey := encodeBlockKey(id, i)
		if b, ok := t.bcache.Get(cachekey); ok {
			pkg.blocks[i] = b
		} else {
			loadField = loadField.Add(v)
		}
	}

	clone, err := pkg.Clone(t.opts.PackSize())
	pkg.Release()
	pkg = nil

	if err != nil {
		clone.Release()
		return nil, err
	}

	// all blocks found in cache
	if len(loadField) == 0 {
		// prepare for efficient writes
		clone.Materialize()
		return clone, nil
	}

	// fetch pack from pool or create new pack
	pkg2 := t.newPackage().PopulateFields(loadField)
	pkg2, err = loadPackTx(tx, t.key, key, pkg2, t.opts.PackSize())
	if err != nil {
		pkg2.Release()
		clone.Release()
		return nil, err
	}

	err = clone.Merge(pkg2)
	pkg2.Release()
	if err != nil {
		clone.Release()
		return nil, err
	}

	// prepare for efficient writes
	clone.Materialize()

	atomic.AddInt64(&t.stats.PacksLoaded, 1)
	atomic.AddInt64(&t.stats.BytesRead, int64(pkg2.size))
	return clone, nil
}

func (t *PackTable) storePack(tx *Tx, pkg *Package) (int, error) {
	key := pkg.Key()

	defer func() {
		id := uint64(pkg.key)
		// remove all blocks from cache
		for _, v := range t.bcache.Keys() {
			if v>>32 == id {
				t.bcache.Remove(v)
			}
		}
	}()

	if pkg.Len() > 0 {
		// build header statistics
		info := pkg.Info()
		err := info.UpdateStats(pkg)
		if err != nil {
			return 0, err
		}

		// optimize/dedup
		pkg.Optimize()

		// write to disk
		n, err := storePackTx(tx, t.key, key, pkg, t.opts.FillLevel)
		if err != nil {
			return 0, err
		}

		// update statistics
		info.Packsize = n
		t.packidx.AddOrUpdate(info)
		atomic.AddInt64(&t.stats.PacksStored, 1)
		atomic.AddInt64(&t.stats.BytesWritten, int64(n))
		atomic.StoreInt64(&t.stats.PacksCount, int64(t.packidx.Len()))
		atomic.StoreInt64(&t.stats.MetaSize, int64(t.packidx.HeapSize()))
		atomic.StoreInt64(&t.stats.TotalSize, int64(t.packidx.TableSize()))

		return n, nil

	} else {
		// pack is empty

		// drop from index
		t.packidx.Remove(pkg.key)

		// remove from storage
		if err := deletePackTx(tx, t.key, key); err != nil {
			return 0, err
		}

		atomic.StoreInt64(&t.stats.PacksCount, int64(t.packidx.Len()))
		atomic.StoreInt64(&t.stats.MetaSize, int64(t.packidx.HeapSize()))
		atomic.StoreInt64(&t.stats.TotalSize, int64(t.packidx.TableSize()))

		return 0, nil
	}
}

// Note: pack must have been storted before splitting
func (t *PackTable) splitPack(tx *Tx, pkg *Package) (int, error) {
	// log.Debugf("%s: split pack %d col=%d row=%d", t.name, pkg.key, pkg.nFields, pkg.nValues)
	// move half of the packs contents to a new pack (don't cache the new pack
	// to avoid possible eviction of the pack we are currently splitting!)
	newpkg := t.newPackage().PopulateFields(nil)
	half := pkg.Len() / 2
	if err := newpkg.AppendFrom(pkg, half, pkg.Len()-half); err != nil {
		return 0, err
	}
	if err := pkg.Delete(half, pkg.Len()-half); err != nil {
		return 0, err
	}

	// store both packs to update stats, this also stores the initial pack
	// on first split which may have not been stored yet
	n, err := t.storePack(tx, pkg)
	if err != nil {
		return 0, err
	}

	// set the new pack's key here to avoid overwrite when the very first pack
	// has never been stored
	newpkg.WithKey(t.packidx.NextKey())

	// save the new pack
	m, err := t.storePack(tx, newpkg)
	if err != nil {
		return 0, err
	}
	newpkg.Release()

	// drop original blocks from cache
	for i := range pkg.fields {
		t.bcache.Remove(encodeBlockKey(pkg.key, i))
	}

	return n + m, nil
}

func (t *PackTable) makePackage() interface{} {
	atomic.AddInt64(&t.stats.PacksAlloc, 1)
	pkg := NewPackage(t.opts.PackSize(), t.packPool)
	_ = pkg.InitFieldsFromEmpty(t.journal.DataPack())
	return pkg
}

func (t *PackTable) newPackage() *Package {
	pkg := t.packPool.Get().(*Package)
	assert.Always(pkg.key == 0, "pack: illegal package reuse", map[string]any{
		"ref":   pkg.refCount,
		"key":   pkg.key,
		"len":   pkg.nValues,
		"cap":   pkg.capHint,
		"dirty": pkg.dirty,
	})
	return pkg.WithCap(t.opts.PackSize())
}

func loadPackTx(tx *Tx, name, key []byte, unpack *Package, sz int) (*Package, error) {
	if unpack == nil {
		unpack = NewPackage(sz, nil)
	}
	b := tx.Bucket(name)
	if b == nil {
		return nil, ErrBucketNotFound
	}
	buf := b.Get(key)
	if buf == nil {
		return nil, ErrPackNotFound
	}
	unpack.SetKey(key)
	if err := unpack.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	unpack.dirty = false
	return unpack, nil
}

func storePackTx(tx *Tx, name, key []byte, p *Package, fill int) (int, error) {
	for _, v := range p.blocks {
		if v == nil {
			return 0, ErrPackStripped
		}
	}
	buf, err := p.MarshalBinary()
	if err != nil {
		return 0, err
	}
	b := tx.Bucket(name)
	if b == nil {
		return 0, ErrBucketNotFound
	}
	b.FillPercent(float64(fill) / 100.0)
	err = b.Put(key, buf)
	if err != nil {
		return 0, err
	}
	p.dirty = false
	tx.pending++
	return len(buf), nil
}

func deletePackTx(tx *Tx, name, key []byte) error {
	b := tx.Bucket(name)
	if b == nil {
		return ErrBucketNotFound
	}
	tx.pending++
	return b.Delete(key)
}
