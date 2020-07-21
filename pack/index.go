// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/packdb-pro/cache"
	"blockwatch.cc/packdb-pro/cache/lru"
	"blockwatch.cc/packdb-pro/hash"
	"blockwatch.cc/packdb-pro/store"
	"blockwatch.cc/packdb-pro/util"
	"blockwatch.cc/packdb-pro/vec"
)

// Collision handling
// - store colliding hashes as duplicates in a pack
// - handle special case where colliding value crosses a pack
// - tombstone stores hash + primary key and we check both values on removal

type IndexType int

type IndexValueFunc func(typ FieldType, val interface{}) uint64
type IndexValueAtFunc func(typ FieldType, pkg *Package, index, pos int) uint64

const (
	IndexTypeHash    IndexType = iota // any col (any type) -> uint64 FNV hash
	IndexTypeInteger                  // any col ((u)int64) -> pk (uint64)
)

func (t IndexType) String() string {
	switch t {
	case IndexTypeHash:
		return "hash"
	case IndexTypeInteger:
		return "int"
	default:
		return "invalid"
	}
}

func (t IndexType) ValueFunc() IndexValueFunc {
	switch t {
	case IndexTypeHash:
		return hashValue
	case IndexTypeInteger:
		return intValue
	default:
		return nil
	}
}

func (t IndexType) ValueAtFunc() IndexValueAtFunc {
	switch t {
	case IndexTypeHash:
		return hashValueAt
	case IndexTypeInteger:
		return intValueAt
	default:
		return nil
	}
}

func (t IndexType) MayHaveCollisions() bool {
	switch t {
	case IndexTypeHash:
		return true
	case IndexTypeInteger:
		return true
	default:
		return false
	}
}

type IndexEntry struct {
	Key uint64 `pack:"K,pk,snappy"` // hash key, i.e. FNV(value)
	Id  uint64 `pack:"I,snappy"`    // OID of table entry
}

type Index struct {
	Name  string    `json:"name"`  // stored in table metadata
	Type  IndexType `json:"typ"`   // stored in table metadata
	Field Field     `json:"field"` // stored in table metadata
	opts  Options   // stored in table metadata

	// function pointers
	indexValue   IndexValueFunc
	indexValueAt IndexValueAtFunc

	table     *Table
	cache     cache.Cache
	journal   *Package   // append log
	tombstone *Package   // delete log
	packs     *PackIndex // in-memory list of pack and block headers
	key       []byte     // bucket name
	metakey   []byte     // metadata bucket name
	packPool  *sync.Pool // buffer pool for new packages
}

type IndexList []*Index

func (l IndexList) FindField(fieldname string) *Index {
	for _, v := range l {
		if v.Field.Name == fieldname {
			return v
		}
	}
	return nil
}

func (t *Table) CreateIndex(name string, field Field, typ IndexType, opts Options) (*Index, error) {
	opts.MergeDefaults()
	if err := opts.Check(); err != nil {
		return nil, err
	}
	field.Flags |= FlagIndexed
	idx := &Index{
		Name:         name,
		Type:         typ,
		Field:        field,
		opts:         opts,
		table:        t,
		packs:        NewPackIndex(nil, 0),
		key:          []byte(t.name + "_" + name + "_index"),
		metakey:      []byte(t.name + "_" + name + "_index_meta"),
		indexValue:   typ.ValueFunc(),
		indexValueAt: typ.ValueAtFunc(),
	}
	idx.packPool = &sync.Pool{
		New: idx.makePackage,
	}
	err := t.db.db.Update(func(dbTx store.Tx) error {
		b := dbTx.Bucket(idx.key)
		if b != nil {
			return ErrIndexExists
		}
		_, err := dbTx.Root().CreateBucketIfNotExists(idx.key)
		if err != nil {
			return err
		}
		meta, err := dbTx.Root().CreateBucketIfNotExists(idx.metakey)
		if err != nil {
			return err
		}
		_, err = meta.CreateBucketIfNotExists(headerKey)
		if err != nil {
			return err
		}
		buf, err := json.Marshal(idx.opts)
		if err != nil {
			return err
		}
		err = meta.Put(optsKey, buf)
		if err != nil {
			return err
		}
		// create empty journal
		idx.journal = NewPackage()
		if err := idx.journal.Init(IndexEntry{}, 1<<uint(idx.opts.JournalSizeLog2)); err != nil {
			return err
		}
		_, err = storePackTx(dbTx, idx.metakey, journalKey, idx.journal, idx.opts.FillLevel)
		if err != nil {
			return err
		}
		// create empty tombstone
		idx.tombstone = NewPackage()
		if err := idx.tombstone.Init(IndexEntry{}, 1<<uint(idx.opts.JournalSizeLog2)); err != nil {
			return err
		}
		_, err = storePackTx(dbTx, idx.metakey, tombstoneKey, idx.tombstone, idx.opts.FillLevel)
		if err != nil {
			return err
		}
		// add index to table's list of indexes and store the list
		meta = dbTx.Bucket(t.metakey)
		t.indexes = append(t.indexes, idx)
		buf, err = json.Marshal(t.indexes)
		if err != nil {
			return err
		}
		err = meta.Put(indexesKey, buf)
		if err != nil {
			return err
		}
		// update index flag on the indexed field
		for i, v := range t.fields {
			if v.Name == idx.Field.Name {
				t.fields[i].Flags |= FlagIndexed
			}
		}
		buf, err = json.Marshal(t.fields)
		if err != nil {
			return err
		}
		err = meta.Put(fieldsKey, buf)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if idx.opts.CacheSize > 0 {
		idx.cache, err = lru.New2QWithEvict(int(idx.opts.CacheSize), idx.onEvictedPackage)
		if err != nil {
			return nil, err
		}
	} else {
		idx.cache = cache.NewNoCache()
	}

	// Note: reindex may take a long time and requires a context which we don't have
	//        here, so it's the job of the caller to ensure the index is built

	log.Debugf("Created %s index %s_%s", typ.String(), t.name, name)
	return idx, nil
}

func (t *Table) CreateIndexIfNotExists(name string, field Field, typ IndexType, opts Options) (*Index, error) {
	idx, err := t.CreateIndex(name, field, typ, opts)
	if err != nil {
		if err != ErrIndexExists {
			return nil, err
		}
		for _, v := range t.indexes {
			if v.Name == name {
				return v, nil
			}
		}
		return nil, ErrIndexNotFound
	}
	return idx, nil
}

func (t *Table) DropIndex(name string) error {
	var (
		pos int = -1
		idx *Index
	)
	for i, v := range t.indexes {
		if v.Name == name {
			pos, idx = i, v
			break
		}
	}
	if idx == nil {
		return ErrNoIndex
	}
	idx.cache.Purge()
	t.indexes = append(t.indexes[:pos], t.indexes[pos+1:]...)

	// update index flag on the indexed field
	for i, v := range t.fields {
		if v.Name == idx.Field.Name {
			t.fields[i].Flags ^= FlagIndexed
		}
	}

	// store table metadata and delete index buckets
	return t.db.db.Update(func(dbTx store.Tx) error {
		meta := dbTx.Bucket(t.metakey)
		buf, err := json.Marshal(t.indexes)
		if err != nil {
			return err
		}
		err = meta.Put(indexesKey, buf)
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
		err = dbTx.Root().DeleteBucket([]byte(t.name + "_" + name + "_index"))
		if err != nil {
			return err
		}
		return dbTx.Root().DeleteBucket([]byte(t.name + "_" + name + "_index_meta"))
	})
}

func (t *Table) OpenIndex(idx *Index, opts ...Options) error {
	if len(opts) > 0 {
		log.Debugf("Opening %s_%s index with opts %#v", t.name, idx.Name, opts[0])
	} else {
		log.Debugf("Opening %s_%s index with default opts", t.name, idx.Name)
	}
	idx.packs = NewPackIndex(nil, 0)
	idx.table = t
	idx.key = []byte(t.name + "_" + idx.Name + "_index")
	idx.metakey = []byte(t.name + "_" + idx.Name + "_index_meta")
	idx.packPool = &sync.Pool{
		New: idx.makePackage,
	}
	idx.indexValue = idx.Type.ValueFunc()
	idx.indexValueAt = idx.Type.ValueAtFunc()

	// check index exists, load journal and tombstone
	err := t.db.db.View(func(dbTx store.Tx) error {
		b := dbTx.Bucket(idx.metakey)
		if b == nil {
			return ErrNoIndex
		}
		buf := b.Get(optsKey)
		if buf == nil {
			return fmt.Errorf("pack: missing options for index %s", idx.cachekey(nil))
		}
		err := json.Unmarshal(buf, &idx.opts)
		if err != nil {
			return err
		}
		idx.journal, err = loadPackTx(dbTx, idx.metakey, journalKey, nil)
		if err != nil {
			return fmt.Errorf("pack: cannot open journal for index %s: %v", idx.cachekey(nil), err)
		}
		idx.journal.initType(IndexEntry{})
		log.Debugf("pack: loaded %s index journal with %d entries", idx.cachekey(nil), idx.journal.Len())
		idx.tombstone, err = loadPackTx(dbTx, idx.metakey, tombstoneKey, nil)
		if err != nil {
			return fmt.Errorf("pack: %s index cannot open tombstone: %v", idx.cachekey(nil), err)
		}
		idx.tombstone.initType(IndexEntry{})
		log.Debugf("pack: index %s loaded tombstone with %d entries",
			idx.cachekey(nil), idx.tombstone.Len())
		return idx.loadPackHeaders(dbTx)
	})
	if err != nil {
		return err
	}
	cacheSize := idx.opts.CacheSize
	if len(opts) > 0 {
		cacheSize = opts[0].CacheSize
		if opts[0].JournalSizeLog2 > 0 {
			idx.opts.JournalSizeLog2 = opts[0].JournalSizeLog2
		}
	}
	if cacheSize > 0 {
		idx.cache, err = lru.New2QWithEvict(int(cacheSize), idx.onEvictedPackage)
		if err != nil {
			return err
		}
	} else {
		idx.cache = cache.NewNoCache()
	}

	return nil
}

func (idx *Index) Options() Options {
	return idx.opts
}

func (idx *Index) loadPackHeaders(dbTx store.Tx) error {
	b := dbTx.Bucket(idx.metakey)
	if b == nil {
		return ErrNoTable
	}
	heads := make(PackageHeaderList, 0)
	bh := b.Bucket(headerKey)
	if bh != nil {
		log.Debugf("pack: %s index loading package headers from bucket", idx.cachekey(nil))
		c := bh.Cursor()
		var err error
		for ok := c.First(); ok; ok = c.Next() {
			head := PackageHeader{}
			err = head.UnmarshalBinary(c.Value())
			if err != nil {
				break
			}
			heads = append(heads, head)
			atomic.AddInt64(&idx.table.stats.MetaBytesRead, int64(len(c.Value())))
		}
		if err != nil {
			heads = heads[:0]
			log.Errorf("pack: header decode for index %s pack %x: %v", idx.cachekey(nil), c.Key(), err)
		} else {
			idx.packs = NewPackIndex(heads, 0)
			log.Debugf("pack: %s index loaded %d package headers", idx.cachekey(nil), idx.packs.Len())
			return nil
		}
	}
	// on error, scan packs
	log.Warnf("pack: scanning headers for index %s...", idx.cachekey(nil))
	c := dbTx.Bucket(idx.key).Cursor()
	pkg := idx.journal.Clone(false, 0)
	for ok := c.First(); ok; ok = c.Next() {
		ph, err := pkg.UnmarshalHeader(c.Value())
		if err != nil {
			return fmt.Errorf("pack: cannot scan index pack %s: %v", idx.cachekey(c.Key()), err)
		}
		// force re-write
		ph.dirty = true
		// don't leak memory from referencing slice data
		ph.Key = make([]byte, len(c.Key()))
		copy(ph.Key, c.Key())
		heads = append(heads, ph)
		atomic.AddInt64(&idx.table.stats.MetaBytesRead, int64(len(c.Value())))
	}
	idx.packs = NewPackIndex(heads, 0)
	log.Debugf("pack: %s index scanned %d package headers", idx.cachekey(nil), idx.packs.Len())
	return nil
}

func (idx *Index) storePackHeaders(dbTx store.Tx) error {
	b := dbTx.Bucket(idx.metakey)
	if b == nil {
		return ErrNoTable
	}

	// pack headers are stored in a nested bucket
	hb := b.Bucket(headerKey)
	for i := range idx.packs.heads {
		if !idx.packs.heads[i].dirty {
			continue
		}
		buf, err := idx.packs.heads[i].MarshalBinary()
		if err != nil {
			return err
		}
		if err := hb.Put(idx.packs.heads[i].Key, buf); err != nil {
			return err
		}
		idx.packs.heads[i].dirty = false
		atomic.AddInt64(&idx.table.stats.MetaBytesWritten, int64(len(buf)))
	}
	return nil
}

func (idx *Index) AddTx(tx *Tx, pkg *Package, srcPos, srcLen int) error {
	// Maps a (hash) key of the indexed field's content to the pk field's
	// content for the package.
	//
	// Appends (key, pk) tuples to journal until full, then flushes/packs
	// the journal into packs. Packs are ordered, but a global order does not
	// exist since packs once stored are not touched again unless entries are
	// removed. Also, we do not check for duplicates, in fact duplicates
	// are stored as is and lookup will find and return all duplicates.
	var pk []uint64
	if col, err := pkg.Column(pkg.pkindex); err != nil {
		return err
	} else {
		pk, _ = col.([]uint64)
	}
	atomic.AddInt64(&idx.table.stats.IndexInsertCalls, 1)

	var count int64
	for i := srcPos; i < srcPos+srcLen; i++ {
		// don't index zero values
		if pkg.IsZeroAt(idx.Field.Index, i) {
			continue
		}

		// build index entry directly from pack value
		entry := IndexEntry{
			Key: idx.indexValueAt(idx.Field.Type, pkg, idx.Field.Index, i),
			Id:  pk[i],
		}

		// append to journal, will sort later
		if err := idx.journal.Push(entry); err != nil {
			return err
		}
		count++
	}

	atomic.AddInt64(&idx.table.stats.IndexInsertedTuples, count)
	return nil
}

func (idx *Index) RemoveTx(tx *Tx, pkg *Package, srcPos, srcLen int) error {
	// Appends (hash) keys to tombstone until full, then flushes/packs the journal
	// and tombstone into stored packs.
	col, _ := pkg.Column(pkg.pkindex)
	pk, _ := col.([]uint64)
	atomic.AddInt64(&idx.table.stats.IndexDeleteCalls, 1)

	var count int64
	for i := srcPos; i < srcPos+srcLen; i++ {
		// don't index zero values
		if pkg.IsZeroAt(idx.Field.Index, i) {
			continue
		}

		// append hash value to tombstone
		if err := idx.tombstone.Push(IndexEntry{
			Key: idx.indexValueAt(idx.Field.Type, pkg, idx.Field.Index, i),
			Id:  pk[i],
		}); err != nil {
			return err
		}
		count++
	}
	atomic.AddInt64(&idx.table.stats.IndexDeletedTuples, count)

	return nil
}

// this index only supports
// - FilterModeEqual
// - FilterModeIn
// - FilterModeNotIn
func (idx *Index) CanMatch(cond Condition) bool {
	if idx.Field.Name != cond.Field.Name {
		return false
	}
	switch cond.Mode {
	case FilterModeEqual, FilterModeIn, FilterModeNotIn:
		return true
	default:
		return false
	}
}

// []in -> []oid
func (idx *Index) LookupTx(ctx context.Context, tx *Tx, cond Condition) ([]uint64, error) {
	if !idx.CanMatch(cond) {
		return nil, fmt.Errorf("pack: condition %s incompatibe with %s index %s_%s",
			cond, idx.Type, idx.table.name, idx.Name)
	}

	// alloc temp slice from pool
	keys := idx.table.pkPool.Get().([]uint64)

	// fill with hash values
	switch cond.Mode {
	case FilterModeEqual:
		// search single value
		if !idx.Field.Type.isZero(cond.Value) {
			keys = append(keys, idx.indexValue(idx.Field.Type, cond.Value))
		}
	case FilterModeIn, FilterModeNotIn:
		// sort and search slice of values
		slice := reflect.ValueOf(cond.Value)
		if slice.Kind() != reflect.Slice {
			return nil, fmt.Errorf("pack: %s index lookup requires slice type, got %T",
				idx.Type, cond.Value)
		}
		for i, l := 0, slice.Len(); i < l; i++ {
			v := slice.Index(i).Interface()
			if !idx.Field.Type.isZero(v) {
				keys = append(keys, idx.indexValue(idx.Field.Type, v))
			}
		}
		vec.Uint64Sorter(keys).Sort()
	}

	res, err := idx.lookupKeys(ctx, tx, keys, cond.Mode == FilterModeNotIn)
	if err != nil {
		return nil, err
	}
	if cond.Mode != FilterModeNotIn {
		idx.table.pkPool.Put(keys[:0])
	}
	return res, nil
}

// Note: index journals are always empty on lookup because tables
//       fill and flush them when they flush their journal.
func (idx *Index) lookupKeys(ctx context.Context, tx *Tx, in []uint64, neg bool) ([]uint64, error) {
	atomic.AddInt64(&idx.table.stats.IndexQueryCalls, 1)
	if len(in) == 0 {
		return []uint64{}, nil
	}

	// alloc result slice from pool, should be returned by caller
	out := idx.table.pkPool.Get().([]uint64)
	var nPacks int

	// optimize for rollback and lookup of most recently added index values
	for nextpack := idx.packs.Len() - 1; nextpack >= 0; nextpack-- {
		// stop when all inputs are matched
		if len(in) == 0 {
			break
		}

		// stop when context is canceled
		if util.InterruptRequested(ctx) {
			out = out[:0]
			idx.table.pkPool.Put(out)
			return nil, ctx.Err()
		}

		// continue with next pack

		// extract min/max values from pack header (this is defined by IndexEntry,
		// so we're safe to assume the following call will not fail); then
		// skip packs that don't contain keys in range (`in` is sorted and gets
		// updated as matches are found)
		min, max := idx.packs.MinMax(nextpack)
		if max < in[0] || min > in[len(in)-1] {
			continue
		}

		// load and cache pack
		ipkg, err := idx.loadPack(tx, idx.packs.heads[nextpack].Key, true)
		if err != nil {
			return nil, err
		}
		nPacks++

		// we use index and value slices
		col, _ := ipkg.Column(0)
		keys, _ := col.([]uint64)
		col, _ = ipkg.Column(1)
		values, _ := col.([]uint64)

		// packs are sorted by indexed key, we use this to improve search performance
		// FIXME: binary search may be more efficient
		for h, i, hl, il := 0, 0, len(keys), len(in); h < hl && i < il; {
			if max < in[i] {
				// no more matches in this pack
				break
			}
			for h < hl && keys[h] < in[i] {
				h++
			}
			if h == hl {
				break
			}
			for i < il && keys[h] > in[i] {
				i++
			}
			if i == il {
				break
			}
			if keys[h] == in[i] {
				// append to result
				out = append(out, values[h])

				// peek the next pack entries to handle key collision
				// h can safely be advanced because a collision in in[i]
				// will have added all colliding target values already
				for ; h+1 < hl && keys[h+1] == in[i]; h++ {
					out = append(out, values[h+1])
				}

				// edge case: when a key collision spans two packs we
				// must not remove in[i] just yet, but instead continue
				// with the next pack; at this point we can only check if
				// min_current == in[i], but we don't know if
				// max_prev == in[i] (giving we traverse packs in reverse
				// order). It is still save to break here.
				if h+1 == hl && min == in[i] {
					break
				}

				// delete found key from input slice
				in = append(in[:i], in[i+1:]...)
				il--
			}
		}
	}

	// `in` contains only missing keys now
	if neg {
		idx.table.pkPool.Put(out[:0])
		out = in
	}

	// sort result before return
	vec.Uint64Sorter(out).Sort()
	atomic.AddInt64(&idx.table.stats.IndexQueriedTuples, int64(len(out)))
	return out, nil
}

func (idx *Index) Reindex(ctx context.Context, flushEvery int, ch chan<- float64) error {
	tx, err := idx.table.db.Tx(true)
	if err != nil {
		return err
	}

	// be panic safe
	defer tx.Rollback()
	if err := idx.ReindexTx(ctx, tx, flushEvery, ch); err != nil {
		tx.Rollback()
		return err
	}

	tx.Commit()
	return nil
}

func (idx *Index) ReindexTx(ctx context.Context, tx *Tx, flushEvery int, ch chan<- float64) error {
	// drop index data partitions
	for i := idx.packs.Len() - 1; i >= 0; i-- {
		key := idx.packs.heads[i].Key
		cachekey := idx.cachekey(key)
		if err := tx.deletePack(idx.key, key); err != nil {
			return err
		}
		idx.cache.Remove(cachekey)
	}
	idx.packs = NewPackIndex(nil, 0)

	// clear and save journal and tombstone
	idx.journal.Clear()
	if _, err := tx.storePack(idx.metakey, journalKey, idx.journal, idx.opts.FillLevel); err != nil {
		return err
	}
	idx.tombstone.Clear()
	if _, err := tx.storePack(idx.metakey, tombstoneKey, idx.tombstone, idx.opts.FillLevel); err != nil {
		return err
	}

	// flush at most every 128 packs
	if flushEvery < 128 {
		flushEvery = 128
	}

	// scan table in pk order block by block and create new index
	for i, ph := range idx.table.packs.heads {
		// stop when context is canceled
		if util.InterruptRequested(ctx) {
			return ctx.Err()
		}

		// load pack (we need pk field and all index fields)
		fields := idx.table.Fields().Select(idx.Field.Name).Add(idx.table.Fields().Pk())
		pkg, err := idx.table.loadPack(tx, ph.Key, false, fields)
		if err != nil {
			return err
		}

		// index all packed rows at once
		err = idx.AddTx(tx, pkg, 0, pkg.Len())
		if err != nil {
			return err
		}

		// return pack to table's (!) pool
		idx.table.recyclePackage(pkg)

		// flush index after every 128 packs
		if i%flushEvery == 0 {
			// signal progress
			select {
			case ch <- float64(i*100) / float64(idx.table.packs.Len()):
			default:
			}
			err = idx.FlushTx(ctx, tx)
			if err != nil {
				return err
			}
		}
	}

	// final flush
	select {
	case ch <- float64(99):
	default:
	}
	err := idx.FlushTx(ctx, tx)
	if err != nil {
		return err
	}
	select {
	case ch <- float64(100):
	default:
	}

	// store journal with remaining data
	if idx.journal.IsDirty() {
		_, err := tx.storePack(idx.metakey, journalKey, idx.journal, idx.opts.FillLevel)
		if err != nil {
			return err
		}
	}
	return nil
}

// saves journal & tombstone on close
func (idx *Index) CloseTx(tx *Tx) error {
	log.Debugf("pack: closing %s index %s with %d/%d records", idx.Type,
		idx.cachekey(nil), idx.journal.Len(), idx.tombstone.Len())
	_, err := tx.storePack(idx.metakey, journalKey, idx.journal, idx.opts.FillLevel)
	if err != nil {
		return err
	}
	_, err = tx.storePack(idx.metakey, tombstoneKey, idx.tombstone, idx.opts.FillLevel)
	if err != nil {
		return err
	}
	if err := idx.storePackHeaders(tx.tx); err != nil {
		return err
	}
	return nil
}

// merge journal entries into data partitions, repack, store, and update indexes
func (idx *Index) FlushTx(ctx context.Context, tx *Tx) error {
	// an empty flush writes dirty pack headers
	atomic.AddInt64(&idx.table.stats.IndexFlushCalls, 1)
	atomic.AddInt64(&idx.table.stats.IndexFlushedTuples, int64(idx.journal.Len()+idx.tombstone.Len()))
	begin := time.Now()

	// requires sorted journal
	if err := idx.journal.PkSort(); err != nil {
		return err
	}

	// requires sorted tombstone
	if err := idx.tombstone.PkSort(); err != nil {
		return err
	}

	// work on hash and value slices
	col, _ := idx.tombstone.Column(0) // idx.tombstone.pkindex
	dead, _ := col.([]uint64)
	col, _ = idx.tombstone.Column(1)
	deadval, _ := col.([]uint64)

	col, _ = idx.journal.Column(0) // idx.journal.pkindex
	pk, _ := col.([]uint64)
	col, _ = idx.journal.Column(1)
	pkval, _ := col.([]uint64)

	// we'll always store full index packs only and keep overflow in the journal
	var nAdd, nDel, nParts, nBytes int

	log.Debugf("flush: %s idx %s %d journal and %d tombstone records",
		idx.Type, idx.cachekey(nil), len(pk), len(dead))

	// remove deleted entries from journal first
	// Note: both tombstone and journal are sorted by pk column (i.e. the hash)
	// Note: 0 is a valid hash value, so cannot be used to mark dead entries,
	//       for this reason we have to re-init pk/pkval and dead/deadval after
	//       deleting entries
	for j, d, jl, dl := 0, 0, len(pk), len(dead); j < jl && d < dl; {
		for d < dl && dead[d] < pk[j] {
			d++
		}
		if d == dl {
			break
		}
		for j < jl && dead[d] > pk[j] {
			j++
		}
		if j == jl {
			break
		}
		if dead[d] == pk[j] {
			// Handle collisions: We must make sure we always only delete
			// the correct entries by checking key AND value. Both tombstone
			// and journal can contain colliding keys and both are only sorted
			// by their pk (the key, i.e. hash), but not by value (the primary
			// table key) as well. The algorithm below takes care of all cases
			// by pair-wise matching colliding values and only deleting the
			// correct journal entries.
			for i := 0; j+i < jl && dead[d] == pk[j+i]; {
				if deadval[d] == pkval[j+i] {
					// shrink journal, will also shrink pk/pkval slices
					idx.journal.Delete(j+i, 1)
					jl--
					nDel++

					// re-init slices
					col, _ = idx.journal.Column(idx.journal.pkindex)
					pk, _ = col.([]uint64)
					col, _ = idx.journal.Column(1)
					pkval, _ = col.([]uint64)
				} else {
					i++
				}
			}

			// remove deleted tombstone
			idx.tombstone.Delete(d, 1)
			dl--

			// re-init slices
			col, _ = idx.tombstone.Column(idx.tombstone.pkindex)
			dead, _ = col.([]uint64)
			col, _ = idx.tombstone.Column(1)
			deadval, _ = col.([]uint64)
		}
	}
	log.Debugf("flush: %s idx %s deleted %d journal entries",
		idx.Type, idx.cachekey(nil), nDel)

	// delete tombstone entries from stored packs
	if idx.tombstone.Len() > 0 {
		// walk all index packs; stop when all tombstone entries are processed
		// multiple iterations may be required in the rare case when a key collision
		// spans multiple packs and multiple colliding key/value pairs are
		// out of order (see inline comments below)
		var packsProcessed int
		for nextPack, lenPacks := 0, idx.packs.Len(); len(dead) > 0 && packsProcessed < 3*lenPacks; nextPack = (nextPack + 1) % lenPacks {
			// check if pack contains any tombstone entry
			//
			// because tomstone journal is sorted by index key we
			// can exclude scanning index packs that are outside the
			// first..last range by just looking at their pack header.
			//
			// we're using two conditions to check first/last inclusion
			// that will check against an indexes' pk column (same type uint64
			// like Tombstone entries, but note that the uint64 stores the key,
			// i.e. the hash of an entry)
			//
			packsProcessed++

			// extract min/max values from pack header (this is defined by IndexEntry,
			// so we're safe to assume the following call will not fail)
			min, max := idx.packs.MinMax(nextPack)

			// skip packs that don't contain hash keys in range (in is sorted and gets
			// updated as matches are found)
			if max < dead[0] || min > dead[len(dead)-1] {
				continue
			}

			// load and scan the pack, and remove dead entries
			pkg, err := idx.loadPack(tx, idx.packs.heads[nextPack].Key, true)
			if err != nil {
				return err
			}

			log.Debugf("flush: %s idx removing dead entries from pack %s",
				idx.Type, idx.cachekey(pkg.key))

			// get pk and value columns
			col, _ = pkg.Column(0)
			pk, _ = col.([]uint64)
			col, _ = pkg.Column(1)
			pkval, _ = col.([]uint64)

			// index packs are sorted by pk (i.e. the hash value)
			before := nDel
			for i, d, il, dl := 0, 0, len(pk), len(dead); i < il && d < dl; {
				if max < dead[d] {
					// no more matches in this pack
					break
				}
				for d < dl && dead[d] < pk[i] {
					d++
				}
				if d == dl {
					break
				}
				for i < il && dead[d] > pk[i] {
					i++
				}
				if i == il {
					break
				}
				if dead[d] == pk[i] {
					// Handle collisions: We must make sure we always only delete
					// the correct entries by checking key AND value. Both tombstone
					// and pack can contain colliding keys and both are only sorted
					// by key (i.e. the hash), but not by value (the mapped primary
					// key). The algorithm below takes care of all cases
					// by pair-wise matching all colliding values and only deleting
					// the correct pack entries. However, when collisions span
					// multiple packs we may only find some values due to lack of sort.
					// In such a case the tombstone list may not become empty in one
					// flush cycle. Hence we continue the outer for loop until all
					// tombstone records have been processed. This comes at the added
					// cost of loading/storing some index packs twice, but since key
					// collisions are rare, this case won't happen very often anyways.
					//
					for j := 0; i+j < il && dead[d] == pk[i+j]; {
						if deadval[d] == pkval[i+j] {
							// shrink pack, will also shrink pk/pkval slices
							pkg.Delete(i+j, 1)
							il--
							nDel++

							// re-init slices
							col, _ = pkg.Column(0)
							pk, _ = col.([]uint64)
							col, _ = pkg.Column(1)
							pkval, _ = col.([]uint64)
						} else {
							j++
						}
					}

					// edge case: when a key collision spans two packs we
					// must not remove the tombstone just yet, but instead continue
					// with the next pack; at this point we can only check if
					// max_current == dead[d], but we don't know if
					// min_next == dead[d] (giving we traverse packs in forward
					// order). It is still save to break here.
					if il > 0 && d+1 == dl && max == dead[d] {
						break
					}

					// remove processed tombstone
					idx.tombstone.Delete(d, 1)
					dl--

					// re-init slices
					col, _ = idx.tombstone.Column(0)
					dead, _ = col.([]uint64)
					col, _ = idx.tombstone.Column(1)
					deadval, _ = col.([]uint64)
				}
			}
			log.Debugf("flush: %s idx removed %d dead entries from pack %s, %d are left",
				idx.Type, nDel-before, idx.cachekey(pkg.key), idx.tombstone.Len())

			// store the shortened index pack. this will update the pack on storage
			// and update its pack index entry to reflect changes in min/max statistics
			n, err := idx.storePack(tx, pkg)
			idx.recyclePackage(pkg)
			if err != nil {
				return err
			}
			nParts++
			nBytes += n

			// commit tx after each N written packs
			if tx.Pending() >= txMaxSize {
				if err := idx.storePackHeaders(tx.tx); err != nil {
					return err
				}
				if err := tx.CommitAndContinue(); err != nil {
					return err
				}
				// stop when context is canceled; this is safe her because
				// tombstone entries are removed when processed, so we only
				// have to save tombstone itself
				if util.InterruptRequested(ctx) {
					_, err := tx.storePack(idx.metakey, tombstoneKey, idx.tombstone, idx.opts.FillLevel)
					if err != nil {
						return err
					}
					if err := tx.Commit(); err != nil {
						return err
					}
					return ctx.Err()
				}
			}
		}
		log.Debugf("flush: %s idx %s removed %d dead entries total, %d are not found",
			idx.Type, idx.cachekey(nil), nDel, idx.tombstone.Len())

		// any remaining tombstone entries are not found, ignore
		if idx.tombstone.Len() > 0 {
			idx.tombstone.Clear()
		}

		// tombstone should be empty by now, write back to disk
		if idx.tombstone.IsDirty() {
			_, err := tx.storePack(idx.metakey, tombstoneKey, idx.tombstone, idx.opts.FillLevel)
			if err != nil {
				return err
			}
			if err := tx.CommitAndContinue(); err != nil {
				return err
			}
		}
	}

	// move journal data into buckets (packs), splitting them when full
	col, _ = idx.journal.Column(idx.journal.pkindex)
	pk, _ = col.([]uint64)

	if idx.journal.Len() > 0 {
		var (
			pkg           *Package
			err           error
			lastpack      int
			nextpack      int = -1
			min, max, rng uint64
			lastkey       uint64
			needsort      bool
			packsz        int = 1 << uint(idx.opts.PackSizeLog2)
		)

		// create an initial bucket on first insert
		if idx.packs.Len() == 0 {
			pkg = idx.packPool.Get().(*Package)
			pkg.key = idx.partkey(idx.packs.Len())
		}

		// walk journal and allocate key->id tuples to buckets
		for i, l := 0, len(pk); i < l; i++ {
			// find best bucket for inserting next journal entry if the
			// current bucket does no longer match; this quick range match
			// may fail and the more complex placement algorithm may still
			// select lastpack when its distance to pk[i] is smallest
			if nextpack < 0 || (pk[i]-min > rng) {
				nextpack, min, max = idx.packs.Best(pk[i])
				rng = max - min + 1 // assume next value is 1 larger than max
			}

			// store last bucket when nextpack changes
			if lastpack != nextpack && pkg != nil {
				if pkg.IsDirty() {
					// keep buckets sorted
					if needsort {
						if err := pkg.PkSort(); err != nil {
							return err
						}
						needsort = false
					}
					n, err := idx.storePack(tx, pkg)
					if err != nil {
						return err
					}
					nParts++
					nBytes += n
				}
				idx.recyclePackage(pkg)
				pkg = nil
				lastkey = 0
				needsort = false
				lastpack = nextpack
			}

			// load the next bucket
			if pkg == nil {
				pkg, err = idx.loadPack(tx, idx.packs.heads[nextpack].Key, true)
				if err != nil {
					return err
				}
				lastkey, _ = pkg.Uint64At(pkg.pkindex, pkg.Len()-1)
			}

			// append journal entry
			err := pkg.AppendFrom(idx.journal, i, 1, false)
			if err != nil {
				return err
			}
			needsort = needsort || pk[i] < lastkey
			lastkey = pk[i]
			min = util.MinU64(min, pk[i])
			max = util.MaxU64(max, pk[i])
			rng = max - min + 1
			nAdd++

			// split bucket when full
			if pkg.Len() == packsz {
				if needsort {
					if err := pkg.PkSort(); err != nil {
						return err
					}
					needsort = false
				}
				n, err := idx.splitPack(tx, pkg)
				if err != nil {
					return err
				}
				nParts++
				nBytes += n
				lastkey, _ = pkg.Uint64At(pkg.pkindex, pkg.Len()-1)
				needsort = false
				nextpack = -1 // force full pack search for next entry

				// commit tx after each N written packs
				if tx.Pending() >= txMaxSize {
					// TODO:
					// - remove processed entries from journal
					// - store journal pack on context cancel
					if err := idx.storePackHeaders(tx.tx); err != nil {
						return err
					}
					if err := tx.CommitAndContinue(); err != nil {
						return err
					}
					// TODO: for a safe return we must also
					// - mark or clear written journal entries
					// - save journal
					// - commit tx
					//
					// // stop when context is canceled
					// if interruptRequested(ctx) {
					// 	return ctx.Err()
					// }
				}
			}
		}

		// store last processed pack
		if pkg != nil && pkg.IsDirty() {
			// keep buckets sorted
			if needsort {
				if err := pkg.PkSort(); err != nil {
					return err
				}
			}
			n, err := idx.storePack(tx, pkg)
			if err != nil {
				return err
			}
			idx.recyclePackage(pkg)
			nParts++
			nBytes += n
		}

		// clear and save journal
		idx.journal.Clear()
		_, err = tx.storePack(idx.metakey, journalKey, idx.journal, idx.opts.FillLevel)
		if err != nil {
			return err
		}
	}

	// store final pack headers
	if err := idx.storePackHeaders(tx.tx); err != nil {
		return err
	}

	log.Debugf("flush: %s index %s %d packs add=%d del=%d total_size=%s in %s",
		idx.Type, idx.cachekey(nil), nParts, nAdd, nDel, util.ByteSize(nBytes),
		time.Since(begin))

	return nil
}

// Note: pack must be storted before splitting
func (idx *Index) splitPack(tx *Tx, pkg *Package) (int, error) {
	// move half of the packs contents to a new pack (don't cache the new pack
	// to avoid possible eviction of the pack we are currently splitting!)
	newpkg := idx.packPool.Get().(*Package)
	newpkg.cached = false
	half := pkg.Len() / 2
	if err := newpkg.AppendFrom(pkg, half, pkg.Len()-half, true); err != nil {
		return 0, err
	}
	if err := pkg.Delete(half, pkg.Len()-half); err != nil {
		return 0, err
	}

	// store both packs to update stats, this also stores the initial pack
	// on first split which may have not been stored yet
	_, err := idx.storePack(tx, pkg)
	if err != nil {
		return 0, err
	}

	// save the new pack
	newpkg.key = idx.partkey(idx.packs.Len())
	n, err := idx.storePack(tx, newpkg)
	if err != nil {
		return 0, err
	}
	idx.recyclePackage(newpkg)
	return n, nil
}

func (idx Index) cachekey(key []byte) string {
	return string(idx.key) + "/" + hex.EncodeToString(key)
}

func (idx Index) partkey(id int) []byte {
	var buf [4]byte
	bigEndian.PutUint32(buf[:], uint32(id))
	return buf[:]
}

func (idx *Index) loadPack(tx *Tx, key []byte, touch bool) (*Package, error) {
	// try cache first
	cachekey := idx.cachekey(key)
	cachefn := idx.cache.Peek
	if touch {
		cachefn = idx.cache.Get
	}
	if cached, ok := cachefn(cachekey); ok {
		atomic.AddInt64(&idx.table.stats.IndexCacheHits, 1)
		return cached.(*Package), nil
	}
	atomic.AddInt64(&idx.table.stats.IndexCacheMisses, 1)

	// if not found, load from storage
	pkg, err := tx.loadPack(idx.key, key, idx.packPool.Get().(*Package))
	if err != nil {
		return nil, err
	}
	atomic.AddInt64(&idx.table.stats.IndexPacksLoaded, 1)
	atomic.AddInt64(&idx.table.stats.IndexBytesRead, int64(pkg.packedsize))

	// add dynamic data
	pkg.key = key
	pkg.tinfo = idx.journal.tinfo
	pkg.pkindex = 0 // make sure first column is always PK in index data types
	pkg.cached = touch

	// store in cache
	if touch {
		updated, _ := idx.cache.Add(cachekey, pkg)
		if updated {
			atomic.AddInt64(&idx.table.stats.IndexCacheUpdates, 1)
		} else {
			atomic.AddInt64(&idx.table.stats.IndexCacheInserts, 1)
		}
	}
	return pkg, nil
}

// Note: we keep empty index pack names to avoid (re)naming issues
func (idx *Index) storePack(tx *Tx, pkg *Package) (int, error) {
	key := pkg.key
	cachekey := idx.cachekey(key)
	if len(key) == 0 {
		log.Errorf("pack: %s_%s index store called with empty pack key", idx.table.name, idx.Name)
	}
	n, err := tx.storePack(idx.key, key, pkg, idx.opts.FillLevel)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&idx.table.stats.IndexPacksStored, 1)
	atomic.AddInt64(&idx.table.stats.IndexBytesWritten, int64(n))

	// keep all packs in index
	idx.packs.AddOrUpdate(pkg.Header())

	// remove empty packs from cache and return to pool
	if pkg.Len() == 0 {
		idx.cache.Remove(cachekey)
	} else if pkg.cached {
		inserted, _ := idx.cache.ContainsOrAdd(cachekey, pkg)
		if inserted {
			atomic.AddInt64(&idx.table.stats.IndexCacheInserts, 1)
		} else {
			atomic.AddInt64(&idx.table.stats.IndexCacheUpdates, 1)
		}
	}
	return n, nil
}

func (idx *Index) makePackage() interface{} {
	atomic.AddInt64(&idx.table.stats.IndexPacksAlloc, 1)
	return idx.journal.Clone(false, 1<<uint(idx.opts.PackSizeLog2))
}

func (idx *Index) onEvictedPackage(key, val interface{}) {
	pkg := val.(*Package)
	pkg.cached = false
	atomic.AddInt64(&idx.table.stats.IndexCacheEvictions, 1)
	idx.recyclePackage(pkg)
}

func (idx *Index) recyclePackage(pkg *Package) {
	if pkg == nil || pkg.cached {
		return
	}
	// don't recycle oversized packs to free memory
	if c := pkg.Cap(); c < 0 || c > 1<<uint(idx.opts.PackSizeLog2) {
		pkg.Release()
		return
	}
	pkg.Clear()
	atomic.AddInt64(&idx.table.stats.IndexPacksRecycled, 1)
	idx.packPool.Put(pkg)
}

func (idx *Index) Size() IndexSizeStats {
	var sz IndexSizeStats
	for _, v := range idx.cache.Keys() {
		val, ok := idx.cache.Peek(v)
		if !ok {
			continue
		}
		pkg := val.(*Package)
		sz.CacheSize += pkg.Size()
	}
	sz.JournalSize = idx.journal.Size()
	sz.TombstoneSize = idx.tombstone.Size()
	sz.TotalSize = sz.JournalSize + sz.TombstoneSize + sz.CacheSize
	return sz
}

// Hash Index
func hashValue(typ FieldType, val interface{}) uint64 {
	h := hash.NewInlineFNV64a()
	var buf [8]byte
	switch typ {
	case FieldTypeBytes:
		h.Write(val.([]byte))
	case FieldTypeBoolean:
		if b, _ := val.(bool); b {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	case FieldTypeInt64:
		bigEndian.PutUint64(buf[:], uint64(val.(int64)))
		h.Write(buf[:])
	case FieldTypeUint64:
		bigEndian.PutUint64(buf[:], val.(uint64))
		h.Write(buf[:])
	case FieldTypeFloat64:
		bigEndian.PutUint64(buf[:], math.Float64bits(val.(float64)))
		h.Write(buf[:])
	case FieldTypeString:
		h.Write([]byte(val.(string)))
	case FieldTypeDatetime:
		bigEndian.PutUint64(buf[:], uint64(val.(time.Time).UnixNano()))
		h.Write(buf[:])
	default:
		panic(fmt.Errorf("hash index: unsupported value type %s", typ))
	}
	return h.Sum64()
}

func hashValueAt(typ FieldType, pkg *Package, index, pos int) uint64 {
	h := hash.NewInlineFNV64a()
	var buf [8]byte
	switch typ {
	case FieldTypeBytes:
		val, _ := pkg.BytesAt(index, pos)
		h.Write(val)
	case FieldTypeBoolean:
		if b, _ := pkg.BoolAt(index, pos); b {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	case FieldTypeInt64:
		val, _ := pkg.Int64At(index, pos)
		bigEndian.PutUint64(buf[:], uint64(val))
		h.Write(buf[:])
	case FieldTypeUint64:
		val, _ := pkg.Uint64At(index, pos)
		bigEndian.PutUint64(buf[:], val)
		h.Write(buf[:])
	case FieldTypeFloat64:
		val, _ := pkg.Float64At(index, pos)
		bigEndian.PutUint64(buf[:], math.Float64bits(val))
		h.Write(buf[:])
	case FieldTypeString:
		val, _ := pkg.StringAt(index, pos)
		h.Write([]byte(val))
	case FieldTypeDatetime:
		val, _ := pkg.TimeAt(index, pos)
		bigEndian.PutUint64(buf[:], uint64(val.UnixNano()))
		h.Write(buf[:])
	default:
		panic(fmt.Errorf("hash index: unsupported value type %s", typ))
	}
	return h.Sum64()
}

// Integer index
func intValue(typ FieldType, val interface{}) uint64 {
	switch typ {
	case FieldTypeInt64:
		return uint64(val.(int64))
	case FieldTypeUint64:
		return val.(uint64)
	case FieldTypeDatetime:
		return uint64(val.(time.Time).UnixNano())
	default:
		// FieldTypeBytes, FieldTypeBoolean, FieldTypeString, FieldTypeFloat64
		return 0
	}
}

func intValueAt(typ FieldType, pkg *Package, index, pos int) uint64 {
	switch typ {
	case FieldTypeInt64, FieldTypeDatetime:
		val, _ := pkg.Int64At(index, pos)
		return uint64(val)
	case FieldTypeUint64:
		val, _ := pkg.Uint64At(index, pos)
		return val
	default:
		// FieldTypeBytes, FieldTypeBoolean, FieldTypeString, FieldTypeFloat64
		return 0
	}
}
