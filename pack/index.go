// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

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

	"blockwatch.cc/knoxdb/cache/rclru"
	"blockwatch.cc/knoxdb/encoding/num"
	"blockwatch.cc/knoxdb/hash"
	"blockwatch.cc/knoxdb/store"
	"blockwatch.cc/knoxdb/util"
)

// Collision handling
// - stores colliding hashes as duplicates
// - handles special case where colliding values cross pack boundaries
// - tombstone stores hash + primary key and we check both values on removal

type IndexType int

type IndexValueFunc func(typ FieldType, val interface{}) uint64
type IndexValueAtFunc func(typ FieldType, pkg *Package, index, pos int) uint64
type IndexZeroAtFunc func(pkg *Package, index, pos int) bool

const (
	IndexTypeNone    IndexType = iota
	IndexTypeHash              // any col (any type) -> uint64 FNV hash
	IndexTypeInteger           // any col ((u)int64) -> pk (uint64)
)

func (t IndexType) String() string {
	switch t {
	case IndexTypeNone:
		return ""
	case IndexTypeHash:
		return "hash"
	case IndexTypeInteger:
		return "int"
	default:
		return "invalid"
	}
}

func (t IndexType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t *IndexType) UnmarshalText(d []byte) error {
	switch string(d) {
	case "":
		*t = IndexTypeNone
	case "hash":
		*t = IndexTypeHash
	case "int":
		*t = IndexTypeInteger
	default:
		return fmt.Errorf("Invalid index type %q", string(d))
	}
	return nil
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

func (t IndexType) ZeroAtFunc() IndexZeroAtFunc {
	switch t {
	case IndexTypeHash:
		return hashZeroAt
	case IndexTypeInteger:
		return intZeroAt
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
	Key uint64 `knox:"K,pk"` // hash key, i.e. FNV(value)
	Id  uint64 `knox:"I"`    // OID of table entry
}

type Index struct {
	Name  string    `json:"name"`  // stored in table metadata
	Type  IndexType `json:"typ"`   // stored in table metadata
	Field *Field    `json:"field"` // stored in table metadata
	opts  Options   // stored in table metadata

	// function pointers
	indexValue   IndexValueFunc
	indexValueAt IndexValueAtFunc
	indexZeroAt  IndexZeroAtFunc

	table     *Table
	cache     rclru.Cache[uint32, *Package]
	journal   *Package   // append log
	tombstone *Package   // delete log
	packidx   *PackIndex // in-memory list of pack and block headers
	key       []byte     // bucket name
	metakey   []byte     // metadata bucket name
	packPool  *sync.Pool // buffer pool for new packages
	stats     TableStats // usage statistics
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

func (t *Table) CreateIndex(name string, field *Field, typ IndexType, opts Options) (*Index, error) {
	opts = DefaultOptions.Merge(opts)
	if err := opts.Check(); err != nil {
		return nil, err
	}
	field.Flags |= FlagIndexed
	maxPackSize := opts.PackSize()
	maxJournalSize := opts.JournalSize()
	idx := &Index{
		Name:         name,
		Type:         typ,
		Field:        field,
		opts:         opts,
		table:        t,
		packidx:      NewPackIndex(nil, 0, maxPackSize),
		key:          []byte(t.name + "_" + name + "_index"),
		metakey:      []byte(t.name + "_" + name + "_index_meta"),
		indexValue:   typ.ValueFunc(),
		indexValueAt: typ.ValueAtFunc(),
		indexZeroAt:  typ.ZeroAtFunc(),
	}
	idx.stats.IndexName = t.name + "_" + name + "_index"
	idx.stats.JournalTuplesThreshold = int64(maxJournalSize)
	idx.stats.TombstoneTuplesThreshold = int64(maxJournalSize)
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
		_, err = meta.CreateBucketIfNotExists(infoKey)
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
		idx.journal = NewPackage(maxJournalSize, nil)
		idx.journal.key = journalKey
		if err := idx.journal.InitType(IndexEntry{}); err != nil {
			return err
		}
		_, err = storePackTx(dbTx, idx.metakey, idx.journal.Key(), idx.journal, idx.opts.FillLevel)
		if err != nil {
			return err
		}
		// create empty tombstone
		idx.tombstone = NewPackage(maxJournalSize, nil)
		idx.tombstone.key = tombstoneKey
		if err := idx.tombstone.InitType(IndexEntry{}); err != nil {
			return err
		}
		_, err = storePackTx(dbTx, idx.metakey, idx.tombstone.Key(), idx.tombstone, idx.opts.FillLevel)
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
		idx.cache, err = rclru.New2Q[uint32, *Package](idx.opts.CacheSizeMBytes())
		if err != nil {
			return nil, err
		}
		idx.stats.CacheCapacity = int64(idx.opts.CacheSizeMBytes())
	} else {
		idx.cache = rclru.NewNoCache[uint32, *Package]()
	}

	// Note: reindex may take a long time and requires a context which we don't have
	//        here, so it's the job of the caller to ensure the index is built

	log.Debugf("Created %s index %s_%s", typ.String(), t.name, name)
	return idx, nil
}

func (t *Table) CreateIndexIfNotExists(name string, field *Field, typ IndexType, opts Options) (*Index, error) {
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
	idx.table = t
	idx.key = []byte(t.name + "_" + idx.Name + "_index")
	idx.metakey = []byte(t.name + "_" + idx.Name + "_index_meta")
	idx.packPool = &sync.Pool{
		New: idx.makePackage,
	}
	idx.stats.IndexName = t.name + "_" + idx.Name + "_index"
	idx.indexValue = idx.Type.ValueFunc()
	idx.indexValueAt = idx.Type.ValueAtFunc()
	idx.indexZeroAt = idx.Type.ZeroAtFunc()

	// check index exists, load journal and tombstone
	err := t.db.db.View(func(dbTx store.Tx) error {
		b := dbTx.Bucket(idx.metakey)
		if b == nil {
			return ErrNoIndex
		}
		buf := b.Get(optsKey)
		if buf == nil {
			return fmt.Errorf("pack: %s missing configuration options", idx.name())
		}
		err := json.Unmarshal(buf, &idx.opts)
		if err != nil {
			return err
		}
		if len(opts) > 0 {
			if opts[0].PackSizeLog2 > 0 && idx.opts.PackSizeLog2 != opts[0].PackSizeLog2 {
				return fmt.Errorf("pack: %s pack size change not allowed", idx.name())
			}
			idx.opts = idx.opts.Merge(opts[0])
		}
		maxPackSize := idx.opts.PackSize()
		maxJournalSize := idx.opts.JournalSize()
		idx.stats.JournalTuplesThreshold = int64(maxJournalSize)
		idx.stats.TombstoneTuplesThreshold = int64(maxJournalSize)
		idx.packidx = NewPackIndex(nil, 0, maxPackSize)
		idx.journal, err = loadPackTx(dbTx, idx.metakey, encodePackKey(journalKey), nil, maxJournalSize)
		if err != nil {
			return fmt.Errorf("pack: %s journal open failed: %v", idx.name(), err)
		}
		if err := idx.journal.InitType(IndexEntry{}); err != nil {
			return err
		}
		log.Debugf("pack: %s loaded journal with %d records", idx.name(), idx.journal.Len())
		idx.tombstone, err = loadPackTx(dbTx, idx.metakey, encodePackKey(tombstoneKey), nil, maxJournalSize)
		if err != nil {
			return fmt.Errorf("pack: %s index cannot open tombstone: %v", idx.name(), err)
		}
		if err := idx.tombstone.InitType(IndexEntry{}); err != nil {
			return err
		}
		idx.tombstone.key = tombstoneKey
		log.Debugf("pack: %s loaded tombstone with %d records", idx.name(), idx.tombstone.Len())
		return idx.loadPackInfo(dbTx)
	})
	if err != nil {
		return err
	}
	if idx.opts.CacheSize > 0 {
		idx.cache, err = rclru.New2Q[uint32, *Package](idx.opts.CacheSizeMBytes())
		if err != nil {
			return err
		}
		idx.stats.CacheCapacity = int64(idx.opts.CacheSizeMBytes())
	} else {
		idx.cache = rclru.NewNoCache[uint32, *Package]()
	}

	return nil
}

func (idx *Index) Options() Options {
	return idx.opts
}

func (idx *Index) PurgeCache() {
	idx.cache.Purge()
}

func (idx *Index) name() string {
	return string(idx.key)
}

func (idx *Index) loadPackInfo(dbTx store.Tx) error {
	b := dbTx.Bucket(idx.metakey)
	if b == nil {
		return ErrNoTable
	}
	packs := make(PackInfoList, 0)
	maxPackSize := idx.opts.PackSize()
	bi := b.Bucket(infoKey)
	if bi != nil {
		c := bi.Cursor()
		var err error
		for ok := c.First(); ok; ok = c.Next() {
			info := PackInfo{}
			err = info.UnmarshalBinary(c.Value())
			if err != nil {
				break
			}
			packs = append(packs, info)
			atomic.AddInt64(&idx.stats.MetaBytesRead, int64(len(c.Value())))
		}
		if err != nil {
			packs = packs[:0]
			log.Errorf("pack: %s info decode failed for pack %x: %v", idx.name(), c.Key(), err)
		} else {
			idx.packidx = NewPackIndex(packs, 0, maxPackSize)
			atomic.StoreInt64(&idx.stats.PacksCount, int64(idx.packidx.Len()))
			atomic.StoreInt64(&idx.stats.MetaSize, int64(idx.packidx.HeapSize()))
			atomic.StoreInt64(&idx.stats.TotalSize, int64(idx.packidx.TableSize()))
			log.Debugf("pack: %s loaded index data for %d packs", idx.name(), idx.packidx.Len())
			return nil
		}
	}
	// on error, scan packs
	log.Warnf("pack: Corrupt or missing pack info for %s! Scanning table. This may take a long time...", idx.name())
	c := dbTx.Bucket(idx.key).Cursor()
	pkg := NewPackage(maxPackSize, nil)
	if err := pkg.InitFieldsFrom(idx.journal); err != nil {
		return err
	}
	for ok := c.First(); ok; ok = c.Next() {
		err := pkg.UnmarshalBinary(c.Value())
		if err != nil {
			return fmt.Errorf("pack: cannot read index pack %s: %v", idx.cachekey(c.Key()), err)
		}
		pkg.SetKey(c.Key())
		// ignore journal and tombstone
		if pkg.IsJournal() || pkg.IsTomb() {
			pkg.Clear()
			continue
		}
		info := pkg.Info()
		_ = info.UpdateStats(pkg)
		packs = append(packs, info)
		atomic.AddInt64(&idx.stats.MetaBytesRead, int64(len(c.Value())))
		pkg.Clear()
	}
	idx.packidx = NewPackIndex(packs, 0, maxPackSize)
	atomic.StoreInt64(&idx.stats.PacksCount, int64(idx.packidx.Len()))
	atomic.StoreInt64(&idx.stats.MetaSize, int64(idx.packidx.HeapSize()))
	atomic.StoreInt64(&idx.stats.TotalSize, int64(idx.packidx.TableSize()))
	log.Debugf("pack: %s scanned %d package headers", idx.name(), idx.packidx.Len())
	return nil
}

func (idx *Index) storePackInfo(dbTx store.Tx) error {
	meta := dbTx.Bucket(idx.metakey)
	if meta == nil {
		return ErrNoTable
	}

	// pack headers are stored in a nested bucket
	hb := meta.Bucket(infoKey)

	// create statistics bucket when missing
	if hb == nil {
		var err error
		hb, err = meta.CreateBucketIfNotExists(infoKey)
		if err != nil {
			return err
		}
	}

	// remove old headers
	for _, k := range idx.packidx.removed {
		hb.Delete(encodePackKey(k))
	}
	idx.packidx.removed = idx.packidx.removed[:0]

	for i := range idx.packidx.packs {
		if !idx.packidx.packs[i].dirty {
			continue
		}
		buf, err := idx.packidx.packs[i].MarshalBinary()
		if err != nil {
			return err
		}
		if err := hb.Put(idx.packidx.packs[i].KeyBytes(), buf); err != nil {
			return err
		}
		idx.packidx.packs[i].dirty = false
		atomic.AddInt64(&idx.stats.MetaBytesWritten, int64(len(buf)))
	}
	return nil
}

func (idx *Index) AddTx(tx *Tx, pkg *Package, srcPos, srcLen int) error {
	// Maps a (hash or int) value of the indexed field's content to primary key.
	//
	// Appends (key, pk) tuples to journal until full, then flushes the journal.
	// Index packs are internally sorted, but a global order does not exist.
	// Once stored, packs are not touched unless entries are removed.
	// No duplicate check is performed. Duplicate key/value pairs are stored
	// as is and lookup will find and return all duplicates.
	pk := pkg.PkColumn()
	atomic.AddInt64(&idx.stats.InsertCalls, 1)

	var count int64
	for i := srcPos; i < srcPos+srcLen; i++ {
		// don't index zero values
		if idx.indexZeroAt(pkg, idx.Field.Index, i) {
			continue
		}

		// build index entry from pack content
		entry := IndexEntry{
			Key: idx.indexValueAt(idx.Field.Type, pkg, idx.Field.Index, i),
			Id:  pk[i],
		}

		// append to journal, will sort on flush
		if err := idx.journal.Push(entry); err != nil {
			return err
		}
		count++
	}

	atomic.AddInt64(&idx.stats.InsertedTuples, count)
	return nil
}

func (idx *Index) RemoveTx(tx *Tx, pkg *Package, srcPos, srcLen int) error {
	// Appends (hash or int) keys to tombstone.
	pk := pkg.PkColumn()
	atomic.AddInt64(&idx.stats.DeleteCalls, 1)

	var count int64
	for i := srcPos; i < srcPos+srcLen; i++ {
		// don't index zero values
		if idx.indexZeroAt(pkg, idx.Field.Index, i) {
			continue
		}

		// build index entry from pack content
		entry := IndexEntry{
			Key: idx.indexValueAt(idx.Field.Type, pkg, idx.Field.Index, i),
			Id:  pk[i],
		}

		// append hash value to tombstone
		if err := idx.tombstone.Push(entry); err != nil {
			return err
		}
		count++
	}
	atomic.AddInt64(&idx.stats.DeletedTuples, count)

	return nil
}

// This index only supports the following condition types on lookup.
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
		return nil, fmt.Errorf("pack: %s: incompatible condition %s", idx.name(), cond)
	}

	// alloc temp slice from pool
	keys := idx.table.u64Pool.Get().([]uint64)

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
			return nil, fmt.Errorf("pack: %s lookup requires slice type, got %T", idx.name(), cond.Value)
		}
		for i, l := 0, slice.Len(); i < l; i++ {
			v := slice.Index(i).Interface()
			if !idx.Field.Type.isZero(v) {
				keys = append(keys, idx.indexValue(idx.Field.Type, v))
			}
		}
		keys = num.Uint64.Sort(keys)
	}

	res, err := idx.lookupKeys(ctx, tx, keys, cond.Mode == FilterModeNotIn)
	if err != nil {
		return nil, err
	}
	if cond.Mode != FilterModeNotIn {
		idx.table.u64Pool.Put(keys[:0])
	}
	return res, nil
}

// Note: index journals are always empty on lookup because tables flush after add/remove.
func (idx *Index) lookupKeys(ctx context.Context, tx *Tx, in []uint64, neg bool) ([]uint64, error) {
	atomic.AddInt64(&idx.stats.QueryCalls, 1)
	if len(in) == 0 {
		return []uint64{}, nil
	}

	// copy input slice to notfound for later negation
	var notfound []uint64
	if neg {
		notfound = make([]uint64, len(in))
		copy(notfound, in)
	}

	// alloc result slice from pool, should be returned by caller
	out := idx.table.u64Pool.Get().([]uint64)
	var nPacks int

	// log.Debugf("%s: searching for %d keys", idx.name(), len(in))
	// log.Debugf("Searching for keys %#v", in)

	// Optimize for rollback and lookup of most recently added index values.
	// Although this only works for integer indexes (hash index are randomized)
	// this helps improve search performance.
	//
	// Both in-slice and index packs are sorted by indexed key which greatly helps
	// search performance because we can use binary search.

	for nextpack := idx.packidx.Len() - 1; nextpack >= 0; nextpack-- {
		// extract min/max values from pack header (this is defined by IndexEntry,
		// so we're safe to assume the following call will not fail); then
		// skip packs that don't contain keys in range
		min, max := idx.packidx.MinMax(nextpack)
		if !num.Uint64.ContainsRange(in, min, max) {
			// log.Debugf("%s: not in pack %03d [%016x:%016x]", idx.name(), nextpack, min, max)
			continue
		}
		// log.Debugf("%s: maybe in pack %03d [%016x:%016x]", idx.name(), nextpack, min, max)

		// stop when context is canceled
		if err := ctx.Err(); err != nil {
			out = out[:0]
			idx.table.u64Pool.Put(out)
			return nil, err
		}

		// load and cache pack
		ipkg, err := idx.loadSharedPack(tx, idx.packidx.packs[nextpack].Key, true)
		if err != nil {
			return nil, err
		}
		nPacks++

		// we use index and value slices
		keys := ipkg.PkColumn()
		col, _ := ipkg.Column(1)
		values, _ := col.([]uint64)

		// start at the first `in` value contained by this index pack
		first := sort.Search(len(in), func(x int) bool { return in[x] >= min })

		// run through pack and in-slice until no more values match
		for k, i, kl, il := 0, first, len(keys), len(in); k < kl && i < il; {

			// find the next matching key or any value > next lookup
			k += sort.Search(kl-k, func(x int) bool { return keys[x+k] >= in[i] })

			// stop at pack end
			if k == kl {
				// log.Debugf("%s: reached pack end", idx.name())
				break
			}

			// if no match was found, advance in-slice
			for i < il && keys[k] > in[i] {
				// log.Debugf("%s: key=0x%016x not found, skipping ahead", idx.name(), in[i])
				i++
			}

			// stop at in-slice end
			if i == il {
				// log.Debugf("%s: no more search keys", idx.name())
				break
			}

			// handle multiple matches
			if keys[k] == in[i] {
				// append to result
				// log.Debugf("%s: match found key=0x%016x val=%d at pos %d/%d in pack %03d [%016x:%016x]",
				// 	idx.name(), keys[k], values[k], k, len(keys), nextpack, min, max)
				out = append(out, values[k])

				// remove found key from control slice
				if notfound != nil {
					notfound = num.Uint64.Remove(notfound, in[i])
				}

				// Peek the next index entries to handle key collisions and
				// multi-matches for integer indexes. K can safely be advanced
				// because collisions/multi-matches for in[i] are directly after
				// the first match.
				for ; k+1 < kl && keys[k+1] == in[i]; k++ {
					// log.Debugf("%s: found more key=0x%016x val=%d in pack %03d [%016x:%016x]",
					// 	idx.name(), keys[k+1], values[k+1], nextpack, min, max)
					out = append(out, values[k+1])
				}

				// next lookup key
				i++
			}
		}
		idx.releaseSharedPack(ipkg)
	}

	// `in` contains only missing keys now
	if neg {
		idx.table.u64Pool.Put(out[:0])
		out = notfound
	}

	// sort result before return
	if len(out) > 1 && !neg {
		out = num.Uint64.Sort(out)
	}
	atomic.AddInt64(&idx.stats.QueriedTuples, int64(len(out)))
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
	for i := idx.packidx.Len() - 1; i >= 0; i-- {
		key := idx.packidx.packs[i].KeyBytes()
		cachekey := idx.packidx.packs[i].Key
		if err := tx.deletePack(idx.key, key); err != nil {
			return err
		}
		idx.cache.Remove(cachekey)
	}
	idx.packidx.Clear()

	// clear and save journal and tombstone
	idx.journal.Clear()
	if _, err := tx.storePack(idx.metakey, idx.journal.Key(), idx.journal, idx.opts.FillLevel); err != nil {
		return err
	}
	idx.tombstone.Clear()
	if _, err := tx.storePack(idx.metakey, idx.tombstone.Key(), idx.tombstone, idx.opts.FillLevel); err != nil {
		return err
	}
	if err := idx.storePackInfo(tx.tx); err != nil {
		return err
	}

	// flush at most every 128 packs
	if flushEvery < 128 {
		flushEvery = 128
	}

	var (
		pkg *Package
		err error
	)
	defer func() {
		idx.releaseSharedPack(pkg)
	}()

	// scan table in pk order block by block and create new index
	for i, ph := range idx.table.packidx.packs {
		// stop when context is canceled
		if err := ctx.Err(); err != nil {
			return err
		}

		// load pack (we need pk field and all index fields)
		fields := idx.table.Fields().Select(idx.Field.Name).Add(idx.table.Fields().Pk())
		pkg, err = idx.table.loadSharedPack(tx, ph.Key, false, fields)
		if err != nil {
			return err
		}

		// index all packed rows at once
		if err := idx.AddTx(tx, pkg, 0, pkg.Len()); err != nil {
			return err
		}

		// return pack to table's (!) pool
		idx.releaseSharedPack(pkg)
		pkg = nil

		// flush index after every 128 packs
		if i%flushEvery == 0 {
			// signal progress
			if ch != nil {
				select {
				case ch <- float64(i*100) / float64(idx.table.packidx.Len()):
				default:
				}
			}
			if err := idx.FlushTx(ctx, tx); err != nil {
				return err
			}
		}
	}

	// final flush (this clears the index journal)
	if ch != nil {
		select {
		case ch <- float64(99):
		default:
		}
	}
	if err := idx.FlushTx(ctx, tx); err != nil {
		return err
	}
	if ch != nil {
		select {
		case ch <- float64(100):
		default:
		}
	}

	// store journal with remaining data (should not be necessary as long as we don't
	// keep index data in the journal)
	if idx.journal.IsDirty() {
		_, err := tx.storePack(idx.metakey, idx.journal.Key(), idx.journal, idx.opts.FillLevel)
		if err != nil {
			return err
		}
	}
	return nil
}

// saves journal & tombstone on close
func (idx *Index) CloseTx(tx *Tx) error {
	log.Debugf("pack: %s closing with %d journal and %d tombstone records", idx.name(), idx.journal.Len(), idx.tombstone.Len())
	_, err := tx.storePack(idx.metakey, idx.journal.Key(), idx.journal, idx.opts.FillLevel)
	if err != nil {
		return err
	}
	_, err = tx.storePack(idx.metakey, idx.tombstone.Key(), idx.tombstone, idx.opts.FillLevel)
	if err != nil {
		return err
	}
	if err := idx.storePackInfo(tx.tx); err != nil {
		return err
	}
	return nil
}

// merge journal entries into data partitions, repack, store, and update indexes
// TODO: replace append+quicksort with reverse mergesort
func (idx *Index) FlushTx(ctx context.Context, tx *Tx) error {
	// an empty flush writes dirty pack headers
	atomic.AddInt64(&idx.stats.FlushCalls, 1)
	atomic.AddInt64(&idx.stats.FlushedTuples, int64(idx.journal.Len()+idx.tombstone.Len()))
	start := time.Now().UTC()
	lvl := log.Level()
	idx.stats.LastFlushTime = start

	// requires sorted journal
	if err := idx.journal.PkSort(); err != nil {
		return err
	}

	// requires sorted tombstone
	if err := idx.tombstone.PkSort(); err != nil {
		return err
	}

	// work on hash and value slices
	dead := idx.tombstone.PkColumn()
	col, _ := idx.tombstone.Column(1)
	deadval, _ := col.([]uint64)

	pk := idx.journal.PkColumn()
	col, _ = idx.journal.Column(1)
	pkval, _ := col.([]uint64)

	var nAdd, nDel, nParts, nBytes int

	// log.Debugf("pack: %s flushing %d journal and %d tombstone records",
	// 	idx.name(), len(pk), len(dead))

	// Mark deleted journal records first (set value to zero; zero keys have
	// meaning for hash indexes)
	if len(pk) > 0 && len(dead) > 0 {
		// start at the first tombstone record that may be in journal
		var d1, j1 int
		d1 = sort.Search(len(dead), func(x int) bool { return dead[x] >= pk[0] })

		// start at the first journal record that may be in tombstone
		if d1 < len(dead) {
			j1 = sort.Search(len(pk), func(x int) bool { return pk[x] >= dead[d1] })
		}

		for j, d, jl, dl := j1, d1, len(pk), len(dead); j < jl && d < dl; {
			// find the next matching journal pos where key >= tomb record
			j += sort.Search(jl-j, func(x int) bool { return pk[x+j] >= dead[d] })

			// stop at pack end
			if j == jl {
				break
			}

			// if no match was found, advance tomb pointer
			for d < dl && pk[j] > dead[d] {
				d++
			}

			// stop at tomb end
			if d == dl {
				break
			}

			// ensure we only delete real matches by checking key AND value
			for dead[d] == pk[j] && j < jl {
				// we expect at most one match in value
				if deadval[d] == pkval[j] {
					// mark journal records as processed
					pkval[j] = 0

					// mark tomb records as processed
					deadval[d] = 0

					// advance pointers
					nDel++
					j++
					break
				}
				j++
			}
			d++
		}
		// log.Debugf("pack: %s flush marked %d dead journal records", idx.name(), nDel)
	}

	// walk journal/tombstone and group updates by pack
	var (
		pkg                         *Package // current target pack
		packsz                      int      // target pack size
		jpos, tpos, jlen, tlen      int      // journal/tomb slice offsets & lengths
		lastpack, nextpack          int      // pack list positions (not keys)
		nextid                      uint64   // next index key to process (tomb or journal)
		packmax, nextmin, globalmax uint64   // data placement hints
		needsort                    bool     // true if current pack needs sort before store
		loop, maxloop               int      // circuit breaker
	)

	// init
	packsz = idx.opts.PackSize()
	jlen, tlen = len(pk), len(dead)
	_, globalmax = idx.packidx.GlobalMinMax()
	maxloop = 2*idx.packidx.Len() + 2*jlen/packsz + 2 // 2x to consider splits

	// create an initial pack on first insert
	if idx.packidx.Len() == 0 {
		pkg = idx.newPackage().WithKey(idx.packidx.NextKey())
		pkg.IncRef()
	}

	// This algorithm works like a merge-sort over a sequence of sorted packs.
	for {
		// stop when all journal and tombstone entries have been processed
		if jpos >= jlen && tpos >= tlen {
			break
		}

		// skip deleted journal entries
		for ; jpos < jlen && pkval[jpos] == 0; jpos++ {
		}

		// skip processed tombstone entries
		for ; tpos < tlen && deadval[tpos] == 0; tpos++ {
		}

		// skip trailing tombstone entries (for unwritten journal entries)
		// TODO: most likely not relevant for index packs
		for ; tpos < tlen && dead[tpos] > globalmax; tpos++ {
		}

		// init on each iteration, either from journal or tombstone
		switch true {
		case jpos < jlen && tpos < tlen:
			nextid = util.MinU64(pk[jpos], dead[tpos])
		case jpos < jlen && tpos >= tlen:
			nextid = pk[jpos]
		case jpos >= jlen && tpos < tlen:
			nextid = dead[tpos]
		default:
			// stop in case remaining journal/tombstone entries were skipped
			break
		}

		// find best pack for inserting/deleting next record
		nextpack, _, packmax, nextmin, _ = idx.packidx.Best(nextid)
		// log.Debugf("Next pack %d max=%d nextmin=%d", nextpack, packmax, nextmin)

		// store last pack when nextpack changes
		if lastpack != nextpack && pkg != nil {
			if pkg.IsDirty() {
				// keep pack sorted
				if needsort {
					pkg.PkSort()
				}
				// log.Debugf("%s: storing pack %d with %d records", idx.name(), pkg.key, pkg.Len())
				n, err := idx.storePack(tx, pkg)
				if err != nil {
					return err
				}
				nParts++
				nBytes += n
				// commit storage tx after each N written packs
				if tx.Pending() >= txMaxSize {
					if err := idx.storePackInfo(tx.tx); err != nil {
						return err
					}
					if err := tx.CommitAndContinue(); err != nil {
						return err
					}
				}
				// update next values after pack index has changed
				nextpack, _, packmax, nextmin, _ = idx.packidx.Best(nextid)
				// log.Debugf("%s: post-store next pack %d max=%d nextmin=%d",
				// 	idx.name(), nextpack, packmax, nextmin)
			}
			// prepare for next pack
			pkg.DecRef()
			pkg = nil
			needsort = false
		}

		// load the next pack
		if pkg == nil {
			var err error
			pkg, err = idx.loadWritablePack(tx, idx.packidx.packs[nextpack].Key)
			if err != nil {
				return err
			}
			lastpack = nextpack
			// log.Debugf("%s: loaded pack %d with %d records", idx.name(), pkg.key, pkg.Len())
		}

		// circuit breaker
		loop++
		if loop > 2*maxloop {
			log.Errorf("pack: %s stopping infinite flush loop %d: tomb-flush-pos=%d/%d journal-flush-pos=%d/%d pack=%d/%d nextid=%d",
				idx.name(), loop, tpos, tlen, jpos, jlen, lastpack, idx.packidx.Len(), nextid,
			)
			return fmt.Errorf("pack: %s infinite flush loop detected. Database is likely corrupted.", idx.name())
		} else if loop > maxloop {
			log.SetLevel(levelDebug)
			log.Debugf("pack: %s circuit breaker activated at loop %d tomb-flush-pos=%d/%d journal-flush-pos=%d/%d pack=%d/%d nextid=%d",
				idx.name(), loop, tpos, tlen, jpos, jlen, lastpack, idx.packidx.Len(), nextid,
			)
		}

		// process tombstone records for this pack (skip for empty packs)
		if tpos < tlen && packmax > 0 && dead[tpos] <= packmax {
			// load current state of pack slices (will change after delete)
			keycol := pkg.PkColumn()
			col, _ := pkg.Column(1)
			valcol, _ := col.([]uint64)

			for ppos := 0; tpos < tlen; tpos++ {
				// skip already processed tombstone records
				if deadval[tpos] == 0 {
					continue
				}

				// next pk to delete
				key := dead[tpos]

				// stop on pack boundary
				if key > packmax {
					break
				}

				// find the next matching key to clear
				ppos += sort.Search(len(keycol)-ppos, func(i int) bool { return keycol[i+ppos] >= key })
				if ppos == len(keycol) || keycol[ppos] != key {
					// clear from tombstone if not found
					deadval[tpos] = 0
					continue
				}

				// count consecutive matches
				n := 1
				for tpos+n < tlen && // until tomb end
					ppos+n < len(keycol) && // until pack end
					keycol[ppos+n] == dead[tpos+n] && // key must match
					valcol[ppos+n] == deadval[tpos+n] { // value must match
					n++
				}

				// remove n records from pack, changes keycol & valcol (!)
				pkg.Delete(ppos, n)

				// mark as processed
				for i := 0; i < n; i++ {
					deadval[tpos+i] = 0
				}
				nDel += n

				// reload current state of pack slices
				keycol = pkg.PkColumn()
				col, _ = pkg.Column(1)
				valcol, _ = col.([]uint64)

				// update pack max
				packmax = 0
				if l := len(keycol); l > 0 {
					packmax = keycol[l-1]
				}

				// advance tomb pointer by one less (for-loop adds +1)
				tpos += n - 1
			}
		}

		// process journal records for this pack (insert only, no update)
		for jpos < jlen {
			// skip deleted journal records
			if pkval[jpos] == 0 {
				jpos++
				continue
			}

			// stop on pack boundary
			if nextmin > 0 && pk[jpos] >= nextmin {
				break
			}

			// count consecutive matches, stop at removed records
			// and when crossing the next pack's boundary
			n, l := 1, pkg.Len()
			for jpos+n < jlen && // until journal end
				l+n < packsz && // until pack is full
				(nextmin == 0 || pk[jpos+n] < nextmin) && // until next pack's min boundary (!invariant)
				pkval[jpos+n] > 0 { // only non-deleted records
				n++
			}

			// append journal records
			if err := pkg.AppendFrom(idx.journal, jpos, n); err != nil {
				return err
			}

			// update state
			needsort = needsort || pk[jpos] < packmax
			packmax = util.MaxU64(packmax, pk[jpos])
			globalmax = util.MaxU64(globalmax, packmax)
			nAdd += n
			jpos += n

			// split when full
			if pkg.Len() == packsz {
				if needsort {
					pkg.PkSort()
					needsort = false
				}
				// log.Debugf("%s: split pack %d with %d records", idx.name(), pkg.key, pkg.Len())
				n, err := idx.splitPack(tx, pkg)
				if err != nil {
					return err
				}
				nParts++
				nBytes += n
				lastpack = -1 // force pack load in next round
				pkg.DecRef()
				pkg = nil

				// commit tx after each N written packs
				if tx.Pending() >= txMaxSize {
					if err := idx.storePackInfo(tx.tx); err != nil {
						return err
					}
					if err := tx.CommitAndContinue(); err != nil {
						return err
					}
					// TODO: for a safe return we must also
					// - mark or clear written journal records
					// - save journal
					// - commit tx
					//
					// // stop when context is canceled
					// if interruptRequested(ctx) {
					// 	return ctx.Err()
					// }
				}

				// leave journal for-loop and trigger new pack selection
				break
			}
		}
	}

	// store last processed pack
	if pkg != nil && pkg.IsDirty() {
		if needsort {
			pkg.PkSort()
			needsort = false
		}
		// log.Debugf("%s: storing final pack %d with %d records", idx.name(), pkg.key, pkg.Len())
		n, err := idx.storePack(tx, pkg)
		if err != nil {
			return err
		}
		pkg.DecRef()
		pkg = nil
		nParts++
		nBytes += n
	}

	// update counters
	atomic.StoreInt64(&idx.stats.PacksCount, int64(idx.packidx.Len()))
	atomic.StoreInt64(&idx.stats.TupleCount, int64(idx.packidx.Count()))
	atomic.StoreInt64(&idx.stats.MetaSize, int64(idx.packidx.HeapSize()))
	atomic.StoreInt64(&idx.stats.TotalSize, int64(idx.packidx.TableSize()))

	idx.stats.LastFlushDuration = time.Since(start)
	log.Debugf("pack: %s flushed %d packs add=%d/%d del=%d/%d total_size=%s in %s",
		idx.name(), nParts, nAdd, idx.journal.Len(), nDel, idx.tombstone.Len(), util.ByteSize(nBytes),
		idx.stats.LastFlushDuration)

	// ignore any remaining records
	idx.tombstone.Clear()
	idx.journal.Clear()

	// store final pack headers
	if err := idx.storePackInfo(tx.tx); err != nil {
		return err
	}

	log.SetLevel(lvl)

	// TODO: we don't store index journals
	// store tomb and journal
	// if idx.tombstone.IsDirty() {
	// 	_, err := tx.storePack(idx.metakey, idx.tombstone.Key(), idx.tombstone, idx.opts.FillLevel)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	// _, err = tx.storePack(idx.metakey, idx.journal.Key(), idx.journal, idx.opts.FillLevel)
	// if err != nil {
	// 	return err
	// }

	return tx.CommitAndContinue()
}

// Note: pack must be storted before splitting
func (idx *Index) splitPack(tx *Tx, pkg *Package) (int, error) {
	// move half of the packidx contents to a new pack (don't cache the new pack
	// to avoid possible eviction of the pack we are currently splitting!)
	newpkg := idx.newPackage().PopulateFields(nil)
	half := pkg.Len() / 2
	if err := newpkg.AppendFrom(pkg, half, pkg.Len()-half); err != nil {
		return 0, err
	}
	if err := pkg.Delete(half, pkg.Len()-half); err != nil {
		return 0, err
	}

	// store both packs to update stats, this also stores the initial pack
	// on first split which may have not been stored yet
	n, err := idx.storePack(tx, pkg)
	if err != nil {
		return 0, err
	}

	// set the new pack's key here to avoid overwrite when the very first pack
	// has never been stored
	newpkg.WithKey(idx.packidx.NextKey())

	// save the new pack
	m, err := idx.storePack(tx, newpkg)
	if err != nil {
		return 0, err
	}
	newpkg.recycle()
	return n + m, nil
}

func (idx Index) cachekey(key []byte) string {
	return string(idx.key) + "/" + hex.EncodeToString(key)
}

func (idx *Index) loadSharedPack(tx *Tx, id uint32, touch bool) (*Package, error) {
	// try cache first
	key := encodePackKey(id)
	cachefn := idx.cache.Peek
	if touch {
		cachefn = idx.cache.Get
	}
	if pkg, ok := cachefn(id); ok {
		return pkg, nil
	}

	// if not found, load from storage
	pkg, err := tx.loadPack(idx.key, key, idx.newPackage(), idx.opts.PackSize())
	if err != nil {
		return nil, err
	}
	pkg.IncRef()

	atomic.AddInt64(&idx.stats.PacksLoaded, 1)
	atomic.AddInt64(&idx.stats.BytesRead, int64(pkg.size))

	// store in cache
	if touch {
		idx.cache.Add(id, pkg)
	}

	return pkg, nil
}

func (idx *Index) loadWritablePack(tx *Tx, id uint32) (*Package, error) {
	// try cache first
	key := encodePackKey(id)
	if pkg, ok := idx.cache.Get(id); ok {
		clone, err := pkg.Clone(idx.opts.PackSize())
		if err != nil {
			return nil, err
		}
		clone.IncRef()
		idx.releaseSharedPack(pkg)

		// prepare for efficient writes
		clone.Materialize()
		return clone, nil
	}

	// load from storage
	pkg := idx.newPackage()
	pkg.IncRef()

	var err error
	pkg, err = tx.loadPack(idx.key, key, pkg, idx.opts.PackSize())
	if err != nil {
		pkg.DecRef()
		return nil, err
	}

	atomic.AddInt64(&idx.stats.PacksLoaded, 1)
	atomic.AddInt64(&idx.stats.BytesRead, int64(pkg.size))
	return pkg, nil
}

// Note: we keep empty index pack names to avoid (re)naming issues
func (idx *Index) storePack(tx *Tx, pkg *Package) (int, error) {
	key := pkg.Key()

	defer func() {
		// remove from cache, returns back to pool
		idx.cache.Remove(pkg.key)
	}()

	// remove empty packs from pack index, storage and cache
	if pkg.Len() > 0 {
		// build header statistics
		info := pkg.Info()
		err := info.UpdateStats(pkg)
		if err != nil {
			return 0, err
		}

		n, err := tx.storePack(idx.key, key, pkg, idx.opts.FillLevel)
		if err != nil {
			return 0, err
		}

		// update pack index
		info.Packsize = n
		idx.packidx.AddOrUpdate(info)
		atomic.AddInt64(&idx.stats.PacksStored, 1)
		atomic.AddInt64(&idx.stats.BytesWritten, int64(n))

		return n, nil

	} else {
		// If pack is empty

		// drop from index
		idx.packidx.Remove(pkg.key)

		// remove from storage
		if err := tx.deletePack(idx.key, key); err != nil {
			return 0, err
		}

		return 0, nil
	}
}

func (idx *Index) makePackage() interface{} {
	atomic.AddInt64(&idx.stats.PacksAlloc, 1)
	pkg := NewPackage(idx.opts.PackSize(), idx.packPool)
	_ = pkg.InitFieldsFrom(idx.journal)
	return pkg
}

func (idx *Index) newPackage() *Package {
	return idx.packPool.Get().(*Package)
}

func (idx *Index) releaseSharedPack(pkg *Package) {
	if pkg == nil {
		return
	}
	if pkg.DecRef() == 0 {
		atomic.AddInt64(&idx.stats.PacksRecycled, 1)
	}
}

func (idx *Index) Stats() TableStats {
	var s TableStats = idx.stats

	// TODO: count live index tuples
	// s.TupleCount = idx.meta.Rows

	s.JournalTuplesCount = int64(idx.journal.Len())
	s.JournalTuplesCapacity = int64(idx.journal.Cap())
	s.JournalSize = int64(idx.journal.HeapSize())

	s.TombstoneTuplesCount = int64(idx.tombstone.Len())
	s.TombstoneTuplesCapacity = int64(idx.tombstone.Cap())
	s.TombstoneSize = int64(idx.tombstone.HeapSize())

	// copy cache stats
	cs := idx.cache.Stats()
	s.CacheHits = cs.Hits
	s.CacheMisses = cs.Misses
	s.CacheInserts = cs.Inserts
	s.CacheEvictions = cs.Evictions
	s.CacheCount = cs.Count
	s.CacheSize = cs.Size

	return s
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
	case FieldTypeInt32:
		bigEndian.PutUint64(buf[:], uint64(val.(int32)))
		h.Write(buf[:])
	case FieldTypeInt16:
		bigEndian.PutUint64(buf[:], uint64(val.(int16)))
		h.Write(buf[:])
	case FieldTypeInt8:
		bigEndian.PutUint64(buf[:], uint64(val.(int8)))
		h.Write(buf[:])
	case FieldTypeUint64:
		bigEndian.PutUint64(buf[:], val.(uint64))
		h.Write(buf[:])
	case FieldTypeUint32:
		bigEndian.PutUint64(buf[:], uint64(val.(uint32)))
		h.Write(buf[:])
	case FieldTypeUint16:
		bigEndian.PutUint64(buf[:], uint64(val.(uint16)))
		h.Write(buf[:])
	case FieldTypeUint8:
		bigEndian.PutUint64(buf[:], uint64(val.(uint8)))
		h.Write(buf[:])
	case FieldTypeFloat64:
		bigEndian.PutUint64(buf[:], math.Float64bits(val.(float64)))
		h.Write(buf[:])
	case FieldTypeFloat32:
		bigEndian.PutUint64(buf[:], math.Float64bits(float64(val.(float32))))
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
	case FieldTypeInt32:
		val, _ := pkg.Int32At(index, pos)
		bigEndian.PutUint64(buf[:], uint64(val))
		h.Write(buf[:])
	case FieldTypeInt16:
		val, _ := pkg.Int16At(index, pos)
		bigEndian.PutUint64(buf[:], uint64(val))
		h.Write(buf[:])
	case FieldTypeInt8:
		val, _ := pkg.Int8At(index, pos)
		bigEndian.PutUint64(buf[:], uint64(val))
		h.Write(buf[:])
	case FieldTypeUint64:
		val, _ := pkg.Uint64At(index, pos)
		bigEndian.PutUint64(buf[:], val)
		h.Write(buf[:])
	case FieldTypeUint32:
		val, _ := pkg.Uint32At(index, pos)
		bigEndian.PutUint64(buf[:], uint64(val))
		h.Write(buf[:])
	case FieldTypeUint16:
		val, _ := pkg.Uint16At(index, pos)
		bigEndian.PutUint64(buf[:], uint64(val))
		h.Write(buf[:])
	case FieldTypeUint8:
		val, _ := pkg.Uint8At(index, pos)
		bigEndian.PutUint64(buf[:], uint64(val))
		h.Write(buf[:])
	case FieldTypeFloat64:
		val, _ := pkg.Float64At(index, pos)
		bigEndian.PutUint64(buf[:], math.Float64bits(val))
		h.Write(buf[:])
	case FieldTypeFloat32:
		val, _ := pkg.Float32At(index, pos)
		bigEndian.PutUint64(buf[:], math.Float64bits(float64(val)))
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
	case FieldTypeInt32:
		return uint64(val.(int32))
	case FieldTypeInt16:
		return uint64(val.(int16))
	case FieldTypeInt8:
		return uint64(val.(int8))
	case FieldTypeUint64:
		return val.(uint64)
	case FieldTypeUint32:
		return uint64(val.(uint32))
	case FieldTypeUint16:
		return uint64(val.(uint16))
	case FieldTypeUint8:
		return uint64(val.(uint8))
	case FieldTypeDatetime:
		return uint64(val.(time.Time).UnixNano())
	default:
		// FieldTypeBytes, FieldTypeBoolean, FieldTypeString, FieldTypeFloat64, FieldTypeFloat32
		return 0
	}
}

func intValueAt(typ FieldType, pkg *Package, index, pos int) uint64 {
	switch typ {
	case FieldTypeInt64, FieldTypeDatetime:
		val, _ := pkg.Int64At(index, pos)
		return uint64(val)
	case FieldTypeInt32:
		val, _ := pkg.Int32At(index, pos)
		return uint64(val)
	case FieldTypeInt16:
		val, _ := pkg.Int16At(index, pos)
		return uint64(val)
	case FieldTypeInt8:
		val, _ := pkg.Int8At(index, pos)
		return uint64(val)
	case FieldTypeUint64:
		val, _ := pkg.Uint64At(index, pos)
		return val
	case FieldTypeUint32:
		val, _ := pkg.Uint32At(index, pos)
		return uint64(val)
	case FieldTypeUint16:
		val, _ := pkg.Uint16At(index, pos)
		return uint64(val)
	case FieldTypeUint8:
		val, _ := pkg.Uint8At(index, pos)
		return uint64(val)
	default:
		// FieldTypeBytes, FieldTypeBoolean, FieldTypeString, FieldTypeFloat64, FieldTypeFloat32
		return 0
	}
}

func hashZeroAt(pkg *Package, index, pos int) bool {
	return pkg.IsZeroAt(index, pos, false)
}

func intZeroAt(pkg *Package, index, pos int) bool {
	return pkg.IsZeroAt(index, pos, true)
}
