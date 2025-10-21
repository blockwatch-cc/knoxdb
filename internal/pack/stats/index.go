// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

// Index implements efficient and scalable pack statistics management storing
// statistics about every data vector as compressed column vectors.
//
// For every data pack these statistics include
// - min/max statistics about each column vector a.k.a zone maps
// - bloom/fuse/bit filters for column vectors (optional, user controlled in schema)
// - range filters for integer columns (optional, integer type data only)
//
// Statistics are used for two major purposes
// - query execution: decide which data packs (ranges) to skip during table scans
// - journal merge: decide which records and tombstones to merge into data packs
//   based on primary key (a.k.a. row_id)
//
// Design considerations
// - index for metadata about pack-oriented tables, history tables and indexes
// - copy-on-write, version controlled contents, delayed garbage collection
// - readers retain references to stable versions, writer produces new version
// - multiple old versions exist as long as readers live, GC with watermark
// - table writers register replaced data packs for GC
// - data packs have a fixed maximum length
// - data packs are referenced by key ([4]byte = big endian uint32)
// - data pack keys are assigned sequentially, sequence may contain gaps post deletion
// - blocks (column vectors) are referenced by schema field id (uint16)
// - row ids are uint64 (`rid` field on main & history tables)
// - row id zero (0) is invalid
// - row ids are auto-generated (users can define an extra primary key field)
// - row ids are not reused after deletion
// - rid min-max ranges never overlap on main tables
// - rid min-max ranges have the same order as pack ids on main tables
// - rid ranges on history tables may overlap (sequenced by record update/delete order)
// - history tables are append only (no deleted/update)
// - main table packs are removed when empty
// - table compaction does a full index rewrite
// - large block deletion/updates may cause full tree rewrites
//
// Data Placement
//
// Records are assigned to data packs based on sequential row ids. Each data pack
// stores a unique non-overlapping range of row ids (main tables only). Rid reuse
// post deletion and out-of-order insert are probibited. Hence we can easily find
// the closest data pack for record placement based on rid column min/max statistics.
//
// The challenging part is that data packs can be removed when empty so we cannot
// directly compute pack id from row id. We need efficient lookup of packs from rids
// (during merge) and efficient search/filter capabilities for scans (during queries).
//
//
// Data Organization
//
// Because the amount of data packs can grow very large (e.g. 15k+ for 1B records
// at pack size 64k) we optimize for efficient lookup and filter operations.
//
// This index design makes two important choices:
// - data pack statistics (min/max) are stored as 2048 long groups of column
//   vectors enabling vectorized scans and compression
// - statistic packs are further organized into an n-ary tree where each node
//   contains aggregated meta-statistics about child statistics column groups
//   (higher order min/max ranges, root node stores full range)
//
// The root node of the tree stores aggregate total ranges for each
// data column and total aggregate sum/count statistics about all data packs.
//
// Meta statistics are stored and updated in wire-encoded format (row storage)
// interleaving min and max values. The statistics schema is directly derived
// from the table schema (every column is duplicated for min & max). We add
// additional statistics columns for extra metadata like data pack key, version,
// sizes and value counts.
//
// Query performance benefits from search space pruning, i.e queries can skip
// metadata branches when column values are clustered. For non-clustered columns,
// the user can request bloom/fuse filters for probabilistic pruning.
//
// Versioning
//
// As the set and contents of data packs changes during table merge and the
// metadata index is updated concurrently by the merge writer, all readers must
// retain a consistent view on the index version and data packs they originally
// started with. The index is therefore reference counted and is atomically
// replaced after a merge operation completes. Readers hold a reference to
// the index version they started reading from and through that, implicitly,
// on all data pack versions referenced by it.
//
// Note there is a distinction between the global index version/epoch (counting
// in merge operation ticks, driven by journal segment ids) and the versions of
// data packs, spacks and tree nodes on disk (counting individually +1 as updates
// happen, encoded in on-disk keys). For efficient GC we keep lists of replaced
// on-disk objects' keys in a tombstone and eventually delete those keys when the
// global index version/epoch watermark increases. This means data packs, filters,
// tree nodes and spacks from different index epochs can co-exist on disk as long
// as a reader holds a reference to an old index. When the last reader finishes
// the index epoch becomes ready for garbage collection, however, we can only GC
// when the epoch watermark increases as well.
//
//
// Tree Algorithm Design
//
// The index tree consists of two node types, internal nodes (inodes) with
// meta-statistics about a sub-tree and leaf nodes (snodes) with meta-statistics
// about a single statistics vector pack and a reference to this pack (spack).
//
// Tree node indexes are zero-based (root = 0) and assigned in breadth-order
// so that nodes can be addressed without pointers, i.e. child and parent
// indexes are directly computed from a node's index.
//
// Data pack statistics are appended in sequential order (with increasing data pack
// key) and may be updated or deleted in arbitrary order. Hence the tree always
// grows to the right side only and may shrink anywhere on snode deletions.
//
// To avoid costly re-calculation of meta-statistics in inodes when a new tree
// level is added we use a depth balanced tree such that all leaf nodes
// are at the same lowestmost level, hence their order remains stable. This
// choice also allows us to split the tree into two separate arrays, one for
// inodes and one for snodes. On growth snodes are simply appended to the
// snode array and on level expansion the inode array is reshuffled but
// does not require recomputation. Removal of snodes, however, leads to a
// compaction of the snode list. We rebuild the entire inode tree in this case
// (its the least error prone and on average the most efficient method compared
// to a complex tree reorganization).
//

// ----------------------------------------------------------------------------
// TODO
// - debug features, packview integration
// - spack append/update
//   - spack disksize is not yet stored
//   - bloom and range index sizes are not yet stored
// - limit string size to max 8 (how do filters change?)

// Ideas
//
// Performance
// - async load many bloom & range filters
// - use (page) cache for bloom and range filters
// - better cache with less locking overhead
//
// More statistics
// - counting HLL
// - aggregate bloom filters on inodes (size/precision?)
//
// HLL management
// func (idx *Index) AddRecord(pkg *pack.Package, n int)       {}
// func (idx *Index) DelRecord(pkg *pack.Package, n int)       {}
// func (idx *Index) AddRange(pkg *pack.Package, from, to int) {}
// func (idx *Index) DelRange(pkg *pack.Package, from, to int) {}
// func (idx Index) Cardinality(col int) uint64                    {}

const (
	STATS_PACK_SIZE       = 2048 // max size of statistics package
	STATS_STRING_MAX_LEN  = 8    // max prefix bytes for string/byte statistics
	STATS_DATA_COL_OFFSET = 5    // start of the first data column in index schema
)

type Features byte

const (
	FeatBloomFilter Features = 1 << iota
	FeatFuseFilter
	FeatBitsFilter
	FeatRangeFilter
	FeatUseCache
)

const FilterMask Features = 0xf

func (f Features) Is(x Features) bool {
	return f&x > 0
}

func (f Features) HasFilter() bool {
	return f&FilterMask > 0
}

type Index struct {
	rc           uint32                // reference count
	epoch        uint32                // epoch sequence number
	schema       *schema.Schema        // statistics schema (meta + min + max)
	view         *schema.View          // helper to extract tree node data from wire format
	wr           *schema.Writer        // wire format builder (writer only)
	table        engine.TableEngine    // table back-reference used for index GC
	rx           int                   // index of the data pack's rowid column
	px           int                   // index of the data pack's primary key column
	nmax         int                   // max data pack size
	tomb         *Tomb                 // per-version tombstone
	db           store.DB              // backend reference for pulling more data
	keys         [STATS_BUCKETS][]byte // statistics bucket keys
	inodes       []*INode              // inner nodes of the binary tree as array
	snodes       []*SNode              // leaf nodes of the binary tree as array
	log          log.Logger            // logger instance
	bytesRead    int64                 // io metrics
	bytesWritten int64                 // io metrics
	use          Features              // index features
	clean        bool                  // no GC required

	// card   []*loglogbeta.LogLogBeta // HLL cardinality estimators [n_columns]
}

func NewIndex() *Index {
	return &Index{
		rc:     1,
		epoch:  0,
		inodes: make([]*INode, 0),
		snodes: make([]*SNode, 0),
		tomb:   NewTomb().WithEpoch(0),
		log:    log.Disabled,
		use:    FeatBloomFilter | FeatFuseFilter | FeatBitsFilter,
		clean:  true,
	}
}

// create a private copy used to update the index during background merge
func (idx *Index) Clone() *Index {
	return &Index{
		rc:           1,                          // start with refcount of 1
		epoch:        idx.epoch,                  // same epoch, writer can set
		schema:       idx.schema,                 // schema is read-only
		view:         schema.NewView(idx.schema), // need private view state
		table:        idx.table,                  // table back-reference
		wr:           idx.wr,                     // writer is stateful, only used during merge
		rx:           idx.rx,                     // config is read-only
		px:           idx.px,                     // config is read-only
		nmax:         idx.nmax,                   // config is read-only
		tomb:         idx.tomb,                   // tomb is read-only
		db:           idx.db,                     // db remains the same
		keys:         idx.keys,                   // bucket keys are read-only
		inodes:       slices.Clone(idx.inodes),   // tree is copy-on-write
		snodes:       slices.Clone(idx.snodes),   // tree is copy-on-write
		log:          idx.log,                    // log is shared and thread-safe
		use:          idx.use,                    // flags are read only
		bytesRead:    idx.bytesRead,              // track metrics across versions
		bytesWritten: idx.bytesWritten,           // track metrics across versions
		clean:        idx.clean,                  // GC required status
	}
}

func (idx *Index) WithDB(db store.DB) *Index {
	idx.db = db
	idx.tomb.WithDB(db)
	return idx
}

func (idx *Index) WithSchema(s *schema.Schema) *Index {
	idx.schema = MakeSchema(s)
	idx.keys = makeStorageKeys([]byte(idx.schema.Name()))
	idx.rx, idx.px = s.RowIdIndex(), s.PkIndex()
	if idx.rx < 0 {
		idx.rx = idx.px
	}
	idx.view = schema.NewView(idx.schema)
	idx.wr = schema.NewWriter(idx.schema, binary.LittleEndian)
	idx.tomb.WithSchema(s, idx.schema, idx.use).WithBucketKey(idx.keys[STATS_TOMB_KEY])
	return idx
}

func (idx *Index) WithEpoch(v uint32) *Index {
	idx.epoch = v
	if v > 0 {
		v-- // track tombs at parent epoch (reclaim by watermark)
	}
	idx.tomb.WithEpoch(v)
	return idx
}

func (idx *Index) WithMaxSize(nmax int) *Index {
	idx.nmax = nmax
	return idx
}

func (idx *Index) WithLogger(l log.Logger) *Index {
	idx.log = l
	return idx
}

func (idx *Index) WithTable(t engine.TableEngine) *Index {
	idx.table = t
	return idx
}

func (idx *Index) WithCache(use bool) *Index {
	if use {
		idx.use |= FeatUseCache
	} else {
		idx.use &^= FeatUseCache
	}
	return idx
}

func (idx *Index) WithFeatures(f Features) *Index {
	if f == 0 {
		idx.use = f
	} else {
		idx.use |= f
	}
	return idx
}

func (idx *Index) AtomicPtr() *AtomicPointer {
	return NewAtomicPtr(idx)
}

func (idx *Index) Release(withGC bool) {
	for {
		rc := atomic.LoadUint32(&idx.rc)
		if rc == 0 {
			return // already cleaned
		}
		if rc == 1 {
			// try clean
			if atomic.CompareAndSwapUint32(&idx.rc, 1, 0) {
				if err := idx.cleanup(withGC); err != nil {
					idx.log.Errorf("stats: cleanup failed: %v", err)
				}
				idx.Free()
				return
			}
			continue
		}
		// Normal decrement
		if atomic.CompareAndSwapUint32(&idx.rc, rc, rc-1) {
			return
		}
	}
}

func (idx *Index) cleanup(withGC bool) error {
	return idx.db.Update(func(tx store.Tx) error {
		// drop self
		if err := idx.dropEpoch(tx); err != nil {
			return err
		}
		idx.clean = false

		// run GC when requested
		if withGC {
			return idx.RunGC(tx)
		}
		return nil
	})
}

func (idx *Index) Free() {
	// idx.log.Warnf("free idx epoch=%d rc=%d", idx.epoch, idx.rc)
	clear(idx.snodes)
	clear(idx.inodes)
	clear(idx.keys[:])
	idx.table = nil
	idx.tomb = nil
	idx.rc = 0
	idx.epoch = 0
	idx.schema = nil
	idx.db = nil
	idx.inodes = nil
	idx.snodes = nil
	idx.view = nil
	idx.wr = nil
	idx.rx = 0
	idx.px = 0
	idx.nmax = 0
	idx.use = 0
	idx.clean = false
}

func (idx *Index) Clear() {
	// idx.log.Warnf("clear idx epoch=%d rc=%d", idx.epoch, idx.rc)
	for _, v := range idx.snodes {
		v.Clear()
	}
	clear(idx.snodes)
	clear(idx.inodes)
	idx.inodes = idx.inodes[:0]
	idx.snodes = idx.snodes[:0]
	idx.clean = true
}

func (idx *Index) Close() {
	// idx.log.Warnf("close idx epoch=%d rc=%d", idx.epoch, idx.rc)
	for _, v := range idx.snodes {
		v.Clear()
	}
	clear(idx.snodes)
	clear(idx.inodes)
	clear(idx.keys[:])
	if idx.tomb != nil {
		idx.tomb.Close()
		idx.tomb = nil
	}
	idx.table = nil
	idx.rc = 0
	idx.epoch = 0
	idx.schema = nil
	idx.db = nil
	idx.inodes = nil
	idx.snodes = nil
	idx.view = nil
	idx.wr = nil
	idx.rx = 0
	idx.px = 0
	idx.nmax = 0
	idx.use = 0
	idx.clean = false
}

// introspect

func (idx *Index) Epoch() uint32 {
	return idx.epoch
}

// num data packs
func (idx *Index) Len() int {
	return idx.root().NPacks(idx.view)
}

// num data rows
func (idx *Index) Count() int {
	return int(idx.root().NValues(idx.view))
}

// true if epoch list is clean
func (idx *Index) IsClean() bool {
	return idx.clean
}

// index i/o metrics
func (idx *Index) Metrics() (bytesRead int64, bytesWritten int64) {
	r := atomic.LoadInt64(&idx.bytesRead)
	w := atomic.LoadInt64(&idx.bytesWritten)
	return r, w
}

// index heap usage in bytes
func (idx *Index) HeapSize() int {
	var sz int
	for _, v := range idx.inodes {
		if v == nil {
			continue
		}
		sz += 24 + len(v.meta)
	}
	for _, v := range idx.snodes {
		sz += 32 + len(v.meta) + v.spack.Size()
	}
	return sz
}

// total on-disk table size in bytes (sum of data pack sizes)
func (idx *Index) TableSize() int {
	return int(idx.root().Size(idx.view))
}

// total on-disk index size in bytes (index packs, bloom, range indexes)
func (idx *Index) IndexSize() int {
	// TODO: bloom and range index sizes
	// TODO: disksize is not yet stored/loaded
	var sz int
	for _, v := range idx.inodes {
		if v == nil {
			continue
		}
		sz += len(v.meta)
	}
	for _, v := range idx.snodes {
		sz += len(v.meta) + v.disksize
	}
	return sz
}

// pack management
func (idx *Index) AddPack(ctx context.Context, pkg *pack.Package) error {
	// lookup pack placement
	node, i, ok := idx.findSNode(pkg.Key())

	// create a new leaf node when not found or full
	if !ok || node.spack.Len() == STATS_PACK_SIZE {
		node, i = idx.addSnode()
	}

	// ensure all stats blocks are loaded and materialized
	node, err := idx.prepareWrite(ctx, node, i)
	if err != nil {
		return err
	}

	// add data pack statistics to node
	if node.AppendPack(pkg) {
		// update spack meta statistics on change
		if node.BuildMetaStats(idx.view, idx.wr) {
			// update meta statistics towards the root on change
			idx.updatePathToRoot(i)
		}
	}

	// build bloom and range filters
	return idx.buildFilters(pkg, node)
}

func (idx *Index) UpdatePack(ctx context.Context, pkg *pack.Package) error {
	// lookup pack placement
	node, i, ok := idx.findSNode(pkg.Key())
	if !ok {
		// should not happen
		return fmt.Errorf("stats: missing record for pack %08x[v%d]", pkg.Key(), pkg.Version())
	}

	// ensure all stats blocks are loaded and materialized
	node, err := idx.prepareWrite(ctx, node, i)
	if err != nil {
		return err
	}

	// update data pack statistics record
	if node.UpdatePack(pkg) {
		// update spack meta statistics on change
		if node.BuildMetaStats(idx.view, idx.wr) {
			// update meta statistics towards the root on change
			idx.updatePathToRoot(i)
		}
	}

	// rebuild bloom and range filters
	return idx.buildFilters(pkg, node)
}

func (idx *Index) DeletePack(ctx context.Context, pkg *pack.Package) error {
	// lookup pack placement
	node, i, ok := idx.findSNode(pkg.Key())
	if !ok {
		// should not happen
		return fmt.Errorf("stats: missing record for pack %08x[v%d]", pkg.Key(), pkg.Version())
	}

	// ensure all stats blocks are loaded and materialized
	node, err := idx.prepareWrite(ctx, node, i)
	if err != nil {
		return err
	}

	// remove data pack statistics record
	ok = node.DeletePack(pkg)

	// empty snodes will be dropped on save

	// handle tree change
	if ok && !node.IsEmpty() {
		// update spack meta statistics on change
		if node.BuildMetaStats(idx.view, idx.wr) {
			// update inodes all the way to root when meta stats have changed
			idx.updatePathToRoot(i)
		}
	}

	// drop bloom and range filters
	return idx.dropFilters(pkg)
}

// external tomb access for scheduling pack deletion
func (idx *Index) Tomb() *Tomb {
	return idx.tomb
}

// The following functions are called by a single background merge thread.
// Access is not shared with concurrent readers.

func (idx *Index) NextKey() uint32 {
	var k uint32
	if l := len(idx.snodes); l > 0 {
		k = idx.snodes[l-1].MaxKey() + 1
	}
	// idx.log.Warnf("Stats epoch %d next key @snode=%d is %d", idx.epoch, len(idx.snodes)-1, k)
	return k
}

func (idx *Index) GlobalMinRid() uint64 {
	val, ok := idx.root().Get(idx.view, minColIndex(idx.rx))
	if !ok {
		return 0
	}
	return val.(uint64)
}

func (idx *Index) GlobalMaxRid() uint64 {
	val, ok := idx.root().Get(idx.view, maxColIndex(idx.rx))
	if !ok {
		return 0
	}
	return val.(uint64)
}

func (idx *Index) GlobalMinPk() uint64 {
	val, ok := idx.root().Get(idx.view, minColIndex(idx.px))
	if !ok {
		return 0
	}
	return val.(uint64)
}

func (idx *Index) GlobalMaxPk() uint64 {
	val, ok := idx.root().Get(idx.view, maxColIndex(idx.px))
	if !ok {
		return 0
	}
	return val.(uint64)
}

func (idx *Index) IsTailFull() bool {
	if idx.Len() == 0 {
		return true
	}
	return idx.snodes[len(idx.snodes)-1].LastNValues() == idx.nmax
}

func (idx *Index) TailInfo() (uint32, uint32, int) {
	if idx.Len() == 0 {
		return 0, 0, 0
	}
	return idx.snodes[len(idx.snodes)-1].LastInfo()
}

// debug use only
func (idx *Index) Get(key uint32) (*Record, bool) {
	node, _, ok := idx.findSNode(key)
	if !ok {
		return nil, false
	}
	pos, ok := node.FindKey(key)
	if !ok {
		return nil, false
	}

	// create iterator pointing to stats record we found
	it := &Iterator{
		ctx:    context.Background(),
		idx:    idx,
		smatch: bitset.New(0),
		vmatch: bitset.New(0),
		snode:  node,
		match:  []uint32{uint32(pos)},
	}
	// load missing fields but don't run an spack query (flt = nil)
	if err := it.snode.Query(it); err != nil {
		// what to do?
		assert.Unreachable("snode query failed", err)
	}
	defer it.Close()

	return NewRecordFromWire(idx.schema, it.ReadWire()), true
}

// Find a candidate data pack to insert/merge a row id into.
//
// Use Cases
// 1. update/tombstone merge -> rid is within exactly one data pack's rid range
// 2. insert -> rid is larger than the last pack's range
//
// Out of order insert is unsupported to preserve the design invariant
// of non-overlapping rid ranges in main table data packs. Without this
// invariant we'd have to split packs as they run full which would violate
// pack order invariant (this is required to quickly lookup packs in the index)
func (idx *Index) FindRid(ctx context.Context, rid uint64) (*Iterator, bool) {
	// create an equal filter condition which will be used to find
	// the matching data pack for this rowid based on min/max statistics
	// this filter ensures the spack min/max rowid columns are loaded
	// other required columns: STATS_ROW_KEY, STATS_ROW_VERSION and
	// STATS_ROW_NVALS are auto-loaded by iterator queries
	flt := idx.makeRidFilter(types.FilterModeEqual, rid)

	// return an iterator for the last data pack in the last spack
	// unless this data pack is full (requires pack order == rid order
	// which is true for regular table packs but not history packs)
	if gmax := idx.GlobalMaxRid(); rid > gmax {
		slen := len(idx.snodes)

		// update filter to search for the gloabl max pk's data pack
		// (we know it exists, but we need to load statistics in order
		// for the merge process to check whether its full)
		flt.Filter.Value = gmax
		flt.Filter.Matcher.WithValue(gmax)

		// init an iterator so that calling next() will run a query
		it := &Iterator{
			ctx:    ctx,
			idx:    idx,
			flt:    flt,
			use:    0,
			vmatch: bitset.New(STATS_PACK_SIZE),
			smatch: bitset.New(slen),
			match:  make([]uint32, 0, 1),
			sx:     slen - 2, // start at last spack (it will +1)
			n:      -1,       // start at first offset (it will +1)
		}
		it.smatch.Set(slen - 1)

		// let the iterator load spack data and point to the last data pack
		return it, it.Next()
	}

	// should find exactly one pack
	return idx.Query(ctx, flt, types.OrderAsc)
}

// Query is used by concurrent TableReaders to produce a private iterator
// for scanning table contents. It efficiently selects all snodes with potential
// query filter matches. The actual spack vector and filter matching is done
// by Iterator.Next() while it progresses through snodes.
func (idx *Index) Query(ctx context.Context, flt *filter.Node, dir types.OrderType) (*Iterator, bool) {
	// Walk inode tree and build a queue of snodes to visit.
	// Each round we pick the next eligible inode and check
	// whether its children match. On match we insert a child's
	// id back into the list. The current inode's bit is cleared.
	//
	// For efficiency we use a bitset to hold node id state. A one
	// in this bitset signals that we still have to visit this node.
	// The tree structure allows us to re-use the same bitset
	// for inodes and snodes. While we work towards the end of the
	// bitset visiting lower inodes, the upper inode bits are already
	// cleared and will become available for setting snode bits as
	// we approach the lowest inode level. This works because the size
	// of the inode tree is one less than the maximum number of snodes.
	//
	// At the end of this algorithm the bitset contains one bits for
	// all snodes that have matched. We use this result to initialize
	// an iterator which will later run vector comparisons inside
	// the snode's statistics packs to find data pack matches.
	view := schema.NewView(idx.schema) // private view for concurrent readers
	maxInodes := len(idx.inodes) - 1
	slen := len(idx.snodes)
	nodeBits := bitset.New(slen + 1)

	// start matching root
	if maxInodes > 0 {
		nodeBits.Set(0)
	}

	// use catch all filter when nil
	if flt == nil {
		flt = idx.makeRidFilter(types.FilterModeGt, 0)
	}

	for n := 0; n < maxInodes; n++ {
		// skip this branch if unset
		if !nodeBits.Contains(n) {
			continue
		}

		// mark this node as processed
		nodeBits.Unset(n)

		// check children
		for _, m := range []int{leftChildIndex(n), rightChildIndex(n)} {
			if m < maxInodes {
				// children at this level are branches (inodes)

				// skip nil branches
				if idx.inodes[m] == nil {
					continue
				}
				// match inode
				if idx.inodes[m].Match(flt, view) {
					nodeBits.Set(m)
				}
			} else {
				// children at this level are leafs (snodes)
				// since snodes are stored in a separate slice
				// we adjust their position index

				// skip nil leafs
				sx := m - maxInodes
				if sx >= slen || idx.snodes[sx] == nil {
					continue
				}
				// match snode
				if idx.snodes[sx].Match(flt, view) {
					nodeBits.Set(sx)
				}
			}
		}
	}

	// stop early when nothing matched
	if nodeBits.None() {
		return nil, false
	}

	// identify if query would benefit from loading any filters
	var use Features
	if idx.use&FilterMask > 0 {
		flt.ForEach(func(f *filter.Filter) error {
			// range filters work in integer type columns only
			if f.Type.IsInt() {
				use |= FeatRangeFilter
			}

			switch f.Mode {
			case types.FilterModeEqual, types.FilterModeIn:
				// bloom filters work only for these modes
			default:
				return nil
			}

			// translate table column index into min statistics column
			field, _ := idx.schema.FieldByIndex(minColIndex(f.Index))

			// check if this field has any filters enabled
			switch field.Index() {
			case types.IndexTypeBloom:
				use |= FeatBloomFilter
			case types.IndexTypeBfuse:
				use |= FeatFuseFilter
			case types.IndexTypeBits:
				use |= FeatBitsFilter
			}
			return nil
		})

		// mask with enabled features
		use &= idx.use
	}

	// create iterator from matching snodes
	it := &Iterator{
		ctx:     ctx,
		idx:     idx,
		flt:     flt,
		use:     use,
		smatch:  nodeBits,
		vmatch:  bitset.New(STATS_PACK_SIZE),
		match:   arena.AllocUint32(STATS_PACK_SIZE),
		sx:      -1, // start at first bit (it will +1)
		n:       -1, // start at first offset (it will +1)
		reverse: dir.IsReverse(),
	}

	// a, b := nodeBits.MinMax()
	// idx.log.Warnf("New IT smatch=%d snodes [%d-%d]", nodeBits.Count(), a, b)

	// start at last bit in snode bitset
	if it.reverse {
		_, last := nodeBits.MinMax()
		it.sx = last + 1
	}

	// init iterator with next match
	return it, it.Next()
}

// root node access
func (idx *Index) root() *INode {
	if len(idx.inodes) == 0 {
		return NewINode()
	}
	return idx.inodes[0]
}

// finds snode where key exists or suggests node to place a new key
func (idx *Index) findSNode(key uint32) (*SNode, int, bool) {
	l := len(idx.snodes)
	// binary search for the first match (this and all following snodes
	// return true for the condition below)
	i := sort.Search(l, func(i int) bool {
		n := idx.snodes[i]
		return key <= n.MaxKey() || // either key may exist
			key > n.MinKey() && n.NPacks() < STATS_PACK_SIZE // or pack has space
	})

	// search returns len when no match was found
	if i < l {
		return idx.snodes[i], i, true
	}
	return nil, -1, false
}

func (idx *Index) addSnode() (*SNode, int) {
	// read current array lengths
	ilen, slen := len(idx.inodes), len(idx.snodes)

	// The tree is considered full when the number of leaf
	// snodes equals the number of inodes (actually we keep the
	// last inode in the array empty, but we still allocate space
	// to make the algorithm simpler. A regular full binary tree
	// has n-1 internal nodes for n leafs)
	isFullLevel := ilen == slen

	// use slen as key so that we assign sequential stats pack keys
	var nextKey uint32
	if slen > 0 {
		nextKey = idx.snodes[slen-1].spack.Key() + 1
	}
	node := NewSNode(nextKey, idx.schema, true)
	idx.snodes = append(idx.snodes, node)
	slen++

	// grow tree when current leaf level is full
	if isFullLevel {
		// Double the inode tree's array size. This ensures the array is
		// always a power of 2 even though the last element remains unused.
		// We use this fact to simplify calculating levels and indices.
		// When tree is empty (ilen = 0), start at size 2
		sz := max(2, ilen*2)
		idx.inodes = slices.Grow(idx.inodes, sz)
		idx.inodes = idx.inodes[:sz]

		// Move existing inodes so the current tree becomes the left
		// subtree. Does not apply on first insert when tree was empty.
		// This avoids having to re-calculate merge statistics in existing
		// inodes, we simply rotate the tree and install a new root.
		// Because we work in-place we have to start at the end of the inode
		// array to prevent overriding pointers when moving.
		for i := ilen - 2; i >= 0; i-- {
			// to = 1                        when i = 0 (root node)
			//      i + 2 ^ trunc(log2(i+1)) otherwise
			to := i + 1<<log2(i+1)
			idx.inodes[to], idx.inodes[i] = idx.inodes[i], nil
		}

		// Calculate the new leaf node's index in breadth-first order inside
		// the new tree. Note we build a breadth balanced tree, so all leafs
		// are at the lowest level and new snodes are always appended at
		// the right side of that level.
		//
		//  sx = new inode count + new snode count
		//     = (sz - 1) + slen -1
		//
		sx := sz + slen - 2

		// init inodes from the new data node up to the root
		for n := parentIndex(sx); n > 0; n = parentIndex(n) {
			idx.inodes[n] = NewINode()
		}

		// init the new root node
		idx.inodes[0] = NewINode()
	} else {
		// init missing inodes from the new data node up to the root
		// (see above for how to calculate the new snode's index)
		sx := ilen + slen - 2
		for n := parentIndex(sx); n > 0; n = parentIndex(n) {
			// stop as soon as we find an inode exists
			if idx.inodes[n] != nil {
				break
			}
			idx.inodes[n] = NewINode()
		}
	}

	return node, slen - 1
}

// Update aggregate statistics on the path from a data node up to the root
// by merging statistics from both children into each parent inode.
func (idx *Index) updatePathToRoot(i int) {
	node := idx.snodes[i]
	ilen, slen := len(idx.inodes), len(idx.snodes)
	var ok bool

	// Find our direct parent inode (to calculate its position in
	// the array format of our binary tree we pretend inode and snode
	// arrays are consecutive. If we had a single array the inode part
	// would actually be one element shorter than here because a binary tree
	// has n-1 internal nodes for n leafs when full. for simplicity we
	// use ilen == slen in this implementation. thats why we need to
	// subtract 1 from ilen)
	p := parentIndex(ilen - 1 + i)
	parent := idx.inodes[p]

	// Identify both children and pass them to Update() which will
	// aggregate both childrens statistics. At the end of the snode
	// array a left child may not yet have a right sibling.
	if i%2 == 0 {
		// we are the left child
		if i == slen-1 {
			// we are the last child
			ok = parent.Update(idx.view, idx.wr, node, nil)
		} else {
			// there is a right child behind us
			ok = parent.Update(idx.view, idx.wr, node, idx.snodes[i+1])
		}
	} else {
		// we are the right child, so pick the left which is guaranteed to
		// exist in front of us
		ok = parent.Update(idx.view, idx.wr, idx.snodes[i-1], node)
	}

	// stop when we're already at root or nothing changed
	if !ok || p == 0 {
		return
	}

	// update inodes all the way to root, stop when aggregate stats have not changed
	// note right children may be missing when tree is not full
	for p = parentIndex(p); ok && p >= 0; p = parentIndex(p) {
		left := idx.inodes[leftChildIndex(p)]
		right := idx.inodes[rightChildIndex(p)]

		// Go is quirky. When we put nil pointers into interfaces the interface
		// does not compare with nil because its type is non nil. See
		// https://go.dev/doc/faq#nil_error
		if right == nil {
			ok = idx.inodes[p].Update(idx.view, idx.wr, left, nil)
		} else {
			ok = idx.inodes[p].Update(idx.view, idx.wr, left, right)
		}
	}
}

// Rebuilds all inodes by merging child statistics. Rebuild happens
// level by level starting at the lowest tree level and working upwards
// to the root. Inodes are numberd 0 (root) .. N in breadth-first order,
// hence we can simply walk the inode array backwards. Careful since not
// all snodes and inodes may exist. All inodes are tagged with version
// ver which is the highest current version +1.
func (idx *Index) rebuildInodeTree(ver uint32) {
	// clear the inode tree first
	clear(idx.inodes)

	// resize inode array
	slen := len(idx.snodes)
	idx.inodes = idx.inodes[:1<<log2ceil(slen)]
	ilen := len(idx.inodes)
	si := ilen - 1

	// the lowest inode level has snodes as children
	for n := ilen - 2; n >= ilen/2-1; n-- {
		li, ri := leftChildIndex(n)-si, rightChildIndex(n)-si

		// skip this node when no left child exists
		if li >= slen {
			continue
		}

		// pick left and right snode (right may not exist but its ok)
		var right Node
		left := idx.snodes[li]
		if ri < slen {
			right = idx.snodes[ri]
		}

		// create new inode and build merged meta statistics
		idx.inodes[n] = NewINode()
		idx.inodes[n].Update(idx.view, idx.wr, left, right)
		idx.inodes[n].SetVersion(idx.view, ver)
	}

	// all upper inode levels have inodes as children
	for n := ilen/2 - 2; n >= 0; n-- {
		li, ri := leftChildIndex(n), rightChildIndex(n)

		// skip this node when no left child exists
		if li >= slen {
			continue
		}

		// pick left and right snode (right may not exist but its ok)
		var right Node
		left := idx.inodes[li]
		if ri < slen {
			right = idx.inodes[ri]
		}

		// create new inode and build merged meta statistics
		idx.inodes[n] = NewINode()
		idx.inodes[n].Update(idx.view, idx.wr, left, right)
		idx.inodes[n].SetVersion(idx.view, ver)
	}
}

func (idx *Index) makeRidFilter(mode types.FilterMode, rid uint64) *filter.Node {
	// when schema has no metadata (px == rx) fall back to pk field
	field, _ := idx.schema.FieldByIndex(minColIndex(idx.rx))
	m := filter.NewFactory(types.FieldTypeUint64).New(mode)
	m.WithValue(rid)
	id := schema.MetaRid
	if idx.rx == idx.px {
		id = uint16(idx.px + 1)
	}
	return &filter.Node{
		Filter: &filter.Filter{
			Name:    strings.TrimPrefix(field.Name(), "min_"),
			Type:    types.BlockUint64,
			Mode:    mode,
			Index:   idx.rx,
			Id:      id,
			Value:   rid,
			Matcher: m,
		},
	}
}
