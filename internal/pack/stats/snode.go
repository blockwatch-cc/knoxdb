// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/store"
)

type SNode struct {
	mu       sync.Mutex    // sync missing data block load across readers
	spack    *pack.Package // statistics union package
	meta     []byte        // wire encoded min/max/sum statistics over pkg content
	disksize int           // statistics package on-disk data size
	dirty    bool          // dirty flag
}

func NewSNode(key uint32, s *schema.Schema, alloc bool) *SNode {
	node := &SNode{
		spack: pack.New().
			WithKey(key).
			WithVersion(1).
			WithMaxRows(STATS_PACK_SIZE).
			WithSchema(s),
		meta: make([]byte, s.WireSize()),
	}
	if alloc {
		node.spack.Alloc()
	}
	return node
}

func (n *SNode) Clear() {
	n.spack.Release()
	n.spack = nil
	n.meta = nil
	n.disksize = 0
	n.dirty = false
}

func (n *SNode) Key() uint32 {
	return n.spack.Key()
}

func (n *SNode) Version() uint32 {
	return n.spack.Version()
}

func (n *SNode) LoadVersion(view *schema.View) {
	v, ok := view.Reset(n.meta).GetPhy(STATS_ROW_VERSION)
	view.Reset(nil)
	if ok {
		n.spack.WithVersion(v.(uint32))
	}
}

func (n *SNode) SetVersion(view *schema.View, ver uint32) {
	view.Reset(n.meta).Set(STATS_ROW_VERSION, ver)
	view.Reset(nil)
	n.spack.WithVersion(ver)
}

func (n *SNode) Bytes() []byte {
	return n.meta
}

func (n *SNode) IsEmpty() bool {
	return n.spack.Len() == 0
}

func (n *SNode) IsWritable() bool {
	return n.spack.IsMaterialized()
}

func (n *SNode) NPacks() int {
	return n.spack.Len()
}

func (n *SNode) MinKey() uint32 {
	if n.spack.Len() > 0 {
		return n.spack.Uint32(STATS_ROW_KEY, 0)
	}
	return 0
}

func (n *SNode) MaxKey() uint32 {
	var k uint32
	if l := n.spack.Len(); l > 0 {
		k = n.spack.Uint32(STATS_ROW_KEY, l-1)
	}
	// fmt.Printf("Snode %d[v%d] %p max key at pos %d is %d\n",
	// 	n.Key(), n.Version(), n, n.spack.Len(), k)

	// operator.NewLogger(os.Stdout, 30).Process(context.Background(), n.spack)

	return k
}

func (n *SNode) LastNValues() int {
	if l := n.spack.Len(); l > 0 {
		return int(n.spack.Uint64(STATS_ROW_NVALS, l-1))
	}
	return 0
}

func (n *SNode) LastInfo() (uint32, uint32, int) {
	l := n.spack.Len()
	if l == 0 {
		return 0, 0, 0
	}
	k := n.spack.Uint32(STATS_ROW_KEY, l-1)
	v := n.spack.Uint32(STATS_ROW_VERSION, l-1)
	s := n.spack.Uint64(STATS_ROW_NVALS, l-1)
	return k, v, int(s)
}

func (n *SNode) FindKey(key uint32) (int, bool) {
	// find pack offset (spack is sorted by data pack key)
	keys := n.spack.Block(STATS_ROW_KEY).Uint32()
	i := sort.Search(n.spack.Len(), func(i int) bool { return keys.Get(i) >= key })

	// unlikely, should not happen
	if i == keys.Len() || keys.Get(i) != key {
		return -1, false
	}

	return i, true
}

func (n *SNode) AppendPack(pkg *pack.Package) bool {
	// append meta statistics
	n.spack.Block(STATS_ROW_KEY).Uint32().Append(pkg.Key())
	n.spack.Block(STATS_ROW_VERSION).Uint32().Append(pkg.Version())
	n.spack.Block(STATS_ROW_SCHEMA).Uint64().Append(pkg.Schema().Hash)
	n.spack.Block(STATS_ROW_NVALS).Uint64().Append(uint64(pkg.Len()))
	n.spack.Block(STATS_ROW_SIZE).Int64().Append(pkg.Stats().SizeDiff())

	// fmt.Printf("append snode %d from pack 0x%08x[v%d]\n",
	// 	n.Key(), pkg.Key(), pkg.Version())

	pstats := pkg.Stats()
	for i, b := range pkg.Blocks() {
		var minv, maxv any
		if b == nil {
			// use zero values for invalid blocks (deleted from schema)
			minv = b.Type().Zero()
			maxv = minv
		} else {
			// reference min/max statistics
			minv = pstats.MinMax[i][0]
			maxv = pstats.MinMax[i][1]
		}

		// calculate data column positions inside statistics schema
		minx, maxx := minColIndex(i), maxColIndex(i)

		// append statistics values
		n.spack.Block(minx).Append(minv)
		n.spack.Block(maxx).Append(maxv)
	}
	n.spack.UpdateLen()
	n.dirty = true

	// fmt.Printf("snode: %x[v%d] append from %x[v%d] len=%d\n",
	// 	n.Key(), n.Version(), pkg.Key(), pkg.Version(), n.spack.Len())

	// operator.NewLogger(os.Stdout, 30).Process(context.Background(), n.spack)

	return n.dirty
}

func (n *SNode) UpdatePack(pkg *pack.Package) bool {
	k, ok := n.FindKey(pkg.Key())
	if !ok {
		// unlikely, should not happen
		assert.Unreachable("update unknown spack")
		return false
	}

	// fmt.Printf("update snode %d[v%d] from pack 0x%08x[v%d] at pos=%d\n",
	// 	n.Key(), n.Version(), pkg.Key(), pkg.Version(), k)

	// update data statistics on change
	pstats := pkg.Stats()
	for i, b := range pkg.Blocks() {
		// skip invalid blocks (deleted from schema) and non-dirty blocks
		if b == nil || !pstats.WasDirty[i] {
			continue
		}

		// reference min/max statistics
		minv := pstats.MinMax[i][0]
		maxv := pstats.MinMax[i][1]

		// calculate data column positions inside statistics schema
		minx, maxx := minColIndex(i), maxColIndex(i)

		// load current min/max values
		mino := n.spack.Block(minx).Get(k)
		maxo := n.spack.Block(maxx).Get(k)

		// set min/max when different
		if !b.Type().EQ(mino, minv) {
			// fmt.Printf("> F#%d min[%d] %v -> %v\n", i, minx, mino, minv)
			n.spack.Block(minx).Set(k, minv)
			n.dirty = true
		}
		if !b.Type().EQ(maxo, maxv) {
			// fmt.Printf("> F#%d max[%d] %v -> %v\n", i, maxx, maxo, maxv)
			n.spack.Block(maxx).Set(k, maxv)
			n.dirty = true
		}
	}

	// update pack statistics on change
	if vid := n.spack.Uint32(STATS_ROW_VERSION, k); vid != pkg.Version() {
		n.spack.Block(STATS_ROW_VERSION).Set(k, pkg.Version())
		n.dirty = true
	}
	if sid := n.spack.Uint64(STATS_ROW_SCHEMA, k); sid != pkg.Schema().Hash {
		n.spack.Block(STATS_ROW_SCHEMA).Set(k, pkg.Schema().Hash)
		n.dirty = true
	}
	if nvals := n.spack.Uint64(STATS_ROW_NVALS, k); nvals != uint64(pkg.Len()) {
		n.spack.Block(STATS_ROW_NVALS).Set(k, uint64(pkg.Len()))
		n.dirty = true
	}
	if diff, sz := n.spack.Int64(STATS_ROW_SIZE, k), pstats.SizeDiff(); diff != 0 {
		n.spack.Block(STATS_ROW_SIZE).Set(k, sz+diff)
		n.dirty = true
	}

	// operator.NewLogger(os.Stdout, 30).Process(context.Background(), n.spack)

	// data may not have changed
	return n.dirty
}

func (n *SNode) DeletePack(pkg *pack.Package) bool {
	// find pack offset (spack is sorted by data pack key)
	keys := n.spack.Block(STATS_ROW_KEY).Uint32().Slice()
	i := sort.Search(len(keys), func(i int) bool { return keys[i] >= pkg.Key() })

	// unlikely, should not happen
	if i == len(keys) || keys[i] != pkg.Key() {
		assert.Unreachable("delete unknown spack")
		return false
	}

	// remove statistics row
	err := n.spack.Delete(i, i+1)
	if err != nil {
		assert.Unreachable("delete unknown spack", err)
	}
	n.dirty = true

	// data has changed (at least num values and num packs)
	return n.dirty
}

func (n *SNode) PrepareWrite(ctx context.Context, b store.Bucket) (*SNode, error) {
	// create clone to prevent overriding spack used by concurrent readers
	clone := &SNode{
		spack:    n.spack.Clone(n.spack.Cap()),
		meta:     bytes.Clone(n.meta),
		disksize: n.disksize,
		dirty:    true,
	}

	// fmt.Printf("snode: %x[v%d] %p -> %p prepare write len=%d\n",
	// 	n.Key(), n.Version(), n, clone, n.spack.Len())

	// load missing blocks (previous version)
	_, err := clone.spack.LoadFromDisk(ctx, b, nil, 0)
	if err != nil {
		return nil, err
	}

	// materialize all blocks in-place
	clone.spack.Materialize()

	// use clone
	return clone, nil
}

func (n *SNode) Match(flt *filter.Node, view *schema.View) bool {
	view.Reset(n.meta)
	defer view.Reset(nil)
	return Match(flt, &ViewReader{view})
}

func (n *SNode) Query(it *Iterator) error {
	// organize data required for this query
	var loadBlocks []uint16

	if !n.spack.IsComplete() {
		// we always need data pack keys, versions, num values and rid columns
		if n.spack.Block(STATS_ROW_KEY) == nil {
			// translate index to field id
			loadBlocks = append(loadBlocks,
				STATS_ROW_KEY+1,
				STATS_ROW_VERSION+1,
				STATS_ROW_NVALS+1,
				uint16(minColIndex(it.idx.rx)+1), // min rid
				uint16(maxColIndex(it.idx.rx)+1), // max rid
			)
		}

		// translate filter indexes (field schema positions zero-based) into
		// spack columns identifiers (uint16, 1-based)
		if it.flt != nil {
			uniqueFields := make(map[uint16]struct{})
			it.flt.ForEach(func(f *filter.Filter) error {
				// skip already processed fields
				if _, ok := uniqueFields[f.Id]; ok {
					return nil
				}
				uniqueFields[f.Id] = struct{}{}

				// translate table column indices into min/max statistics column ids
				minx, maxx := minColIndex(f.Index), maxColIndex(f.Index)

				// identify missing statistics blocks and load by id (not by index)
				if n.spack.Block(minx) == nil {
					loadBlocks = append(loadBlocks, uint16(minx+1))
				}
				if n.spack.Block(maxx) == nil {
					loadBlocks = append(loadBlocks, uint16(maxx+1))
				}

				return nil
			})
		} else {
			// load all missing fields
			loadBlocks = loadBlocks[:0]
			for i, b := range n.spack.Blocks() {
				if b != nil {
					continue
				}
				// translate block index into field id (the meta schema uses
				// consecutive ids, so id == index+1)
				loadBlocks = append(loadBlocks, uint16(i)+1)
			}
		}
	}

	// optimized fast path when all data is in memory
	if len(loadBlocks) == 0 && it.use == 0 {
		if it.flt != nil {
			// reset match vector
			it.vmatch.Resize(n.spack.Len()).Zero()

			// match minmax ranges
			_, it.vmatch = matchVector(it.flt, n.spack, nil, it.vmatch)

			// convert bitset to indexes
			it.match = it.vmatch.Indexes(it.match)
		}
		return nil
	}

	// run with backend query to load missing data
	return it.idx.db.View(func(tx store.Tx) error {
		// check blocks are loaded, load missing spack blocks during query
		if len(loadBlocks) > 0 {
			var (
				loadedBlocks int
				cache        block.BlockCachePartition
			)

			// lock snode
			n.mu.Lock()

			// use cache with a private partition key
			if it.idx.use.Is(FeatUseCache) {
				cache = engine.GetEngine(it.ctx).BlockCache(it.idx.schema.Hash)
				loadedBlocks = n.spack.LoadFromCache(cache, loadBlocks)
			}

			// load remaining blocks from disk
			if len(loadBlocks) != loadedBlocks {
				nBytes, err := n.spack.LoadFromDisk(
					it.ctx,
					it.idx.statsBucket(tx), // tablename_stats
					loadBlocks,             // required/missing field indexes
					0,                      // read block/pack length from storage
				)
				if err != nil {
					n.mu.Unlock()
					return err
				}
				atomic.AddInt64(&it.idx.bytesRead, int64(nBytes))

				// add blocks to cache
				if it.idx.use.Is(FeatUseCache) {
					n.spack.AddToCache(cache)
				}
			}
			n.mu.Unlock()
		}

		// init filter buckets if required
		buckets := make(map[int]store.Bucket)
		if it.use.HasFilter() {
			buckets[STATS_FILTER_KEY] = it.idx.filterBucket(tx)
		}

		// run vectorized queries for filter types, load & check bloom,
		// fuse and bitset filters on demand
		if it.flt != nil {
			var m int

			// reset match vector
			it.vmatch.Resize(n.spack.Len()).Zero()

			// match minmax ranges and optional filters
			m, it.vmatch = matchVector(it.flt, n.spack, buckets, it.vmatch)
			if it.vmatch.None() {
				it.match = it.match[:0]
				return nil
			}
			atomic.AddInt64(&it.idx.bytesRead, int64(m))

			// convert bitset to indexes
			it.match = it.vmatch.Indexes(it.match)
		}

		return nil
	})
}

func (n *SNode) BuildMetaStats(view *schema.View, wr *schema.Writer) bool {
	// allocate meta buffer when nil
	if n.meta == nil {
		n.meta = make([]byte, wr.Len())
	}

	// use current statistics as baseline
	view.Reset(n.meta)
	wr.Reset()

	// aggregate across all statistics columns
	var dirty bool
	for i, b := range n.spack.Blocks() {
		// load current value
		curr, _ := view.GetPhy(i)

		// find new value
		var val any
		switch i {
		case STATS_ROW_KEY: // min key
			val = b.Uint32().Get(0)

		case STATS_ROW_VERSION: // use spack version
			val = n.spack.Version()

		case STATS_ROW_SCHEMA: // sum data packs
			val = uint64(n.spack.Len())

		case STATS_ROW_NVALS:
			// sum data pack rows
			var sum uint64
			for _, v := range b.Uint64().Slice() {
				sum += v
			}
			val = sum
		case STATS_ROW_SIZE:
			// sum data pack sizes
			var sum int64
			for _, v := range b.Int64().Slice() {
				sum += v
			}
			val = sum

		default:
			// calculate data column stats when changed
			if b != nil && b.IsDirty() {
				if (i-STATS_DATA_COL_OFFSET)%2 == 0 {
					// min fields -> min of min
					val = b.Min()
				} else {
					// max fields -> max of max
					val = b.Max()
				}
			} else {
				// copy current value when block is unavailable (deleted) or unchanged
				val = curr
			}
		}
		// set dirty flag when any of the statistics has actually changed
		dirty = dirty || !b.Type().EQ(curr, val)

		// write val to builder (even if not changed)
		wr.Write(i, val)
	}

	// reset view to release buffer reference
	view.Reset(nil)

	// on change, get new wire encoded data from builder
	if dirty {
		n.meta = wr.Bytes()
	}
	wr.Reset()

	return dirty
}
