// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"bytes"
	"context"
	"sort"
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
	spack    atomic.Pointer[pack.Package] // statistics union package
	meta     []byte                       // wire encoded min/max/sum statistics over pkg content
	disksize int                          // statistics package on-disk data size
	dirty    bool                         // dirty flag
}

func NewSNode(key uint32, s *schema.Schema, alloc bool) *SNode {
	pkg := pack.New().
		WithKey(key).
		WithVersion(1).
		WithMaxRows(STATS_PACK_SIZE).
		WithSchema(s)
	if alloc {
		pkg.Alloc()
	}
	node := &SNode{
		meta: make([]byte, s.WireSize()),
	}
	node.spack.Store(pkg)
	return node
}

func (n *SNode) Clear() {
	pkg := n.spack.Load()
	pkg.Release()
	n.meta = nil
	n.disksize = 0
	n.dirty = false
}

func (n *SNode) Key() uint32 {
	return n.spack.Load().Key()
}

func (n *SNode) Version() uint32 {
	return n.spack.Load().Version()
}

func (n *SNode) LoadVersion(view *schema.View) {
	v, ok := view.Reset(n.meta).GetPhy(STATS_ROW_VERSION)
	view.Reset(nil)
	if ok {
		n.spack.Load().WithVersion(v.(uint32))
	}
}

func (n *SNode) SetVersion(view *schema.View, ver uint32) {
	view.Reset(n.meta).Set(STATS_ROW_VERSION, ver)
	view.Reset(nil)
	n.spack.Load().WithVersion(ver)
}

func (n *SNode) Bytes() []byte {
	return n.meta
}

func (n *SNode) IsEmpty() bool {
	return n.spack.Load().Len() == 0
}

func (n *SNode) IsWritable() bool {
	return n.spack.Load().IsMaterialized()
}

func (n *SNode) NPacks() int {
	return n.spack.Load().Len()
}

func (n *SNode) MinKey() uint32 {
	pkg := n.spack.Load()
	if pkg.Len() > 0 {
		return pkg.Uint32(STATS_ROW_KEY, 0)
	}
	return 0
}

func (n *SNode) MaxKey() uint32 {
	var k uint32
	pkg := n.spack.Load()
	if l := pkg.Len(); l > 0 {
		k = pkg.Uint32(STATS_ROW_KEY, l-1)
	}
	// fmt.Printf("Snode %d[v%d] %p max key at pos %d is %d\n",
	// 	n.Key(), n.Version(), n, pkg.Len(), k)

	// operator.NewLogger(os.Stdout, 30).Process(context.Background(), pkg)

	return k
}

func (n *SNode) LastNValues() int {
	pkg := n.spack.Load()
	if l := pkg.Len(); l > 0 {
		return int(pkg.Uint64(STATS_ROW_NVALS, l-1))
	}
	return 0
}

func (n *SNode) LastInfo() (uint32, uint32, int) {
	pkg := n.spack.Load()
	l := pkg.Len()
	if l == 0 {
		return 0, 0, 0
	}
	k := pkg.Uint32(STATS_ROW_KEY, l-1)
	v := pkg.Uint32(STATS_ROW_VERSION, l-1)
	s := pkg.Uint64(STATS_ROW_NVALS, l-1)
	return k, v, int(s)
}

func (n *SNode) FindKey(key uint32) (int, bool) {
	// find pack offset (spack is sorted by data pack key)
	pkg := n.spack.Load()
	keys := pkg.Block(STATS_ROW_KEY).Uint32()
	i := sort.Search(pkg.Len(), func(i int) bool { return keys.Get(i) >= key })

	// unlikely, should not happen
	if i == keys.Len() || keys.Get(i) != key {
		return -1, false
	}

	return i, true
}

func (n *SNode) AppendPack(src *pack.Package) bool {
	// append meta statistics
	pkg := n.spack.Load()
	pkg.Block(STATS_ROW_KEY).Uint32().Append(src.Key())
	pkg.Block(STATS_ROW_VERSION).Uint32().Append(src.Version())
	pkg.Block(STATS_ROW_SCHEMA).Uint64().Append(src.Schema().Hash)
	pkg.Block(STATS_ROW_NVALS).Uint64().Append(uint64(src.Len()))
	pkg.Block(STATS_ROW_SIZE).Int64().Append(src.Stats().SizeDiff())

	// fmt.Printf("append snode %d from pack 0x%08x[v%d]\n",
	// 	n.Key(), src.Key(), src.Version())

	pstats := src.Stats()
	for i, b := range src.Blocks() {
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
		pkg.Block(minx).Append(minv)
		pkg.Block(maxx).Append(maxv)
	}
	pkg.UpdateLen()
	n.dirty = true

	// fmt.Printf("snode: %x[v%d] append from %x[v%d] len=%d\n",
	// 	n.Key(), n.Version(), pkg.Key(), pkg.Version(), pkg.Len())

	// operator.NewLogger(os.Stdout, 30).Process(context.Background(), pkg)

	return n.dirty
}

func (n *SNode) UpdatePack(src *pack.Package) bool {
	k, ok := n.FindKey(src.Key())
	if !ok {
		// unlikely, should not happen
		assert.Unreachable("update unknown spack")
		return false
	}
	pkg := n.spack.Load()

	// fmt.Printf("update snode %d[v%d] from pack 0x%08x[v%d] at pos=%d\n",
	// 	n.Key(), n.Version(), src.Key(), src.Version(), k)

	// update data statistics on change
	pstats := src.Stats()
	for i, b := range src.Blocks() {
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
		mino := pkg.Block(minx).Get(k)
		maxo := pkg.Block(maxx).Get(k)

		// set min/max when different
		if !b.Type().EQ(mino, minv) {
			// fmt.Printf("> F#%d min[%d] %v -> %v\n", i, minx, mino, minv)
			pkg.Block(minx).Set(k, minv)
			n.dirty = true
		}
		if !b.Type().EQ(maxo, maxv) {
			// fmt.Printf("> F#%d max[%d] %v -> %v\n", i, maxx, maxo, maxv)
			pkg.Block(maxx).Set(k, maxv)
			n.dirty = true
		}
	}

	// update pack statistics on change
	if vid := pkg.Uint32(STATS_ROW_VERSION, k); vid != src.Version() {
		pkg.Block(STATS_ROW_VERSION).Set(k, src.Version())
		n.dirty = true
	}
	if sid := pkg.Uint64(STATS_ROW_SCHEMA, k); sid != src.Schema().Hash {
		pkg.Block(STATS_ROW_SCHEMA).Set(k, src.Schema().Hash)
		n.dirty = true
	}
	if nvals := pkg.Uint64(STATS_ROW_NVALS, k); nvals != uint64(src.Len()) {
		pkg.Block(STATS_ROW_NVALS).Set(k, uint64(src.Len()))
		n.dirty = true
	}
	if diff, sz := pkg.Int64(STATS_ROW_SIZE, k), pstats.SizeDiff(); diff != 0 {
		pkg.Block(STATS_ROW_SIZE).Set(k, sz+diff)
		n.dirty = true
	}

	// operator.NewLogger(os.Stdout, 30).Process(context.Background(), pkg)

	// data may not have changed
	return n.dirty
}

func (n *SNode) DeletePack(src *pack.Package) bool {
	// find pack offset (spack is sorted by data pack key)
	pkg := n.spack.Load()
	keys := pkg.Block(STATS_ROW_KEY).Uint32().Slice()
	i := sort.Search(len(keys), func(i int) bool { return keys[i] >= src.Key() })

	// unlikely, should not happen
	if i == len(keys) || keys[i] != src.Key() {
		assert.Unreachable("delete unknown spack")
		return false
	}

	// remove statistics row
	err := pkg.Delete(i, i+1)
	if err != nil {
		assert.Unreachable("delete unknown spack", err)
	}
	n.dirty = true

	// data has changed (at least num values and num packs)
	return n.dirty
}

func (n *SNode) PrepareWrite(ctx context.Context, b store.Bucket) (*SNode, error) {
	// create clone to prevent overriding spack used by concurrent readers
	src := n.spack.Load()
	pkg := src.Clone(src.Cap())

	// load missing blocks (previous version)
	_, err := pkg.LoadFromDisk(ctx, b, nil, 0)
	if err != nil {
		return nil, err
	}

	// materialize all blocks in-place
	pkg.Materialize()

	// use clone
	clone := &SNode{
		meta:     bytes.Clone(n.meta),
		disksize: n.disksize,
		dirty:    true,
	}
	clone.spack.Store(pkg)

	return clone, nil
}

func (n *SNode) Match(flt *filter.Node, view *schema.View) bool {
	view.Reset(n.meta)
	defer view.Reset(nil)
	return Match(flt, &ViewReader{view})
}

func (n *SNode) Query(it *Iterator) error {
	var (
		loadBlocks []uint16
		pkg        = n.spack.Load()
	)

	if !pkg.IsComplete() {
		// identify which fields to load: it.ids contains field ids
		// which we translate back to block positions (the stats index
		// uses consecutive ids even for metadata fields)
		if it.flt != nil {
			for _, id := range it.ids {
				if pkg.Block(int(id-1)) == nil {
					loadBlocks = append(loadBlocks, id)
				}
			}
		} else {
			// load all missing fields
			for i, b := range pkg.Blocks() {
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
			it.vmatch.Resize(pkg.Len()).Zero()

			// match minmax ranges
			_, it.vmatch = matchVector(it.flt, pkg, nil, it.vmatch)

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

			// copy spack to prevent concurrent access
			pkg = pkg.Copy()

			// use cache with a private partition key
			if it.idx.use.Is(FeatUseCache) {
				cache = engine.GetEngine(it.ctx).BlockCache(it.idx.schema.Hash)
				loadedBlocks = pkg.LoadFromCache(cache, loadBlocks)
			}

			// load remaining blocks from disk
			if len(loadBlocks) != loadedBlocks {
				nBytes, err := pkg.LoadFromDisk(
					it.ctx,
					it.idx.statsBucket(tx), // tablename_stats
					loadBlocks,             // required/missing field indexes
					0,                      // read block/pack length from storage
				)
				if err != nil {
					return err
				}
				atomic.AddInt64(&it.idx.bytesRead, int64(nBytes))

				// add blocks to cache
				if it.idx.use.Is(FeatUseCache) {
					pkg.AddToCache(cache)
				}
			}

			// install as new spack
			n.spack.Store(pkg)
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
			it.vmatch.Resize(pkg.Len()).Zero()

			// match minmax ranges and optional filters
			m, it.vmatch = matchVector(it.flt, pkg, buckets, it.vmatch)

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
	var (
		dirty bool
		pkg   = n.spack.Load()
	)
	for i, b := range pkg.Blocks() {
		// load current value
		curr, _ := view.GetPhy(i)

		// find new value
		var val any
		switch i {
		case STATS_ROW_KEY: // min key
			val = b.Uint32().Get(0)

		case STATS_ROW_VERSION: // use spack version
			val = pkg.Version()

		case STATS_ROW_SCHEMA: // sum data packs
			val = uint64(pkg.Len())

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
