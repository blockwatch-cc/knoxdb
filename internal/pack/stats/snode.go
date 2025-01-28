// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"bytes"
	"context"
	"sort"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/schema"
)

type SNode struct {
	spack    *pack.Package // statistics union package
	meta     []byte        // wire encoded min/max/sum statistics over pkg content
	disksize int           // statistics package on-disk data size
	dirty    bool          // dirty flag
}

func NewSNode(key uint32, s *schema.Schema, alloc bool) *SNode {
	node := &SNode{
		spack: pack.New().
			WithKey(key).
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

func (n SNode) Key() uint32 {
	return n.spack.Key()
}

func (n *SNode) Bytes() []byte {
	return n.meta
}

func (n SNode) IsEmpty() bool {
	return n.spack.Len() == 0
}

func (n SNode) IsWritable() bool {
	return n.spack.IsMaterialized()
}

func (n SNode) NPacks() int {
	return n.spack.Len()
}

func (n SNode) MinKey() uint32 {
	if n.spack.Len() > 0 {
		return n.spack.Uint32(STATS_ROW_KEY, 0)
	}
	return 0
}

func (n SNode) MaxKey() uint32 {
	if l := n.spack.Len(); l > 0 {
		return n.spack.Uint32(STATS_ROW_KEY, n.spack.Len()-1)
	} else {
		return 0
	}
}

func (n SNode) FindKey(key uint32) (int, bool) {
	// find pack offset (spack is sorted by data pack key)
	keys := n.spack.Block(STATS_ROW_KEY).Uint32().Slice()
	i := sort.Search(len(keys), func(i int) bool { return keys[i] >= key })

	// unlikely, should not happen
	if i == len(keys) || keys[i] != key {
		return -1, false
	}

	return i, true
}

func (n *SNode) AppendPack(pkg *pack.Package) bool {
	// append meta statistics
	n.spack.Block(STATS_ROW_KEY).Uint32().Append(pkg.Key())
	n.spack.Block(STATS_ROW_SCHEMA).Uint64().Append(pkg.Schema().Hash())
	n.spack.Block(STATS_ROW_NVALS).Int64().Append(int64(pkg.Len()))
	n.spack.Block(STATS_ROW_SIZE).Int64().Append(pkg.Analysis().SizeDiff())

	fields := pkg.Schema().Exported()
	for i, b := range pkg.Blocks() {
		var minv, maxv any
		if b == nil {
			// use zero values for invalid blocks (deleted from schema)
			minv = cmp.Zero(types.BlockTypes[fields[i].Type])
			maxv = minv
		} else {
			// calculate min/max statistics
			minv, maxv = b.MinMax()
		}

		// calculate data column positions inside statistics schema
		minx, maxx := minColIndex(i), maxColIndex(i)

		// append statistics values
		n.spack.Block(minx).Append(minv)
		n.spack.Block(maxx).Append(maxv)
	}
	n.spack.UpdateLen()
	n.dirty = true
	return n.dirty
}

func (n *SNode) UpdatePack(pkg *pack.Package) bool {
	k, ok := n.FindKey(pkg.Key())
	if !ok {
		// unlikely, should not happen
		return false
	}

	// update data statistics on change
	analyze := pkg.Analysis()
	for i, b := range pkg.Blocks() {
		// skip invalid blocks (deleted from schema) and non-dirty blocks
		if b == nil || !analyze.WasDirty[i] {
			continue
		}

		// calculate min/max statistics
		minv, maxv := b.MinMax()

		// calculate data column positions inside statistics schema
		minx, maxx := minColIndex(i), maxColIndex(i)

		// load current min/max values
		mino := n.spack.Block(minx).Get(k)
		maxo := n.spack.Block(maxx).Get(k)

		// set min/max when different
		if !cmp.EQ(b.Type(), mino, minv) {
			n.spack.Block(minx).Set(k, minv)
			n.dirty = true
		}
		if !cmp.EQ(b.Type(), maxo, maxv) {
			n.spack.Block(maxx).Set(k, maxv)
			n.dirty = true
		}
	}

	// update pack statistics on change
	if sid := n.spack.Uint64(STATS_ROW_SCHEMA, k); sid != pkg.Schema().Hash() {
		n.spack.Block(STATS_ROW_SCHEMA).Set(k, pkg.Schema().Hash())
		n.dirty = true
	}
	if svals := n.spack.Int64(STATS_ROW_NVALS, k); svals != int64(pkg.Len()) {
		n.spack.Block(STATS_ROW_NVALS).Set(k, int64(pkg.Len()))
		n.dirty = true
	}
	if diff, sz := n.spack.Int64(STATS_ROW_SIZE, k), analyze.SizeDiff(); diff != 0 {
		n.spack.Block(STATS_ROW_SIZE).Set(k, sz+diff)
		n.dirty = true
	}

	// data may not have changed
	return n.dirty
}

func (n *SNode) DeletePack(pkg *pack.Package) bool {
	// find pack offset (spack is sorted by data pack key)
	keys := n.spack.Block(STATS_ROW_KEY).Uint32().Slice()
	i := sort.Search(len(keys), func(i int) bool { return keys[i] >= pkg.Key() })

	// unlikely, should not happen
	if i == len(keys) || keys[i] != pkg.Key() {
		assert.Unreachable("delete non existing stats pack")
		return false
	}

	// remove statistics row
	err := n.spack.Delete(i, 1)
	if err != nil {
		assert.Unreachable("delete", err)
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

	// load missing blocks
	_, err := clone.spack.Load(ctx, b, false, 0, nil, 0)
	if err != nil {
		return nil, err
	}

	// materialize in-place
	clone.spack.Materialize()

	// use clone
	return clone, nil
}

func (n *SNode) Match(flt *query.FilterTreeNode, view *schema.View) bool {
	view.Reset(n.meta)
	defer view.Reset(nil)
	return matchView(flt, view)
}

func (n *SNode) Query(it *Iterator) error {
	// organize data required for this query
	var loadBlocks []uint16

	// we always need data pack keys and num values
	if n.spack.Block(STATS_ROW_KEY) == nil {
		// translate index to field id
		loadBlocks = append(loadBlocks, STATS_ROW_KEY+1, STATS_ROW_NVALS+1)
	}

	// translate filter indexes -> spack columns
	if it.flt != nil {
		uniqueFields := make(map[uint16]struct{})
		it.flt.ForEach(func(f *query.Filter) error {
			// skip already processed fields
			if _, ok := uniqueFields[f.Index]; ok {
				return nil
			}
			uniqueFields[f.Index] = struct{}{}

			// translate table column indices into min/max statistics columns
			minx, maxx := minColIndex(f.Index), maxColIndex(f.Index)

			// identify missing statistics blocks and load by id (not by index)
			if n.spack.Block(int(minx)) == nil {
				loadBlocks = append(loadBlocks, minx+1)
			}
			if n.spack.Block(int(maxx)) == nil {
				loadBlocks = append(loadBlocks, maxx+1)
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
			loadBlocks = append(loadBlocks, uint16(i)+1)
		}
	}

	// optimized fast path when all data is in memory
	if len(loadBlocks) == 0 && it.use == 0 {
		if it.flt != nil {
			_, it.vmatch = matchVector(it.flt, n.spack, nil, it.vmatch)
			it.match = it.vmatch.Indexes(it.match)
		}
		return nil
	}

	// run with backend query to load missing data
	return it.idx.db.View(func(tx store.Tx) error {
		// check blocks are loaded, load missing spack blocks during query
		if len(loadBlocks) > 0 {
			n, err := n.spack.Load(
				it.ctx,
				it.idx.statsBucket(tx),      // tablename_stats
				it.idx.use.Is(FeatUseCache), // use cache
				it.idx.schema.Hash(),        // unique cache key
				loadBlocks,                  // required/missing field indexes
				0,                           // read block/pack length from storage
			)
			if err != nil {
				return err
			}
			atomic.AddInt64(&it.idx.bytesRead, int64(n))
		}

		// init filter buckets if required
		buckets := make(map[int]store.Bucket)
		if it.use.Is(FeatBloomFilter) {
			buckets[STATS_BLOOM_KEY] = it.idx.bloomBucket(tx)
		}
		if it.use.Is(FeatFuseFilter) {
			buckets[STATS_FUSE_KEY] = it.idx.fuseBucket(tx)
		}
		if it.use.Is(FeatBitsFilter) {
			buckets[STATS_BITS_KEY] = it.idx.bitsBucket(tx)
		}

		// run vectorized queries for filter types, load & check bloom,
		// fuse and bitset filters on demand
		if it.flt != nil {
			var m int
			m, it.vmatch = matchVector(it.flt, n.spack, buckets, it.vmatch)
			if it.vmatch.Count() == 0 {
				return nil
			}
			atomic.AddInt64(&it.idx.bytesRead, int64(m))

			// convert bitset to indexes
			it.match = it.vmatch.Indexes(it.match)
		}

		return nil
	})
}

func (n *SNode) BuildMetaStats(view *schema.View, build *schema.Builder) bool {
	// allocate meta buffer when nil
	if n.meta == nil {
		n.meta = make([]byte, build.WireSize())
	}

	// use current statistics as baseline
	view.Reset(n.meta)
	build.Reset()

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

		case STATS_ROW_SCHEMA: // sum data packs
			val = uint64(n.spack.Len())

		case STATS_ROW_NVALS, STATS_ROW_SIZE:
			// sum data pack rows
			// sum data pack sizes
			var sum int64
			for _, v := range b.Int64().Slice() {
				sum += v
			}
			val = sum

		default:
			// calculate data column stats when changed
			if b != nil && b.IsDirty() {
				if i%2 == 0 {
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
		dirty = dirty || !cmp.EQ(b.Type(), curr, val)

		// write val to builder (even if not changed)
		build.Write(i, val)
	}

	// reset view to release buffer reference
	view.Reset(nil)

	// on change, get new wire encoded data from builder
	if dirty {
		n.meta = build.Bytes()
	}
	build.Reset()

	return dirty
}
