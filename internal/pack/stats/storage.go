// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/store"
	"blockwatch.cc/knoxdb/pkg/util"
)

const STATS_BUCKETS = 6

const (
	STATS_BLOCK_KEY = iota
	STATS_TREE_KEY
	STATS_FILTER_KEY
	STATS_RANGE_KEY
	STATS_EPOCH_KEY
	STATS_TOMB_KEY
)

const (
	KIND_INVALID byte = iota
	KIND_INODE
	KIND_SNODE
)

var (
	BlockKeySuffix  = []byte("_stats_block") // stats vector bucket
	TreeKeySuffix   = []byte("_stats_tree")  // stats tree bucket
	FilterKeySuffix = []byte("_filter")      // bloom/bits/fuse filter bucket
	RangeKeySuffix  = []byte("_range")       // range filter bucket
	EpochKeySuffix  = engine.EpochKeySuffix  // live epochs bucket
	TombKeySuffix   = engine.TombKeySuffix   // version tomb bucket
)

func encodeNodeKey(kind byte, id, key, ver uint32) []byte {
	var b [1 + 3*num.MaxVarintLen32]byte
	b[0] = kind
	buf := num.AppendUvarint(b[:1], uint64(id))
	buf = num.AppendUvarint(buf, uint64(key))
	buf = num.AppendUvarint(buf, uint64(ver))
	return buf
}

func decodeNodeKey(buf []byte) (kind byte, id, key, ver uint32) {
	kind = buf[0]
	buf = buf[1:]
	var vals [3]uint32
	for i := range 3 {
		v, n := num.Uvarint(buf)
		if n == 0 {
			break
		}
		vals[i] = uint32(v)
		buf = buf[n:]
	}
	id = vals[0]
	key = vals[1]
	ver = vals[2]
	return
}

// Store identifies updated inodes and snodes and stores tombstones for the previous
// version of each node and snode spack, then writes new node versions and spacks
// to disk. Store works within a single storage layer transaction hence updates
// to tomb, tree and block buckets are atomic.
//
// In rare cases, ie. when an entire snode becomes empty as result of a large delete
// or update operation, the tree is reorganized and all nodes are rewritten. Even with
// a table size of 1Bn records and pack size of 8192 the tree contains only 60 snodes
// (each covering 2048 data packs) and 6 levels of inodes. Rewrite impact remains small.
//
// TODO: after 4,294,967,295 node rewrites the uint32 version counter wraps around.
// This causes problems with i/snodes loading. We walk the node bucket backwards
// with the assumption to see the most recent version of each node first and will skip
// any duplicate. Our only relief when the first node hits this case is a full tree
// rewrite. Just the inner inode tree and all snodes is sufficient, the snode spack's
// block versions can wrap around just as the data block versions.
func (idx *Index) Store(ctx context.Context, tx store.Tx) error {
	// resolve buckets
	tree := idx.treeBucket(tx)
	blocks := idx.statsBucket(tx)

	// create buckets if not exist
	if tree == nil || blocks == nil {
		for _, k := range idx.keys {
			if _, err := tx.Root().CreateBucket(k); err != nil {
				return err
			}
		}
		tree = idx.treeBucket(tx)
		blocks = idx.statsBucket(tx)
	}

	// identify empty snodes for garbage collection
	var (
		k         int
		haveEmpty bool
		tomb      = idx.tomb.NewWriter(tx)
	)
	defer tomb.Close()
	for i, n := range idx.snodes {
		if !n.IsEmpty() {
			idx.snodes[k] = n
			k++
			continue
		}

		// we need pack key and version
		key, ver := n.Key(), n.Version()

		// mark empty snodes for gc
		if err := tomb.AddNode(tx, encodeNodeKey(KIND_SNODE, uint32(i), key, ver)); err != nil {
			return err
		}

		// mark empty snode packs for gc
		if err := tomb.AddSPack(tx, key, ver); err != nil {
			return err
		}

		// clear node
		n.Clear()

		// set nil
		idx.snodes[i] = nil
		haveEmpty = true
	}
	idx.snodes = idx.snodes[:k]

	// rebuild the inode tree (its less complex to rebuild all inodes than to rearrange them)
	if haveEmpty {
		// mark all inodes for gc, identify a non-colliding node version
		var vmin, vmax uint32 = 1<<32 - 1, 0
		for i, n := range idx.inodes {
			if n == nil {
				continue
			}
			ver := n.Version(idx.view)
			vmin = min(vmin, ver)
			vmax = max(vmax, ver)
			key := encodeNodeKey(KIND_INODE, uint32(i), 0, ver)
			if err := tomb.AddNode(tx, key); err != nil {
				return err
			}
		}

		// rebuild inode tree, use node version outside min-max range to prevent collision
		// with existing stored versions. Only reset to 1 when there are no prior epochs
		// waiting for GC and v1 is not in use by any inode.
		if vmin > 1 && idx.numEpochs(tx) <= 1 {
			idx.rebuildInodeTree(1)
		} else {
			idx.rebuildInodeTree(vmax + 1)
		}

	} else if idx.epoch > 1 {
		// mark dirty inodes for gc (skip on initial store, i.e. idx.epoch == 1)
		for i, n := range idx.inodes {
			if n == nil || !n.dirty {
				continue
			}
			key := encodeNodeKey(KIND_INODE, uint32(i), 0, n.Version(idx.view))
			if err := tomb.AddNode(tx, key); err != nil {
				return err
			}
		}
	}

	// activate current index epoch
	if err := idx.addEpoch(tx); err != nil {
		return err
	}

	// idx.log.Debugf("store %d snodes, %d inodes", len(idx.snodes), len(idx.inodes))

	// store dirty inodes
	for i, inode := range idx.inodes {
		if inode == nil || !inode.dirty {
			continue
		}

		// update inode version
		ver := inode.Version(idx.view)

		// patch new version
		inode.SetVersion(idx.view, ver+1)

		// key is tree node kind + id (u32) + 0 + version
		key := encodeNodeKey(KIND_INODE, uint32(i), 0, inode.Version(idx.view))
		// idx.log.Tracef("store inode %d [%x]", i, key)
		err := tree.Put(key, inode.meta)
		if err != nil {
			return err
		}
		inode.dirty = false
		idx.bytesWritten += int64(len(inode.meta))
		idx.clean = false
	}

	// store dirty snodes
	for i, snode := range idx.snodes {
		if !snode.dirty {
			continue
		}
		pkg := snode.spack.Load()
		skey := pkg.Key()
		sver := pkg.Version()

		// mark previous snodes for gc
		key := encodeNodeKey(KIND_SNODE, uint32(i), skey, sver)
		if err := tomb.AddNode(tx, key); err != nil {
			return err
		}

		// mark previous snode packs for gc
		if err := tomb.AddSPack(tx, skey, sver); err != nil {
			return err
		}

		// update version
		snode.SetVersion(idx.view, sver+1)

		// key is tree node kind + id (u32) + spack key (u32) + version
		key = encodeNodeKey(KIND_SNODE, uint32(i), skey, sver+1)
		// idx.log.Tracef("store snode %d [%x]", i, key)
		err := tree.Put(key, snode.meta)
		if err != nil {
			return err
		}
		idx.bytesWritten += int64(len(snode.meta))

		// package blocks
		snode.disksize, err = pkg.StoreToDisk(ctx, blocks)
		if err != nil {
			return err
		}
		snode.dirty = false
		idx.bytesWritten += int64(snode.disksize)
		idx.clean = false

		// operator.NewLogger(os.Stdout, 30).Process(context.Background(), pkg)
	}

	return nil
}

func (idx *Index) Load(ctx context.Context, tx store.Tx) error {
	// load tree
	tree := idx.treeBucket(tx)
	if tree == nil {
		return store.ErrBucketNotFound
	}
	blocks := idx.statsBucket(tx)
	if blocks == nil {
		return store.ErrBucketNotFound
	}

	// check if we need to GC after crash
	idx.clean = !idx.NeedCleanup(tx)

	// walk reverse finds snode entries first
	c := tree.Cursor(store.ReverseCursor)
	defer c.Close()
	var (
		lastKind        byte
		lastId, lastKey uint32 = 1<<32 - 1, 1<<32 - 1
	)
	for ok := c.Last(); ok; ok = c.Prev() {
		// read tree node type, id and snode pack key, ignore version
		kind, id, key, _ := decodeNodeKey(c.Key())

		// FIXME: on version wrap-around (versions are per-node update, not global
		// unless a tree rewrite happens regularly a wrap around remains unlikely)
		// the latest version is < the largest and the reverse cursor finds the older
		// version first

		// skip earlier versions of the same node
		if id == lastId && key == lastKey && lastKind == kind {
			continue
		}
		lastId = id
		lastKey = key
		lastKind = kind

		// init node index calculations
		ilen := len(idx.inodes)

		// init tree sizes from highest snode key id on storage
		if ilen == 0 {
			ilen = 1 << util.Log2(int(id*2)+2) // num inodes is the full inode tree plus 1 extra
			slen := int(id) + 1                // num snodes is exact count
			// idx.log.Debugf("load %d snodes, %d inodes", slen, ilen)
			idx.inodes = slices.Grow(idx.inodes, ilen)
			idx.inodes = idx.inodes[:ilen]
			idx.snodes = slices.Grow(idx.snodes, slen)
			idx.snodes = idx.snodes[:slen]
		}

		// identify node kind from id and create node
		switch kind {
		case KIND_SNODE:
			// snode
			// idx.log.Debugf("load snode %d [%x]", id, c.Key())
			node := NewSNode(key, idx.schema, false)
			node.meta = bytes.Clone(c.Value())
			node.LoadVersion(idx.view)
			idx.snodes[id] = node
			idx.bytesRead += int64(len(c.Value()))

			// load key and nvals columns
			pkg := node.spack.Load()
			n, err := pkg.LoadFromDisk(
				ctx,
				blocks, // bucket
				[]uint16{ // field ids!! (id = index + 1)
					STATS_ROW_KEY + 1,
					STATS_ROW_VERSION + 1,
					STATS_ROW_NVALS + 1,
					uint16(minColIndex(idx.rx) + 1), // min rid
					uint16(maxColIndex(idx.rx) + 1), // max rid
				},
				0, // len from store
			)
			if err != nil {
				return err
			}
			idx.bytesRead += int64(n)

			// idx.log.Debugf("loaded snode %d %d[v%d]", id, node.Key(), node.Version())
			// operator.NewLogger(os.Stdout, 30).Process(context.Background(), pkg)

		case KIND_INODE:
			// inode
			// idx.log.Tracef("load inode %d [%x]", id, c.Key())
			idx.inodes[id] = NewINode()
			idx.inodes[id].meta = bytes.Clone(c.Value())
			idx.bytesRead += int64(len(c.Value()))
		default:
			return fmt.Errorf("invalid tree node kind")
		}
	}

	return nil
}

func (idx *Index) Drop(ctx context.Context, tx store.Tx) error {
	idx.Clear()
	for _, k := range idx.keys {
		_ = tx.Root().DeleteBucket(k)
	}
	return nil
}

func (idx *Index) prepareWrite(ctx context.Context, node *SNode, i int) (*SNode, error) {
	if node.IsWritable() {
		return node, nil
	}
	err := idx.db.View(func(tx store.Tx) error {
		clone, err := node.PrepareWrite(ctx, idx.statsBucket(tx))
		if err != nil {
			return err
		}
		// replace existing node with clone, don't clear shared original
		idx.snodes[i] = clone
		node = clone
		return nil
	})
	return node, err
}

// stats block vectors
func (idx *Index) statsBucket(tx store.Tx) store.Bucket {
	return idx.bucket(tx, STATS_BLOCK_KEY)
}

// meta stats
func (idx *Index) treeBucket(tx store.Tx) store.Bucket {
	return idx.bucket(tx, STATS_TREE_KEY)
}

// bloom filters, key = pack key << 32 || field index
func (idx *Index) filterBucket(tx store.Tx) store.Bucket {
	return idx.bucket(tx, STATS_FILTER_KEY)
}

// range filters, key = pack key << 32 || field index
func (idx *Index) rangeBucket(tx store.Tx) store.Bucket {
	return idx.bucket(tx, STATS_RANGE_KEY)
}

func (idx *Index) epochBucket(tx store.Tx) store.Bucket {
	return idx.bucket(tx, STATS_EPOCH_KEY)
}

func (idx *Index) tombBucket(tx store.Tx) store.Bucket {
	return idx.bucket(tx, STATS_TOMB_KEY)
}

func (idx *Index) tableBucket(tx store.Tx) store.Bucket {
	return tx.Bucket(append([]byte(idx.schema.Name), engine.DataKeySuffix...))
}

func (idx *Index) bucket(tx store.Tx, id int) store.Bucket {
	b := tx.Bucket(idx.keys[id])
	if b != nil {
		b.FillPercent(1.0)
	}
	return b
}

func makeStorageKeys(name []byte) [STATS_BUCKETS][]byte {
	makekey := func(k []byte) []byte {
		return bytes.Join([][]byte{name, k}, nil)
	}
	return [STATS_BUCKETS][]byte{
		makekey(BlockKeySuffix),
		makekey(TreeKeySuffix),
		makekey(FilterKeySuffix),
		makekey(RangeKeySuffix),
		makekey(EpochKeySuffix),
		makekey(TombKeySuffix),
	}
}
