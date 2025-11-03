// Copyright (c) 2024-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
)

// This index supports the following condition types on lookup.
// - hash: EQ, IN, NI (single or composite EQ)
// - int:  EQ, IN, NI, LT, LE GT, GE, RG (single condition)
func (idx *Index) CanMatch(c engine.QueryCondition) bool {
	node, ok := c.(*filter.Node)
	if !ok {
		idx.log.Error("no filer node")
		return false
	}

	// simple conditions
	if node.IsLeaf() {
		return !idx.IsComposite() && idx.canMatchFilter(node.Filter)
	}

	// composite conditions (all index fields must be preset in the query
	// and have matching EQ conditions)
	if !idx.IsComposite() {
		return false
	}

	// check composite case first (all fields must have matching EQ conditions)
	// but order does not matter; compare all but last schema field (= pk)
	for _, field := range idx.sindex.Fields {
		var canMatchField bool
		for _, c := range node.Children {
			if !c.IsLeaf() {
				continue
			}
			if field.Name == c.Filter.Name && c.Filter.Mode == types.FilterModeEqual {
				canMatchField = true
				break
			}
		}
		if !canMatchField {
			return false
		}
	}
	return true
}

func (idx *Index) canMatchFilter(f *filter.Filter) bool {
	if !idx.sindex.Contains(f.Name) {
		return false
	}
	switch f.Mode {
	case types.FilterModeEqual:
		return true
	case types.FilterModeIn:
		return idx.sindex.Type == types.IndexTypeHash
	case types.FilterModeLt,
		types.FilterModeLe,
		types.FilterModeGt,
		types.FilterModeGe,
		types.FilterModeRange:
		return idx.sindex.Type == types.IndexTypeInt
	default:
		return false
	}
}

func (idx *Index) Query(ctx context.Context, c engine.QueryCondition) (*xroar.Bitmap, bool, error) {
	node, ok := c.(*filter.Node)
	if !ok {
		return nil, false, fmt.Errorf("invalid filter type %T", c)
	}

	if !node.IsLeaf() {
		return nil, false, fmt.Errorf("unexpected branch node")
	}

	// cross-check if we can match this
	if !idx.canMatchFilter(node.Filter) {
		return nil, false, nil
	}

	// choose the query algorithm (lookup or scan)
	var (
		bits *xroar.Bitmap
		err  error
	)
	switch idx.sindex.Type {
	case types.IndexTypeHash:
		// convert query values to hash values and lookup
		bits, err = idx.lookupKeys(ctx, idx.convert.QueryKeys(node))

	case types.IndexTypeInt:
		// execute the condition directly (like on table scans)
		bits, err = idx.queryKeys(ctx, idx.convert.QueryNode(node))
	}
	if err != nil {
		return nil, false, err
	}

	// collide depend on method
	canCollide := idx.sindex.Type == types.IndexTypeHash
	return bits, canCollide, err
}

func (idx *Index) QueryComposite(ctx context.Context, c engine.QueryCondition) (*xroar.Bitmap, bool, error) {
	node, ok := c.(*filter.Node)
	if !ok {
		return nil, false, fmt.Errorf("invalid condition type %T", c)
	}

	if node.IsLeaf() {
		return nil, false, fmt.Errorf("invalid leaf node")
	}

	if !idx.IsComposite() {
		return nil, false, nil
	}

	// convert equal query conditions to composite hash for lookup
	bits, err := idx.lookupKeys(ctx, idx.convert.QueryKeys(node))
	if err != nil {
		return nil, false, err
	}

	// composite mode uses hash and is therefore not collision free
	return bits, true, err
}

// Range scans for LE, LT, GE, GT, RG (int type only)
func (idx *Index) queryKeys(ctx context.Context, node *filter.Node) (*xroar.Bitmap, error) {
	var (
		bits = xroar.New()
		it   = NewScanIterator(idx, node, true)
	)

	// cleanup and log on exit
	defer func() {
		atomic.AddInt64(&idx.metrics.QueriedTuples, int64(bits.Count()))
		it.Close()
	}()

	for {
		// check context
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// load next pack with potential matches
		pkg, hits, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}

		// finish when no more packs or no more keys are available
		if pkg == nil {
			break
		}

		for _, i := range hits {
			// read pk from index row
			rid := pkg.Uint64(1, int(i))

			// skip broken records (invalid rid)
			if rid == 0 {
				continue
			}

			// add to result
			// idx.log.Infof("set key %d", rid)
			bits.Set(rid)
		}
	}

	return bits, nil
}

// lookup only matches EQ, IN, NI (list of search keys is known)
func (idx *Index) lookupKeys(ctx context.Context, keys []uint64) (*xroar.Bitmap, error) {
	// gracefully handle empty query list
	if len(keys) == 0 {
		return xroar.New(), nil
	}
	// idx.log.Infof("lookup keys %v", keys)
	var (
		next         int
		nKeysMatched uint32
		nKeys        = uint32(len(keys))
		maxKey       = keys[nKeys-1]
		it           = NewLookupIterator(idx, keys, true)
		bits         = xroar.New()
		in           = keys
	)

	// cleanup and log on exit
	defer func() {
		atomic.AddInt64(&idx.metrics.QueriedTuples, int64(nKeysMatched))
		it.Close()
	}()

	// stop when all inputs are matched
	for nKeysMatched < nKeys {
		// check context
		if err := ctx.Err(); err != nil {
			idx.log.Error(err)
			return nil, err
		}

		// load next pack with potential matches, use pack max index key to break early
		pkg, kmax, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}

		// finish when no more packs or no more keys are available
		if pkg == nil {
			// idx.log.Infof("no more packs")
			break
		}
		// idx.log.Infof("next pack len=%d up to max key=0x%016x", pkg.Len(), kmax)

		// access key columns (used for binary search below)
		k0 := pkg.Block(0).Uint64() // index pks
		k1 := pkg.Block(1).Uint64() // table pks
		packLen := k0.Len()

		// loop over the remaining (unresolved) keys, packs are sorted by pk
		pos := 0
		for _, key := range in[next:] {
			// idx.log.Infof("looking for key=0x%016x", key)

			// no more matches in this pack?
			if kmax < key || k0.Get(pos) > maxKey {
				// idx.log.Infof("no more matches in this pack")
				break
			}

			// find pk in pack
			n := sort.Search(packLen-pos, func(i int) bool { return k0.Get(pos+i) >= key })

			// skip when not found
			if pos+n >= packLen || k0.Get(pos+n) != key {
				// idx.log.Infof("lookup key not found")
				next++
				continue
			}
			// idx.log.Infof("at pos %d found=%016x", pos+n, k0.Get(pos+n))
			pos += n

			// on match, add row id to result
			nKeysMatched++
			// idx.log.Infof("add Result %d", k1.Get(pos))
			bits.Set(k1.Get(pos))

			// Note: index may not be unique as updates merge duplicates which
			// are not removed until GC; we must all matching row ids and let
			// the query engine later decide which row is visible under MVCC.
			// Multi-matches are in sort order.
			for pos+1 < packLen && k0.Get(pos+1) == key {
				pos++
				bits.Set(k1.Get(pos))
			}

			next++
		}

		// Alternatiev algo: compare performance
		//
		// start at the first `in` value contained by this index pack
		// minPk := idx.meta.MinMaxByKey(pkg.Key()) // or return from iterator
		// first := sort.Search(len(keys), func(x int) bool { return in[x] >= minPk })

		// // run through pack and in-slice until no more values match
		// for k, i, kl, il := 0, first, len(k0), len(in); k < kl && i < il; {

		//  // find the next matching key or any value > next lookup
		//  k += sort.Search(kl-k, func(x int) bool { return k0[x+k] >= in[i] })

		//  // stop at pack end
		//  if k == kl {
		//      // log.Debugf("%s: reached pack end", idx.name())
		//      break
		//  }

		//  // if no match was found, advance in-slice
		//  for i < il && k0[k] > in[i] {
		//      i++
		//  }

		//  // stop at in-slice end
		//  if i == il {
		//      break
		//  }

		//  // handle multiple matches
		//  if k0[k] == in[i] {
		//      // append to result
		//      bits.Set(pKeys[k])

		//      // Peek the next index entries to handle key collisions and
		//      // multi-matches for integer indexes. K can safely be advanced
		//      // because collisions/multi-matches for in[i] are directly after
		//      // the first match.
		//      for ; k+1 < kl && k0[k+1] == in[i]; k++ {
		//          bits.Set(pKeys[k+1])
		//      }

		//      // next lookup key
		//      i++
		//  }
		// }
	}

	return bits, nil
}

// PK -> RID lookup, keys are sorted, ridMap is allocated
// Note: the index is typically stale when updates/deletes are waiting
// in journal even if committed. Index is only updated during journal merge.
// This means all matches found here must be cross-checked against the journal
// by our query engine under MVCC.
func (idx *Index) Lookup(ctx context.Context, keys []uint64, ridMap map[uint64]uint64) error {
	// gracefully handle empty query list
	if len(keys) == 0 {
		return nil
	}
	// idx.log.Infof("lookup keys %v", keys)
	var (
		next         int
		nKeysMatched uint32
		nKeys        = uint32(len(keys))
		maxKey       = keys[nKeys-1]
		it           = NewLookupIterator(idx, keys, true)
		in           = keys
	)

	// cleanup and log on exit
	defer func() {
		atomic.AddInt64(&idx.metrics.QueriedTuples, int64(nKeysMatched))
		it.Close()
	}()

	// stop when all inputs are matched
	for nKeysMatched < nKeys {
		// check context
		if err := ctx.Err(); err != nil {
			return err
		}

		// load next pack with potential matches, use pack max index key to break early
		pkg, kmax, err := it.Next(ctx)
		if err != nil {
			return err
		}

		// finish when no more packs or no more keys are available
		if pkg == nil {
			break
		}

		// access key columns (used for binary search below)
		k0 := pkg.Block(0).Uint64() // index pks
		k1 := pkg.Block(1).Uint64() // table pks
		packLen := k0.Len()

		// loop over the remaining (unresolved) keys, packs are sorted by pk
		pos := 0
		for _, key := range in[next:] {
			// idx.log.Debugf("looking for ik=0x%016x", key)

			// no more matches in this pack?
			if kmax < key || k0.Get(pos) > maxKey {
				// idx.log.Debug("no more matches in this pack")
				break
			}

			// find key in remainder of pack
			n := sort.Search(packLen-pos, func(i int) bool { return k0.Get(pos+i) >= key })

			// skip when not found
			if pos+n >= packLen || k0.Get(pos+n) != key {
				// idx.log.Debug("lookup key not found")
				next++
				continue
			}
			// idx.log.Debugf("at pos %d found=%016x", pos+n, k0.Get(pos+n))
			pos += n

			// on match, add row id to result
			nKeysMatched++
			// idx.log.Debugf("add result %d => %d", key, k1.Get(pos))
			ridMap[key] = k1.Get(pos)

			// Multi-matches are in sort order.
			for pos+1 < packLen && k0.Get(pos+1) == key {
				pos++
				ridMap[key] = k1.Get(pos)
			}

			next++
		}
	}

	return nil
}
