// Copyright (c) 2024-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/bitmap"
)

// This index supports the following condition types on lookup.
// - hash: EQ, IN, NI (single or composite EQ)
// - int:  EQ, IN, NI, LT, LE GT, GE, RG (single condition)
func (idx *Index) CanMatch(c engine.QueryCondition) bool {
	node, ok := c.(*query.FilterTreeNode)
	if !ok {
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
	nfields := idx.schema.NumFields()
	for _, field := range idx.schema.Exported()[:nfields-1] {
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

func (idx *Index) canMatchFilter(f *query.Filter) bool {
	if !idx.schema.CanMatchFields(f.Name) {
		return false
	}
	switch f.Mode {
	case types.FilterModeEqual:
		return true
	case types.FilterModeIn:
		return idx.opts.Type == types.IndexTypeHash
	case types.FilterModeLt,
		types.FilterModeLe,
		types.FilterModeGt,
		types.FilterModeGe,
		types.FilterModeRange:
		return idx.opts.Type == types.IndexTypeInt
	default:
		return false
	}
}

func (idx *Index) Query(ctx context.Context, c engine.QueryCondition) (*bitmap.Bitmap, bool, error) {
	node, ok := c.(*query.FilterTreeNode)
	if !ok {
		return nil, false, fmt.Errorf("invalid condition type %T", c)
	}

	if !node.IsLeaf() {
		return nil, false, fmt.Errorf("invalid branch node")
	}

	// cross-check if we can match this
	if !idx.canMatchFilter(node.Filter) {
		return nil, false, nil
	}

	// choose the query algorithm (lookup or scan)
	var (
		bits *bitmap.Bitmap
		err  error
	)
	switch idx.opts.Type {
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
	canCollide := idx.opts.Type == types.IndexTypeHash
	return bits, canCollide, err
}

func (idx *Index) QueryComposite(ctx context.Context, c engine.QueryCondition) (*bitmap.Bitmap, bool, error) {
	node, ok := c.(*query.FilterTreeNode)
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
func (idx *Index) queryKeys(ctx context.Context, node *query.FilterTreeNode) (*bitmap.Bitmap, error) {
	var (
		bits = bitmap.New()
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
			pk := pkg.Uint64(1, int(i))

			// skip broken records (invalid pk)
			if pk == 0 {
				continue
			}

			// add to result
			// idx.log.Infof("Set key %d", pk)
			bits.Set(pk)
		}
	}

	return &bits, nil
}

// lookup only matches EQ, IN, NI (list of search keys is known)
func (idx *Index) lookupKeys(ctx context.Context, keys []uint64) (*bitmap.Bitmap, error) {
	var (
		next         int
		nKeysMatched uint32
		nKeys        = uint32(len(keys))
		maxKey       = keys[nKeys-1]
		it           = NewLookupIterator(idx, keys, true)
		bits         = bitmap.New()
		in           = keys
	)

	// cleanup and log on exit
	defer func() {
		atomic.AddInt64(&idx.metrics.QueriedTuples, int64(nKeysMatched))
		it.Close()
	}()

	for {
		// stop when all inputs are matched
		if nKeys == nKeysMatched {
			break
		}

		// check context
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// load next pack with potential matches, use pack max index key to break early
		pkg, maxIk, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}

		// finish when no more packs or no more keys are available
		if pkg == nil {
			// idx.log.Infof("No more packs")
			break
		}
		// idx.log.Infof("Next pack len=%d up to max ik=%d", pkg.Len(), maxIk)

		// access key columns (used for binary search below)
		iKeys := pkg.Block(0).Uint64().Slice() // index pks
		pKeys := pkg.Block(1).Uint64().Slice() // table pks
		packLen := len(iKeys)

		// loop over the remaining (unresolved) keys, packs are sorted by pk
		pos := 0
		for _, ik := range in[next:] {
			// idx.log.Infof("Looking for ik=0x%016x", ik)

			// no more matches in this pack?
			if maxIk < ik || iKeys[pos] > maxKey {
				// idx.log.Infof("No more matches in this pack")
				break
			}

			// find pk in pack
			n := sort.Search(packLen-pos, func(i int) bool { return iKeys[pos+i] >= ik })

			// skip when not found
			if pos+n >= packLen || iKeys[pos+n] != ik {
				// idx.log.Infof("Lookup key not found")
				next++
				continue
			}
			// idx.log.Infof("At pos %d found=%016x", pos+n, iKeys[pos+n])
			pos += n

			// on match, add table primary key to result
			nKeysMatched++
			// idx.log.Infof("Add Result %d", pKeys[pos])
			bits.Set(pKeys[pos])
			next++
		}

		// Alternatiev algo: compare performance
		//
		// start at the first `in` value contained by this index pack
		// minPk := idx.meta.MinMaxByKey(pkg.Key()) // or return from iterator
		// first := sort.Search(len(keys), func(x int) bool { return in[x] >= minPk })

		// // run through pack and in-slice until no more values match
		// for k, i, kl, il := 0, first, len(iKeys), len(in); k < kl && i < il; {

		//  // find the next matching key or any value > next lookup
		//  k += sort.Search(kl-k, func(x int) bool { return iKeys[x+k] >= in[i] })

		//  // stop at pack end
		//  if k == kl {
		//      // log.Debugf("%s: reached pack end", idx.name())
		//      break
		//  }

		//  // if no match was found, advance in-slice
		//  for i < il && iKeys[k] > in[i] {
		//      i++
		//  }

		//  // stop at in-slice end
		//  if i == il {
		//      break
		//  }

		//  // handle multiple matches
		//  if iKeys[k] == in[i] {
		//      // append to result
		//      bits.Set(pKeys[k])

		//      // Peek the next index entries to handle key collisions and
		//      // multi-matches for integer indexes. K can safely be advanced
		//      // because collisions/multi-matches for in[i] are directly after
		//      // the first match.
		//      for ; k+1 < kl && iKeys[k+1] == in[i]; k++ {
		//          bits.Set(pKeys[k+1])
		//      }

		//      // next lookup key
		//      i++
		//  }
		// }
	}

	return &bits, nil
}
