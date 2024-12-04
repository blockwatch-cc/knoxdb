// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/hash/fnv"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
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
	if node.IsLeaf() {
		// simple conditions
		return idx.canMatchFilter(node.Filter)
	} else {
		// composite conditions (all index fields must be preset in the query
		// and have matching EQ conditions)
		if !idx.IsComposite() {
			return false
		}

		// check composite case first (all fields must have matching EQ conditions)
		// but order does not matter; compare all but last schema field (= pk)
		nfields := idx.convert.Schema().NumFields()
		for _, field := range idx.convert.Schema().Fields()[:nfields-1] {
			var canMatchField bool
			for _, c := range node.Children {
				if !c.IsLeaf() {
					continue
				}
				if field.Name() == c.Filter.Name && c.Filter.Mode == types.FilterModeEqual {
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
}

func (idx *Index) canMatchFilter(f *query.Filter) bool {
	if !idx.convert.Schema().CanMatchFields(f.Name) {
		return false
	}
	switch f.Mode {
	case types.FilterModeEqual,
		types.FilterModeIn,
		types.FilterModeNotIn:
		return true
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
		// convert query values to hash values
		keys := idx.hashFilterValue(node.Filter)

		// lookup hash values
		bits, err = idx.lookupKeys(ctx, keys, node.Filter.Mode)

	case types.IndexTypeInt:

		// execute the condition directly (like on table scans)
		bits, err = idx.queryKeys(ctx, node)
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

	// identify eligible conditions for constructing multi-field lookups
	eq := make(map[string]*query.FilterTreeNode) // all equal child conditions
	for _, child := range node.Children {
		if child.Filter.Mode == types.FilterModeEqual {
			eq[child.Filter.Name] = child
		}
	}

	// try combine multiple AND leaf conditions into longer an index key,
	// all index fields must be abailable
	buf := new(bytes.Buffer)
	nfields := idx.convert.Schema().NumFields()
	for _, field := range idx.convert.Schema().Fields()[:nfields-1] {
		name := field.Name()
		node, ok := eq[name]
		if !ok {
			// empty result if we cannot build a hash from all index fields
			return nil, false, nil
		}
		err := field.Encode(buf, node.Filter.Value)
		if err != nil {
			return nil, false, err
		}
		// set skip flags signalling this condition has been processed
		node.Skip = true
		delete(eq, name)
	}

	// create single hash key from composite EQ conditions
	keys := []uint64{fnv.Sum64a(buf.Bytes())}

	// lokup matching pks
	bits, err := idx.lookupKeys(ctx, keys, types.FilterModeEqual)
	if err != nil {
		return nil, false, err
	}

	// composite mode uses hash and is therefore not collision free
	return bits, true, err
}

func (idx *Index) hashFilterValue(f *query.Filter) []uint64 {
	// produce output hash (uint64) from field data encoded to wire format
	// use schema field encoding helper to translate Go types from query
	field := idx.convert.Schema().Fields()[0]
	buf := bytes.NewBuffer(nil)

	switch f.Mode {
	case types.FilterModeIn, types.FilterModeNotIn:
		// slice
		rval := reflect.ValueOf(f.Value)
		if rval.Kind() != reflect.Slice {
			return nil
		}
		res := make([]uint64, rval.Len())
		for i := range res {
			buf.Reset()
			_ = field.Encode(buf, rval.Index(i).Interface())
			res[i] = fnv.Sum64a(buf.Bytes())
		}
		return res
	case types.FilterModeEqual:
		// single
		_ = field.Encode(buf, f.Value)
		return []uint64{fnv.Sum64a(buf.Bytes())}
	default:
		// unreachable
		assert.Unreachable("invalid filter mode for pack hash query", "mode", f.Mode)
		return nil
	}
}

// Range scans for LE, LT, GE, GT, RG (int type only)
func (idx *Index) queryKeys(ctx context.Context, node *query.FilterTreeNode) (*bitmap.Bitmap, error) {
	var (
		bits = bitmap.New()
		it   = NewIndexScanIterator(idx, node, true)
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

		for _, idx := range hits {
			// read pk from index row
			pk := pkg.Uint64(1, int(idx))

			// skip broken records (invalid pk)
			if pk == 0 {
				continue
			}

			// add to result
			bits.Set(pk)
		}
	}

	return &bits, nil
}

// lookup only matches EQ, IN, NI (list of search keys is known)
func (idx *Index) lookupKeys(ctx context.Context, keys []uint64, mode types.FilterMode) (*bitmap.Bitmap, error) {
	var (
		next         int
		nKeysMatched uint32
		nKeys        = uint32(len(keys))
		maxKey       = keys[nKeys-1]
		it           = NewIndexLookupIterator(idx, keys, true)
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

		// load next pack with potential matches, use pack max pk to break early
		pkg, maxPk, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}

		// finish when no more packs or no more keys are available
		if pkg == nil {
			break
		}

		// access key columns (used for binary search below)
		iKeys := pkg.Block(0).Uint64().Slice() // index pks
		pKeys := pkg.Block(1).Uint64().Slice() // table pks
		packLen := len(iKeys)

		// loop over the remaining (unresolved) keys, packs are sorted by pk
		pos := 0
		for _, pk := range in[next:] {
			// no more matches in this pack?
			if maxPk < pk || iKeys[pos] > maxKey {
				break
			}

			// find pk in pack
			idx := sort.Search(packLen-pos, func(i int) bool { return iKeys[pos+i] >= pk })
			pos += idx
			if pos >= packLen || iKeys[pos] != pk {
				next++
				continue
			}

			// on match, add table primary key to result
			nKeysMatched++
			bits.Set(pKeys[pos]) // pkg.Uint64(1, pos)
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

	// post process matches in case a negative query mode was selected
	if mode == types.FilterModeNotIn {
		// return only missing keys (not found)
		miss := bitmap.New()
		for _, v := range keys {
			if bits.Contains(v) {
				continue
			}
			miss.Set(v)
		}
		bits.Free()
		bits = miss
	}

	return &bits, nil
}
