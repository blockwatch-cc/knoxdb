// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"
	"sort"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/pkg/slicex"
)

func (t *Table) Lookup(ctx context.Context, pks []uint64) (engine.QueryResult, error) {
	// we need noon-zero, unique and sorted pks
	pks = slicex.RemoveZeros(slicex.Unique(pks))

	// prepare result
	res := NewResult(
		pack.New().
			WithKey(pack.ResultKeyId).
			WithMaxRows(len(pks)).
			WithSchema(t.schema).
			Alloc(),
	)

	// protect journal access
	t.mu.RLock()
	defer t.mu.RUnlock()
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	// execute query
	err := t.doLookup(ctx, pks, res)
	if err != nil {
		res.Close()
		return nil, err
	}

	return res, nil
}

func (t *Table) StreamLookup(ctx context.Context, pks []uint64, fn func(engine.QueryRow) error) error {
	// we need noon-zero, unique and sorted pks
	pks = slicex.RemoveZeros(slicex.Unique(pks))

	// prepare result
	res := NewStreamResult(fn)
	defer res.Close()

	// protect journal access
	t.mu.RLock()
	defer t.mu.RUnlock()
	atomic.AddInt64(&t.stats.StreamCalls, 1)

	// execute query
	err := t.doLookup(ctx, pks, res)
	if err != nil && err != engine.EndStream {
		return err
	}

	return nil
}

// unsafe when called concurrently! lock table _before_ starting bolt tx!
func (t *Table) doLookup(ctx context.Context, pks []uint64, res QueryResultConsumer) error {
	var (
		nRowsMatched uint32
	)

	// make a temporary query plan
	plan := query.NewQueryPlan().WithTable(t).WithLogger(t.log)

	// cleanup and log on exit
	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(nRowsMatched))
	}()

	// remove deleted records
	if t.journal.TombLen() > 0 {
		for i, pk := range pks {
			if !t.journal.IsDeleted(pk) {
				continue
			}
			pks[i] = 0
		}
		// remove zeros again
		pks = slicex.RemoveZeros(pks)
	}

	// early return if all pks are deleted or out of range
	if len(pks) == 0 || pks[0] > t.state.Sequence {
		return nil
	}

	// keep max pk to lookup
	maxRows := uint32(len(pks))
	maxNonZeroId := pks[maxRows-1]

	// lookup journal first (Note: its sorted by pk)
	var (
		idx, last  int
		needUpdate bool
		jlen       = t.journal.Len()
		jpack      = t.journal.Data
	)
	for i, pk := range pks {
		// no more matches in journal?
		if last == jlen {
			break
		}

		// not in journal?
		idx, last = t.journal.PkIndex(pk, last)
		if idx < 0 {
			continue
		}

		// emit record
		nRowsMatched++
		if err := res.Append(jpack, idx, 1); err != nil {
			return err
		}

		// mark pk as processed (set 0)
		pks[i] = 0
		needUpdate = true
	}
	if needUpdate {
		// remove processed ids
		pks = slicex.RemoveZeros(pks)
	}

	// return early when everything was found in journal
	if len(pks) == 0 {
		return nil
	}

	// PACK SCAN, iterator uses range checks
	var nextid int
	it := NewLookupIterator(plan, pks)
	defer it.Close()

	for {
		// stop when all inputs are matched
		if maxRows == nRowsMatched {
			break
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		// load next pack with potential matches, use pack max pk to break early
		pkg, maxPk, err := it.Next(ctx)
		if err != nil {
			return err
		}

		// finish when no more packs are found
		if pkg == nil {
			break
		}

		// access primary key column (used for binary search below)
		ppk := pkg.PkColumn()
		ppl := len(ppk)

		// loop over the remaining (unresolved) pks, packs are sorted by pk
		pos := 0
		for _, pk := range pks[nextid:] {
			// no more matches in this pack?
			if maxPk < pk || ppk[pos] > maxNonZeroId {
				break
			}

			// find pk in pack
			idx := sort.Search(ppl-pos, func(i int) bool { return ppk[pos+i] >= pk })
			pos += idx
			if pos >= ppl || ppk[pos] != pk {
				nextid++
				continue
			}

			// on match, copy result from package
			nRowsMatched++
			if err := res.Append(pkg, pos, 1); err != nil {
				return err
			}
			nextid++
		}
	}

	return nil
}
