// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
)

// Merges results from a chain of journal segments under snapshot isolation
// rules. Guarantees to find the last visible version of each matching record
// or excludes the record when deleted. An epoch id (from table state,
// TableReader or IndexReader) ensures merged segments are skipped.
//
// Returns a stable read-only result containing (a) private copies of matching
// segment data packs with added selection vectors and (b) a global view
// of the tombstone. The tomb view is used during query processing to
// exclude deleted records from TableReader scans. The segment data pack
// matches function like regular table scan matches.
//
// The merge result is concurrency safe, i.e. readers can process a query
// without additional locks while a concurrent writer can add new data the
// journal in insert/update/delete calls.
func (j *Journal) Query(plan *query.QueryPlan, epoch uint32) *Result {
	// TODO: lock-free segment walk
	// - ideally only active segment requires lock
	// - use linked list for passive segments and optimistic locks
	// - requires max size array and rotation (is this desirable?)
	// - walk may conflict with rotation and free after merge (SegmentStateMerged)
	// j.mu.RLock()
	// defer j.mu.RUnlock()

	// alloc result and match bitset
	res := NewResult()
	bits := bitset.New(j.maxsz)

	// Single-pass merge
	// Walk segments in backwards order starting at tip. This ensures we first
	// find all snapshot visible tombstones (row ids) and use them to hide
	// deleted/replaced records from the query result as we walk segments.
	seg := j.tip
	for seg != nil {
		// skip merged and empty segments
		if seg.Id() <= epoch || seg.canDrop() {
			// plan.Log.Debugf("skip journal query segment %d", seg.Id())
			seg = seg.parent
			continue
		}

		// step 1: identify deleted records
		seg.MergeDeleted(res.tomb, plan.Snap)

		// step 2: match filters, apply snapshot visibility rules and tomb
		seg.Match(plan.Filters, plan.Snap, res.tomb, bits)

		// add segment to result if it has any match
		if bits.Any() {
			// plan.Log.Debugf("using journal segment %d with %d matches", seg.Id(), bits.Count())
			res.Append(seg, bits)
		}

		// next segment in history order
		seg = seg.parent
	}

	// free scratch
	bits.Close()

	return res
}

// Identify most recent visible row ids for primary keys in map. Walk segments in
// backwards order and keep max(rid). When the first rid is found in a visible
// tombstones or when an rid cannot be resolved return false. Only return true
// if all pks have been successfully resolved.
func (j *Journal) Lookup(ridMap map[uint64]uint64, snap *types.Snapshot) bool {
	// TODO: lock-free segment walk

	// stage 1: find highest visible rid for each pk
	// start at tip then load next segment in history order
	for seg := j.tip; seg != nil; seg = seg.parent {
		seg.LookupRids(ridMap, snap)
	}

	// check if all pks are resolved
	for _, rid := range ridMap {
		if rid == 0 {
			return false
		}
	}

	// stage 2: check tombs whether any found rid has been visibly deleted
	// again start at tip then load next segment in history order
	// stop at first deletion (our only use-case for lookup is for
	// update calls which fail when a user tries to update any deleted record)
	for seg := j.tip; seg != nil; seg = seg.parent {
		if !seg.CheckRids(ridMap, snap) {
			return false
		}
	}

	return true
}
