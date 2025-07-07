// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"context"
	"fmt"
	"maps"
	"os"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/operator"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/query"
	etests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJournalInsert(t *testing.T) {
	ctx, j, makeRecord := setupJournalTest(t)
	xid := engine.GetTxId(ctx)

	// insert single
	pk, n, err := j.InsertRecords(ctx, makeRecord(0))
	require.NoError(t, err)
	require.Equal(t, uint64(1), pk, "assigned pk")
	require.Equal(t, 1, n, "insert count")
	require.Equal(t, 1, j.Len(), "record count")
	require.Equal(t, 1, j.NumSegments(), "seg count")
	require.Equal(t, 1, j.NumTuples(), "tuple count")
	require.Equal(t, 0, j.NumTombstones(), "tomb count")
	require.Equal(t, 127, j.Capacity(), "capacity")
	require.Equal(t, uint32(1), j.Tip().Id())
	require.True(t, j.Tip().ContainsTx(xid))
	require.True(t, j.Tip().IsActiveTx(xid))
	require.False(t, j.Tip().IsEmpty())
	require.False(t, j.Tip().IsDone())
	require.True(t, j.Tip().ContainsRid(1))

	// commit
	require.False(t, j.CommitTx(xid))
	require.True(t, j.Tip().ContainsTx(xid))
	require.False(t, j.Tip().IsActiveTx(xid))
	require.False(t, j.Tip().IsEmpty())
	require.False(t, j.Tip().IsFull())
	require.True(t, j.Tip().IsDone())
	require.True(t, j.Tip().ContainsRid(1))
	require.Equal(t, 1, j.Tip().Len())
	require.Equal(t, 1, j.Tip().Data().Len())
	require.Nil(t, j.Tip().Aborted())
	require.Nil(t, j.Tip().Replaced())
	require.Equal(t, uint64(2), j.Tip().State().NextPk)
	require.Equal(t, uint64(2), j.Tip().State().NextRid)
	require.Equal(t, uint64(1), j.Tip().State().NRows)
}

func TestJournalUpdate(t *testing.T) {
	ctx, j, makeRecord := setupJournalTest(t)
	xid := engine.GetTxId(ctx)

	// insert 1st
	_, _, err := j.InsertRecords(ctx, makeRecord(0))
	require.NoError(t, err)

	// insert 2nd
	_, _, err = j.InsertRecords(ctx, makeRecord(0))
	require.NoError(t, err)

	// update 1st
	n, err := j.UpdateRecords(ctx, makeRecord(1), map[uint64]uint64{1: 1})
	require.NoError(t, err)
	require.Equal(t, 1, n, "update count")

	// commit
	require.False(t, j.CommitTx(xid))

	// check state
	require.Equal(t, 3, j.Len(), "record count")
	require.Equal(t, 1, j.NumSegments(), "seg count")
	require.Equal(t, 3, j.NumTuples(), "tuple count")
	require.Equal(t, 1, j.NumTombstones(), "tomb count")
	require.Equal(t, 125, j.Capacity(), "capacity")
	require.Equal(t, uint32(1), j.Tip().Id())
	require.True(t, j.Tip().ContainsTx(xid))
	require.False(t, j.Tip().IsActiveTx(xid))
	require.False(t, j.Tip().IsEmpty())
	require.True(t, j.Tip().IsDone())
	require.True(t, j.Tip().ContainsRid(1))
	require.True(t, j.Tip().ContainsRid(2))
	require.True(t, j.Tip().ContainsRid(3))
	require.Equal(t, 3, j.Tip().Len())
	require.Equal(t, 3, j.Tip().Data().Len())
	require.Nil(t, j.Tip().Aborted())
	require.NotNil(t, j.Tip().Replaced())
	require.Equal(t, 1, j.Tip().Replaced().Count())
	require.Equal(t, uint64(3), j.Tip().State().NextPk)
	require.Equal(t, uint64(4), j.Tip().State().NextRid)
	require.Equal(t, uint64(2), j.Tip().State().NRows)
}

func TestJournalDelete(t *testing.T) {
	ctx, j, makeRecord := setupJournalTest(t)
	xid := engine.GetTxId(ctx)

	// insert 1st
	_, _, err := j.InsertRecords(ctx, makeRecord(0))
	require.NoError(t, err)

	// insert 2nd
	_, _, err = j.InsertRecords(ctx, makeRecord(0))
	require.NoError(t, err)

	// delete 1st
	data := j.Tip().Data().Copy()
	data.WithSelection([]uint32{0})
	n, err := j.DeletePack(ctx, data)
	require.NoError(t, err)
	require.Equal(t, 1, n, "update count")

	// commit
	require.False(t, j.CommitTx(xid))

	// check state
	require.Equal(t, 2, j.Len(), "record count")
	require.Equal(t, 1, j.NumSegments(), "seg count")
	require.Equal(t, 2, j.NumTuples(), "tuple count")
	require.Equal(t, 1, j.NumTombstones(), "tomb count")
	require.Equal(t, 126, j.Capacity(), "capacity")
	require.Equal(t, uint32(1), j.Tip().Id())
	require.True(t, j.Tip().ContainsTx(xid))
	require.False(t, j.Tip().IsActiveTx(xid))
	require.False(t, j.Tip().IsEmpty())
	require.True(t, j.Tip().IsDone())
	require.True(t, j.Tip().ContainsRid(1))
	require.True(t, j.Tip().ContainsRid(2))
	require.Equal(t, 2, j.Tip().Len())
	require.Equal(t, 2, j.Tip().Data().Len())
	require.Nil(t, j.Tip().Aborted())
	require.NotNil(t, j.Tip().Replaced())
	require.Equal(t, 1, j.Tip().Replaced().Count())
	require.Equal(t, uint64(3), j.Tip().State().NextPk)
	require.Equal(t, uint64(3), j.Tip().State().NextRid)
	require.Equal(t, uint64(1), j.Tip().State().NRows)
}

func TestJournalRotate(t *testing.T) {
	ctx, j, makeRecord := setupJournalTest(t)
	xid := engine.GetTxId(ctx)

	// insert 128+1 records
	for i := range 128 + 1 {
		pk, n, err := j.InsertRecords(ctx, makeRecord(0))
		require.NoError(t, err)
		require.Equal(t, 1, n, "n")
		require.Equal(t, uint64(i+1), pk, "pk")
	}
	require.True(t, j.CommitTx(xid), "have mergable segment")

	// should have 2 segments now
	require.Equal(t, 129, j.Len(), "record count")
	require.Equal(t, 2, j.NumSegments(), "seg count")
	require.Equal(t, 129, j.NumTuples(), "tuple count")
	require.Equal(t, 0, j.NumTombstones(), "tomb count")
	require.Equal(t, 127, j.Capacity(), "capacity")
	require.Equal(t, uint32(2), j.Tip().Id())
	require.True(t, j.Tip().ContainsTx(xid))
	require.False(t, j.Tip().IsActiveTx(xid))
	require.False(t, j.Tip().IsEmpty())
	require.True(t, j.Tip().IsDone())
	require.False(t, j.Tip().ContainsRid(1))
	require.False(t, j.Tip().ContainsRid(128))
	require.True(t, j.Tip().ContainsRid(129))
	require.Equal(t, uint64(130), j.Tip().State().NextPk)
	require.Equal(t, uint64(130), j.Tip().State().NextRid)
	require.Equal(t, uint64(129), j.Tip().State().NRows)

	// can we merge?
	seg, err := j.NextMergable()
	require.NoError(t, err)
	require.NotNil(t, seg)
	require.Equal(t, uint32(1), seg.Id())
	require.True(t, seg.ContainsTx(xid))
	require.False(t, seg.IsActiveTx(xid))
	require.False(t, seg.IsEmpty())
	require.True(t, seg.IsFull())
	require.True(t, seg.IsDone())
	require.True(t, seg.ContainsRid(1))
	require.True(t, seg.ContainsRid(128))
	require.False(t, seg.ContainsRid(129))
	require.Equal(t, 128, seg.Len())
	require.Equal(t, 128, seg.Data().Len())
	require.Nil(t, seg.Aborted())
	require.Nil(t, seg.Replaced())
	require.Equal(t, uint64(129), seg.State().NextPk)
	require.Equal(t, uint64(129), seg.State().NextRid)
	require.Equal(t, uint64(128), seg.State().NRows)
	require.Equal(t, seg, j.tip.parent)

	// confirm merge
	j.ConfirmMerged(ctx, seg)
	require.Equal(t, 1, j.NumSegments(), "seg count")
	require.Equal(t, 1, j.NumTuples(), "tuple count")
	require.Equal(t, 0, j.NumTombstones(), "tomb count")
	require.Nil(t, j.tip.parent)
}

func TestJournalRotateAborted(t *testing.T) {
	ctx, j, makeRecord := setupJournalTest(t)
	xid := engine.GetTxId(ctx)

	// insert and commit one record
	pk, n, err := j.InsertRecords(ctx, makeRecord(0))
	require.NoError(t, err)
	require.Equal(t, 1, n, "n")
	require.Equal(t, uint64(1), pk, "pk")
	require.False(t, j.CommitTx(xid))

	// open new tx
	ctx = setupNextTx(t, ctx)
	xid = engine.GetTxId(ctx)

	// update first record
	n, err = j.UpdateRecords(ctx, makeRecord(1), map[uint64]uint64{1: 1})
	require.NoError(t, err)
	require.Equal(t, 1, n, "update count")
	require.False(t, j.CommitTx(xid))
	require.Nil(t, j.Tip().Aborted())

	// snapshot state
	state := j.Tip().State()

	// open new tx
	ctx = setupNextTx(t, ctx)
	xid = engine.GetTxId(ctx)

	// insert 64 more records
	for i := range 64 {
		pk, n, err := j.InsertRecords(ctx, makeRecord(0))
		require.NoError(t, err)
		require.Equal(t, 1, n, "n")
		require.Equal(t, uint64(i+2), pk, "pk")
	}

	// update 64 inserted records in same tx (spills into next segment)
	for i := range 64 {
		// note: due to the update of pk 1, the rid for new inserts start at 3
		ridMap := map[uint64]uint64{uint64(i + 2): uint64(i + 3)}
		n, err := j.UpdateRecords(ctx, makeRecord(i+2), ridMap)
		require.NoError(t, err)
		require.Equal(t, 1, n, "n")
	}

	// abort tx – should roll back so we have
	// - 2 segments
	// - tip contains 2 aborted records only
	// - seg 1 contains 2 live records only
	// - tip state and seg 1 should be rolled back to snapshot
	require.True(t, j.AbortTx(xid))
	seg, err := j.NextMergable()
	require.NoError(t, err)
	require.NotNil(t, seg)

	// check expectations
	require.Equal(t, 2, j.NumSegments(), "seg count")
	require.Equal(t, 130, j.Len(), "record count")
	require.Equal(t, 130, j.NumTuples(), "tuple count")
	require.Equal(t, 1, j.NumTombstones(), "tomb count") // no aborted tombstones
	require.Equal(t, 126, j.Capacity(), "capacity")

	// tip
	// t.Logf("Tip: %#v", j.Tip())
	require.Equal(t, uint32(2), j.Tip().Id())
	require.True(t, j.Tip().ContainsTx(xid))
	require.False(t, j.Tip().IsActiveTx(xid))
	require.True(t, j.Tip().IsEmpty()) // everything aborted
	require.True(t, j.Tip().IsDone())
	require.False(t, j.Tip().ContainsRid(1))
	require.False(t, j.Tip().ContainsRid(128))
	require.False(t, j.Tip().ContainsRid(129)) // false because empty
	require.NotNil(t, j.Tip().Aborted())
	require.Equal(t, j.Tip().Data().Len(), j.Tip().Aborted().Len())
	require.Nil(t, j.Tip().Replaced())
	require.Equal(t, 2, j.Tip().Aborted().Count())
	require.Equal(t, 2, j.Tip().Data().Len())
	require.Equal(t, 0, j.Tip().Tomb().Len())
	require.Equal(t, state, j.Tip().State())

	// seg
	// t.Logf("Seg: %#v", seg)
	require.Equal(t, uint32(1), seg.Id())
	require.True(t, seg.ContainsTx(xid))
	require.False(t, seg.IsActiveTx(xid))
	require.False(t, seg.IsEmpty())
	require.True(t, seg.IsFull())
	require.True(t, seg.IsDone())
	require.True(t, seg.ContainsRid(1))
	require.True(t, seg.ContainsRid(2))
	require.False(t, seg.ContainsRid(3)) // reset, 3.. is aborted
	require.False(t, seg.ContainsRid(126))
	require.False(t, seg.ContainsRid(127))
	require.Equal(t, 128, seg.Len())
	require.Equal(t, 128, seg.Data().Len())
	require.NotNil(t, seg.Aborted())
	require.NotNil(t, seg.Replaced())
	require.Equal(t, seg.Data().Len(), seg.Aborted().Len())
	require.Equal(t, seg.Data().Len(), seg.Replaced().Len())
	require.Equal(t, 1, seg.Replaced().Count())    // in-segment upd before rotate
	require.Equal(t, 64+62, seg.Aborted().Count()) // ins + upd
	require.Equal(t, 128, seg.Data().Len())
	require.Equal(t, 1, seg.Tomb().Len()) // only one non-aborted update
	require.Equal(t, state, seg.State())
}

func TestJournalRandom(t *testing.T) {
	ctx, j, makeRecord := setupJournalTest(t)

	var (
		liveSnap = make(map[uint64]uint64) // live pk -> rid mappings
		live     = make(map[uint64]uint64) // temp live pk -> rid mapping (merged on commit)
		pksSnap  = slicex.NewOrderedIntegers[uint64](make([]uint64, 0)).SetUnique()
		pks      = slicex.NewOrderedIntegers[uint64](make([]uint64, 0)).SetUnique()
		snap     = j.Tip().State()
	)

	defer func() {
		if t.Failed() {
			// print contents of the top two segments
			fmt.Printf("Tip #%d ---------- \n", j.tip.Id())
			operator.NewLogOperator(os.Stdout, 128).Process(ctx, j.tip.data)
			if l := len(j.tail); l > 0 {
				fmt.Printf("Seg #%d ---------- \n", j.tail[l-1].Id())
				operator.NewLogOperator(os.Stdout, 128).Process(ctx, j.tail[l-1].data)
			}
		}
	}()

	validate := func(_ context.Context) {
		require.Greater(t, j.Capacity(), 0, "always free capacity")
		require.Equal(t, snap, j.Tip().State())
		for _, seg := range j.Segments() {
			validateSegment(t, seg)
		}
	}

	// seed with 16 values
	for i := range 16 {
		pk, n, err := j.InsertRecords(ctx, makeRecord(0))
		require.NoError(t, err)
		require.Equal(t, 1, n, "n")
		require.Equal(t, uint64(i+1), pk, "pk")
		live[pk] = pk
		pks.Insert(pk)
	}
	t.Log("Insert 1..16")
	t.Logf("Commit %d", engine.GetTxId(ctx))
	j.CommitTx(engine.GetTxId(ctx))
	ctx = setupNextTx(t, ctx)
	snap = j.Tip().State()
	liveSnap = maps.Clone(live)
	pksSnap = pks.Clone()

	// run 10k random actions
	actions := 10000
	if testing.Short() {
		actions = 100
	}
	for range actions {
		xid := engine.GetTxId(ctx)
		switch util.RandIntn(5) {
		case 0: // insert
			t.Logf("X-%d Insert %d[%d] into #%d",
				xid, j.Tip().State().NextPk, j.Tip().State().NextRid, j.Tip().Id())
			pk, n, err := j.InsertRecords(ctx, makeRecord(0))
			require.NoError(t, err)
			require.Equal(t, 1, n, "n = 1")
			require.Equal(t, pk, j.Tip().State().NextPk-1, "pk reflects state")
			live[pk] = j.Tip().State().NextRid - 1
			pks.Insert(pk)

		case 1: // update a random live record
			if len(live) > 0 {
				pk := pks.Values[util.RandIntn(len(live))]
				t.Logf("X-%d Update %d[%d] => [%d] into #%d",
					xid, pk, live[pk], j.Tip().State().NextRid, j.Tip().Id())
				// note update rewrites the live map with new rid
				n, err := j.UpdateRecords(ctx, makeRecord(int(pk)), live)
				require.NoError(t, err)
				require.Equal(t, 1, n, "n = 1")
			}

		case 2: // delete a random live record
			if len(live) > 0 {
				pk := pks.Values[util.RandIntn(len(live))]
				// delete is a bit more complicated because we need a
				// data pack with selection of the record we aim to
				// delete. we use a journal query to identify the
				// live record which has the nice side effect that we
				// can also test the query code this way.
				t.Logf("X-%d Delete %d[%d] into #%d", xid, pk, live[pk], j.Tip().Id())
				plan := &query.QueryPlan{
					Filters: filter.NewNode().AddLeaf(
						filter.NewFilter(j.schema.Pk(), j.schema.PkIndex(), types.FilterModeEqual, pk),
					),
					Snap: engine.GetSnapshot(ctx),
					Log:  j.log,
				}
				// query and check result contains exactly 1 hit
				res := j.Query(plan, 0) // skip no epochs (we don't merge)
				require.Greater(t, res.Len(), 0, "no live record match")
				require.Equal(t, 1, res.Len(), "more than one live record match %v", res.pkgs[0].Selected())
				require.False(t, res.IsEmpty())

				// delete it
				for pkg := range res.Iterator() {
					n, err := j.DeletePack(ctx, pkg)
					require.NoError(t, err)
					require.Equal(t, 1, n, "update count")
					delete(live, pk)
				}
				res.Close()
				pks.Remove(pk)
			}

		case 3: // commit
			t.Logf("X-%d Commit", xid)
			j.CommitTx(xid)

			// make a new snapshot for comparison
			snap = j.Tip().State()
			liveSnap = maps.Clone(live)
			pksSnap = pks.Clone()

			validate(ctx)

			// prepare next tx
			ctx = setupNextTx(t, ctx)

		case 4: // abort
			t.Logf("X-%d Abort", xid)
			j.AbortTx(xid)
			validate(ctx)

			// prepare next tx
			ctx = setupNextTx(t, ctx)

			// restore earlier snapshot for comparison
			live = maps.Clone(liveSnap)
			pks = pksSnap.Clone()
		}
	}

	// final commit (in case none happened)
	t.Logf("X-%d Commit", engine.GetTxId(ctx))
	j.CommitTx(engine.GetTxId(ctx))
	snap = j.Tip().State()
	liveSnap = maps.Clone(live)
	pksSnap = pks.Clone()
	validate(ctx)
}

func validateSegment(t *testing.T, s *Segment) {
	// validate invariants
	// - aborted records
	//   - have no tombstone
	//   - have xmin = xmax = 0
	//   - have aborted bit set
	// - committed deleted/updated records
	//   - have xmin and xmax set
	//   - have replaced bit set
	//   - do not have aborted flag set
	// - committed inserted records
	//   - have xmin set
	//   - do not have xmax set
	//   - have 0 < rid == ref
	// - committed updated records
	//   - have xmin set
	//   - do not have xmax set
	//   - have rid != ref, 0 < ref < rid

	pks := s.data.Pks().Slice()
	xmins := s.data.Xmins().Slice()
	xmaxs := s.data.Xmaxs().Slice()
	rids := s.data.RowIds().Slice()
	refs := s.data.RefIds().Slice()
	dels := s.data.Dels()

	// track suspicious tombstones
	suspiciousTomb := make(map[uint64]uint64) // rid -> pk

	// aborted
	var nAborted uint32
	for i := range s.data.Len() {
		if xmins[i] == 0 {
			nAborted++
			require.Equal(t, uint64(0), xmaxs[i], "xmax = 0")
			// its possible a row id is used for inserts/updates
			// that are later aborted. this can happen multiple times
			// until at some point a tx succeeds and leaves a real
			// tombstone. hence we cannot treat the presence of
			// a tomstone for aborted records as bug per se.
			// we can however put such a record on watch and
			// reconcile after commit
			if s.tomb.RowIds().Contains(rids[i]) {
				suspiciousTomb[rids[i]] = pks[i]
			}
			require.NotNil(t, s.aborted, "aborted bitset not allocated")
			require.True(t, s.aborted.Contains(i), "has aborted bit %d[%d] not set", pks[i], rids[i])
		}
		if s.aborted != nil && s.aborted.Contains(i) {
			require.Equal(t, uint64(0), xmins[i], "aborted bit %d[%d] with xmin != 0", pks[i], rids[i])
			require.Equal(t, uint64(0), xmaxs[i], "aborted bit %d[%d] with xmax != 0", pks[i], rids[i])
			if s.tomb.RowIds().Contains(rids[i]) {
				suspiciousTomb[rids[i]] = pks[i]
			}
		}
	}

	// we don't see aborted deletes here (tomstone clears)
	require.LessOrEqual(t, nAborted, s.nAbort)

	// simplify checks, alloc replaced/aborted bitsets if nil
	if s.aborted == nil {
		s.aborted = bitset.New(s.data.Len())
	}
	if s.replaced == nil {
		s.replaced = bitset.New(s.data.Len())
	}

	// deleted/updated records
	var nInserted, nUpdated, nDeleted uint32
	for i := range s.data.Len() {
		if xmins[i] == 0 {
			continue // skip aborted
		}
		switch {
		case xmaxs[i] > 0 && dels.Get(i): // deleted insert or update
			nDeleted++
			require.True(t, s.ContainsRid(rids[i]), "is in segment")
			require.True(t, s.tomb.RowIds().Contains(rids[i]), "has tombstone")
			require.True(t, s.replaced.Contains(i), "has replaced bit")
			require.False(t, s.aborted.Contains(i), "no aborted bit")
			delete(suspiciousTomb, rids[i])

		case xmaxs[i] > 0 && !dels.Get(i): // replaced insert or update pre-image
			// nUpdated++
			require.True(t, s.ContainsRid(rids[i]), "is in segment")
			require.True(t, s.tomb.RowIds().Contains(rids[i]), "has tombstone")
			require.True(t, s.replaced.Contains(i), "has replaced bit")
			require.False(t, s.aborted.Contains(i), "no aborted bit")
			delete(suspiciousTomb, rids[i])

		case xmaxs[i] == 0: // insert or update post-image
			if refs[i] == rids[i] {
				nInserted++
			} else {
				nUpdated++
			}
			require.True(t, s.ContainsRid(rids[i]), "is in segment")
			require.False(t, s.replaced.Contains(i), "live record %d[%d] with replaced bit", pks[i], rids[i])
			require.False(t, s.aborted.Contains(i), "live record %d[%d] with aborted bit", pks[i], rids[i])

		default:
			require.Fail(t, "invalid state", "record[%d] rid=%d ref=%d xmin=%d xmax=%d del=%t",
				i, rids[i], refs[i], xmins[i], xmaxs[i], dels.Get(i),
			)
		}
	}

	if len(suspiciousTomb) > 0 {
		for k, v := range suspiciousTomb {
			assert.Fail(t, "invalid tombstone", "aborted rec %d[%d] with tombstone", v, k)
		}
	}

	// segment does not reduce counts on abort, so we see must see lower values here
	require.LessOrEqual(t, nInserted, s.nInsert, "nInsert")
	require.LessOrEqual(t, nUpdated, s.nUpdate, "nUpdate")
	require.LessOrEqual(t, nDeleted, s.nDelete, "nDelete")

	// note: we cannot compare sum of counters because segment changes them in a way
	// that we cannot reconstruct from walking the pack contents
}

func setupNextTx(t *testing.T, ctx context.Context) context.Context {
	tx := engine.GetTx(ctx)
	e := tx.Engine()
	tx.Close()
	ctx, _, _, _, err := e.WithTransaction(context.Background(), engine.TxFlagNoWal)
	require.NoError(t, err)
	return ctx
}

func setupJournalTest(t *testing.T) (context.Context, *Journal, func(int) []byte) {
	// create test engine
	e := etests.NewTestEngine(t, etests.NewTestDatabaseOptions(t, "mem"))

	// create test journal
	j := NewJournal(testSchema.WithMeta(), 128, 64).
		WithLogger(log.Log).
		WithState(engine.NewObjectState("tst"))

	// create tx without wal support (wo don't want to write)
	ctx, _, _, _, err := e.WithTransaction(context.Background(), engine.TxFlagNoWal)
	require.NoError(t, err)

	// create record producer helper
	enc := schema.NewGenericEncoder[schema.BaseModel]()
	makeRecord := func(i int) []byte {
		buf, err := enc.Encode(schema.BaseModel{Id: uint64(i)}, nil)
		require.NoError(t, err)
		return buf
	}

	return ctx, j, makeRecord
}
