// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/require"
)

var testSchema = schema.MustSchemaOf(schema.BaseModel{})

func TestSegmentInsert(t *testing.T) {
	seg := newSegment(testSchema.WithMeta(), 42, 8).setState(SegmentStateActive)
	enc := schema.NewGenericEncoder[schema.BaseModel]()
	require.Equal(t, uint32(42), seg.Id())
	require.NotNil(t, seg.Data())
	require.NotNil(t, seg.Tomb())
	require.Equal(t, 0, seg.Len())
	require.Equal(t, 0, seg.Data().Len())
	require.True(t, seg.IsEmpty())
	require.False(t, seg.IsDone())
	require.False(t, seg.ContainsTx(1))
	require.False(t, seg.ContainsRid(1))

	// insert val
	buf, err := enc.Encode(schema.BaseModel{Id: 1}, nil)
	require.NoError(t, err)
	seg.InsertRecord(1, 1, buf)
	require.Equal(t, 1, seg.Data().Len())
	require.False(t, seg.IsEmpty())
	require.False(t, seg.IsDone())
	require.True(t, seg.ContainsTx(1))
	require.True(t, seg.ContainsRid(1))
	require.True(t, seg.ContainsTx(1))
	require.True(t, seg.IsActiveTx(1))

	// commit
	seg.CommitTx(1)
	require.True(t, seg.ContainsTx(1))
	require.False(t, seg.IsActiveTx(1))
	require.True(t, seg.IsDone())
}

func TestSegmentUpdate(t *testing.T) {
	seg := newSegment(testSchema.WithMeta(), 42, 8).setState(SegmentStateActive)
	enc := schema.NewGenericEncoder[schema.BaseModel]()

	// insert val1
	buf, err := enc.Encode(schema.BaseModel{Id: 1}, nil)
	require.NoError(t, err)
	seg.InsertRecord(1, 1, buf)

	// update val
	buf, err = enc.Encode(schema.BaseModel{Id: 2}, nil)
	require.NoError(t, err)
	seg.UpdateRecord(1, 2, 1, buf)
	require.True(t, seg.ContainsTx(1))
	require.True(t, seg.IsActiveTx(1))
	seg.CommitTx(1)

	// check state
	require.True(t, seg.IsDone())
	require.False(t, seg.IsEmpty())
	require.Equal(t, 2, seg.Len()) // 1 ins, 1 upd
	require.Equal(t, 2, seg.Data().Len())
	require.Equal(t, 1, seg.Tomb().Len())
	require.True(t, seg.ContainsTx(1))
	require.True(t, seg.ContainsRid(1))
	require.True(t, seg.ContainsRid(2))
	require.True(t, seg.ContainsTx(1))
	require.False(t, seg.IsActiveTx(1))
}

func TestSegmentDelete(t *testing.T) {
	seg := newSegment(testSchema.WithMeta(), 42, 8).setState(SegmentStateActive)
	enc := schema.NewGenericEncoder[schema.BaseModel]()

	// insert val 1 & 2 and commit
	buf, err := enc.Encode(schema.BaseModel{Id: 1}, nil)
	require.NoError(t, err)
	seg.InsertRecord(1, 1, buf)
	buf, err = enc.Encode(schema.BaseModel{Id: 2}, nil)
	require.NoError(t, err)
	seg.InsertRecord(1, 2, buf)
	seg.CommitTx(1)

	// delete val 2
	seg.NotifyDelete(2, 2)
	require.True(t, seg.ContainsTx(2), "xid 2 is known")
	require.True(t, seg.IsActiveTx(2), "xid 2 is active")
	require.True(t, seg.ContainsRid(2), "rid 2 is known")

	// check own visibility
	snap := types.NewSnapshot(2, 1, 3) // 2 is the only tx
	set := xroar.New()
	seg.MergeDeleted(set, snap)
	require.Equal(t, 1, set.Count(), "numDeleted")
	require.True(t, set.Contains(2), "rid 2 is deleted")
	require.False(t, seg.IsDone())

	// abort
	seg.AbortTx(2)
	require.True(t, seg.IsDone())
	require.True(t, seg.ContainsTx(2), "xid 2 is known")
	require.True(t, seg.ContainsRid(2), "rid 2 is known")
	require.False(t, seg.IsActiveTx(2), "xid 2 is no longer active")
	set.Reset()
	snap = types.NewSnapshot(3, 3, 3)
	seg.MergeDeleted(set, snap)
	require.False(t, set.Contains(2), "aborted delete rid 2 is no longer visible")

}

func TestSegmentMatch(t *testing.T) {
	// 4 xids: 1 aborted, 2 committed, 1 open
	// insert, update, delete mix
	// snapshot at 3rd xid
	seg := newSegment(testSchema.WithMeta(), 42, 8).setState(SegmentStateActive)
	enc := schema.NewGenericEncoder[schema.BaseModel]()

	// xid 1 committed
	buf, err := enc.Encode(schema.BaseModel{Id: 1}, nil)
	require.NoError(t, err)
	seg.InsertRecord(1, 1, buf)
	seg.CommitTx(1)

	// xid 2 aborted
	buf, err = enc.Encode(schema.BaseModel{Id: 2}, nil)
	require.NoError(t, err)
	seg.InsertRecord(2, 2, buf)
	seg.AbortTx(2)

	// xid 3 committed, replaces rid 1
	buf, err = enc.Encode(schema.BaseModel{Id: 1}, nil)
	require.NoError(t, err)
	seg.UpdateRecord(3, 2, 1, buf)
	seg.CommitTx(3)

	// xid 4 open
	buf, err = enc.Encode(schema.BaseModel{Id: 3}, nil)
	require.NoError(t, err)
	seg.InsertRecord(4, 3, buf)

	// query at 3
	snap := types.NewSnapshot(3, 3, 4) // pretend 3 is the only tx
	set := xroar.New()
	seg.MergeDeleted(set, snap)
	t.Logf("Tomb set with %d vals", set.Count())
	t.Logf("Tomb %#v", seg.tomb.stones)

	// logOp := operator.NewLogger(os.Stdout, 8)
	// logOp.Process(context.Background(), seg.data)

	// all matches
	fltAll := filter.NewNode().AddLeaf(
		filter.NewFilter(testSchema.Field(0), 0, types.FilterModeTrue, nil))
	bits := bitset.New(seg.Len())
	seg.Match(fltAll, snap, set, bits)
	require.Equal(t, 1, bits.Count(), "all match count")
	require.False(t, bits.Contains(0), "updated record must not be visible")
	require.False(t, bits.Contains(1), "aborted record must not be visible")
	require.True(t, bits.Contains(2), "update post-image must be visible")
	require.False(t, bits.Contains(3), "future record must not be visible")

	// should not match anything
	fltNonExistAborted := filter.NewNode().AddLeaf(
		filter.NewFilter(testSchema.Field(0), 0, types.FilterModeEqual, uint64(2)))
	bits = bitset.New(seg.Len())
	seg.Match(fltNonExistAborted, snap, set, bits)
	require.Equal(t, 0, bits.Count(), "none match count")

}

func TestSegmentStateUpdates(t *testing.T) {
	seg := newSegment(testSchema.WithMeta(), 42, 8).
		setState(SegmentStateActive).
		WithState(engine.NewObjectState("test")).
		setCheckpoint(42)
	enc := schema.NewGenericEncoder[schema.BaseModel]()
	makeRecord := func(i int) []byte {
		buf, err := enc.Encode(schema.BaseModel{Id: uint64(i)}, nil)
		require.NoError(t, err)
		return buf
	}

	var xid types.XID = 1

	// xid-1: insert 1 + commit
	seg.InsertRecord(xid, seg.tstate.NextRid, makeRecord(1))
	seg.tstate.NextPk++
	seg.tstate.NextRid++
	seg.tstate.NRows++
	seg.CommitTx(xid)
	require.Equal(t, uint64(2), seg.tstate.NextPk, "nextPk")
	require.Equal(t, uint64(2), seg.tstate.NextRid, "nextRid")
	require.Equal(t, uint64(1), seg.tstate.NRows, "nrows")
	require.Equal(t, wal.LSN(42), seg.tstate.Checkpoint)
	xid++

	// xid-2: insert + commit again
	seg.InsertRecord(xid, seg.tstate.NextRid, makeRecord(2))
	seg.tstate.NextPk++
	seg.tstate.NextRid++
	seg.tstate.NRows++
	seg.CommitTx(xid)
	require.Equal(t, uint64(3), seg.tstate.NextPk, "nextPk")
	require.Equal(t, uint64(3), seg.tstate.NextRid, "nextRid")
	require.Equal(t, uint64(2), seg.tstate.NRows, "nrows")
	require.Equal(t, wal.LSN(42), seg.tstate.Checkpoint)
	xid++

	// xid-3: insert 3 + abort (must reset state)
	seg.InsertRecord(xid, seg.tstate.NextRid, makeRecord(3))
	seg.tstate.NextPk++
	seg.tstate.NextRid++
	seg.tstate.NRows++
	seg.AbortTx(xid)
	require.Equal(t, uint64(3), seg.tstate.NextPk, "nextPk")
	require.Equal(t, uint64(3), seg.tstate.NextRid, "nextRid")
	require.Equal(t, uint64(2), seg.tstate.NRows, "nrows")
	require.Equal(t, wal.LSN(42), seg.tstate.Checkpoint)
	xid++

	// xid-4: update 1 + commit
	seg.UpdateRecord(xid, seg.tstate.NextRid, 1, makeRecord(1))
	seg.tstate.NextRid++
	seg.CommitTx(xid)
	require.Equal(t, uint64(3), seg.tstate.NextPk, "nextPk")
	require.Equal(t, uint64(4), seg.tstate.NextRid, "nextRid")
	require.Equal(t, uint64(2), seg.tstate.NRows, "nrows")
	require.Equal(t, wal.LSN(42), seg.tstate.Checkpoint)
	xid++

	// xid-5: update 2 + abort (must reset state)
	seg.UpdateRecord(xid, seg.tstate.NextRid, 2, makeRecord(2))
	seg.tstate.NextRid++
	seg.AbortTx(xid)
	require.Equal(t, uint64(3), seg.tstate.NextPk, "nextPk")
	require.Equal(t, uint64(4), seg.tstate.NextRid, "nextRid")
	require.Equal(t, uint64(2), seg.tstate.NRows, "nrows")
	require.Equal(t, wal.LSN(42), seg.tstate.Checkpoint)
	xid++

	// xid-6: delete + commit
	seg.NotifyDelete(xid, 1)
	seg.tstate.NRows--
	seg.CommitTx(xid)
	require.Equal(t, uint64(3), seg.tstate.NextPk, "nextPk")
	require.Equal(t, uint64(4), seg.tstate.NextRid, "nextRid")
	require.Equal(t, uint64(1), seg.tstate.NRows, "nrows")
	require.Equal(t, wal.LSN(42), seg.tstate.Checkpoint)
	xid++

	// xid-7: delete + abort (must reset state)
	seg.NotifyDelete(xid, 2)
	seg.tstate.NRows--
	seg.AbortTx(xid)
	require.Equal(t, uint64(3), seg.tstate.NextPk, "nextPk")
	require.Equal(t, uint64(4), seg.tstate.NextRid, "nextRid")
	require.Equal(t, uint64(1), seg.tstate.NRows, "nrows")
	require.Equal(t, wal.LSN(42), seg.tstate.Checkpoint)
	// xid++

	// operator.NewLogger(os.Stdout, 8).Process(context.Background(), seg.data)
	// t.Logf("State: %#v", seg.tstate)
}
