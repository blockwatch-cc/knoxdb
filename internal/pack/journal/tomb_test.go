// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"context"
	"testing"

	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"github.com/stretchr/testify/require"
)

func TestTombAppend(t *testing.T) {
	tomb := newTomb(8)
	require.Equal(t, 0, tomb.Len())
	tomb.Append(1, 1, true)
	require.True(t, tomb.dirty)
	require.Equal(t, 1, tomb.Len())
	require.Equal(t, 1, tomb.RowIds().Count())
	require.True(t, tomb.RowIds().Contains(1))
	require.Equal(t, types.XID(1), tomb.Stones()[0].Xid)
	tomb.Append(2, 3, true)
	require.True(t, tomb.RowIds().Contains(3))
	tomb.Append(1, 2, true)
	require.True(t, tomb.RowIds().Contains(2))
	require.Equal(t, 3, tomb.Len())
	require.Equal(t, types.XID(1), tomb.Stones()[0].Xid)
	require.Equal(t, types.XID(1), tomb.Stones()[1].Xid)
	require.Equal(t, types.XID(2), tomb.Stones()[2].Xid)
	require.Equal(t, 3, tomb.RowIds().Count())
	require.True(t, tomb.dirty)
	tomb.Reset()
	require.Equal(t, 0, tomb.Len())
	require.False(t, tomb.dirty)
}

func TestTombAbort(t *testing.T) {
	tomb := newTomb(8)
	tomb.Append(1, 1, true)
	tomb.Append(2, 3, true)
	tomb.Append(1, 2, true)
	tomb.CommitTx(1)
	tomb.AbortTx(2)
	require.Equal(t, 2, tomb.Len())
	require.Equal(t, types.XID(1), tomb.Stones()[0].Xid)
	require.Equal(t, types.XID(1), tomb.Stones()[1].Xid)
	require.Equal(t, 2, tomb.RowIds().Count())
}

func TestTombAbortActive(t *testing.T) {
	tomb := newTomb(8)
	tomb.Append(1, 1, true)
	tomb.Append(2, 3, true)
	tomb.Append(1, 2, true)
	tomb.AbortActiveTx(1)
	require.Equal(t, 1, tomb.Len())
	require.Equal(t, types.XID(2), tomb.Stones()[0].Xid)
	require.Equal(t, 1, tomb.RowIds().Count())
}

func TestTombLoadStore(t *testing.T) {
	tomb := newTomb(8)
	tomb.Append(1, 1, true)
	tomb.Append(2, 3, true)
	tomb.Append(1, 2, true)
	require.True(t, tomb.dirty)
	ctx := context.Background()
	db, err := store.Create("mem", "tombtest")
	require.NoError(t, err)
	defer db.Close()
	err = db.Update(func(tx store.Tx) error {
		b, err := tx.Root().CreateBucket([]byte("tomb"))
		if err != nil {
			return err
		}
		return tomb.Store(ctx, b, 42)
	})
	require.NoError(t, err)
	require.False(t, tomb.dirty)

	tomb = newTomb(8)
	err = db.View(func(tx store.Tx) error {
		b := tx.Bucket([]byte("tomb"))
		return tomb.Load(ctx, b, 42)
	})
	require.NoError(t, err)

	require.Equal(t, 3, tomb.Len())
	require.Equal(t, 3, tomb.RowIds().Count())
	require.True(t, tomb.RowIds().Contains(1))
	require.True(t, tomb.RowIds().Contains(2))
	require.True(t, tomb.RowIds().Contains(3))
	require.Equal(t, types.XID(1), tomb.Stones()[0].Xid)
	require.Equal(t, types.XID(1), tomb.Stones()[1].Xid)
	require.Equal(t, types.XID(2), tomb.Stones()[2].Xid)
	require.False(t, tomb.dirty)
}

func TestTombMerge(t *testing.T) {
	tomb := newTomb(8)
	tomb.Append(1, 1, true) // tx 1 is complete
	tomb.Append(2, 2, true) // tx 2 is active (should be visible)
	tomb.Append(3, 3, true) // tx 3 is active (concurrent, unvisible)
	snap := types.NewSnapshot(2, 1, 4).AddActive(3)
	set := xroar.New()
	tomb.MergeVisible(set, snap)
	require.True(t, set.Contains(1))  // visible, committed
	require.True(t, set.Contains(2))  // visible, self
	require.False(t, set.Contains(3)) // invisible, concurrent
}

func BenchmarkTombAdd(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		tomb := newTomb(c.N)
		b.Run(c.Name, func(b *testing.B) {
			for b.Loop() {
				for i := range c.N {
					tomb.Append(types.XID(i+1), uint64(i+1), true)
				}
				tomb.Clear()
			}
		})
	}
}

func BenchmarkTombMerge(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		tomb := newTomb(c.N)
		snap := types.NewSnapshot(types.XID(c.N/2), types.XID(c.N/2), types.XID(c.N)+1)
		for i := range c.N {
			tomb.Append(types.XID(i+1), uint64(i+1), true)
			if i > c.N/2 {
				snap.AddActive(types.XID(i + 1))
			}
		}
		set := xroar.New()
		b.Run(c.Name, func(b *testing.B) {
			for b.Loop() {
				tomb.MergeVisible(set, snap)
			}
		})
	}
}
