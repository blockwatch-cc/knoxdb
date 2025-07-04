// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTx(t *testing.T) {
	e := NewTestEngine(t, NewTestDatabaseOptions(t, "mem"))

	// 1 start write tx
	t1 := e.NewTransaction(0)
	<-e.writeToken

	// check write tx
	// - xid
	// - horizon (xmin)
	// - snapshot
	// - tx list
	require.NotNil(t, t1)
	assert.Equal(t, XID(1), t1.Id(), "xid")
	assert.Equal(t, TxFlags(0), t1.flags, "flags")
	assert.Equal(t, XID(1), e.xmin, "xmin")
	assert.Equal(t, XID(2), e.xnext, "xnext")
	assert.Equal(t, ReadTxOffset, e.vnext, "vnext")
	assert.Equal(t, e.xnext, t1.snap.Xmax, "snap.xmax == xnext")
	assert.Equal(t, t1.id, t1.snap.Xmin, "snap.xmin == xid")
	assert.Equal(t, t1.id, t1.snap.Xown, "snap.xown == xid")
	assert.True(t, t1.snap.Safe, "snap.safe")
	assert.Len(t, e.txs, 1, "txs len")
	assert.False(t, t1.IsClosed(), "open")
	assert.False(t, t1.IsAborted(), "not yet aborted")
	assert.False(t, t1.IsCommitted(), "not yet commited")

	// 2 keep write tx and start read tx
	t2 := e.NewTransaction(TxFlagReadOnly)

	// check read tx
	// - xid
	// - horizon (xmin)
	// - snapshot
	// - tx list
	// - snapshot does not see t1
	require.NotNil(t, t2)
	assert.Equal(t, ReadTxOffset, t2.Id(), "xid")
	assert.Equal(t, TxFlagReadOnly, t2.flags, "flags")
	assert.Equal(t, XID(1), e.xmin, "xmin")
	assert.Equal(t, XID(2), e.xnext, "xnext")
	assert.Equal(t, ReadTxOffset+1, e.vnext, "vnext")
	assert.Equal(t, e.xnext, t2.snap.Xmax, "snap.xmax == xnext")
	assert.Equal(t, t1.id, t2.snap.Xmin, "snap.xmin == t1.xid")
	assert.Equal(t, XID(0), t2.snap.Xown, "snap.xown == 0")
	assert.False(t, t2.snap.Safe, "snap.safe")
	assert.Len(t, e.txs, 2, "txs len")
	assert.False(t, t2.snap.IsVisible(t1.id), "t2 cannot see t1")
	assert.True(t, t2.snap.IsConflict(t1.id), "t2 has RW conflict with t1")
	assert.False(t, t2.IsClosed(), "open")
	assert.False(t, t2.IsAborted(), "not yet aborted")
	assert.False(t, t2.IsCommitted(), "not yet commited")

	// 3 close t1 (will send write token)
	require.NoError(t, t1.Abort(), "close t1")

	// check
	// - horizon (xmin)
	// - tx list
	assert.True(t, t1.IsClosed(), "closed")
	assert.Len(t, e.txs, 1, "txs len")
	assert.Equal(t, XID(2), e.xmin, "xmin")
	assert.True(t, t1.IsClosed(), "closed")
	assert.True(t, t1.IsAborted(), "aborted")
	assert.False(t, t1.IsCommitted(), "not commited")

	// 4 start new read tx
	t3 := e.NewTransaction(TxFlagReadOnly)
	require.NotNil(t, t3)
	assert.Equal(t, ReadTxOffset+1, t3.Id(), "xid")
	assert.Equal(t, TxFlagReadOnly, t3.flags, "flags")
	assert.Equal(t, XID(2), e.xmin, "xmin")
	assert.Equal(t, XID(2), e.xnext, "xnext")
	assert.Equal(t, ReadTxOffset+2, e.vnext, "vnext")
	assert.Equal(t, e.xnext, t3.snap.Xmax, "snap.xmax == xnext")
	assert.Equal(t, XID(2), t3.snap.Xmin, "snap.xmin == t1.xid")
	assert.Equal(t, XID(0), t3.snap.Xown, "snap.xown == 0")
	assert.True(t, t3.snap.Safe, "snap.safe")
	assert.Len(t, e.txs, 2, "txs len")
	assert.True(t, t3.snap.IsVisible(t1.id), "t3 can see effects of t1")
	assert.False(t, t3.snap.IsConflict(t1.id), "t3 has no conflict with t1")
	assert.False(t, t3.IsClosed(), "open")
	assert.False(t, t3.IsAborted(), "not yet aborted")
	assert.False(t, t3.IsCommitted(), "not yet commited")

	// cleanup
	require.NoError(t, t2.Abort(), "close t2")
	require.NoError(t, t3.Commit(), "close t3")
	assert.Len(t, e.txs, 0, "txs len")
	assert.True(t, t2.IsClosed(), "closed")
	assert.True(t, t3.IsClosed(), "closed")
	assert.True(t, t2.IsAborted(), "aborted")
	assert.True(t, t3.IsCommitted(), "commited")
}

func TestWithReadTx(t *testing.T) {
	e := NewTestEngine(t, NewTestDatabaseOptions(t, "mem"))
	ctx := context.Background()
	ctx, tx, commit, abort, err := e.WithTransaction(ctx, TxFlagReadOnly)

	// check return args
	require.NoError(t, err)
	require.NotNil(t, tx, "tx")
	require.NotNil(t, commit, "commit func")
	require.NotNil(t, abort, "abort func")

	// check context
	require.NotNil(t, GetEngine(ctx), "ctx.engine")
	require.NotNil(t, GetTransaction(ctx), "ctx.tx")
	require.NotNil(t, GetSnapshot(ctx), "ctx.snap")
	require.Equal(t, XID(ReadTxOffset), GetTxId(ctx), "ctx.xid")

	// check we get the same tx, context, and noop funcs when calling again
	ctx2, tx2, commit2, abort2, err := e.WithTransaction(ctx, TxFlagReadOnly)

	// check return args
	require.NoError(t, err)
	require.NotNil(t, tx2, "tx")
	require.NotNil(t, commit2, "commit func")
	require.NotNil(t, abort2, "abort func")

	// check context
	require.NotNil(t, GetEngine(ctx2), "ctx.engine")
	require.NotNil(t, GetTransaction(ctx2), "ctx.tx")
	require.NotNil(t, GetSnapshot(ctx2), "ctx.snap")
	require.Equal(t, XID(ReadTxOffset), GetTxId(ctx2), "ctx.xid")

	// check we get an error when we try switching to read-write
	_, _, _, _, err = e.WithTransaction(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, ErrTxReadonly, err)
}

func TestWithWriteTx(t *testing.T) {
	e := NewTestEngine(t, NewTestDatabaseOptions(t, "mem"))
	ctx := context.Background()
	ctx, tx, commit, abort, err := e.WithTransaction(ctx)

	// check return args
	require.NoError(t, err)
	require.NotNil(t, tx, "tx")
	require.NotNil(t, commit, "commit func")
	require.NotNil(t, abort, "abort func")

	// check context
	require.NotNil(t, GetEngine(ctx), "ctx.engine")
	require.NotNil(t, GetTransaction(ctx), "ctx.tx")
	require.NotNil(t, GetSnapshot(ctx), "ctx.snap")
	require.Equal(t, XID(1), GetTxId(ctx), "ctx.xid")

	// check we get the same tx, context, and noop funcs when calling again
	ctx2, tx2, commit2, abort2, err := e.WithTransaction(ctx)

	// check return args
	require.NoError(t, err)
	require.NotNil(t, tx2, "tx")
	require.NotNil(t, commit2, "commit func")
	require.NotNil(t, abort2, "abort func")

	// check context
	require.NotNil(t, GetEngine(ctx2), "ctx.engine")
	require.NotNil(t, GetTransaction(ctx2), "ctx.tx")
	require.NotNil(t, GetSnapshot(ctx2), "ctx.snap")
	require.Equal(t, XID(1), GetTxId(ctx2), "ctx.xid")

	// check we get no error when we try switching to read-only
	_, _, _, _, err = e.WithTransaction(ctx, TxFlagReadOnly)
	assert.NoError(t, err)
}

func TestConcurrentReadTx(t *testing.T) {
	e := NewTestEngine(t, NewTestDatabaseOptions(t, "mem"))
	ctx := context.Background()

	// writer
	_, _, _, abort, err := e.WithTransaction(ctx)
	require.NoError(t, err)

	// reader 1
	require.Eventually(t, func() bool {
		_, _, _, abort, err := e.WithTransaction(ctx, TxFlagReadOnly)
		require.NoError(t, err)
		require.NoError(t, abort())
		return true
	}, 10*time.Millisecond, 5*time.Millisecond)

	// reader 2
	require.Eventually(t, func() bool {
		_, _, _, abort, err := e.WithTransaction(ctx, TxFlagReadOnly)
		require.NoError(t, err)
		require.NoError(t, abort())
		return true
	}, 10*time.Millisecond, 5*time.Millisecond)

	require.NoError(t, abort())
}

func TestWriteTxWait(t *testing.T) {
	e := NewTestEngine(t, NewTestDatabaseOptions(t, "mem"))
	ctx := context.Background()

	// non error cases
	// nowait (with no tx)
	{
		_, _, _, abort, err := e.WithTransaction(ctx, TxFlagNoWait)
		require.NoError(t, err)
		require.NoError(t, abort())
		require.Len(t, e.txs, 0, "txs")
	}

	// timeout (with 1st tx release)
	{
		var lastXid XID
		e.opts.TxWaitTimeout = time.Second
		_, tx, _, abort, err := e.WithTransaction(ctx)
		require.NoError(t, err)
		go func() {
			time.Sleep(20 * time.Millisecond)
			xid := tx.id
			require.NoError(t, abort())
			atomic.StoreUint64((*uint64)(&lastXid), uint64(xid))
		}()
		require.Eventually(t, func() bool {
			_, tx, _, abort, err := e.WithTransaction(ctx)
			xid := tx.id
			require.NoError(t, err)
			require.NoError(t, abort())
			atomic.StoreUint64((*uint64)(&lastXid), uint64(xid))
			return true
		}, 2*e.opts.TxWaitTimeout, 5*time.Millisecond)
		assert.True(t, tx.IsClosed(), "1st tx closed")
		assert.Equal(t, XID(3), lastXid, "last tx closed")
		assert.Len(t, e.txs, 0, "txs")
		e.opts.TxWaitTimeout = 0
	}

	// unlimited (with 1st tx release)
	{
		var lastXid XID
		_, tx, _, abort, err := e.WithTransaction(ctx)
		require.NoError(t, err)
		go func() {
			time.Sleep(20 * time.Millisecond)
			require.NoError(t, abort())
			atomic.StoreUint64((*uint64)(&lastXid), uint64(tx.id))
		}()
		require.Eventually(t, func() bool {
			_, tx, _, abort, err := e.WithTransaction(ctx)
			xid := tx.id
			require.NoError(t, err)
			require.NoError(t, abort())
			atomic.StoreUint64((*uint64)(&lastXid), uint64(xid))
			return true
		}, time.Second, 5*time.Millisecond)
		assert.True(t, tx.IsClosed(), "1st tx closed")
		assert.Equal(t, XID(5), lastXid, "last tx closed")
		assert.Len(t, e.txs, 0, "txs")
	}

	// error cases with concurrent writer
	_, tx, _, _, err := e.WithTransaction(ctx)
	require.NoError(t, err)

	// nowait throws error
	{
		_, _, _, _, err := e.WithTransaction(ctx, TxFlagNoWait)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrTxConflict)
	}

	// with timeout throws error
	{
		e.opts.TxWaitTimeout = time.Second
		require.Eventually(t, func() bool {
			_, _, _, _, err := e.WithTransaction(ctx)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrTxTimeout)
			return true
		}, 2*e.opts.TxWaitTimeout, 5*time.Millisecond)
		e.opts.TxWaitTimeout = 0
	}

	// unlimited, throws error on shutdown
	{
		go func() {
			// simulate shutdown (order matters!)
			e.shutdown.Store(true)
			tx.Kill(ErrDatabaseShutdown)
			close(e.writeToken)
		}()
		require.Eventually(t, func() bool {
			_, _, _, _, err := e.WithTransaction(ctx)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrDatabaseShutdown)
			return true
		}, time.Second, 5*time.Millisecond)
	}
}

func TestTxReadOnlyEngine(t *testing.T) {
	e := NewTestEngine(t, NewTestDatabaseOptions(t, "mem").WithReadOnly())
	ctx := context.Background()

	// read is ok
	_, _, _, _, err := e.WithTransaction(ctx, TxFlagReadOnly)
	require.NoError(t, err)

	// write on read-only engine throws error
	_, _, _, _, err = e.WithTransaction(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDatabaseReadOnly)
}

func TestReadTxShutdown(t *testing.T) {
	e := NewTestEngine(t, NewTestDatabaseOptions(t, "mem"))
	ctx := context.Background()

	// open tx before shutdown
	ctx, tx, commit, abort, err := e.WithTransaction(ctx, TxFlagReadOnly)

	// simulate shutdown (order matters!)
	go func() {
		e.shutdown.Store(true)
		tx.Kill(ErrDatabaseShutdown)
		close(e.writeToken)
	}()

	// tx should be aborted, context canceled
	require.Eventually(t, func() bool {
		<-ctx.Done()
		return true
	}, time.Second, 5*time.Millisecond)

	assert.True(t, tx.IsClosed())
	assert.True(t, tx.IsAborted())
	assert.Error(t, tx.Err())
	assert.ErrorIs(t, tx.Err(), ErrDatabaseShutdown)

	// funcs return the error
	err = abort()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseShutdown)
	err = commit()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseShutdown)
}

func TestWriteTxShutdown(t *testing.T) {
	e := NewTestEngine(t, NewTestDatabaseOptions(t, "mem"))
	ctx := context.Background()

	// open tx before shutdown
	ctx, tx, commit, abort, err := e.WithTransaction(ctx)

	// simulate shutdown (order matters!)
	go func() {
		e.shutdown.Store(true)
		tx.Kill(ErrDatabaseShutdown)
		close(e.writeToken)
	}()

	// tx should be aborted, context canceled
	require.Eventually(t, func() bool {
		<-ctx.Done()
		return true
	}, time.Second, 5*time.Millisecond)

	assert.True(t, tx.IsClosed())
	assert.True(t, tx.IsAborted())
	assert.Error(t, tx.Err())
	assert.ErrorIs(t, tx.Err(), ErrDatabaseShutdown)

	// funcs return the error
	err = abort()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseShutdown)
	err = commit()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseShutdown)
}

func TestTxCallbacks(t *testing.T) {
	e := NewTestEngine(t, NewTestDatabaseOptions(t, "mem"))
	ctx := context.Background()
	truthy := func() bool { return true }
	falsy := func() bool { return false }

	// test commit
	{
		_, tx, commit, _, err := e.WithTransaction(ctx)
		require.NoError(t, err)

		tx.OnCommit(func(ctx context.Context) error {
			require.Eventually(t, truthy, 20*time.Millisecond, 5*time.Millisecond, "committed")
			return nil
		})
		tx.OnAbort(func(ctx context.Context) error {
			require.Never(t, falsy, 20*time.Millisecond, 5*time.Millisecond, "aborted")
			return nil
		})
		require.NoError(t, commit())
	}

	// test abort
	{
		_, tx, _, abort, err := e.WithTransaction(ctx)
		require.NoError(t, err)

		tx.OnCommit(func(ctx context.Context) error {
			require.Never(t, falsy, 20*time.Millisecond, 5*time.Millisecond, "committed")
			return nil
		})
		tx.OnAbort(func(ctx context.Context) error {
			require.Eventually(t, truthy, 20*time.Millisecond, 5*time.Millisecond, "aborted")
			return nil
		})
		require.NoError(t, abort())
	}
}

func TestTxLocks(t *testing.T) {
	// - locks count after commit/abort
	e := NewTestEngine(t, NewTestDatabaseOptions(t, "mem"))
	ctx := context.Background()

	// committing
	{
		assert.Equal(t, 0, e.lm.Len())
		ctx, tx, commit, _, err := e.WithTransaction(ctx)
		require.NoError(t, err)
		tx.Lock(ctx, 1)
		tx.RLock(ctx, 2)
		assert.Equal(t, 2, e.lm.Len())
		require.NoError(t, commit())
		assert.Equal(t, 0, e.lm.Len())
	}

	// aborting
	{
		ctx, tx, _, abort, err := e.WithTransaction(ctx)
		require.NoError(t, err)
		tx.Lock(ctx, 1)
		tx.RLock(ctx, 2)
		assert.Equal(t, 2, e.lm.Len())
		require.NoError(t, abort())
		assert.Equal(t, 0, e.lm.Len())
	}
}

func TestTxFail(t *testing.T) {
	e := NewTestEngine(t, NewTestDatabaseOptions(t, "mem"))
	ctx := context.Background()
	ctx, tx, _, _, err := e.WithTransaction(ctx)
	require.NoError(t, err)
	tx.Fail(ErrTxConflict)
	assert.False(t, tx.IsClosed())
	assert.False(t, tx.IsAborted())
	assert.False(t, tx.IsCommitted())
	assert.Error(t, ctx.Err())
	assert.Error(t, tx.Err())
	assert.ErrorIs(t, tx.Err(), ErrTxConflict)
}

func BenchmarkReadTx(b *testing.B) {
	e := NewTestEngine(b, NewTestDatabaseOptions(b, "mem"))
	ctx := context.Background()
	for b.Loop() {
		_, _, _, abort, _ := e.WithTransaction(ctx, TxFlagReadOnly)
		abort()
	}
}

func BenchmarkWriteTxCommit(b *testing.B) {
	e := NewTestEngine(b, NewTestDatabaseOptions(b, "mem"))
	ctx := context.Background()
	for b.Loop() {
		_, _, commit, _, _ := e.WithTransaction(ctx)
		commit()
	}
}

func BenchmarkWriteTxAbort(b *testing.B) {
	e := NewTestEngine(b, NewTestDatabaseOptions(b, "mem"))
	ctx := context.Background()
	for b.Loop() {
		_, _, _, abort, _ := e.WithTransaction(ctx)
		abort()
	}
}
