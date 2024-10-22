// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func withTx(ctx context.Context, id int) context.Context {
	return context.WithValue(ctx, TransactionKey{}, &Tx{id: uint64(id)})
}

func makeTxCtx(id int) context.Context {
	return withTx(context.Background(), id)
}

func TestLockShared(t *testing.T) {
	m := NewLockManager()

	// first shared lock
	ok1, err := m.Lock(makeTxCtx(1), LockModeShared)
	require.True(t, ok1)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())

	// second shared lock
	ok2, err := m.Lock(makeTxCtx(2), LockModeShared)
	require.True(t, ok2)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())

	// release first locker
	m.Done(uint64(1))
	require.Equal(t, 1, m.Len())

	// third shared lock
	ok3, err := m.Lock(makeTxCtx(3), LockModeShared)
	require.True(t, ok3)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())

	// release all locks
	m.Done(uint64(2))
	m.Done(uint64(3))
	require.Equal(t, 0, m.Len())
}

func TestLockExclusive(t *testing.T) {
	m := NewLockManager().WithTimeout(10 * time.Millisecond)

	// first exclusive lock
	ok1, err := m.Lock(makeTxCtx(1), LockModeExclusive)
	require.True(t, ok1)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())

	// second exclusive lock (will timeout)
	ok2, err := m.Lock(makeTxCtx(2), LockModeExclusive)
	require.False(t, ok2)
	require.ErrorIs(t, err, ErrLockTimeout)
	require.Equal(t, 1, m.Len()) // still first lock exists

	// release first locker
	m.Done(uint64(1))
	require.Equal(t, 0, m.Len()) // no more lock

	// third exclusive lock
	ok3, err := m.Lock(makeTxCtx(3), LockModeExclusive)
	require.True(t, ok3)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())

	// release
	m.Done(uint64(3))
	require.Equal(t, 0, m.Len()) // no more lock

	// concurrent locks
	m.WithTimeout(10 * time.Second)
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		time.Sleep(10 * time.Millisecond)
		ok, err := m.Lock(withTx(ctx, 1), LockModeExclusive)
		if err != nil {
			return err
		}
		require.True(t, ok)
		time.Sleep(200 * time.Millisecond)
		m.Done(uint64(1))
		return nil
	})
	g.Go(func() error {
		time.Sleep(100 * time.Millisecond)
		ok, err := m.Lock(withTx(ctx, 2), LockModeExclusive)
		if err != nil {
			return err
		}
		require.True(t, ok)
		time.Sleep(200 * time.Millisecond)
		m.Done(uint64(2))
		return nil
	})

	require.NoError(t, g.Wait())
}

func TestLockTimeout(t *testing.T) {

}

func TestLockObjectShared(t *testing.T) {
	var r1 uint64 = 1 // resource oids
	m := NewLockManager()

	// first shared lock
	ok1, err := m.LockObject(makeTxCtx(1), LockModeShared, r1)
	require.True(t, ok1)
	require.NoError(t, err)
	require.Equal(t, 2, m.Len()) // global + object

	// second shared lock
	ok2, err := m.LockObject(makeTxCtx(2), LockModeShared, r1)
	require.True(t, ok2)
	require.NoError(t, err)
	require.Equal(t, 2, m.Len())

	// release first locker
	m.Done(uint64(1))
	require.Equal(t, 2, m.Len())

	// third shared lock
	ok3, err := m.LockObject(makeTxCtx(3), LockModeShared, r1)
	require.True(t, ok3)
	require.NoError(t, err)
	require.Equal(t, 2, m.Len())

	// release all locks
	m.Done(uint64(2))
	m.Done(uint64(3))
	require.Equal(t, 0, m.Len())

}

func TestLockObjectExclusive(t *testing.T) {
	var r1 uint64 = 1 // resource oids
	m := NewLockManager().WithTimeout(10 * time.Millisecond)

	// first exclusive lock
	ok1, err := m.LockObject(makeTxCtx(1), LockModeExclusive, r1)
	require.True(t, ok1)
	require.NoError(t, err)
	require.Equal(t, 2, m.Len()) // global + object

	// second exclusive lock (will timeout)
	ok2, err := m.LockObject(makeTxCtx(2), LockModeExclusive, r1)
	require.False(t, ok2)
	require.ErrorIs(t, err, ErrLockTimeout)
	require.Equal(t, 2, m.Len()) // still first lock exists

	// release first locker
	m.Done(uint64(1))
	require.Equal(t, 0, m.Len()) // no more lock

	// third exclusive lock
	ok3, err := m.LockObject(makeTxCtx(3), LockModeExclusive, r1)
	require.True(t, ok3)
	require.NoError(t, err)
	require.Equal(t, 2, m.Len())

	// release all locks
	m.Done(uint64(3))
	require.Equal(t, 0, m.Len())
}

func TestLockNoTx(t *testing.T) {
	m := NewLockManager()
	ok, err := m.Lock(context.Background(), LockModeShared)
	require.False(t, ok)
	require.ErrorIs(t, err, ErrNoTx)
}

func TestLockDeadlock(t *testing.T) {
	//                 ┏━━━━┓
	//   granted───────┃ x1 ┃──────waiting
	//      │          ┗━━━━┛            │
	//      │                            │
	//   ┌──▼─┐                       ┌──▼─┐
	//   │ R1 │                       │ R2 │
	//   └────┘                       └──▲─┘
	// next wait?     Deadlock       granted
	//                                   │
	//   ┏━━━━┓                       ┏━━┻━┓
	//   ┃ x3 ┃                       ┃ x2 ┃
	//   ┗━┳━━┛                       ┗━━┳━┛
	//     │                             │
	//     │           ┌────┐            │
	//     └─granted───▶ R3 ◀────waiting─┘
	//                 └────┘
	var r1, r2, r3 uint64 = 1, 2, 3 // resource oids
	g, ctx := errgroup.WithContext(context.Background())
	x1, x2, x3 := withTx(ctx, 1), withTx(ctx, 2), withTx(ctx, 3)
	m := NewLockManager()

	ok1, err := m.LockObject(x1, LockModeExclusive, r1)
	require.True(t, ok1)
	require.NoError(t, err)
	require.Equal(t, 2, m.Len()) // global + object

	ok2, err := m.LockObject(x2, LockModeExclusive, r2)
	require.True(t, ok2)
	require.NoError(t, err)
	require.Equal(t, 3, m.Len()) // global + object

	ok3, err := m.LockObject(x3, LockModeExclusive, r3)
	require.True(t, ok3)
	require.NoError(t, err)
	require.Equal(t, 4, m.Len()) // global + object

	g.Go(func() error {
		// blocks
		_, err := m.LockObject(x1, LockModeExclusive, r2)
		return err
	})
	time.Sleep(10 * time.Millisecond)
	g.Go(func() error {
		// blocks
		_, err := m.LockObject(x2, LockModeExclusive, r3)
		return err
	})
	time.Sleep(10 * time.Millisecond)
	g.Go(func() error {
		// deadlock
		_, err := m.LockObject(x3, LockModeExclusive, r1)
		return err
	})

	require.ErrorIs(t, g.Wait(), ErrDeadlock)
}

func TestLockContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	x1 := withTx(ctx, 1)
	m := NewLockManager()
	cancel()

	ok1, err := m.Lock(x1, LockModeExclusive)
	require.False(t, ok1)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, m.Len())

	ok2, err := m.Lock(withTx(ctx, 2), LockModeExclusive)
	require.False(t, ok2)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, m.Len())

}

func BenchmarkLockObject(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1, 2, 8, 32} {
		// open locks for N resources (actually N+1 due to shared global lock)
		m := NewLockManager()
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				xid := 1
				for pb.Next() {
					x := withTx(ctx, xid)
					for i := 0; i < n; i++ {
						_, _ = m.LockObject(x, LockModeShared, uint64(i))
					}
					m.Done(uint64(xid))
					xid++
				}
			})
		})
	}
}

func BenchmarkLockGlobal(b *testing.B) {
	ctx := context.Background()
	m := NewLockManager()
	b.RunParallel(func(pb *testing.PB) {
		xid := 1
		for pb.Next() {
			x := withTx(ctx, xid)
			_, _ = m.Lock(x, LockModeShared)
			m.Done(uint64(xid))
			xid++
		}
	})
}
