// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func withTx(ctx context.Context, id int) context.Context {
	return context.WithValue(ctx, TransactionKey{}, &Tx{id: uint64(id), ctx: ctx})
}

func makeTxCtx(id int) context.Context {
	return withTx(context.Background(), id)
}

func TestLockShared(t *testing.T) {
	m := NewLockManager()

	// first shared lock
	ok1, err := m.Lock(makeTxCtx(1), LockModeShared, 1)
	require.True(t, ok1)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())

	// second shared lock
	ok2, err := m.Lock(makeTxCtx(2), LockModeShared, 1)
	require.True(t, ok2)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())

	// release first locker
	m.Done(uint64(1))
	require.Equal(t, 1, m.Len())

	// third shared lock
	ok3, err := m.Lock(makeTxCtx(3), LockModeShared, 1)
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
	ok1, err := m.Lock(makeTxCtx(1), LockModeExclusive, 1)
	require.True(t, ok1)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())

	// second exclusive lock (will timeout)
	ok2, err := m.Lock(makeTxCtx(2), LockModeExclusive, 1)
	require.False(t, ok2)
	require.ErrorIs(t, err, ErrLockTimeout)
	require.Equal(t, 1, m.Len()) // still first lock exists

	// release first locker
	m.Done(uint64(1))
	require.Equal(t, 0, m.Len()) // no more lock

	// third exclusive lock
	ok3, err := m.Lock(makeTxCtx(3), LockModeExclusive, 1)
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
		ok, err := m.Lock(withTx(ctx, 1), LockModeExclusive, 1)
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
		ok, err := m.Lock(withTx(ctx, 2), LockModeExclusive, 1)
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

func TestLockConcurrent(t *testing.T) {
	m := NewLockManager().WithTimeout(10 * time.Second)
	g, ctx := errgroup.WithContext(context.Background())
	n := 64
	g.SetLimit(n)
	for i := 1; i <= n; i++ {
		k := i
		g.Go(func() error {
			for j := 4 * k; j <= 4*k+4; j++ {
				_, err := m.Lock(withTx(ctx, j), LockModeExclusive, 1)
				if err != nil {
					return err
				}
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				m.Done(uint64(j))
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())
}

func TestLockNoTx(t *testing.T) {
	m := NewLockManager()
	ok, err := m.Lock(context.Background(), LockModeShared, 1)
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
	g, ctx := errgroup.WithContext(context.Background())
	x1, x2, x3 := withTx(ctx, 1), withTx(ctx, 2), withTx(ctx, 3)
	m := NewLockManager()

	ok1, err := m.Lock(x1, LockModeExclusive, 1)
	require.True(t, ok1)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len()) // global + object

	ok2, err := m.Lock(x2, LockModeExclusive, 2)
	require.True(t, ok2)
	require.NoError(t, err)
	require.Equal(t, 2, m.Len()) // global + object

	ok3, err := m.Lock(x3, LockModeExclusive, 3)
	require.True(t, ok3)
	require.NoError(t, err)
	require.Equal(t, 3, m.Len()) // global + object

	g.Go(func() error {
		// blocks
		t.Log("Locking X1->R2")
		_, err := m.Lock(x1, LockModeExclusive, 2)
		return err
	})
	time.Sleep(10 * time.Millisecond)
	g.Go(func() error {
		// blocks
		t.Log("Locking X2->R3")
		_, err := m.Lock(x2, LockModeExclusive, 3)
		return err
	})
	time.Sleep(10 * time.Millisecond)
	g.Go(func() error {
		// deadlock
		t.Log("Locking X3->R1")
		_, err := m.Lock(x3, LockModeExclusive, 1)
		return err
	})

	require.ErrorIs(t, g.Wait(), ErrDeadlock)
}

func TestLockContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	x1 := withTx(ctx, 1)
	m := NewLockManager()
	cancel()

	ok1, err := m.Lock(x1, LockModeExclusive, 1)
	require.False(t, ok1)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, m.Len())

	ok2, err := m.Lock(withTx(ctx, 2), LockModeExclusive, 1)
	require.False(t, ok2)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, m.Len())

}

func BenchmarkLockShared(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1, 2, 8, 32} {
		// open locks for N resources
		m := NewLockManager().WithTimeout(0)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				xid := 1
				for pb.Next() {
					x := withTx(ctx, xid)
					for i := 0; i < n; i++ {
						_, _ = m.Lock(x, LockModeShared, uint64(i))
					}
					m.Done(uint64(xid))
					xid++
				}
			})
		})
	}
}

func BenchmarkLockExclusive(b *testing.B) {
	ctx := context.Background()
	m := NewLockManager().WithTimeout(0)
	b.ReportAllocs()
	var id int64 = 1
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			xid := atomic.AddInt64(&id, 1)
			x := withTx(ctx, int(xid))
			_, _ = m.Lock(x, LockModeExclusive, 1)
			m.Done(uint64(xid))
		}
	})
}
