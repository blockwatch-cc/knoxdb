// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestLockShared(t *testing.T) {
	m := NewLockManager()

	// first shared lock
	require.NoError(t, m.Lock(context.Background(), 1, LockModeShared, 1))
	require.Equal(t, 1, m.Len())

	// second shared lock
	require.NoError(t, m.Lock(context.Background(), 2, LockModeShared, 1))
	require.Equal(t, 1, m.Len())

	// release first locker
	m.Done(1)
	require.Equal(t, 1, m.Len())

	// third shared lock
	require.NoError(t, m.Lock(context.Background(), 3, LockModeShared, 1))
	require.Equal(t, 1, m.Len())

	// release all locks
	m.Done(2)
	m.Done(3)
	require.Equal(t, 0, m.Len())
}

func TestLockExclusive(t *testing.T) {
	m := NewLockManager().WithTimeout(10 * time.Millisecond)

	// first exclusive lock
	require.NoError(t, m.Lock(context.Background(), 1, LockModeExclusive, 1))
	require.Equal(t, 1, m.Len())

	// second exclusive lock (will timeout)
	require.ErrorIs(t, m.Lock(context.Background(), 2, LockModeExclusive, 1), ErrLockTimeout)
	require.Equal(t, 1, m.Len()) // still first lock exists

	// release first locker
	m.Done(1)
	require.Equal(t, 0, m.Len()) // no more lock

	// third exclusive lock
	require.NoError(t, m.Lock(context.Background(), 3, LockModeExclusive, 1))
	require.Equal(t, 1, m.Len())

	// release
	m.Done(3)
	require.Equal(t, 0, m.Len()) // no more lock

	// concurrent locks
	m.WithTimeout(10 * time.Second)
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		time.Sleep(10 * time.Millisecond)
		err := m.Lock(ctx, 1, LockModeExclusive, 1)
		if err != nil {
			return err
		}
		time.Sleep(200 * time.Millisecond)
		m.Done(1)
		return nil
	})
	g.Go(func() error {
		time.Sleep(100 * time.Millisecond)
		err := m.Lock(ctx, 2, LockModeExclusive, 1)
		if err != nil {
			return err
		}
		time.Sleep(200 * time.Millisecond)
		m.Done(2)
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
				err := m.Lock(ctx, XID(j), LockModeExclusive, 1)
				if err != nil {
					return err
				}
				time.Sleep(time.Duration(util.RandIntn(10)) * time.Millisecond)
				m.Done(XID(j))
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())
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
	m := NewLockManager()

	require.NoError(t, m.Lock(ctx, 1, LockModeExclusive, 1))
	require.Equal(t, 1, m.Len())

	require.NoError(t, m.Lock(ctx, 2, LockModeExclusive, 2))
	require.Equal(t, 2, m.Len())
	require.NoError(t, m.Lock(ctx, 3, LockModeExclusive, 3))
	require.Equal(t, 3, m.Len())

	g.Go(func() error {
		// blocks
		t.Log("Locking X1->R2")
		return m.Lock(ctx, 1, LockModeExclusive, 2)
	})
	time.Sleep(10 * time.Millisecond)
	g.Go(func() error {
		// blocks
		t.Log("Locking X2->R3")
		return m.Lock(ctx, 2, LockModeExclusive, 3)
	})
	time.Sleep(10 * time.Millisecond)
	g.Go(func() error {
		// deadlock
		t.Log("Locking X3->R1")
		return m.Lock(ctx, 3, LockModeExclusive, 1)
	})

	require.ErrorIs(t, g.Wait(), ErrDeadlock)
}

func TestLockContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	m := NewLockManager()
	cancel()

	// context is only checked when waiting
	require.NoError(t, m.Lock(ctx, 1, LockModeExclusive, 1))
	require.Equal(t, 1, m.Len())

	require.ErrorIs(t, m.Lock(ctx, 2, LockModeExclusive, 1), context.Canceled)
	require.Equal(t, 1, m.Len())

	m.Done(1)
	m.Done(2)

	require.NoError(t, m.Lock(ctx, 1, LockModeShared, 1))
	require.Equal(t, 1, m.Len())
	require.NoError(t, m.Lock(ctx, 2, LockModeShared, 1))
	require.Equal(t, 1, m.Len())
}

func BenchmarkLockShared(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1, 2, 8, 32} {
		// open locks for N resources
		m := NewLockManager().WithTimeout(0)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				var xid XID = 1
				for pb.Next() {
					for i := range n {
						_ = m.Lock(ctx, xid, LockModeShared, uint64(i))
					}
					m.Done(xid)
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
			xid := XID(atomic.AddInt64(&id, 1))
			_ = m.Lock(ctx, xid, LockModeExclusive, 1)
			m.Done(xid)
		}
	})
}
