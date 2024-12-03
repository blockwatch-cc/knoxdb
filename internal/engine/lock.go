// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// TODO
// - predicate lock (match range overlaps, =,<,> cond)
// - see query.FilterTreeNode.Overlaps() // unimplemneted

var (
	ErrLockTimeout = errors.New("canceled due to lock timeout")
	ErrDeadlock    = errors.New("deadlock detected")
)

type LockMode byte

const (
	LockModeShared LockMode = iota
	LockModeExclusive
)

// LockType defines the scope of a lock. Scopes may express nested or overlap.
// in which case a txn may hold multiple locks on the same resource (i.e.
// a table lock and multiple row locks identified by row id ranges).
type LockType byte

const (
	LockTypeObject    LockType = iota // single container object (table, store, index)
	LockTypePredicate                 // query condition (may be used for row id or key ranges)
)

// invariants
// - at most one lock per type & resource combination exists (either shared or exclusive state)
// - no locks with count == 0 and waiting == 0 exist
// - a shared lock has waiting > 0 only if exclusive lock is requested
// - an exclusive lock has waiting > 0 if subsequent exclusive or shared locks are requested
type LockManager struct {
	mu      sync.Mutex
	timeout time.Duration
	locks   []*lock            // all granted locks, use chan for exclusive access
	granted map[uint64][]*lock // map of tx id to locks granted
	nlocks  int64              // total number of locks currently in existence
}

func NewLockManager() *LockManager {
	m := &LockManager{
		timeout: 10 * time.Second,
		locks:   make([]*lock, 0),
		granted: make(map[uint64][]*lock),
	}
	return m
}

func (m *LockManager) WithTimeout(t time.Duration) *LockManager {
	m.timeout = t
	return m
}

func (m *LockManager) Len() int {
	return int(atomic.LoadInt64(&m.nlocks))
}

// waits for all locks to be released
func (m *LockManager) Wait() {
	for m.Len() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
}

// drops any outstanding locks (call after cancelling tx contexts)
func (m *LockManager) Clear() {
	// exclusive access
	m.mu.Lock()
	defer m.mu.Unlock()

	// unblock waiters (acquire() may have already unblocked them when
	// tx contexts were canceled so this may be a noop), still cleaning up
	// references
	for _, lock := range m.locks {
		for _, v := range lock.waiters {
			close(v)
			clear(lock.waiters)
			clear(lock.waiting)
		}
		lock.count = 0
	}
	clear(m.locks)
	clear(m.granted)
	m.nlocks = 0
}

// Lock represents a lock on a unique database resource. Each resource has at most
// one lock assigned. During its lifecycle, the lock may change state between shared
// and exclusive multiple times, as request order requires. To save memory we only
// store the waiting transaction id for pending lock requests. Other arguments
// are available in the stack frame of the goroutine that called acquire(). This
// goroutine is able to identify itself and continues to take the lock once its
// xid is the next in order. When multiple goroutines attempt to acquire the same
// resource lock under the same xid, the Go scheduler determines order.
type lock struct {
	typ       LockType                 // object or predicate
	exclusive bool                     // flag indicating if this lock is exclusive or shared
	oid       uint64                   // container id when type is object or predicate
	count     int                      // shared lock reference counter
	waiting   []uint64                 // xids waiting to inherit or replace the lock (in request order)
	waiters   map[uint64]chan struct{} //wait channels per xid
}

func (l *lock) empty() bool {
	return len(l.waiting) == 0
}

func (l *lock) pop() {
	if len(l.waiting) > 0 {
		delete(l.waiters, l.waiting[0])
		l.waiting = l.waiting[1:]
	}
}

func (l *lock) yield() {
	if len(l.waiting) > 0 {
		close(l.waiters[l.waiting[0]])
	}
}

func (l *lock) wait(xid uint64) chan struct{} {
	l.waiting = append(l.waiting, xid)
	ch := make(chan struct{}, 1)
	if l.waiters == nil {
		l.waiters = make(map[uint64]chan struct{})
	}
	l.waiters[xid] = ch
	return ch
}

func (l *lock) drop(xid uint64) {
	l.waiting = slices.DeleteFunc(l.waiting, func(i uint64) bool {
		return i == xid
	})
	if ch, ok := l.waiters[xid]; ok {
		close(ch)
		delete(l.waiters, xid)
	}
}

var lockPool = sync.Pool{
	New: func() any { return new(lock) },
}

// Lock obtains a lock on a specific object.
func (m *LockManager) Lock(ctx context.Context, xid uint64, mode LockMode, oid uint64) error {
	// upgrade context with timeout
	if m.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.timeout)
		defer cancel()
	}

	return m.acquire(ctx, xid, mode, LockTypeObject, oid, nil)
}

// TODO: not yet supported
func (m *LockManager) LockPredicate(ctx context.Context, xid uint64, mode LockMode, oid uint64, pred ConditionMatcher) error {
	tx := GetTransaction(ctx)

	// check tx status
	if err := tx.Err(); err != nil {
		return err
	}

	// upgrade context with timeout
	// if m.timeout > 0 {
	// 	var cancel context.CancelFunc
	// 	ctx, cancel = context.WithTimeout(ctx, m.timeout)
	// 	defer cancel()
	// }

	//  //  obtain a nested shared object lock
	//  ok, err = m.acquire(ctx, tx.id, LockModeShared, LockTypeObject, oid, nil)
	//  if err != nil {
	//      return false, err
	//  }

	// // then obtain the predicate lock
	// return m.acquire(ctx, tx.id, LockModeShared, LockTypePredicate, oid, pred)
	return ErrNotImplemented
}

// Done releases all locks acquired by the given transaction xid.
func (m *LockManager) Done(xid uint64) {
	// exclusive access
	m.mu.Lock()
	defer m.mu.Unlock()

	// release all locks owned by xid
	for _, l := range m.granted[xid] {
		l.count--
		l.yield()
	}
	delete(m.granted, xid)

	// cleanup unused locks and return state to channel
	m.locks = slices.DeleteFunc(m.locks, func(l *lock) bool {
		if l.count == 0 && len(l.waiting) == 0 {
			lockPool.Put(l)
			return true
		}
		return false
	})
	atomic.StoreInt64(&m.nlocks, int64(len(m.locks)))
}

func (m *LockManager) acquire(ctx context.Context, xid uint64, mode LockMode, typ LockType, oid uint64, pred ConditionMatcher) error {
	// sync state access
	m.mu.Lock()

	// find existing lock for this resource or create new lock object
	l, isNew := getOrCreateLock(m.locks, typ, oid, mode == LockModeExclusive)

	// TODO: handle predicate locks
	// for predicate locks, determine overlap on exclusive locks
	// (exclusive locks require disjunct predicates)
	// if mode == LockModeExclusive && typ == LockTypePredicate {
	// }

	var (
		isGranted, wasGranted, isDeadlock bool
		wait                              chan struct{}
	)

	switch {
	case isNew:
		// new shared or exclusive lock, add to state and grant
		m.locks = append(m.locks, l)
		atomic.AddInt64(&m.nlocks, 1)
		isGranted = true
		m.granted[xid] = append(m.granted[xid], l)

	case slices.Contains(m.granted[xid], l):
		// the xid already holds a lock to this resource
		wasGranted = true
		switch mode {
		case LockModeExclusive:
			// try upgrade shared to exclusive when possible
			switch {
			case l.exclusive:
				// we already hold the max priority lock
				isGranted = true
			case l.count == 1 && l.empty():
				// we're the only shared holder, upgrade to exclusive
				l.exclusive = true // reset from shared
				isGranted = true
			default:
				// others are waiting
				// detect deadlock situation before we start wait
				if m.detectDeadlock(l, xid) {
					isDeadlock = true
				} else {
					// wait until we're the only holder or we're next in line
					wait = l.wait(xid)
				}
			}
		case LockModeShared:
			// always true
			isGranted = true
		}

	default:
		// first time this xid touches the lock
		switch mode {
		case LockModeExclusive:
			switch {
			case l.count == 0 && l.empty():
				// lock is free right now
				l.exclusive = true // potentially reset from shared
				l.count = 1
				isGranted = true

			default:
				// lock is occupied or others are waiting
				// detect deadlock situation before we start wait
				if m.detectDeadlock(l, xid) {
					isDeadlock = true
				} else {
					// wait for the lock to become available
					wait = l.wait(xid)
				}
			}

		case LockModeShared:
			switch {
			case !l.exclusive && l.empty():
				// lock is shared and nobody else is waiting for exclusive access
				l.count++
				isGranted = true

			case l.exclusive && l.count == 0 && l.empty():
				// exclusive lock was just released and we are alone
				l.count++
				l.exclusive = false // reset to shared
				isGranted = true

			case !l.empty():
				// wait behind others
				// detect deadlock situation before we start wait
				if m.detectDeadlock(l, xid) {
					isDeadlock = true
				} else {
					// wait behind potential exclusive lock requests
					wait = l.wait(xid)
				}
			default:
				m.mu.Unlock()
				panic(fmt.Errorf("Unhandled shared case lock=%#v %v %v %v", l, xid, mode, typ))
			}
		}

		// keep list of granted locks for bulk release at txn close
		if isGranted {
			m.granted[xid] = append(m.granted[xid], l)
		}
	}

	// return state to channel
	m.mu.Unlock()

	// return success
	if isGranted {
		return nil
	}

	// return error
	if isDeadlock {
		return ErrDeadlock
	}

	// passive wait for next state change or abort
	select {
	case <-ctx.Done():
		// remove self from lock waitlist (l is initialized here)
		m.mu.Lock()
		l.drop(xid)
		m.mu.Unlock()

		// translate timeout error
		if ctx.Err() == context.DeadlineExceeded {
			return ErrLockTimeout
		}

		return ctx.Err()

	case <-wait:
		// lock is granted to us now
		m.mu.Lock()

		// update lock state
		l.pop()
		l.exclusive = mode == LockModeExclusive
		if l.exclusive {
			l.count = 1
		} else {
			l.count++
		}
		if !wasGranted {
			m.granted[xid] = append(m.granted[xid], l)
		}

		m.mu.Unlock()

		return nil
	}
}

// Drop releases a single lock after it has been granted. its required to roll back
// high level locks in case a lower level lock fails.
func (m *LockManager) drop(xid uint64, mode LockMode, typ LockType, oid uint64, pred ConditionMatcher) {
	// exclusive access
	m.mu.Lock()
	defer m.mu.Unlock()

	// release the specific lock
	for i, l := range m.granted[xid] {
		if l.typ != typ || l.oid != oid {
			continue
		}

		// remove from xid's own granted list
		m.granted[xid] = append(m.granted[xid][:i], m.granted[xid][i+1:]...)

		// reduce ref and remove from lock manager if last ref was dropped
		l.count--
		l.yield()
		if l.count == 0 && len(l.waiting) == 0 {
			m.locks = slices.DeleteFunc(m.locks, func(l2 *lock) bool {
				if l2 == l {
					lockPool.Put(l)
					return true
				}
				return false
			})
			atomic.AddInt64(&m.nlocks, -1)
		}
		break
	}
}

func getOrCreateLock(locks []*lock, typ LockType, oid uint64, exclusive bool) (*lock, bool) {
	for _, l := range locks {
		if l.typ != typ || l.oid != oid {
			continue
		}
		return l, false
	}
	l := lockPool.Get().(*lock)
	l.typ = typ
	l.oid = oid
	l.count = 1
	l.exclusive = exclusive
	return l, true
}

// Find cycle in dependency graph, starting at current xid. We are not yet waiting
// on the next lock, but of lock is found in any of our dependecies granted lists
// then we are about to get a deadlock.
func (m *LockManager) detectDeadlock(next *lock, xid uint64) bool {
	return m.hasLoopTo(m.granted[xid], next, xid)
}

// detect a potential loop in granted locks and waiters
func (m *LockManager) hasLoopTo(locks []*lock, next *lock, self uint64) bool {
	for _, l := range locks {
		if l == next {
			return true
		}
		for _, waiter := range l.waiting {
			if waiter == self {
				continue
			}
			if m.hasLoopTo(m.granted[waiter], next, self) {
				return true
			}
		}
	}
	return false
}
