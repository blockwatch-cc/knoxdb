// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// TODO
// - predicate lock (match range overlaps, =,<,> cond)
// - see query.FilterTreeNode.Overlaps() // unimplemneted

var (
	ErrLockTimeout  = errors.New("canceled due to lock timeout")
	ErrDeadlock     = errors.New("deadlock detected")
	ErrLockConflict = errors.New("conflict when upgrading a lock")
)

type LockMode byte

const (
	LockModeShared LockMode = iota
	LockModeExclusive
)

// LockType defines a hierarchy of scopes starting at the widest "global" scope
// (the entire database), down to individual rows (identified by row id range)
type LockType byte

const (
	LockTypeGlobal LockType = iota // entire database across all objects
	LockTypeObject                 // single container object (table, store, index)
	// LockTypePredicate           // query condition (may be used for row id or key ranges)
)

// invariants
// - at most one lock per type & resource combination exists (either shared or exclusive state)
// - no locks with count = 0 and waiting ==0 exist
// - a shared lock has waiting > 0 only if exclusive lock is requested
// - an exclusive lock has waiting > 0 if subsequent exclusive or shared locks are requested
type LockManager struct {
	timeout time.Duration
	locks   chan []*lock       // all granted locks, use chan for exclusive access
	granted map[uint64][]*lock // map of tx id to locks granted
	nlocks  int64
}

func NewLockManager() *LockManager {
	m := &LockManager{
		timeout: 10 * time.Second,
		locks:   make(chan []*lock, 1),
		granted: make(map[uint64][]*lock),
	}
	m.locks <- make([]*lock, 0)
	return m
}

func (m *LockManager) WithTimeout(t time.Duration) *LockManager {
	m.timeout = t
	return m
}

func (m *LockManager) Len() int {
	return int(atomic.LoadInt64(&m.nlocks))
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
	typ       LockType                 // global, object or predicate
	exclusive bool                     // flag indicating if this lock is exclusive or shared
	entity    uint64                   // container id when type is object or predicate
	count     int                      // shared lock reference counter
	waiting   []uint64                 // xids waiting to inherit or replace the lock (in request order)
	waiters   map[uint64]chan struct{} //wait channels per xid
}

func (l *lock) isNext(xid uint64) bool {
	return len(l.waiting) == 0 || l.waiting[0] == xid
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
	delete(l.waiters, xid)
}

var lockPool = sync.Pool{
	New: func() any { return new(lock) },
}

// Lock obtains a global database lock in shared or exclusive mode.
func (m *LockManager) Lock(ctx context.Context, mode LockMode) (bool, error) {
	tx := GetTransaction(ctx)
	if tx == nil {
		return false, ErrNoTx
	}

	// add timeout to context
	if m.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.timeout)
		defer cancel()
	}

	return m.acquire(ctx, tx.id, mode, LockTypeGlobal, 0, nil)
}

// LockObject obtains lock on a specific object. First it also takes a shared global
// lock to preserve lock nesting requirements.
func (m *LockManager) LockObject(ctx context.Context, mode LockMode, oid uint64) (bool, error) {
	tx := GetTransaction(ctx)
	if tx == nil {
		return false, ErrNoTx
	}

	// add timeout to context
	if m.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.timeout)
		defer cancel()
	}

	// first obtain a shared global lock
	_, err := m.acquire(ctx, tx.id, LockModeShared, LockTypeGlobal, 0, nil)
	if err != nil {
		return false, err
	}

	// obtain object lock
	_, err = m.acquire(ctx, tx.id, mode, LockTypeObject, oid, nil)
	if err != nil {
		// rollback global lock on error (timeout, context canceled)
		m.drop(tx.id, LockModeShared, LockTypeGlobal, 0, nil)
		return false, err
	}

	return true, nil
}

// Done releases all locks acquired by the given transaction xid.
func (m *LockManager) Done(xid uint64) {
	// exclusive access
	locks := <-m.locks

	// release all locks owned by xid
	for _, l := range m.granted[xid] {
		l.count--
		l.yield()
	}
	delete(m.granted, xid)

	// cleanup unused locks and return state to channel
	locks = slices.DeleteFunc(locks, func(l *lock) bool {
		if l.count == 0 && len(l.waiting) == 0 {
			lockPool.Put(l)
			return true
		}
		return false
	})
	atomic.StoreInt64(&m.nlocks, int64(len(locks)))
	m.locks <- locks
}

func (m *LockManager) acquire(ctx context.Context, xid uint64, mode LockMode, typ LockType, entity uint64, pred ConditionMatcher) (bool, error) {
	if pred != nil {
		return false, ErrNotImplemented
	}

	// check for early cancel
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	var (
		isNew, isWaiting, isGranted, isDeadlock bool
		l                                       *lock
		wait                                    chan struct{}
	)
	for {
		if isWaiting {
			// passive wait for next state change or abort
			select {
			case <-ctx.Done():
				// remove self from lock waitlist (l is initialized here)
				locks := <-m.locks
				l.drop(xid)
				m.locks <- locks

				// translate timeout error
				if ctx.Err() == context.DeadlineExceeded {
					return false, ErrLockTimeout
				}

				return false, ctx.Err()

			case <-wait:
			}
		}

		// sync state access
		locks := <-m.locks

		// find existing lock for this resource or create new lock object
		if l == nil {
			l, isNew = getOrCreateLock(locks, typ, entity, mode == LockModeExclusive)
		} else {
			isNew = false
		}

		// TODO: handle predicate locks
		// for predicate locks, determine overlap on exclusive locks
		// (exclusive locks require disjunct predicates)
		// if mode == LockModeExclusive && typ == LockTypePredicate {
		// }

		switch {
		case isNew:
			// new shared or exclusive lock, add to state and grant
			locks = append(locks, l)
			atomic.AddInt64(&m.nlocks, 1)
			isGranted = true
			m.granted[xid] = append(m.granted[xid], l)

		case slices.Contains(m.granted[xid], l):
			// the xid already holds a lock to this resource
			switch mode {
			case LockModeExclusive:
				// try upgrade shared to exclusive when possible
				switch {
				case l.exclusive:
					// we already hold the max priority lock
					isGranted = true
				case l.count == 1 && l.isNext(xid):
					// wait until we're the only shared holder and upgrade to exclusive
					l.pop()
					l.exclusive = true // reset from shared
					isGranted = true
				case !isWaiting:
					// detect deadlock situation before we start wait
					if m.detectDeadlock(l, xid) {
						isDeadlock = true
					} else {
						// wait until we're the only holder or we're next in line
						wait = l.wait(xid)
						isWaiting = true
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
				case l.count == 0 && l.isNext(xid):
					// lock is available right now and we are only or next in line
					l.pop()
					l.exclusive = true // potentially reset from shared
					l.count = 1
					isGranted = true

				case l.count > 0 && !isWaiting:
					// detect deadlock situation before we start wait
					if m.detectDeadlock(l, xid) {
						isDeadlock = true
					} else {
						// wait for the lock to become available
						wait = l.wait(xid)
						isWaiting = true
					}
				}

			case LockModeShared:
				switch {
				case !l.exclusive && l.isNext(xid):
					// lock is shared and nobody else is waiting for exclusive access or we are next
					l.pop()
					l.count++
					isGranted = true

				case l.exclusive && l.count == 0 && l.isNext(xid):
					// exclusive lock was just released and we are only or next in line
					l.pop()
					l.count++
					l.exclusive = false // reset to shared
					isGranted = true

				case len(l.waiting) > 0 && !isWaiting:
					// detect deadlock situation before we start wait
					if m.detectDeadlock(l, xid) {
						isDeadlock = true
					} else {
						// wait behind potential exclusive lock requests
						wait = l.wait(xid)
						isWaiting = true
					}
				}
			}

			// keep list of granted locks for bulk release at txn close
			if isGranted {
				m.granted[xid] = append(m.granted[xid], l)
			}
		}

		// return state to channel
		m.locks <- locks

		// return success
		if isGranted {
			return true, nil
		}

		// return error
		if isDeadlock {
			return false, ErrDeadlock
		}
	}
}

// Drop releases a single lock after it has been granted. its required to roll back
// high level locks in case a lower level lock fails.
func (m *LockManager) drop(xid uint64, mode LockMode, typ LockType, entity uint64, pred ConditionMatcher) {
	// exclusive access
	locks := <-m.locks

	// release the specific lock
	for i, l := range m.granted[xid] {
		if l.typ != typ || l.entity != entity {
			continue
		}

		// remove from xid's own granted list
		m.granted[xid] = append(m.granted[xid][:i], m.granted[xid][i+1:]...)

		// reduce ref and remove from lock manager if last ref was dropped
		l.count--
		l.yield()
		if l.count == 0 && len(l.waiting) == 0 {
			locks = slices.DeleteFunc(locks, func(l2 *lock) bool {
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

	m.locks <- locks
}

func getOrCreateLock(locks []*lock, typ LockType, entity uint64, exclusive bool) (*lock, bool) {
	for _, l := range locks {
		if l.typ != typ || l.entity != entity {
			continue
		}
		return l, false
	}
	l := lockPool.Get().(*lock)
	l.typ = typ
	l.entity = entity
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

// TODO: not yet supported
// func (m *LockManager) LockPredicate(ctx context.Context, mode LockMode, odi uint64, pred ConditionMatcher) (bool, error) {
//  tx := GetTransaction(ctx)
//  if tx == nil {
//      return false, ErrNoTx
//  }

//  // first obtain a shared global lock
//  ok, err := m.acquire(ctx, tx.id, LockModeShared, LockTypeGlobal, 0, nil)
//  if err != nil {
//      return false, err
//  }

//  // then obtain a nested shared object lock
//  ok, err = m.acquire(ctx, tx.id, LockModeShared, LockTypeObject, oid, nil)
//  if err != nil {
//      return false, err
//  }

//  // then obtain the predicate lock
//  return m.acquire(ctx, tx.id, LockModeShared, LockTypePredicate, oid, pred)
// }
