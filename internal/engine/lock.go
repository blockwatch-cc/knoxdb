// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/pkg/assert"
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
	locks   []*lock         // all granted locks, use chan for exclusive access
	granted map[XID][]*lock // map of tx id to locks granted
	nlocks  int64           // total number of locks currently in existence
}

func NewLockManager() *LockManager {
	m := &LockManager{
		timeout: 10 * time.Second,
		locks:   make([]*lock, 0),
		granted: make(map[XID][]*lock),
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
		runtime.Gosched()
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
		w := lock.front
		for w != nil {
			next := w.next
			if w.ch != nil {
				close(w.ch)
				w.ch = nil
			}
			w.next = nil
			waiterPool.Put(w)
			w = next
		}
		lock.count = 0
		lock.front = nil
		lock.back = nil
	}
	clear(m.locks)
	clear(m.granted)
	atomic.StoreInt64(&m.nlocks, 0)
}

// lock represents a lock on a unique database resource. Each resource has at most
// one lock assigned. During its lifecycle, a lock may change state between shared
// and exclusive as request order requires. We store info about each waiting
// transaction as FIFO queue (linked list). This queue may contain a mix of shared
// and exclusive lock waiter transactions. We uses a per transaction channel
// and select to wait for a lock to become available. In this case the signalling channel
// is closed in yield(). Yield either unblocks a single exclusive lock request or
// multiple share lock requests at once. It is up to the acquire() func to pop()
// the waiter struct from the FIFO queue to confirms the lock has been consumed.
// The order in which pop removes FIFO entries for multiple shared locks is not
// important, only that the correct number of pop() operations happen.
// When a transaction completes (commit/abort) it eventually calls Done() to return
// the lock which yields to the next waiter(s). Should a goroutine that waits for
// a lock exit on early on context cancellation (i.e. a lock timeout or other
// cancellation reason), then the corresponding FIFO entry will be dropped.
type lock struct {
	typ       LockType // object or predicate
	exclusive bool     // flag indicating if this lock is exclusive or shared
	oid       uint64   // container id when type is object or predicate
	count     int      // shared lock reference counter
	front     *waiter  // next waiter in queue or nil
	back      *waiter  // last waiter in queue or nil
}

var (
	waiterPool = sync.Pool{
		New: func() any { return new(waiter) },
	}

	lockPool = sync.Pool{
		New: func() any { return new(lock) },
	}
)

// waiter represents a single transaction waiting on a lock to be released. Waiters
// form a fifo queue (linked list)
type waiter struct {
	next *waiter
	xid  XID
	ch   chan struct{}
	excl bool
}

// func (l *lock) numWaiters() int {
// 	var n int
// 	for w := l.front; w != nil; n, w = n+1, w.next {
// 	}
// 	return n
// }

// func (l *lock) listWaiters() []XID {
// 	n := make([]XID, 0)
// 	for w := l.front; w != nil; w = w.next {
// 		n = append(n, w.xid)
// 	}
// 	return n
// }

func (l *lock) empty() bool {
	return l.front == nil
}

func (l *lock) pop() {
	if l.front != nil {
		// pop first queue element
		w := l.front
		l.front = l.front.next
		if l.front == nil {
			l.back = nil
		}

		// clear and return to pool
		w.next = nil
		if w.ch != nil {
			close(w.ch)
			w.ch = nil
		}
		waiterPool.Put(w)
	}
}

func (l *lock) yield() {
	// skip on empty waiters or when we have already yielded to the next
	if l.front == nil || l.front.ch == nil {
		return
	}

	if l.front.excl {
		// exclusive lock: yield once
		close(l.front.ch)
		l.front.ch = nil
	} else {
		// shared lock: yield to all shared waiters in line up until an
		// exlusive lock waiter is encountered
		for w := l.front; w != nil && !w.excl; w = w.next {
			close(w.ch)
			w.ch = nil
		}
	}
}

func (l *lock) wait(xid XID, isExcl bool) chan struct{} {
	w := waiterPool.Get().(*waiter)
	w.next = nil
	w.xid = xid
	w.ch = make(chan struct{}, 1)
	w.excl = isExcl
	if l.back == nil {
		l.front = w
	} else {
		l.back.next = w
	}
	l.back = w
	return w.ch
}

func (l *lock) drop(xid XID) {
	if l.front == nil {
		return
	}

	// drop first element
	if l.front.xid == xid {
		w := l.front
		l.front = w.next
		if w.ch != nil {
			close(w.ch)
			w.ch = nil
		}
		w.next = nil
		waiterPool.Put(w)
		if l.back == w {
			l.back = l.front
		}
		return
	}

	// drop 2nd+ element
	for prev, w := l.front, l.front.next; w != nil; prev, w = w, w.next {
		if w.xid != xid {
			continue
		}
		prev.next = w.next
		if w.ch != nil {
			close(w.ch)
			w.ch = nil
		}
		w.next = nil
		waiterPool.Put(w)

		// in case we dropped the last element
		if l.back == w {
			l.back = prev
		}
		break
	}
}

// Lock obtains a lock on a specific object.
func (m *LockManager) Lock(ctx context.Context, xid XID, mode LockMode, oid uint64) error {
	// upgrade context with timeout
	if m.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.timeout)
		defer cancel()
	}

	return m.acquire(ctx, xid, mode, LockTypeObject, oid, nil)
}

// TODO: not yet supported
func (m *LockManager) LockPredicate(ctx context.Context, xid XID, mode LockMode, oid uint64, pred ConditionMatcher) error {
	tx := GetTx(ctx)

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
func (m *LockManager) Done(xid XID) {
	// exclusive access
	m.mu.Lock()
	defer m.mu.Unlock()

	// release all locks owned by xid
	for _, l := range m.granted[xid] {
		l.count--
		// fmt.Printf("Unlock for xid=%d cnt=%d wait=%d\n", xid, l.count, len(l.waiting))
		// yield to the next waiter when count drops to zero
		// - shared: all current shared holders have unlocked
		// - exclusive: the single exclusive holder has unlocked
		if l.count == 0 && !l.empty() {
			// fmt.Printf("yield lock from xid=%d on oid=%d cnt=%d to xid=%d waiters=%v\n",
			// 	xid, l.oid, l.count, l.front.xid, l.listWaiters())
			l.yield()
		}
		assert.Always(l.count >= 0, "negative lock count", l)
	}
	delete(m.granted, xid)

	// cleanup unused locks
	m.locks = slices.DeleteFunc(m.locks, func(l *lock) bool {
		if l.count == 0 && l.empty() {
			lockPool.Put(l)
			return true
		}
		return false
	})
	atomic.StoreInt64(&m.nlocks, int64(len(m.locks)))
}

func (m *LockManager) acquire(ctx context.Context, xid XID, mode LockMode, typ LockType, oid uint64, _ ConditionMatcher) error {
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
					wait = l.wait(xid, mode == LockModeExclusive)
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
					wait = l.wait(xid, mode == LockModeExclusive)
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

			case !l.empty() || (l.exclusive && l.count > 0):
				// wait behind others
				// detect deadlock situation before we start wait
				if m.detectDeadlock(l, xid) {
					isDeadlock = true
				} else {
					// wait behind potential exclusive lock requests
					wait = l.wait(xid, mode == LockModeExclusive)
				}
			default:
				m.mu.Unlock()
				panic(fmt.Errorf("unhandled shared case lock=%#v %v %v %v", l, xid, mode, typ))
			}
		}

		// keep list of granted locks for bulk release at txn close
		if isGranted {
			m.granted[xid] = append(m.granted[xid], l)
		}
	}

	// release mutex after shared access region
	m.mu.Unlock()

	// return success
	if isGranted {
		// fmt.Printf("Lock for xid=%d cnt=%d wait=%d waiters=%v\n", xid, l.count, l.numWaiters(), l.listWaiters())
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
		// fmt.Printf("Lock for xid=%d cnt=%d wait=%d\n", xid, l.count, l.numWaiters())

		m.mu.Unlock()

		return nil
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
	l.front = nil
	l.back = nil
	return l, true
}

// Find cycle in dependency graph, starting at current xid. We are not yet waiting
// on the next lock, but if the lock is found in any of our dependencies granted lists
// then we are about to get a deadlock.
func (m *LockManager) detectDeadlock(next *lock, xid XID) bool {
	return m.hasLoopTo(m.granted[xid], next, xid)
}

// detect a potential loop in granted locks and waiters
func (m *LockManager) hasLoopTo(locks []*lock, next *lock, self XID) bool {
	for _, l := range locks {
		if l == next {
			return true
		}
		for w := l.front; w != nil; w = w.next {
			if w.xid == self {
				continue
			}
			if m.hasLoopTo(m.granted[w.xid], next, self) {
				return true
			}
		}
	}
	return false
}

// Drop releases a single lock after it has been granted. its required to roll back
// high level locks in case a lower level lock fails.
// func (m *LockManager) drop(xid XID, _ LockMode, typ LockType, oid uint64, _ ConditionMatcher) {
// 	// exclusive access
// 	m.mu.Lock()
// 	defer m.mu.Unlock()

// 	// release the specific lock
// 	for i, l := range m.granted[xid] {
// 		if l.typ != typ || l.oid != oid {
// 			continue
// 		}

// 		// remove from xid's own granted list
// 		m.granted[xid] = append(m.granted[xid][:i], m.granted[xid][i+1:]...)

// 		// reduce ref and remove from lock manager if last ref was dropped
// 		l.count--
// 		l.yield()
// 		if l.count == 0 && len(l.waiting) == 0 {
// 			m.locks = slices.DeleteFunc(m.locks, func(l2 *lock) bool {
// 				if l2 == l {
// 					lockPool.Put(l)
// 					return true
// 				}
// 				return false
// 			})
// 			atomic.AddInt64(&m.nlocks, -1)
// 		}
// 		break
// 	}
// }
