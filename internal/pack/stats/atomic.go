// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import "sync/atomic"

// AtomicPointer wraps Index and ensures multiple readers have stable
// access to the latest reference counted version of the wrapped
// pointer. A single writer may install a new version at any time
// without locking. After an update, future readers will see the new
// version only. The wrapped index will be closed once when the last
// reader releases the object (calls Release on the Index pointer).
// In case no reader is present, the writer releases the Index on update.
type AtomicPointer struct {
	ptr atomic.Pointer[Index]
}

func NewAtomicPtr(idx *Index) *AtomicPointer {
	p := &AtomicPointer{}
	p.ptr.Store(idx)
	return p
}

// Get returns the current wrapped pointer without reference counting.
// This method is unsafe. It does not provide any guarantee the underlying
// index is or remains valid. A concurrent call to Update may replace
// the pointer and close the old version at any time.
func (p *AtomicPointer) Get() *Index {
	return p.ptr.Load()
}

// Retain returns the current version of the wrapped pointer and
// guarantees the object remains valid until the user calls Release
// on the returned Index explicitly.
func (p *AtomicPointer) Retain() *Index {
	for {
		idx := p.ptr.Load()
		rc := atomic.LoadUint32(&idx.rc)

		// retry until new object is installed
		if rc == 0 {
			continue
		}

		// inc refcount but retry if it has changed meanwhile
		if atomic.CompareAndSwapUint32(&idx.rc, rc, rc+1) {
			return idx
		}
	}
}

// Update replaces the wrapped pointer with a new version and
// attempts to release the old object if no longer in use (i.e.
// reference count drops to zero.) Note the new index idx must
// be initialized with a reference count of 1.
func (p *AtomicPointer) Update(idx *Index) {
	old := p.ptr.Swap(idx)
	old.Release(true)
}
