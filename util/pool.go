// Copyright (c) 2013-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

// GenericPool is a pool of types that can be re-used.  Items in
// this pool will not be garbage collected when not in use.
type GenericPool struct {
	pool chan interface{}
	fn   func() interface{}
}

// NewGeneric returns a Generic pool with capacity for max items
// to be pool.
func NewGenericPool(max int, fn func() interface{}) *GenericPool {
	return &GenericPool{
		pool: make(chan interface{}, max),
		fn:   fn,
	}
}

// Get returns a item from the pool or a new instance if the pool
// is empty.  Items returned may not be in the zero state and should
// be reset by the caller.
func (p *GenericPool) Get() interface{} {
	var c interface{}
	select {
	case c = <-p.pool:
	default:
		c = p.fn()
	}

	return c
}

// Put returns an item back to the pool.  If the pool is full, the item
// is discarded.
func (p *GenericPool) Put(c interface{}) {
	select {
	case p.pool <- c:
	default:
	}
}

// Sized is a pool of types that can be re-used.  Items in
// this pool will not be garbage collected when not in use.
type SizedPool struct {
	pool chan interface{}
	fn   func(sz int) interface{}
}

// NewSized returns a Sized pool with capacity for max items
// to be pool.
func NewSizedPool(max int, fn func(sz int) interface{}) *SizedPool {
	return &SizedPool{
		pool: make(chan interface{}, max),
		fn:   fn,
	}
}

// Get returns a item from the pool or a new instance if the pool
// is empty.  Items returned may not be in the zero state and should
// be reset by the caller.
func (p *SizedPool) Get(sz int) interface{} {
	var c interface{}
	select {
	case c = <-p.pool:
	default:
		c = p.fn(sz)
	}

	return c
}

// Put returns an item back to the pool.  If the pool is full, the item
// is discarded.
func (p *SizedPool) Put(c interface{}) {
	select {
	case p.pool <- c:
	default:
	}
}
