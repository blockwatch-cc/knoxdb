// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"context"
	"sync/atomic"
)

type Future struct {
	valid atomic.Uint32
	done  chan struct{}
	err   error
}

func NewFuture() *Future {
	return &Future{
		done: make(chan struct{}, 1),
	}
}

func (f *Future) IsValid() bool {
	return f.valid.Load() == 0
}

func (f *Future) Wait() {
	<-f.done
}

func (f *Future) WaitContext(ctx context.Context) {
	select {
	case <-f.done:
	case <-ctx.Done():
	}
}

func (f *Future) Close() {
	if f.valid.CompareAndSwap(0, 1) {
		close(f.done)
	}
}

func (f *Future) CloseErr(err error) {
	if f.valid.CompareAndSwap(0, 1) {
		close(f.done)
		f.err = err
	}
}

func (f *Future) Err() error {
	return f.err
}
