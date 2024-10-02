// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/types"
)

func (e *Engine) NewSnapshot() *types.Snapshot {
	e.mu.RLock()
	s := &types.Snapshot{
		Xmin: e.xmin,
		Xmax: atomic.LoadUint64(&e.xnext),
	}
	for _, x := range e.txs {
		s.Xact |= 1 << (x.id - e.xmin)
	}
	e.mu.RUnlock()
	return s
}

// func (e *Engine) CreateSnapshot(name string) (*Snapshot, error) {

// }

// func (e *Engine) DropSnapshot(name string) error {

// }

// func (e *Engine) RollbackSnapshot(name string) error {

// }
