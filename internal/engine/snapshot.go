// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import "blockwatch.cc/knoxdb/internal/types"

func (e *Engine) NewSnapshot() *types.Snapshot {
	// e.RLock()
	s := &types.Snapshot{
		Xmin: e.xmin,
	}
	for _, x := range e.txs {
		s.Xact |= 1 << (x.id - e.xmin)
	}
	// e.RUnlock()
	return s
}

// func (e *Engine) CreateSnapshot(name string) (*Snapshot, error) {

// }

// func (e *Engine) DropSnapshot(name string) error {

// }

// func (e *Engine) RollbackSnapshot(name string) error {

// }
