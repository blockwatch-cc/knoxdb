// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"blockwatch.cc/knoxdb/internal/types"
)

// Must be called holding the engine lock
func (e *Engine) NewSnapshot(id uint64) *types.Snapshot {
	s := &types.Snapshot{
		Xown: id,
		Xmin: e.xmin,
		Xmax: id,
		Safe: len(e.txs) == 0,
	}
	for _, x := range e.txs {
		s.Xact |= 1 << (x.id - e.xmin)
	}
	return s
}

// func (e *Engine) CreateSnapshot(name string) (*Snapshot, error) {

// }

// func (e *Engine) DropSnapshot(name string) error {

// }

// func (e *Engine) RollbackSnapshot(name string) error {

// }
