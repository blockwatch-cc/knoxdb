// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"blockwatch.cc/knoxdb/internal/types"
)

// Must be called holding the engine lock
func (e *Engine) NewSnapshot(id uint64) *types.Snapshot {
	s := types.NewSnapshot(id, e.xmin)
	for _, x := range e.txs {
		if x.IsReadOnly() {
			continue
		}
		s.AddActive(x.id)
	}
	return s
}

// func (e *Engine) CreateSnapshot(name string) (*Snapshot, error) {

// }

// func (e *Engine) DropSnapshot(name string) error {

// }

// func (e *Engine) RollbackSnapshot(name string) error {

// }
