// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import "blockwatch.cc/knoxdb/internal/types"

// convert containers to optimized format (not in journal and tomb)
func (p *Package) Optimize() *Package {
	for _, b := range p.blocks {
		if b == nil {
			continue
		}
		b.Optimize()
	}
	return p
}

// convert containers to writable format (not in journal and tomb)
func (p *Package) Materialize() *Package {
	for _, b := range p.blocks {
		if b == nil {
			continue
		}
		b.Materialize()
	}
	return p
}

func (p *Package) IsMaterialized() bool {
	fields := p.schema.Exported()
	for i, b := range p.blocks {
		if b == nil {
			if !fields[i].Flags.Is(types.FieldFlagDeleted) {
				return false
			}
		}
		if !b.IsMaterialized() {
			return false
		}
	}
	return true
}
