// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

// convert containers to optimized format (not in journal and tomb)
func (p *Package) Optimize() *Package {
	if p.key >= tombstoneKeyId {
		return p
	}
	for _, b := range p.blocks {
		b.Optimize()
	}
	return p
}

// convert containers to writable format (not in journal and tomb)
func (p *Package) Materialize() *Package {
	if p.key >= tombstoneKeyId {
		return p
	}
	for _, b := range p.blocks {
		b.Materialize()
	}
	return p
}
