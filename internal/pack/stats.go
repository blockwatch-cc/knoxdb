// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

type Stats struct {
	WasDirty []bool   // flag indicating this block was dirty and rewritten
	DiffSize []int    // storage size diff in bytes after encoding/compression (when dirty)
	MinMax   [][2]any // type-based min/max statistics
	Unique   []int    // cardinalities
}

func (s *Stats) Close() {
	clear(s.MinMax)
	s.MinMax = nil
}

func (s *Stats) SizeDiff() int64 {
	var sum int
	for _, v := range s.DiffSize {
		sum += v
	}
	return int64(sum)
}

func (p Package) Stats() *Stats {
	return p.stats
}

func (p *Package) CloseStats() *Package {
	p.stats.Close()
	p.stats = nil
	return p
}

func (p *Package) WithStats() *Package {
	if p.stats == nil {
		p.stats = &Stats{
			WasDirty: make([]bool, len(p.blocks)),
			DiffSize: make([]int, len(p.blocks)),
			MinMax:   make([][2]any, len(p.blocks)),
			Unique:   make([]int, len(p.blocks)),
		}
	}
	for i, b := range p.blocks {
		if b == nil || !b.IsDirty() {
			continue
		}
		p.stats.WasDirty[i] = true
	}
	return p
}
