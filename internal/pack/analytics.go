// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

type Analysis struct {
	WasDirty  []bool   // flag indicating this block was dirty and rewritten
	DiffSize  []int    // storage size diff in bytes after encoding/compression (when dirty)
	MinMax    [][2]any // per block min/max values (when dirty)
	NumUnique []int    // per block cardinality (when dirty)
}

func (a Analysis) SizeDiff() int64 {
	var sum int
	for _, v := range a.DiffSize {
		sum += v
	}
	return int64(sum)
}

func (p Package) Analysis() *Analysis {
	return p.analyze
}

func (p *Package) FreeAnalysis() {
	p.analyze = nil
}

func (p *Package) WithAnalysis() *Package {
	if p.analyze == nil {
		p.analyze = &Analysis{
			WasDirty: make([]bool, len(p.blocks)),
			DiffSize: make([]int, len(p.blocks)),
		}
	}
	for i, b := range p.blocks {
		if b == nil || !b.IsDirty() {
			continue
		}
		p.analyze.WasDirty[i] = true
	}
	return p
}
