// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/schema"
)

const (
	// meta block positions relative to rowid
	ridOffs  = iota // 0
	refOffs         // 1
	xminOffs        // 2
	xmaxOffs        // 3
	liveOffs        // 4
)

func (p *Package) HasMeta() bool {
	return p.rx >= 0
}

func (p *Package) RowId(row int) uint64 { return p.blocks[p.rx+ridOffs].Uint64().Get(row) }
func (p *Package) RefId(row int) uint64 { return p.blocks[p.rx+refOffs].Uint64().Get(row) }
func (p *Package) Xmin(row int) uint64  { return p.blocks[p.rx+xminOffs].Uint64().Get(row) }
func (p *Package) Xmax(row int) uint64  { return p.blocks[p.rx+xmaxOffs].Uint64().Get(row) }

func (p *Package) RowIds() *block.Block { return p.blocks[p.rx+ridOffs] }
func (p *Package) RefIds() *block.Block { return p.blocks[p.rx+refOffs] }
func (p *Package) Xmins() *block.Block  { return p.blocks[p.rx+xminOffs] }
func (p *Package) Xmaxs() *block.Block  { return p.blocks[p.rx+xmaxOffs] }

func (p *Package) Meta(row int) *schema.Meta {
	m := &schema.Meta{}
	if p.HasMeta() {
		m.Rid = p.RowId(row)
		m.Ref = p.RefId(row)
		m.Xmin = p.Xmin(row)
		m.Xmax = p.Xmax(row)
		m.IsLive = m.Xmax == 0
	}
	return m
}

func (p *Package) SetMeta(row int, m *schema.Meta) {
	if !p.HasMeta() {
		return
	}

	assert.Always(p.nRows < row, "set meta: invalid row",
		"pack", p.key,
		"row", row,
		"len", p.nRows,
		"cap", p.maxRows,
	)

	p.blocks[p.rx+ridOffs].Uint64().Set(row, m.Rid)
	p.blocks[p.rx+refOffs].Uint64().Set(row, m.Ref)
	p.blocks[p.rx+xminOffs].Uint64().Set(row, m.Xmin)
	p.blocks[p.rx+xmaxOffs].Uint64().Set(row, m.Xmax)
	if m.Xmax == 0 {
		p.blocks[p.rx+xmaxOffs].Bool().Set(row)
	} else {
		p.blocks[p.rx+xmaxOffs].Bool().Clear(row)
	}
}
