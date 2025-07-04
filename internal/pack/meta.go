// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/schema"
)

const (
	// meta block positions relative to rowid
	ridOffs  = iota // 0
	refOffs         // 1
	xminOffs        // 2
	xmaxOffs        // 3
	delOffs         // 4
)

func (p *Package) HasMeta() bool {
	return p.rx >= 0
}

func (p *Package) Pk(row int) uint64    { return p.blocks[p.px].Uint64().Get(row) }
func (p *Package) RowId(row int) uint64 { return p.blocks[p.rx+ridOffs].Uint64().Get(row) }
func (p *Package) RefId(row int) uint64 { return p.blocks[p.rx+refOffs].Uint64().Get(row) }
func (p *Package) Xmin(row int) types.XID {
	return types.XID(p.blocks[p.rx+xminOffs].Uint64().Get(row))
}
func (p *Package) Xmax(row int) types.XID {
	return types.XID(p.blocks[p.rx+xmaxOffs].Uint64().Get(row))
}
func (p *Package) IsDel(row int) bool { return p.blocks[p.rx+delOffs].Bool().Get(row) }

func (p *Package) Pks() types.NumberAccessor[uint64]    { return p.blocks[p.px].Uint64() }
func (p *Package) RowIds() types.NumberAccessor[uint64] { return p.blocks[p.rx+ridOffs].Uint64() }
func (p *Package) RefIds() types.NumberAccessor[uint64] { return p.blocks[p.rx+refOffs].Uint64() }
func (p *Package) Xmins() types.NumberAccessor[uint64]  { return p.blocks[p.rx+xminOffs].Uint64() }
func (p *Package) Xmaxs() types.NumberAccessor[uint64]  { return p.blocks[p.rx+xmaxOffs].Uint64() }
func (p *Package) Dels() bitset.BitmapAccessor          { return p.blocks[p.rx+delOffs].Bool() }

func (p *Package) PkBlock() *block.Block    { return p.blocks[p.px] }
func (p *Package) RowIdBlock() *block.Block { return p.blocks[p.rx+ridOffs] }
func (p *Package) RefIdBlock() *block.Block { return p.blocks[p.rx+refOffs] }
func (p *Package) XminBlock() *block.Block  { return p.blocks[p.rx+xminOffs] }
func (p *Package) XmaxBlock() *block.Block  { return p.blocks[p.rx+xmaxOffs] }
func (p *Package) DelBlock() *block.Block   { return p.blocks[p.rx+delOffs] }

func (p *Package) Meta(row int) *schema.Meta {
	m := &schema.Meta{}
	if p.HasMeta() {
		m.Rid = p.RowId(row)
		m.Ref = p.RefId(row)
		m.Xmin = p.Xmin(row)
		m.Xmax = p.Xmax(row)
		m.IsDel = p.IsDel(row)
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
	p.blocks[p.rx+xminOffs].Uint64().Set(row, uint64(m.Xmin))
	p.blocks[p.rx+xmaxOffs].Uint64().Set(row, uint64(m.Xmax))
	if m.Xmax > 0 {
		p.blocks[p.rx+delOffs].Bool().Set(row)
	} else {
		p.blocks[p.rx+delOffs].Bool().Unset(row)
	}
}
