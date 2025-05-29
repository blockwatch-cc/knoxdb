// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"reflect"
	"sync"
	"time"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var (
	// allocation pool
	packagePool = sync.Pool{
		New: func() any { return &Package{} },
	}

	zeroTime  = time.Time{}
	szPackage = int(reflect.TypeOf(Package{}).Size())
)

type Package struct {
	key      uint32         // identity
	nRows    int            // current number or rows
	maxRows  int            // max number of rows (== block allocation size)
	px       int            // primary key index (position in schema)
	rx       int            // row id index (position in schema)
	schema   *schema.Schema // logical data types for column vectors
	blocks   []*block.Block // physical column vectors, maybe nil when unsued
	stats    *Stats         // vector and encoder statistics for metadata index
	selected []uint32       // selection vector used in operator pipelines
}

func New() *Package {
	return packagePool.Get().(*Package)
}

func NewFrom(src *Package) *Package {
	return New().
		WithKey(src.Key()).
		WithMaxRows(src.maxRows).
		WithSchema(src.schema)
}

func (p *Package) Key() uint32 {
	return p.key
}

func (p *Package) IsNil() bool {
	for _, b := range p.blocks {
		if b != nil {
			return false
		}
	}
	return true
}

func (p *Package) IsDirty() bool {
	for _, b := range p.blocks {
		if b == nil {
			continue
		}
		if b.IsDirty() {
			return true
		}
	}
	return false
}

func (p Package) Schema() *schema.Schema {
	return p.schema
}

func (p *Package) WithKey(k uint32) *Package {
	p.key = k
	return p
}

func (p *Package) WithMaxRows(sz int) *Package {
	p.maxRows = sz
	return p
}

func (p *Package) WithSchema(s *schema.Schema) *Package {
	p.blocks = make([]*block.Block, s.NumFields())
	p.px = s.PkIndex()
	p.rx = s.RowIdIndex()
	p.schema = s
	return p
}

func (p *Package) WithBlock(i int, b *block.Block) *Package {
	if p.blocks[i] != nil {
		p.blocks[i].Deref()
		p.blocks[i] = nil
	}
	p.blocks[i] = b
	p.nRows = b.Len()
	return p
}

func (p *Package) WithSelection(sel []uint32) *Package {
	p.selected = sel
	return p
}

func (p *Package) Alloc() *Package {
	if p.maxRows == 0 || p.schema == nil {
		return p
	}

	// alloc missing block storage
	for i, field := range p.schema.Fields() {
		// skip existing blocks
		if p.blocks[i] != nil {
			continue
		}

		// skip deleted fields
		if field.Is(types.FieldFlagDeleted) {
			continue
		}

		// allocate block
		p.blocks[i] = block.New(field.Type().BlockType(), p.maxRows)
	}

	return p
}

// Clone creates a private materialized copy of a pack with new allocated
// block storage. The capacity of the clone is defined in sz and may be
// larger than the length of the source pack.
func (p *Package) Clone(sz int) *Package {
	clone := New().
		WithKey(p.key).
		WithSchema(p.schema).
		WithMaxRows(sz)
	clone.nRows = p.nRows
	for i, b := range p.blocks {
		if b == nil {
			continue
		}
		// alloc sz capacity and copy len block data
		clone.blocks[i] = b.Clone(sz)
	}
	return clone
}

// Copy creates a shallow copy of pack referencing all data vectors.
func (p *Package) Copy() *Package {
	cp := New().
		WithKey(p.key).
		WithSchema(p.schema).
		WithMaxRows(p.maxRows).
		WithSelection(p.selected)
	cp.nRows = p.nRows
	for i, b := range p.blocks {
		cp.blocks[i] = b
		if b != nil {
			p.blocks[i].Ref()
		}
	}
	return cp
}

// convert block containers to writable form in-place
func (p *Package) Materialize() *Package {
	for i, b := range p.blocks {
		if b == nil {
			continue
		}
		clone := b.Clone(p.maxRows)
		b.Deref()
		p.blocks[i] = clone
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

func (p *Package) Cols() int {
	return p.schema.NumFields()
}

func (p *Package) Len() int {
	return p.nRows
}

func (p *Package) Cap() int {
	return p.maxRows
}

func (p *Package) IsFull() bool {
	return p.nRows == p.maxRows
}

func (p *Package) IsComplete() bool {
	fields := p.schema.Exported()
	for i, b := range p.blocks {
		if b == nil {
			if !fields[i].Flags.Is(types.FieldFlagDeleted) {
				return false
			}
		}
	}
	return true
}

func (p *Package) CanGrow(n int) bool {
	return p.nRows+n <= p.maxRows
}

func (p *Package) NumSelected() int {
	return len(p.selected)
}

func (p *Package) Selected() []uint32 {
	return p.selected
}

func (p *Package) Size() int {
	var sz int = szPackage
	for _, b := range p.blocks {
		if b == nil {
			continue
		}
		sz += b.Size()
	}
	return sz
}

func (p *Package) Blocks() []*block.Block {
	return p.blocks
}

func (p *Package) Block(i int) *block.Block {
	return p.blocks[i]
}

// Clear empties a pack but retains structure and allocated blocks.
func (p *Package) Clear() {
	for _, b := range p.blocks {
		if b == nil {
			continue
		}
		b.Clear()
	}
	if p.stats != nil {
		p.stats.Close()
		p.stats = nil
	}
	p.nRows = 0
	p.selected = nil
}

// Release frees package and drops block references.
func (p *Package) Release() {
	assert.Always(p != nil, "nil package release, potential use after free")
	for i := range p.blocks {
		if p.blocks[i] == nil {
			continue
		}
		p.blocks[i].Deref()
		p.blocks[i] = nil
	}
	if p.stats != nil {
		p.stats.Close()
		p.stats = nil
	}
	p.key = 0
	p.nRows = 0
	p.maxRows = 0
	p.px = 0
	p.rx = 0
	p.schema = nil
	p.selected = nil
	p.blocks = p.blocks[:0]
	packagePool.Put(p)
}
