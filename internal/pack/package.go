// Copyright (c) 2024-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"sync"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var (
	pool      = sync.Pool{New: func() any { return &Package{} }}
	zeroTime  = time.Time{}
	szPackage = int(unsafe.Sizeof(Package{}))
)

type Package struct {
	key      uint32         // identity
	version  uint32         // version epoch (set on write, only 16 bits used on storage)
	nRows    int            // current number of rows
	maxRows  int            // max number of rows (== block allocation size)
	px       int            // primary key index (position in schema)
	rx       int            // row id index (position in schema)
	schema   *schema.Schema // logical data types for column vectors (required)
	blocks   []*block.Block // physical column vectors, maybe nil when unsued
	stats    *Stats         // vector and encoder statistics for metadata index (optional)
	selected []uint32       // selection vector used in operator pipelines (optional)
}

func New() *Package {
	return pool.Get().(*Package)
}

func NewFrom(src *Package) *Package {
	return New().
		WithKey(src.Key()).
		WithVersion(src.version).
		WithMaxRows(src.maxRows).
		WithSchema(src.schema)
}

func (p *Package) WithKey(k uint32) *Package {
	p.key = k
	return p
}

func (p *Package) WithVersion(v uint32) *Package {
	p.version = v
	return p
}

func (p *Package) WithMaxRows(sz int) *Package {
	p.maxRows = sz
	return p
}

func (p *Package) WithSchema(s *schema.Schema) *Package {
	if cap(p.blocks) >= s.NumFields() {
		p.blocks = p.blocks[:s.NumFields()]
	} else {
		p.blocks = make([]*block.Block, s.NumFields())
	}
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

func (p *Package) Key() uint32 {
	return p.key
}

func (p Package) Version() uint32 {
	return p.version
}

func (p Package) Schema() *schema.Schema {
	return p.schema
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

func (p *Package) FreeSpace() int {
	return p.maxRows - p.nRows
}

func (p *Package) IsFull() bool {
	return p.nRows == p.maxRows
}

func (p *Package) CanGrow(n int) bool {
	return p.nRows+n <= p.maxRows
}

func (p *Package) NumSelected() int {
	if p.selected == nil {
		return p.nRows
	}
	return len(p.selected)
}

func (p *Package) Selected() []uint32 {
	return p.selected
}

func (p *Package) Blocks() []*block.Block {
	return p.blocks
}

func (p *Package) Block(i int) *block.Block {
	return p.blocks[i]
}

func (p *Package) Alloc() *Package {
	if p.maxRows == 0 || p.schema == nil {
		return p
	}

	// alloc missing block storage
	for i, field := range p.schema.Fields {
		// skip existing blocks
		if p.blocks[i] != nil {
			continue
		}

		// skip deleted fields
		if field.Is(types.FieldFlagDeleted) {
			continue
		}

		// allocate block
		p.blocks[i] = block.New(field.Type.BlockType(), p.maxRows)
	}

	return p
}

// Clone creates a private materialized copy of a pack with new allocated
// block storage. The capacity of the clone is defined in sz and may be
// larger than the length of the source pack.
func (p *Package) Clone(sz int) *Package {
	clone := New()
	clone.nRows = p.nRows
	clone.key = p.key
	clone.version = p.version
	clone.nRows = p.nRows
	clone.maxRows = p.maxRows
	clone.px = p.px
	clone.rx = p.rx
	clone.schema = p.schema
	clone.blocks = make([]*block.Block, len(p.blocks))
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
		WithVersion(p.version).
		WithSchema(p.schema).
		WithMaxRows(p.maxRows)
	cp.nRows = p.nRows
	for i, b := range p.blocks {
		if b != nil {
			b.Ref()
			cp.blocks[i] = b
		}
	}
	return cp
}

func (p *Package) Size() int {
	sz := szPackage
	for _, b := range p.blocks {
		if b == nil {
			continue
		}
		sz += b.Size()
	}
	return sz
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
	if p.selected != nil {
		// don't free selected, its owned by outside callers
		p.selected = nil
	}
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
	if p.selected != nil {
		// don't release. selection vector almost always comes from outside
		p.selected = nil
	}
	p.key = 0
	p.version = 0
	p.nRows = 0
	p.maxRows = 0
	p.px = 0
	p.rx = 0
	p.schema = nil
	p.blocks = p.blocks[:0]
	pool.Put(p)
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

func (p *Package) MaterializeBlock(i int) *Package {
	if i < 0 || i >= len(p.blocks) || p.blocks[i] == nil {
		return p
	}
	clone := p.blocks[i].Clone(p.maxRows)
	p.blocks[i].Deref()
	p.blocks[i] = clone
	return p
}

func (p *Package) IsMaterialized() bool {
	for i, b := range p.blocks {
		if b == nil {
			if !p.schema.Fields[i].Is(types.FieldFlagDeleted) {
				return false
			}
		}
		if !b.IsMaterialized() {
			return false
		}
	}
	return true
}

func (p *Package) IsComplete() bool {
	for i, b := range p.blocks {
		if b == nil {
			if !p.schema.Fields[i].Is(types.FieldFlagDeleted) {
				return false
			}
		}
	}
	return true
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
