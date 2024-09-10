// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"encoding/binary"
	"reflect"
	"sort"
	"sync"
	"time"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var (
	// reserved package keys
	JournalKeyId   uint32 = 0xFFFFFFFF
	TombstoneKeyId uint32 = 0xFFFFFFFE
	ResultKeyId    uint32 = 0xFFFFFFFD

	// allocation poool
	packagePool = sync.Pool{
		New: func() any { return &Package{} },
	}

	zeroTime = time.Time{}

	szPackage = int(reflect.TypeOf(Package{}).Size())
)

type Package struct {
	key     uint32         // identity
	nRows   int            // current number or rows
	maxRows int            // max number of rows (== block allocation size)
	pkIdx   int            // primary key index (position in schema)
	schema  *schema.Schema // mapping from fields to blocks in query order
	blocks  []*block.Block // loaded blocks (in schema order)
}

func New() *Package {
	return packagePool.Get().(*Package)
}

func (p *Package) KeyBytes() []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], p.key)
	return buf[:]
}

func (p Package) Key() uint32 {
	return p.key
}

func (p Package) IsJournal() bool {
	return p.key == JournalKeyId
}

func (p Package) IsTomb() bool {
	return p.key == TombstoneKeyId
}

func (p Package) IsResult() bool {
	return p.key == ResultKeyId
}

func (p Package) IsNil() bool {
	for _, b := range p.blocks {
		if b != nil {
			return false
		}
	}
	return true
}

func (p Package) IsDirty() bool {
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

// TODO: do we need a normative way to say packs are readonly (shared data)?
// func (p Package) IsWriteable() bool {
// 	return p.key >= ResultKeyId
// }

func (p Package) PkIdx() int {
	return p.pkIdx
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
	p.pkIdx = s.PkIndex()
	p.schema = s
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
		p.blocks[i] = block.New(blockTypes[field.Type()], p.maxRows)
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

func (p Package) Cols() int {
	return p.schema.NumFields()
}

func (p Package) Len() int {
	return p.nRows
}

func (p Package) Cap() int {
	return p.maxRows
}

func (p *Package) IsFull() bool {
	return p.nRows == p.maxRows
}

func (p *Package) CanGrow(n int) bool {
	if p.key == ResultKeyId {
		return true
	}
	return p.nRows+n <= p.maxRows
}

func (p *Package) HeapSize() int {
	var sz int = szPackage
	for _, v := range p.blocks {
		sz += v.HeapSize()
	}
	return sz
}

func (p Package) Blocks() []*block.Block {
	return p.blocks
}

func (p Package) Block(i int) *block.Block {
	return p.blocks[i]
}

// TODO: where is Clear() required outside of tests?
func (p *Package) Clear() {
	for _, b := range p.blocks {
		if b == nil {
			continue
		}
		b.Clear()
	}
	p.nRows = 0
}

func (p *Package) Release() {
	assert.Always(p != nil, "nil package release, potential use after free")
	for i := range p.blocks {
		if p.blocks[i] == nil {
			continue
		}
		p.blocks[i].DecRef()
		p.blocks[i] = nil
	}
	p.key = 0
	p.nRows = 0
	p.maxRows = 0
	p.pkIdx = 0
	p.schema = nil
	p.blocks = p.blocks[:0]
	packagePool.Put(p)
}

// inline sort package by primary key, only available for materialized/writable packs
func (p *Package) PkSort() {
	if !sort.IsSorted(p) {
		sort.Sort(p)
	}
}

func (p *Package) Less(i, j int) bool {
	assert.Always(p.pkIdx >= 0, "pksort requires primary key column")
	return p.blocks[p.pkIdx].Uint64().Less(i, j)
}

func (p *Package) Swap(i, j int) {
	for _, b := range p.blocks {
		if b == nil {
			continue
		}
		p.blocks[i].Swap(i, j)
	}
}
