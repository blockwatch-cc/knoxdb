// Copyright (c) 2014 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"encoding/binary"
	"reflect"
	"sync"
	"time"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var (
	// reserved package keys
	journalKeyId   uint32 = 0xFFFFFFFF
	tombstoneKeyId uint32 = 0xFFFFFFFE
	resultKeyId    uint32 = 0xFFFFFFFD
	journalKey            = []byte("_journal")
	tombstoneKey          = []byte("_tombstone")
	resultKey             = []byte("_result")

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
	pkIdx   int            // primary key index
	schema  *schema.Schema // mapping from fields to blocks in query order
	blocks  []*block.Block // loaded blocks (in schema order)

	dirty bool // pack is updated, needs to be written
	// size  int  // storage size in bytes
}

func New() *Package {
	return packagePool.Get().(*Package)
}

func (p *Package) Key() []byte {
	return encodePackKey(p.key)
}

func (p Package) KeyU32() uint32 {
	return p.key
}

func encodePackKey(key uint32) []byte {
	switch key {
	case journalKeyId:
		return journalKey
	case tombstoneKeyId:
		return tombstoneKey
	case resultKeyId:
		return resultKey
	default:
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], key)
		return buf[:]
	}
}

func (p Package) IsJournal() bool {
	return p.key == journalKeyId
}

func (p Package) IsTomb() bool {
	return p.key == tombstoneKeyId
}

func (p Package) IsResult() bool {
	return p.key == resultKeyId
}

func (p Package) IsWriteable() bool {
	return p.key >= resultKeyId
}

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
	for i, field := range s.Exported() {
		p.blocks[i] = block.New(blockTypes[field.Type], p.maxRows)
	}
	p.pkIdx = s.PkIndex()
	p.schema = s
	return p
}

// func (p *Package) WithBlock(i int, b *block.Block) *Package {
// 	p.blocks[i] = b
// 	p.nRows = b.Len()
// 	return p
// }

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
	if p.key == resultKeyId {
		return true
	}
	return p.nRows+n <= p.maxRows
}

// func (p *Package) HeapSize() int {
// 	var sz int = szPackage
// 	for _, v := range p.blocks {
// 		sz += v.HeapSize()
// 	}
// 	return sz
// }

// func (p Package) DiskSize() int {
// 	return p.size
// }

func (p Package) Blocks() []*block.Block {
	return p.blocks
}

func (p Package) Block(i int) *block.Block {
	return p.blocks[i]
}

// func (p Package) NewMetadata() metadata.PackMetadata {
// 	h := metadata.PackMetadata{
// 		Key:      p.key,
// 		SchemaId: p.schema.Key(),
// 		NValues:  p.nRows,
// 		Blocks:   make([]BlockMetadata, p.schema.NumFields()),
// 		// Size:     p.size,
// 		// dirty: true,
// 	}
// 	for i, v := range p.blocks {
// 		if v == nil {
// 			h.Blocks[i] = metadata.EmptyBlockMetadata
// 		}
// 		h.Blocks[i] = metadata.NewBlockMetadata(v, p.schema.Field(i))
// 	}
// 	return h
// }

// func (p Package) UpdateMetadata(m *metadata.PackMetadata) error {
// 	// check schema
// 	// TODO: allow schema changes
// 	if m.SchemaId != pkg.schema.Key() {
// 		return fmt.Errorf("knox: schema mismatch in pack %08x: %08x != %08x ",
// 			m.key, m.SchemaId, pkg.schema.Key())
// 	}

// 	for i, b := range p.blocks {
// 		// nil blocks refer to deleted fields
// 		if b == nil {
// 			m.Blocks[i] = metadata.EmptyBlockMetadata
// 		}

// 		// skip unchanged blocks
// 		if !b.IsDirty() {
// 			continue
// 		}

// 		// // sanity check
// 		// if have, want := m.Blocks[i].Type, b.Type(); have != want {
// 		// 	return fmt.Errorf("knox: block type mismatch in pack %08x/%08x: %s != %s ",
// 		// 		m.Key, p.key, have, want)
// 		// }

// 		// create new metadata
// 		m.Blocks[i] = NewBlockMetadata(b, pkg.schema.Field(i))

// 		// signal that this pack metadata must be saved
// 		m.Dirty = true
// 	}
// 	return nil
// }

// TODO: where is this required outside of tests?
func (p *Package) Clear() {
	for _, b := range p.blocks {
		if b == nil {
			continue
		}
		b.Clear()
	}
	p.nRows = 0
	// p.size = 0
	p.dirty = false
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
	p.dirty = false
	p.blocks = p.blocks[:0]
	packagePool.Put(p)
}
