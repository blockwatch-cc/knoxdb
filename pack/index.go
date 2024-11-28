// Copyright (c) 2018-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package pack

import "fmt"

// HOWTO express layout?
// PackIndex
// BTreeIndex
// LSMIndex

type IndexData struct {
	Kind   IndexKind `json:"kind"`   // stored in table metadata
	Fields FieldList `json:"fields"` // stored in table metadata
}

type IndexList []Index

func (l IndexList) Find(name string) Index {
	for _, v := range l {
		if v.Fields().String() == name {
			return v
		}
	}
	return nil
}

// Collision handling
// - stores colliding hashes as duplicates
// - handles special case where colliding values cross pack boundaries
// - tombstone stores hash + primary key and we check both values on removal

type IndexKind int

type IndexValueFunc func(typ FieldType, val interface{}) uint64
type IndexValueAtFunc func(typ FieldType, pkg *Package, index, pos int) uint64
type IndexZeroAtFunc func(pkg *Package, index, pos int) bool

const (
	IndexKindNone      IndexKind = iota
	IndexKindHash                // any col (any type) -> uint64 FNV hash
	IndexKindInteger             // any col ((u)int64) -> pk (uint64)
	IndexKindComposite           // multiple cols binary key + binary pk (uint64, 8 byte)
)

func (t IndexKind) String() string {
	switch t {
	case IndexKindNone:
		return ""
	case IndexKindHash:
		return "hash"
	case IndexKindInteger:
		return "int"
	case IndexKindComposite:
		return "composite"
	default:
		return "invalid"
	}
}

func (t IndexKind) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t *IndexKind) UnmarshalText(d []byte) error {
	switch string(d) {
	case "":
		*t = IndexKindNone
	case "hash":
		*t = IndexKindHash
	case "int":
		*t = IndexKindInteger
	case "composite":
		*t = IndexKindComposite
	default:
		return fmt.Errorf("Invalid index type %q", string(d))
	}
	return nil
}

func (t IndexKind) ValueFunc() IndexValueFunc {
	switch t {
	case IndexKindHash, IndexKindComposite:
		return hashValue
	case IndexKindInteger:
		return intValue
	default:
		return nil
	}
}

func (t IndexKind) ValueAtFunc() IndexValueAtFunc {
	switch t {
	case IndexKindHash, IndexKindComposite: // FIXME
		return hashValueAt
	case IndexKindInteger:
		return intValueAt
	default:
		return nil
	}
}

func (t IndexKind) ZeroAtFunc() IndexZeroAtFunc {
	switch t {
	case IndexKindHash, IndexKindComposite: // FIXME
		return hashZeroAt
	case IndexKindInteger:
		return intZeroAt
	default:
		return nil
	}
}

func (t IndexKind) MayHaveCollisions() bool {
	switch t {
	case IndexKindHash:
		return true
	case IndexKindInteger:
		return true
	default:
		return false
	}
}
