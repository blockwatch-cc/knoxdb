// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"errors"

	"blockwatch.cc/knoxdb/pkg/schema"
)

const (
	// reserved metadata field ids
	MetaRid    uint16 = 0xFFFF
	MetaRef    uint16 = 0xFFFE
	MetaXmin   uint16 = 0xFFFD
	MetaXmax   uint16 = 0xFFFC
	MetaDel    uint16 = 0xFFFB
	MetaAction uint16 = 0xFFFA
)

// Internal schema for record metadata
type Meta struct {
	Rid   uint64 `knox:"$rid,metadata,id=0xffff"`  // unique row id
	Ref   uint64 `knox:"$ref,metadata,id=0xfffe"`  // previous version, ref == rid on first insert
	Xmin  XID    `knox:"$xmin,metadata,id=0xfffd"` // txid where this row was created
	Xmax  XID    `knox:"$xmax,metadata,id=0xfffc"` // txid where this row was deleted
	IsDel bool   `knox:"$del,metadata,id=0xfffb"`  // record was deleted (true) or updated (false)
}

var (
	MetaFieldIds = []uint16{MetaRid, MetaRef, MetaXmin, MetaXmax, MetaDel}
	MetaSchema   = &schema.Schema{
		Name: "meta",
		Fields: []*schema.Field{
			{Name: "$rid", Id: MetaRid, Type: FT_U64, Flags: F_METADATA},
			{Name: "$ref", Id: MetaRef, Type: FT_U64, Flags: F_METADATA},
			{Name: "$xmin", Id: MetaXmin, Type: FT_U64, Flags: F_METADATA},
			{Name: "$xmax", Id: MetaXmax, Type: FT_U64, Flags: F_METADATA},
			{Name: "$del", Id: MetaDel, Type: FT_BOOL, Flags: F_METADATA},
		},
	}

	ErrNoMeta = errors.New("schema: missing metadata fields")
)

type TableSchema struct {
	*schema.Schema
}

func NewTableSchema() *TableSchema {
	return &TableSchema{schema.NewSchema()}
}

// MakeTableSchema extends a schema with metadata fields. The extended schema
// will have the same identity as the original. Metadata is treated
// as internal info and skipped by struct encoders.
func MakeTableSchema(s *schema.Schema) (*TableSchema, error) {
	// ensure no collision with user defined fields
	for _, v := range s.Fields {
		for _, vv := range MetaSchema.Fields {
			if v.Name == vv.Name {
				return nil, schema.ErrDuplicateName
			}
			if v.Id == vv.Id {
				return nil, schema.ErrDuplicateId
			}
		}
	}

	// clone schema and add metadata fields (internal fields
	// don't change hash and are not exported to user structs)
	s = s.Clone()
	s.Fields = append(s.Fields, MetaSchema.Fields...)

	// switch pk to $rowid when pk field is missing
	if s.PkId() == 0 {
		if rid, ok := s.FindId(MetaRid); ok {
			rid.Flags |= F_PRIMARY
		}
	}

	return &TableSchema{s.Finalize()}, nil
}

func (s *TableSchema) Base() *schema.Schema {
	return s.Schema
}

// switch primary key field to row id (used on history tables)
func (s *TableSchema) ResetPk(id uint16) {
	// both fields exist (but may be the same)
	pk, rid := s.Pk(), s.RowId()
	if pk == rid {
		return
	}

	// flip primary key flag
	pk.Flags &^= F_PRIMARY
	rid.Flags |= F_PRIMARY

	// flag changes affect schema hash
	s.Finalize()
}

func (s *TableSchema) RowId() *schema.Field {
	for _, f := range s.Fields {
		if f.Id == MetaRid {
			return f
		}
	}
	return nil
}

func (s *TableSchema) RowIdIndex() int {
	for i, f := range s.Fields {
		if f.Id == MetaRid {
			return i
		}
	}
	return -1
}
