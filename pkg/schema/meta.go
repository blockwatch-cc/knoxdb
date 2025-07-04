// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import "blockwatch.cc/knoxdb/internal/types"

const (
	// reserved metadata field ids
	MetaRid  uint16 = 0xFFFF
	MetaRef  uint16 = 0xFFFE
	MetaXmin uint16 = 0xFFFD
	MetaXmax uint16 = 0xFFFC
	MetaDel  uint16 = 0xFFFB
)

// Internal schema for record metadata
type Meta struct {
	Rid   uint64    `knox:"$rid,internal,id=0xffff"`  // unique row id
	Ref   uint64    `knox:"$ref,internal,id=0xfffe"`  // previous version, ref == rid on first insert
	Xmin  types.XID `knox:"$xmin,internal,id=0xfffd"` // txid where this row was created
	Xmax  types.XID `knox:"$xmax,internal,id=0xfffc"` // txid where this row was deleted
	IsDel bool      `knox:"$del,internal,id=0xfffb"`  // record was deleted (true) or updated (false)
}

var (
	MetaSchema   = MustSchemaOf(Meta{})
	MetaFieldIds = []uint16{MetaRid, MetaRef, MetaXmin, MetaXmax, MetaDel}
)

func (s *Schema) WithMeta() *Schema {
	// check if metadata fields already exist
	if _, ok := s.FieldById(MetaRid); ok {
		return s
	}

	// ensure no collision with user defined fields
	for _, v := range s.fields {
		for _, vv := range MetaSchema.fields {
			if v.name == vv.name {
				return s
			}
			if v.id == vv.id {
				return s
			}
		}
	}

	// add metadata fields (internal fields don't change hash)
	clone := s.Clone()
	clone.fields = append(clone.fields, MetaSchema.fields...)
	return clone.Finalize()
}

func (s *Schema) HasMeta() bool {
	return s.RowIdIndex() >= 0
}
