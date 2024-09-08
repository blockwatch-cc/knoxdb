package pack

import (
	"sync"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// Compatibility helpers used during refactoring to v2

func schemaToFields(s *schema.Schema) FieldList {
	fields := make(FieldList, s.NumFields())
	for i, id := range s.FieldIDs() {
		f, _ := s.FieldById(id)
		field := &Field{
			Index: int(id),
			Name:  f.Name(),
			Alias: f.Name(),
			Type:  s2fType[f.Type()],
			Scale: int(f.Scale()),
		}
		if f.Is(types.FieldFlagPrimary) {
			field.Flags |= FlagPrimary
		}
		if f.Is(types.FieldFlagIndexed) {
			field.Flags |= FlagIndexed
		}
		if f.Compress().Is(types.FieldCompressSnappy) {
			field.Flags |= FlagCompressSnappy
		}
		if f.Compress().Is(types.FieldCompressLZ4) {
			field.Flags |= FlagCompressLZ4
		}
		if f.Index().Is(types.IndexTypeBloom) {
			field.Flags |= FlagBloom
			field.Flags &^= FlagIndexed
		}
		fields[i] = field
	}
	return fields
}

func fieldsToSchema(name string, fields FieldList, tinfo *typeInfo) *schema.Schema {
	sval, ok := schemaRegistry.Load(name)
	if ok {
		return sval.(*schema.Schema)
	}

	s := schema.NewSchema().WithName(name)
	for i, f := range fields {
		// bloom index has moved from flag to index
		index := types.IndexType(f.IKind)
		if f.Flags&FlagBloom > 0 {
			index = types.IndexTypeBloom
		}

		// flags are split to flags + compression
		flags := types.FieldFlags(f.Flags & 0x3)
		var comp types.FieldCompression
		if f.Flags&FlagCompressSnappy > 0 {
			comp |= types.FieldCompressSnappy
		}
		if f.Flags&FlagCompressLZ4 > 0 {
			comp |= types.FieldCompressLZ4
		}

		field := schema.NewField(f2sType[f.Type]).
			WithName(f.Alias).
			WithFlags(flags).
			WithScale(f.Scale).
			WithCompression(comp).
			WithIndex(index)

		// add info from known go type
		if tinfo != nil {
			field = field.WithGoType(
				tinfo.fields[i].typ,
				tinfo.fields[i].idx,
				tinfo.fields[i].offset,
			)
		}

		// attach field in order (there is only one order in v1)
		s.WithField(field)
	}
	s.Complete()
	schemaRegistry.Store(name, s)
	return s
}

var (
	schemaRegistry sync.Map

	f2sType = map[FieldType]types.FieldType{
		FieldTypeUndefined:  types.FieldTypeInvalid,
		FieldTypeDatetime:   types.FieldTypeDatetime,
		FieldTypeInt64:      types.FieldTypeInt64,
		FieldTypeUint64:     types.FieldTypeUint64,
		FieldTypeFloat64:    types.FieldTypeFloat64,
		FieldTypeBoolean:    types.FieldTypeBoolean,
		FieldTypeString:     types.FieldTypeString,
		FieldTypeBytes:      types.FieldTypeBytes,
		FieldTypeInt32:      types.FieldTypeInt32,
		FieldTypeInt16:      types.FieldTypeInt16,
		FieldTypeInt8:       types.FieldTypeInt8,
		FieldTypeUint32:     types.FieldTypeUint32,
		FieldTypeUint16:     types.FieldTypeUint16,
		FieldTypeUint8:      types.FieldTypeUint8,
		FieldTypeFloat32:    types.FieldTypeFloat32,
		FieldTypeInt256:     types.FieldTypeInt256,
		FieldTypeInt128:     types.FieldTypeInt128,
		FieldTypeDecimal256: types.FieldTypeDecimal256,
		FieldTypeDecimal128: types.FieldTypeDecimal128,
		FieldTypeDecimal64:  types.FieldTypeDecimal64,
		FieldTypeDecimal32:  types.FieldTypeDecimal32,
	}
	s2fType = map[types.FieldType]FieldType{}
)

func init() {
	for k, v := range f2sType {
		s2fType[v] = k
	}
}
