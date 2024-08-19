package pack

import (
	"sync"

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
		if f.Is(schema.FieldFlagPrimary) {
			field.Flags |= FlagPrimary
		}
		if f.Is(schema.FieldFlagIndexed) {
			field.Flags |= FlagIndexed
		}
		if f.Compress().Is(schema.FieldCompressSnappy) {
			field.Flags |= FlagCompressSnappy
		}
		if f.Compress().Is(schema.FieldCompressLZ4) {
			field.Flags |= FlagCompressLZ4
		}
		if f.Index().Is(schema.IndexKindBloom) {
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
		index := schema.IndexKind(f.IKind)
		if f.Flags&FlagBloom > 0 {
			index = schema.IndexKindBloom
		}

		// flags are split to flags + compression
		flags := schema.FieldFlags(f.Flags & 0x3)
		var comp schema.FieldCompression
		if f.Flags&FlagCompressSnappy > 0 {
			comp |= schema.FieldCompressSnappy
		}
		if f.Flags&FlagCompressLZ4 > 0 {
			comp |= schema.FieldCompressLZ4
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

	f2sType = map[FieldType]schema.FieldType{
		FieldTypeUndefined:  schema.FieldTypeInvalid,
		FieldTypeDatetime:   schema.FieldTypeDatetime,
		FieldTypeInt64:      schema.FieldTypeInt64,
		FieldTypeUint64:     schema.FieldTypeUint64,
		FieldTypeFloat64:    schema.FieldTypeFloat64,
		FieldTypeBoolean:    schema.FieldTypeBoolean,
		FieldTypeString:     schema.FieldTypeString,
		FieldTypeBytes:      schema.FieldTypeBytes,
		FieldTypeInt32:      schema.FieldTypeInt32,
		FieldTypeInt16:      schema.FieldTypeInt16,
		FieldTypeInt8:       schema.FieldTypeInt8,
		FieldTypeUint32:     schema.FieldTypeUint32,
		FieldTypeUint16:     schema.FieldTypeUint16,
		FieldTypeUint8:      schema.FieldTypeUint8,
		FieldTypeFloat32:    schema.FieldTypeFloat32,
		FieldTypeInt256:     schema.FieldTypeInt256,
		FieldTypeInt128:     schema.FieldTypeInt128,
		FieldTypeDecimal256: schema.FieldTypeDecimal256,
		FieldTypeDecimal128: schema.FieldTypeDecimal128,
		FieldTypeDecimal64:  schema.FieldTypeDecimal64,
		FieldTypeDecimal32:  schema.FieldTypeDecimal32,
	}
	s2fType = map[schema.FieldType]FieldType{}
)

func init() {
	for k, v := range f2sType {
		s2fType[v] = k
	}
}
