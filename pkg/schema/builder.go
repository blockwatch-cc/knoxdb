// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"slices"
	"strings"

	"blockwatch.cc/knoxdb/pkg/schema/enum"
)

// SchemaOf creates a new schema for a struct or table
// from provided fields and options.
func SchemaOf(fields []*Field, opts ...Option) *Schema {
	s := NewSchema(opts...)

	// clone fields, relink child fields and assign new ids
	for _, f := range fields {
		f = f.Clone()
		if f.Id == 0 {
			f.Id = s.nextFieldId()
		}
		s.Fields = append(s.Fields, f)

		// unroll nested child schema fields in pre-order;
		// append, relink, assign new ids
		if f.Child != nil {
			// clone child schema including all nested fields
			newChild := f.Child.Clone()

			// enforce all child schema versions are equal
			newChild.Version = s.Version

			// add child fields to main schema
			for _, c := range newChild.Fields {
				// always create new ids
				c.Id = s.nextFieldId()
				s.Fields = append(s.Fields, c)
			}

			// replace child pointer
			f.Child = newChild
		}
	}

	// finalize child fields backwards for bottom up hashing
	for _, f := range slices.Backward(s.Fields) {
		if f.Child == nil {
			continue
		}
		f.Child.Finalize()
	}

	// relevel the type tree and assign parents
	s.Relevel(0, 0)

	// finalize, apply opts again
	return s.Finalize(opts...)
}

// FieldOf creates a new field with type and options. It allows
// easy construction of primitive type fields, but also provides
// advanced users the ability to create customized complex fields
// using options. Note not all combinations of types, flags and
// options will produce valid fields for schemas and indexes. See
// options documentation for details.
func FieldOf(typ FieldType, opts ...FieldOption) *Field {
	return NewField(typ, opts...)
}

// EnumOf creates a new enu field from an enum dictionary.
func EnumOf(e *enum.EnumDictionary, opts ...FieldOption) *Field {
	return NewField(Uint16, append([]FieldOption{
		WithEnum(e),
		WithName(e.Name()),
	}, opts...)...)
}

// ArrayOf creates a new fixed length string or byte array.
func ArrayOf(typ FieldType, n int, opts ...FieldOption) *Field {
	return NewField(typ, append([]FieldOption{WithArray(n)}, opts...)...)
}

// ListOf creates a new list (slice) of a primitive type. Options
// apply to the list field. To control options of the inner type
// use ListFor with a pre-built schema.
func ListOf(typ FieldType, opts ...FieldOption) *Field {
	// prepare child type
	child := NewField(typ, WithNullable(typ == Bytes || typ == Binary))

	// use dummy to extract field name from options
	dummy := NewField(typ, opts...)
	name := dummy.Name
	if name != "" {
		child.Name = name + "." + ElementName
	} else {
		child.Name = ElementName
	}

	s := &Schema{
		Name:    dummy.Name,
		Fields:  []*Field{child},
		Version: 1,
	}

	return NewField(List, append([]FieldOption{
		WithChildSchema(s),
		WithNullable(),
	},
		opts...)...,
	)
}

// ListFor creates a new list for complex types like structs or
// arrays. Options apply to the outer list type. Options for content
// types should be set when constructing fields for the internal schema.
func ListFor(s *Schema, opts ...FieldOption) *Field {
	f := NewField(List, append([]FieldOption{
		WithNullable(),
		WithChildSchema(s),
	}, opts...)...)

	for _, v := range f.Child.Fields {
		if f.Name != "" {
			if v.Name == "" {
				v.Name = f.Name + "." + ElementName
			} else {
				v.Name = strings.Join([]string{f.Name, ElementName, v.Name}, ".")
			}
		}
	}

	return f
}

// IndexOf creates a new table index for one or multiple fields in
// a base schema. Options control which fields are indexed and which
// extra fields are included in the index.
func IndexOf(base *Schema, typ IndexType, opts ...IndexOption) *IndexSchema {
	if typ == PrimaryKeyIndex {
		opts = append([]IndexOption{WithIndexFieldId(base.PkId())}, opts...)
	}
	return NewIndexSchema(typ, base, opts...)
}

func MapOf(keyT, valT FieldType, opts ...FieldOption) *Field {
	dummy := NewField(String, opts...)
	if dummy.Name == "" {
		dummy.Name = EntriesName
	}
	fKey := FieldOf(keyT, WithName(dummy.Name+"."+KeyName))
	fVal := FieldOf(valT, WithName(dummy.Name+"."+ValueName), WithNullable(valT == Bytes || valT == Binary))

	s := &Schema{
		Name:    dummy.Name,
		Fields:  []*Field{fKey, fVal},
		Version: 1,
	}
	return NewField(Map, append([]FieldOption{
		WithChildSchema(s),
		WithNullable(),
		WithName(EntriesName),
	},
		opts...)...,
	)
}

// TODO: Uups, no struct in struct
func MapFor(keyT, valT *Schema, opts ...FieldOption) *Field {
	fKey := FieldOf(0, WithName(KeyName), WithChildSchema(keyT))
	fVal := FieldOf(0, WithName(ValueName), WithChildSchema(valT), WithNullable())

	dummy := NewField(String, opts...)
	if dummy.Name == "" {
		dummy.Name = EntriesName
	}
	s := &Schema{
		Name:    dummy.Name,
		Fields:  []*Field{fKey, fVal},
		Version: 1,
	}
	return NewField(Map, append([]FieldOption{
		WithChildSchema(s),
		WithNullable(),
		WithName(EntriesName),
	},
		opts...)...,
	)
}
