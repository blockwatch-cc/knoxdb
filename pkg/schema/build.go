// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"fmt"
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

			// clone case schemas and relink to the new field
			// pointers in newChild
			if f.Cases != nil {
				newCases := make([]*Schema, len(*f.Cases))
				for i, cs := range *f.Cases {
					newSchema := &Schema{
						Name:    cs.Name,
						Version: cs.Version,
						Fields:  slices.Clone(cs.Fields),
					}
					// relink fields
					if ok := newSchema.relink(newChild); ok {
						newCases[i] = newSchema.Finalize()
					} else {
						panic(fmt.Errorf("schema %s: failed to relink nested variant field %q", s.Name, f.Name))
					}
				}
				f.Cases = &newCases
			}

			// add child fields to main schema, always create new ids
			for _, c := range newChild.Fields {
				c.Id = s.nextFieldId()
				s.Fields = append(s.Fields, c)
			}

			// replace child pointer
			f.Child = newChild
		}
	}

	// relevel the type tree and assign parents
	s.Relevel(0, 0)

	// finalize child fields backwards for bottom up hashing
	for _, f := range slices.Backward(s.Fields) {
		if f.Child == nil {
			continue
		}
		f.Child.Finalize()
	}

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
	switch typ {
	case Date:
		opts = append(opts, WithScale(TIME_SCALE_DAY))
	case Bytes, Binary:
		opts = append([]FieldOption{WithNullable()}, opts...)
	case Union:
		// clone union metadata and nest field names
		ut := UnionType.Clone()
		ut.Name = peekFieldName(opts...)
		for _, f := range ut.Fields {
			f.Name = ut.Name + "." + f.Name
		}
		opts = append(
			append([]FieldOption{WithNullable()}, opts...),
			WithChildSchema(ut), // append last
		)
	}
	return NewField(typ, opts...)
}

// EnumOf creates a new enu field from an enum dictionary.
func EnumOf(e *enum.Dictionary, opts ...FieldOption) *Field {
	return NewField(Uint16, append([]FieldOption{
		WithEnum(e),
		WithName(e.Name()),
	}, opts...)...)
}

// ArrayOf creates a new fixed length string or byte array.
func ArrayOf(typ FieldType, n int, opts ...FieldOption) *Field {
	return FieldOf(typ, append([]FieldOption{WithArray(n), WithNullable(false)}, opts...)...)
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

// ListOf creates a new list (slice) of a primitive type. Options
// apply to the list field. To control options of the inner type
// use ListFor with a pre-built schema.
func ListOf(typ FieldType, opts ...FieldOption) *Field {
	// peek field name from options
	name := peekFieldName(opts...)
	if name != "" {
		name = name + "." + ElementName
	} else {
		name = ElementName
	}

	// prepare child type
	child := FieldOf(typ, WithName(name), WithNullable(typ.NullableDefault()))

	s := SchemaOf([]*Field{child}, Name(name), Version(1))

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
		WithChildSchema(s.Clone()),
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

// MapOf creates a new map type from primitive types for key and value.
// Options apply to the outer map type. For controlling key and value
// type options use MapFor.
func MapOf(keyT, valT FieldType, opts ...FieldOption) *Field {
	// peek field name
	name := peekFieldName(opts...)
	if name == "" {
		name = EntriesName
	}

	// create schema
	s := SchemaOf([]*Field{
		FieldOf(keyT, WithName(name+"."+KeyName)),
		FieldOf(valT,
			WithName(name+"."+ValueName),
			WithNullable(valT.NullableDefault()),
		),
	},
		Name(name),
		Version(1),
	)

	// wrap into map field
	return NewField(Map, append([]FieldOption{
		WithName(name),
		WithChildSchema(s),
		WithNullable(),
	},
		opts...)...,
	)
}

// MapFor creates a new map type with a primitive key and a complex value.
// Options apply to the outer map type. Unlike Go built-in hash maps,
// schema maps are represented as lists of key/value structs. Maps contain
// unique sorted keys and it is permitted to nest lists and maps into a
// map value type field.
func MapFor(keyT FieldType, valS *Schema, opts ...FieldOption) *Field {
	// peek field name and enum
	dummy := NewField(String, opts...)
	name := dummy.Name
	if name == "" {
		name = EntriesName
	}

	// create map schema which is essentially a list of {key,value} pairs
	// however, because there is no support for struct-in-struct (yet?),
	// we allow only a single key field and merge value fields into the
	// same struct and then use this struct as child schema on a Map field
	// SchemaOf clones and assignes new sequential ids
	s := SchemaOf(
		append(
			[]*Field{FieldOf(keyT,
				WithName(name+"."+KeyName), // prefix key
				WithEnum(dummy.Enum)),      // allow enum keys
			},
			valS.Fields..., // SchemaOf will clone those fields
		),
		Name(name),
		Version(1),
	)

	// prefix value fields
	for _, f := range s.Fields[1:] {
		if f.Name == "" {
			f.Name = name + "." + ValueName
		} else {
			f.Name = name + "." + ValueName + "." + f.Name
		}
		if f.Child != nil {
			for _, cf := range f.Child.Fields {
				cf.Name = f.Name + "." + cf.Name
			}
		}
	}

	// wrap into map field
	return NewField(Map, append([]FieldOption{
		WithChildSchema(s),
		WithNullable(),
		WithName(EntriesName),
	},
		opts...)...,
	)
}

// VariantFor creates a new tagged union of user-defined schemas, where each
// record (or row) uses exactly one of the defined type cases. Variant fields
// use a distinct field structure internally
//
//		              Variant Field
//		      +-------------+--------------------------+
//	          |                                      Cases
//	          |                                  +-----+-----+
//		    Child                                T1    T2    TN        (schemas)
//		      |                                  |     |     |
//	+---------+-----------+-----------+          |     |     |
//	|         |           |           |          |     |     |
//	vtag(u8) vidx(u32)  T1/F0..n .. TN/F0..n   F0..n  F0..n  F0..n     (fields)
//	__________________  ____________________   ___________________
//	Metadata            Flat case fields       Nested case schemas
func VariantFor(cases []*Schema, opts ...FieldOption) *Field {
	// create field
	field := FieldOf(Variant, opts...)

	// create metadata schema
	field.Child = SchemaOf([]*Field{
		// type id
		FieldOf(Uint8, WithName(field.Name+"."+VariantTagName), WithFlags(FlagMetadata)),
		// offsets
		FieldOf(Uint32, WithName(field.Name+"."+VariantIndexName), WithFlags(FlagMetadata)),
	}, Name(field.Name), Version(1))

	// alloc cases slice
	caseSchemas := make([]*Schema, len(cases))
	field.Cases = &caseSchemas

	// attach variant case schemas
	for i, cs := range cases {
		// clone child schema; we're going to change fields
		cs = cs.Clone()

		// prefix name and assign case id (+1 since caseid=0 has meaning)
		for _, cf := range cs.Fields {
			cf.Name = strings.Join([]string{field.Name, cs.Name, cf.Name}, ".")
			cf.CaseId = uint8(i + 1)
		}

		// store as case
		caseSchemas[i] = cs

		// collect per-case fields into child schema and assign unique ids
		// to each case schema field (this preserves the invariant that a
		// schema must contain unique ids which is required for correct
		// le-linking when the variant field is used in higher-level schemas)
		for _, cf := range cs.Fields {
			cf.Id = field.Child.nextFieldId()
			field.Child.Fields = append(field.Child.Fields, cf)
		}
	}

	return field
}
