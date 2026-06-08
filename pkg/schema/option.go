// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import "blockwatch.cc/knoxdb/pkg/schema/enum"

// Option defines a function type for schema options.
type Option func(*Schema)

// Enums adds enums from the provided registry to a schema's
// enum fields. Us this option with reflect.SchemaOf and SchemaFor[T]
// to initialize the schema with enums.
func Enums(r *enum.EnumRegistry) Option {
	return func(s *Schema) {
		s.UseEnums(r)
	}
}

func Name(n string) Option {
	return func(s *Schema) {
		if len(n) > 0 {
			s.Name = n
		}
	}
}

func Version(v uint32) Option {
	return func(s *Schema) {
		s.Version = v
	}
}

// FieldOption defines a function type for field options.
type FieldOption func(*Field)

func WithName(n string) FieldOption {
	return func(f *Field) {
		f.Name = n
	}
}

func WithParentId(id uint16) FieldOption {
	return func(f *Field) {
		f.ParentId = id
	}
}

func WithArray[T int | uint8](n T) FieldOption {
	return func(f *Field) {
		if n > 0 {
			f.Flags |= FlagArray
		} else {
			f.Flags &^= FlagArray
		}
		f.Scale = uint8(n)
	}
}

func WithScale[T int | ~uint8](n T) FieldOption {
	return func(f *Field) {
		f.Flags &^= FlagArray
		f.Scale = uint8(n)
	}
}

func WithFilter(t FilterType) FieldOption {
	return func(f *Field) {
		f.Filter = t
	}
}

func WithCompression(c Compression) FieldOption {
	return func(f *Field) {
		f.Compress = c
	}
}

func WithFlags(v FieldFlags) FieldOption {
	return func(f *Field) {
		f.Flags |= v
	}
}

func WithNullable(b ...bool) FieldOption {
	return func(f *Field) {
		if len(b) == 0 || b[0] {
			f.Flags |= FlagNullable
		} else {
			f.Flags &^= FlagNullable
		}
	}
}

func WithEnum(e *enum.EnumDictionary) FieldOption {
	return func(f *Field) {
		if e != nil {
			f.Flags |= FlagEnum
			f.Type = Uint16
		} else {
			f.Flags &^= FlagEnum
		}
		f.Enum = e
	}
}

func WithChildSchema(s *Schema) FieldOption {
	return func(f *Field) {
		f.Child = s.Clone()
	}
}

// IndexOption defines a function type for index options.
type IndexOption func(*IndexSchema)

func WithIndexName(name string) IndexOption {
	return func(idx *IndexSchema) {
		idx.Name = name
	}
}

func WithIndexField(name string) IndexOption {
	return func(idx *IndexSchema) {
		f, ok := idx.Base.Find(name)
		if ok {
			if len(idx.Fields) == 0 {
				idx.Name = makeIndexName(idx.Type, idx.Base, f)
			}
			idx.Fields = append(idx.Fields, f)
		}
	}
}

func WithIndexFieldId(id uint16) IndexOption {
	return func(idx *IndexSchema) {
		f, ok := idx.Base.FindId(id)
		if ok {
			if len(idx.Fields) == 0 {
				idx.Name = makeIndexName(idx.Type, idx.Base, f)
			}
			idx.Fields = append(idx.Fields, f)
		}
	}
}

func WithExtraField(name string) IndexOption {
	return func(idx *IndexSchema) {
		f, ok := idx.Base.Find(name)
		if ok {
			idx.Extra = append(idx.Extra, f)
		}
	}
}

func WithExtraFieldId(id uint16) IndexOption {
	return func(idx *IndexSchema) {
		f, ok := idx.Base.FindId(id)
		if ok {
			idx.Extra = append(idx.Extra, f)
		}
	}
}
