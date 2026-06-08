// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reflect

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"blockwatch.cc/knoxdb/pkg/schema"
)

// Lessons learned
// - native types int/uint are disabled due to size ambiguity
// - anon struct embedding is disabled for low relevance
// - marshaler interfaces are too expensive for encoding (interface checks)
// - keep Go type/reflect and memory layout info out of main schema
// - Go hash maps are too expensive to walk (reflect and range make copies)
//   and almost impossible to walk from unsafe.Pointer struct fields

const TAG_NAME = "knox"

var schemaRegistry sync.Map

func LookupSchema(typ reflect.Type) (*schema.Schema, bool) {
	sval, ok := schemaRegistry.Load(typ)
	if ok {
		return sval.(*schema.Schema), ok
	}
	return nil, ok
}

func SchemaFor[T any](opts ...schema.Option) (*schema.Schema, error) {
	return detectSchema(reflect.TypeFor[T](), TAG_NAME, opts...)
}

func MustSchemaFor[T any](opts ...schema.Option) *schema.Schema {
	s, err := SchemaFor[T](opts...)
	if err != nil {
		panic(err)
	}
	return s
}

func SchemaOf(m any, opts ...schema.Option) (*schema.Schema, error) {
	// interface must not be nil
	if m == nil {
		return nil, schema.ErrNilValue
	}
	// validate type
	val := reflect.Indirect(reflect.ValueOf(m))
	if !val.IsValid() {
		return nil, fmt.Errorf("invalid value of type %T", m)
	}
	return detectSchema(reflect.TypeOf(m), TAG_NAME, opts...)
}

func MustSchemaOf(m any, opts ...schema.Option) *schema.Schema {
	s, err := SchemaOf(m, opts...)
	if err != nil {
		panic(err)
	}
	return s
}

func SchemaOfTag(m any, tag string, opts ...schema.Option) (*schema.Schema, error) {
	// interface must not be nil
	if m == nil {
		return nil, schema.ErrNilValue
	}
	// validate type
	val := reflect.Indirect(reflect.ValueOf(m))
	if !val.IsValid() {
		return nil, fmt.Errorf("invalid value of type %T", m)
	}
	return detectSchema(reflect.TypeOf(m), tag, opts...)
}

func detectSchema(typ reflect.Type, tag string, opts ...schema.Option) (*schema.Schema, error) {
	typ, err := unwrapEligibleType(typ)
	if err != nil {
		return nil, err
	}

	// lookup registry
	if s, ok := LookupSchema(typ); ok {
		// apply schema options
		if len(opts) > 0 {
			// clone to keep registered schema immutable
			s = s.Clone().Finalize(opts...)
		}

		return s, nil
	}

	// create new schema
	b := newBuilder(typ, tag)

	// process struct and detect fields
	if _, err := b.inferStruct(typ); err != nil {
		return nil, err
	}

	// use the flattened builder schema from now on
	s := b.schema

	// relevel the type tree and assign parents
	s.Relevel(0, 0)

	// calculate wire size
	s.Finalize(opts...)

	// validate schema conformance
	if err := s.Validate(); err != nil {
		return nil, err
	}

	// register schema
	schemaRegistry.Store(typ, s)

	return s, nil
}

func unwrapEligibleType(typ reflect.Type) (reflect.Type, error) {
	// must be a struct, pointer to struct or slice of struct
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("type %s (%s) is not a struct", typ, typ.Kind())
	}
	return typ, nil
}

// Produces a dynamic struct type only using native types like
// int64 for Decimal64, [16]byte for Int128, etc. to make internal
// types compatible with external libraries.
func NativeStructType(s *schema.Schema) reflect.Type {
	sfields := make([]reflect.StructField, 0, len(s.Fields))
	for _, f := range s.Fields {
		if !f.IsVisible() {
			continue
		}
		var rtyp reflect.Type
		switch f.Type {
		case schema.Timestamp, schema.Time, schema.Date, schema.Int64, schema.Decimal64:
			rtyp = typeOfInt64
		case schema.Int32, schema.Decimal32:
			rtyp = typeOfInt32
		case schema.Int16:
			rtyp = typeOfInt16
		case schema.Int8:
			rtyp = typeOfInt8
		case schema.Uint64:
			rtyp = typeOfUint64
		case schema.Uint32:
			rtyp = typeOfUint32
		case schema.Uint16:
			rtyp = typeOfUint16
		case schema.Uint8:
			rtyp = typeOfUint8
		case schema.Float64:
			rtyp = typeOfFloat64
		case schema.Float32:
			rtyp = typeOfFloat32
		case schema.Boolean:
			rtyp = typeOfBool
		case schema.String, schema.Text:
			rtyp = typeOfString
		case schema.Bytes, schema.Bigint, schema.Binary:
			if f.IsArray() {
				rtyp = reflect.ArrayOf(int(f.Scale), reflect.TypeFor[byte]())
			} else {
				rtyp = typeOfByteSlice
			}
		case schema.Int256, schema.Decimal256:
			rtyp = reflect.TypeFor[[32]byte]()
		case schema.Int128, schema.Decimal128:
			rtyp = reflect.TypeFor[[16]byte]()
		case schema.List:
			// TODO: use native types only
			if len(f.Child.Fields) == 1 {
				rtyp = reflect.SliceOf(TypeOf(f))
			} else {
				rtyp = reflect.SliceOf(StructTypeOf(f.Child, f.Name+"."+schema.ElementName+"."))
			}
		default:
			continue
		}
		sfields = append(sfields, reflect.StructField{
			Name: toCamelCase(sanitize(f.Name), sep),
			Type: rtyp,
		})
	}
	return reflect.StructOf(sfields)
}

// Produces a dynamic Go struct type compatible with SchemaOf which
// uses native and custom types (e.g. num.Int128, num.Decimal64).
// Adds struct tags, but excludes index tags.
func StructTypeOf(s *schema.Schema, prefix ...string) reflect.Type {
	// pre-alloc space for all top-level fields
	sfields := make([]reflect.StructField, 0, s.NumFields())

	// selective skip and prefix trim for nested names; Note this
	// method is called recursive StructTypeOf -> TypeOf -> StructTypeOf
	var dropPrefix string
	if len(prefix) > 0 {
		dropPrefix = prefix[0]
	}
	lvl := s.Fields[0].Level
	for _, f := range s.Fields {
		// skip deleted and metadata fields
		if !f.IsVisible() {
			continue
		}

		// skip nested fields with same prefix as parent
		if f.Level != lvl {
			continue
		}

		// adjust field name by stripping the common prefix on
		// nested names
		name := strings.TrimPrefix(f.Name, dropPrefix)

		// write struct tag `knox:"name,flags"`
		tag := fmt.Sprintf(`%s:"%s,%s`, TAG_NAME, name, makeTag(f))

		// recurse into lists and add list element tag info
		if f.Type == schema.List {
			if len(f.Child.Fields) == 1 {
				if ctag := makeTag(f.Child.Fields[0]); ctag != "" {
					tag += "," + schema.ElementName + "=" + ctag
				}
			}
		}

		// recurse into maps and add key/val element tag info
		if f.Type == schema.Map {
			if ctag := makeTag(f.Child.Fields[0]); ctag != "" {
				tag += "," + schema.KeyName + "=" + ctag
			}
			if len(f.Child.Fields) == 2 {
				if ctag := makeTag(f.Child.Fields[1]); ctag != "" {
					tag += "," + schema.ValueName + "=" + ctag
				}
			}
		}

		// close struct tag
		tag += `"`

		// create Go struct field
		sfields = append(sfields, reflect.StructField{
			Name: toCamelCase(sanitize(name), sep),
			Type: TypeOf(f),
			Tag:  reflect.StructTag(tag),
		})
	}
	return reflect.StructOf(sfields)
}

func makeTag(f *schema.Field) string {
	// id
	tag := fmt.Sprintf(`id=%d`, f.Id)

	// type modifier
	switch f.Type {
	case schema.Time:
		tag += ",time"
	case schema.Date:
		tag += ",date"
	case schema.Text:
		tag += ",text"
	case schema.Binary:
		tag += ",blob"
	case schema.String:
		if f.IsArray() {
			tag += fmt.Sprintf(",array=%d", f.Scale)
		}
	default:
		// scale for byte, decimal, timestamps
		if !f.IsArray() && f.Scale > 0 {
			tag += fmt.Sprintf(",scale=%d", f.Scale)
		}
	}

	// flags
	if flags := f.Flags &^ schema.FlagArray; flags > 0 {
		tag += "," + flags.String()
	}

	// slices are nullable by default, so check for notnull
	switch f.Type {
	case schema.List, schema.Map, schema.Bytes, schema.Binary:
		if f.Flags&schema.FlagNullable == 0 {
			tag += ",notnull"
		}
	}

	// compression
	if f.IsCompressed() {
		tag += ",zip=" + f.Compress.String()
	}
	return tag
}

// TypeOf returns the correct Go reflect.Type for a schema field.
func TypeOf(f *schema.Field) reflect.Type {
	if f.Type == schema.Bytes && f.IsArray() {
		return reflect.ArrayOf(int(f.Scale), reflect.TypeFor[byte]())
	}
	if f.Type == schema.Uint16 && f.IsEnum() {
		return reflect.TypeFor[string]()
	}
	if f.Type == schema.List {
		if f.Child.NumFields() == 1 {
			return reflect.SliceOf(TypeOf(f.Child.Fields[0]))
		} else {
			return reflect.SliceOf(StructTypeOf(f.Child, f.Name+"."+schema.ElementName+"."))
		}
	}
	if f.Type == schema.Map {
		keyT := TypeOf(f.Child.Fields[0])
		var valT reflect.Type
		if len(f.Child.Fields) == 2 {
			valT = TypeOf(f.Child.Fields[1])
		} else {
			valS := &schema.Schema{
				Name:    f.Child.Name,
				Version: f.Child.Version,
				Fields:  f.Child.Fields[1:],
			}
			valT = StructTypeOf(valS, f.Name+"."+schema.ValueName+".")
		}
		return reflect.MapOf(keyT, valT)
	}
	return reflect.TypeOf(f.Type.Zero())
}
