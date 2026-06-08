// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reflect

import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/schema"
)

var layoutRegistry sync.Map

func LookupLayout(typ reflect.Type) (*Layout, bool) {
	val, ok := layoutRegistry.Load(typ)
	if ok {
		return val.(*Layout), ok
	}
	return nil, ok
}

// Layout encodes the memory layout for a schema's Go type.
// It is an abbreviated version of reflect.Type but in sync
// with the structure of Schema/Fields and meant for use by
// encoders and decoders.
type Layout struct {
	Type        reflect.Type       // reference to type for reflect.MakeSlice
	Offsets     []uintptr          // struct field offsets
	Size        uintptr            // in-memory size of this type with padding
	Children    map[uint32]*Layout // nested types by field id (uint32 is more efficient)
	Marshaler   unsafe.Pointer     // ptr to type itab
	Unmarshaler unsafe.Pointer     // ptr to type itab
}

func LayoutOf(m any, s *schema.Schema) (*Layout, error) {
	// interface must not be nil
	if m == nil {
		return nil, schema.ErrNilValue
	}
	// validate type
	val := reflect.Indirect(reflect.ValueOf(m))
	if !val.IsValid() {
		return nil, fmt.Errorf("invalid value of type %T", m)
	}
	typ, err := unwrapEligibleType(reflect.TypeOf(m))
	if err != nil {
		return nil, err
	}
	return inferStructLayout(typ, s, TAG_NAME)
}

func LayoutFor[T any]() (*Layout, error) {
	s, err := SchemaFor[T]()
	if err != nil {
		return nil, err
	}
	typ, err := unwrapEligibleType(reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	return inferStructLayout(typ, s, TAG_NAME)
}

func inferStructLayout(typ reflect.Type, s *schema.Schema, tag string) (*Layout, error) {
	// lookup registry
	if l, ok := LookupLayout(typ); ok {
		return l, nil
	}

	// create a layout
	layout := &Layout{
		Type:    typ,
		Size:    typ.Size(),
		Offsets: make([]uintptr, len(s.Fields)),
	}

	// check if the type implements marshalers
	if typ.Implements(marshalerType) || typ.Implements(unmarshalerType) {
		val := reflect.New(typ)
		if ival, ok := val.Elem().Interface().(schema.Marshaler); ok {
			layout.Marshaler = (*iface)(unsafe.Pointer(&ival)).itab
		}
		if ival, ok := val.Interface().(schema.Unmarshaler); ok {
			layout.Unmarshaler = (*iface)(unsafe.Pointer(&ival)).itab
		}
	}

	var (
		r   int
		lvl = s.Fields[0].Level
	)

inferLoop:
	for i, f := range s.Fields {
		// next top-level schema field
		if f.Level != lvl {
			continue
		}

		// skip invisible fields
		if !f.IsVisible() {
			r++
			continue
		}

		// get the matching reflect struct field
		sf := typ.Field(r)

		// skip private and empty reflect fields
		for !sf.IsExported() || sf.Tag.Get(tag) == "-" || sf.Type == emptyType {
			r++
			if r == typ.NumField() {
				break inferLoop
			}
			sf = typ.Field(r)
		}

		// collect struct field
		layout.Offsets[i] = sf.Offset
		r++

		// recurse on slice and map types (note map keys are primitive types only)
		if f.Child != nil {
			// unwrap pointer
			t := sf.Type
			if t.Kind() == reflect.Pointer {
				t = t.Elem()
			}

			// trim map key field from schema
			childSchema := f.Child
			if f.Type == schema.Map {
				childSchema = &schema.Schema{
					Name:    f.Child.Name,
					Version: f.Child.Version,
					Fields:  f.Child.Fields[1:],
				}
			}

			// recurse into slice element or map value type
			child, err := inferLayout(t.Elem(), childSchema, tag)
			if err != nil {
				return nil, err
			}
			if layout.Children == nil {
				layout.Children = make(map[uint32]*Layout)
			}
			layout.Children[uint32(f.Id)] = child
		}
	}

	// register layout
	layoutRegistry.Store(typ, layout)

	return layout, nil
}

var (
	marshalerType   = reflect.TypeFor[schema.Marshaler]()
	unmarshalerType = reflect.TypeFor[schema.Unmarshaler]()
)

type iface struct {
	itab unsafe.Pointer // Pointer to interface table (type + method pointers)
	data unsafe.Pointer // Pointer to the concrete value
}

func inferLayout(typ reflect.Type, s *schema.Schema, tag string) (*Layout, error) {
	if typ.Kind() == reflect.Struct && len(s.Fields) > 1 {
		return inferStructLayout(typ, s, tag)
	}

	// create a layout
	layout := &Layout{
		Type:    typ,
		Size:    typ.Size(),
		Offsets: make([]uintptr, 1),
	}

	f := s.Fields[0]

	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}

	// check if the type implements marshalers
	if typ.Implements(marshalerType) || typ.Implements(unmarshalerType) {
		val := reflect.New(typ)
		if ival, ok := val.Elem().Interface().(schema.Marshaler); ok {
			layout.Marshaler = (*iface)(unsafe.Pointer(&ival)).itab
		}
		if ival, ok := val.Interface().(schema.Unmarshaler); ok {
			layout.Unmarshaler = (*iface)(unsafe.Pointer(&ival)).itab
		}
	}

	// recurse on slice and map types
	if f.Child != nil {
		// trim map key field from schema
		childSchema := f.Child
		if f.Type == schema.Map {
			childSchema = &schema.Schema{
				Name:    f.Child.Name,
				Version: f.Child.Version,
				Fields:  f.Child.Fields[1:],
			}
		}

		// recurse into slice element or map value type
		child, err := inferLayout(typ.Elem(), childSchema, tag)
		if err != nil {
			return nil, err
		}
		if layout.Children == nil {
			layout.Children = make(map[uint32]*Layout)
		}
		layout.Children[uint32(f.Id)] = child
	}

	return layout, nil
}
