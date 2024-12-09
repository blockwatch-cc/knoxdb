// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Copyright (c) 2017 Alexander Eichhorn
// Author: alex@blockwatch.cc

package csv

import (
	"encoding"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

const tagName = "csv"

// typeInfo holds details for the xml representation of a type.
type typeInfo struct {
	fields []fieldInfo
}

// fieldInfo holds details for the xmp representation of a single field.
type fieldInfo struct {
	idx   []int
	name  string
	flags fieldFlags
}

func (f fieldInfo) String() string {
	s := fmt.Sprintf("FieldInfo: %s %v", f.name, f.idx)
	if f.flags&fAny > 0 {
		s += " Any"
	}
	return s
}

type fieldFlags int

const (
	fElement fieldFlags = 1 << iota
	fAny
	fMode = fElement | fAny
)

var tinfoMap = make(map[reflect.Type]*typeInfo)
var tinfoLock sync.RWMutex

var (
	textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	textMarshalerType   = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	marshalerType       = reflect.TypeOf((*Marshaler)(nil)).Elem()
	unmarshalerType     = reflect.TypeOf((*Unmarshaler)(nil)).Elem()
)

// getTypeInfo returns the typeInfo structure with details necessary
// for marshaling and unmarshaling typ.
func getTypeInfo(typ reflect.Type) (*typeInfo, error) {
	tinfoLock.RLock()
	tinfo, ok := tinfoMap[typ]
	tinfoLock.RUnlock()
	if ok {
		return tinfo, nil
	}
	tinfo = &typeInfo{}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("type %s (%s) is not a struct", typ.String(), typ.Kind())
	}
	n := typ.NumField()
	for i := 0; i < n; i++ {
		f := typ.Field(i)
		if (f.PkgPath != "" && !f.Anonymous) || f.Tag.Get(tagName) == "-" {
			continue // Private field
		}

		// For embedded structs, embed its fields.
		if f.Anonymous {
			t := f.Type
			if t.Kind() == reflect.Ptr {
				t = t.Elem()
			}
			if t.Kind() == reflect.Struct {
				inner, err := getTypeInfo(t)
				if err != nil {
					return nil, err
				}
				for _, finfo := range inner.fields {
					finfo.idx = append([]int{i}, finfo.idx...)
					if err := addFieldInfo(typ, tinfo, &finfo); err != nil {
						return nil, err
					}
				}
				continue
			}
		}

		finfo := structFieldInfo(&f)

		// Add the field if it doesn't conflict with other fields.
		if err := addFieldInfo(typ, tinfo, finfo); err != nil {
			return nil, err
		}
	}
	tinfoLock.Lock()
	tinfoMap[typ] = tinfo
	tinfoLock.Unlock()
	return tinfo, nil
}

// structFieldInfo builds and returns a fieldInfo for f.
func structFieldInfo(f *reflect.StructField) *fieldInfo {
	finfo := &fieldInfo{idx: f.Index}
	tag := f.Tag.Get(tagName)

	// Parse flags.
	tokens := strings.Split(tag, ",")
	if len(tokens) == 1 {
		finfo.flags = fElement
	} else {
		tag = tokens[0]
		for _, flag := range tokens[1:] {
			if flag == "any" {
				finfo.flags |= fAny
			}
		}

		// Validate the flags used: all combinations are allowed;
		// when `any` is used alone it defaults to `element`
		switch mode := finfo.flags & fMode; mode {
		case 0, fAny:
			finfo.flags |= fElement
		}
	}

	if tag != "" {
		finfo.name = tag
	} else {
		// Use field name as default.
		finfo.name = f.Name
	}

	return finfo
}

func addFieldInfo(typ reflect.Type, tinfo *typeInfo, newf *fieldInfo) error {
	var conflicts []int
	// Find all conflicts.
	for i := range tinfo.fields {
		oldf := &tinfo.fields[i]
		if newf.name == oldf.name {
			conflicts = append(conflicts, i)
		}
	}

	// Return the first error.
	for _, i := range conflicts {
		oldf := &tinfo.fields[i]
		f1 := typ.FieldByIndex(oldf.idx)
		f2 := typ.FieldByIndex(newf.idx)
		return fmt.Errorf("csv: %s field %q with tag %q conflicts with field %q with tag %q", typ, f1.Name, f1.Tag.Get(tagName), f2.Name, f2.Tag.Get(tagName))
	}

	// Without conflicts, add the new field and return.
	tinfo.fields = append(tinfo.fields, *newf)
	return nil
}

// value returns v's field value corresponding to finfo.
// It's equivalent to v.FieldByIndex(finfo.idx), but initializes
// and dereferences pointers as necessary.
func (finfo *fieldInfo) value(v reflect.Value) reflect.Value {
	for i, x := range finfo.idx {
		if i > 0 {
			t := v.Type()
			if t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct {
				if v.IsNil() {
					v.Set(reflect.New(v.Type().Elem()))
				}
				v = v.Elem()
			}
		}
		v = v.Field(x)
	}

	return v
}

// Load value from interface, but only if the result will be
// usefully addressable.
// func derefIndirect(v interface{}) reflect.Value {
// 	return derefValue(reflect.ValueOf(v))
// }

func derefValue(val reflect.Value) reflect.Value {
	if val.Kind() == reflect.Interface && !val.IsNil() {
		e := val.Elem()
		if e.Kind() == reflect.Ptr && !e.IsNil() {
			val = e
		}
	}

	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			val.Set(reflect.New(val.Type().Elem()))
		}
		val = val.Elem()
	}
	return val
}

func indirectType(typ reflect.Type) reflect.Type {
	if typ.Kind() == reflect.Ptr {
		val := reflect.New(typ.Elem())
		return val.Elem().Type()
	}
	return typ
}
