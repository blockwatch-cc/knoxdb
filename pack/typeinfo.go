// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

const (
	tagName  = "pack"
	tagAlias = "json"
)

// typeInfo holds details for the representation of a type.
type typeInfo struct {
	name   string
	fields []fieldInfo
	gotype bool
}

func (t *typeInfo) PkColumn() int {
	for i, finfo := range t.fields {
		if finfo.flags&FlagPrimary > 0 {
			return i
		}
	}
	return -1
}

// fieldInfo holds details for the representation of a single field.
type fieldInfo struct {
	idx       []int
	name      string
	alias     string
	flags     FieldFlags
	precision int
	typname   string
}

func (f fieldInfo) String() string {
	s := fmt.Sprintf("FieldInfo: %s typ=%s idx=%v prec=%d",
		f.name, f.typname, f.idx, f.precision)
	if f.flags&FlagPrimary > 0 {
		s += " Primary"
	}
	if f.flags&FlagIndexed > 0 {
		s += " Indexed"
	}
	if f.flags&FlagConvert > 0 {
		s += " Convert"
	}
	if f.flags&FlagCompressLZ4 > 0 {
		s += " LZ4"
	}
	if f.flags&FlagCompressSnappy > 0 {
		s += " Snappy"
	}
	return s
}

var tinfoMap = make(map[reflect.Type]*typeInfo)
var tinfoLock sync.RWMutex

var (
	textUnmarshalerType   = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	textMarshalerType     = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	binaryUnmarshalerType = reflect.TypeOf((*encoding.BinaryUnmarshaler)(nil)).Elem()
	binaryMarshalerType   = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
	byteSliceType         = reflect.TypeOf([]byte(nil))
)

// getTypeInfo returns the typeInfo structure with details necessary
// for marshaling and unmarshaling typ.
func getTypeInfo(v interface{}) (*typeInfo, error) {
	val := reflect.Indirect(reflect.ValueOf(v))
	if !val.IsValid() {
		return nil, fmt.Errorf("pack: invalid value of type %T", v)
	}
	return getReflectTypeInfo(val.Type())
}

func getReflectTypeInfo(typ reflect.Type) (*typeInfo, error) {
	tinfoLock.RLock()
	tinfo, ok := tinfoMap[typ]
	tinfoLock.RUnlock()
	if ok {
		return tinfo, nil
	}
	tinfo = &typeInfo{
		name:   typ.String(),
		gotype: true,
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("pack: type %s (%s) is not a struct", typ.String(), typ.Kind())
	}
	n := typ.NumField()
	for i := 0; i < n; i++ {
		f := typ.Field(i)
		if (f.PkgPath != "" && !f.Anonymous) || f.Tag.Get(tagName) == "-" {
			// skip private fields
			continue
		}

		// For embedded structs, embed its fields.
		if f.Anonymous {
			t := f.Type
			if t.Kind() == reflect.Ptr {
				t = t.Elem()
			}
			if t.Kind() == reflect.Struct {
				inner, err := getReflectTypeInfo(t)
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

		finfo, err := structFieldInfo(typ, &f)
		if err != nil {
			return nil, err
		}

		// pk field must be of type int64 or uint64
		if finfo.flags&FlagPrimary > 0 {
			switch f.Type.Kind() {
			case reflect.Uint64:
			default:
				return nil, fmt.Errorf("pack: invalid primary key type %T", f.Type)
			}
		}

		// extract long name
		if a := f.Tag.Get(tagAlias); a != "-" {
			finfo.alias = strings.Split(a, ",")[0]
		}

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
func structFieldInfo(typ reflect.Type, f *reflect.StructField) (*fieldInfo, error) {
	finfo := &fieldInfo{idx: f.Index, typname: f.Type.String()}
	tag := f.Tag.Get(tagName)

	tokens := strings.Split(tag, ",")
	if len(tokens) > 1 {
		tag = tokens[0]
		for _, flag := range tokens[1:] {
			switch ff := strings.Split(flag, "="); ff[0] {
			case "pk":
				finfo.flags |= FlagPrimary
			case "index":
				finfo.flags |= FlagIndexed
			case "convert":
				finfo.flags |= FlagConvert
				finfo.precision = maxPrecision
			case "lz4":
				finfo.flags |= FlagCompressLZ4
			case "snappy":
				finfo.flags |= FlagCompressSnappy
			case "precision":
				if len(ff) > 1 {
					prec, err := strconv.Atoi(ff[1])
					if err != nil {
						return nil, fmt.Errorf("pack: invalid field precision '%s'", ff[1])
					}
					if prec < 0 || prec > 15 {
						return nil, fmt.Errorf("pack: field precision '%d' out of bounds [0,15]", prec)
					}
					finfo.precision = prec
				}
			}
		}
	}

	if tag != "" {
		finfo.name = tag
	} else {
		finfo.name = f.Name
	}

	return finfo, nil
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
		return fmt.Errorf("%s: %s field %q with tag %q conflicts with field %q with tag %q",
			tagName, typ, f1.Name, f1.Tag.Get(tagName), f2.Name, f2.Tag.Get(tagName))
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
func derefIndirect(v interface{}) reflect.Value {
	return derefValue(reflect.ValueOf(v))
}

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
