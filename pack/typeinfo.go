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

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/encoding/decimal"
	"blockwatch.cc/knoxdb/filter/bloom"
)

const (
	tagName  = "knox"
	tagAlias = "json"
)

var (
	szPackInfo    = int(reflect.TypeOf(PackInfo{}).Size())
	szBlockInfo   = int(reflect.TypeOf(BlockInfo{}).Size())
	szBloomFilter = int(reflect.TypeOf(bloom.Filter{}).Size())
	szPackIndex   = int(reflect.TypeOf(PackIndex{}).Size())
	szPackage     = int(reflect.TypeOf(Package{}).Size())
	szField       = int(reflect.TypeOf(Field{}).Size())
	szBlock       = int(reflect.TypeOf(block.Block{}).Size())
)

// typeInfo holds details for the representation of a type.
type typeInfo struct {
	name   string
	fields []*fieldInfo
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

func (t *typeInfo) Clone() *typeInfo {
	clone := &typeInfo{
		name:   t.name,
		fields: make([]*fieldInfo, len(t.fields), len(t.fields)),
		gotype: t.gotype,
	}
	for i, v := range t.fields {
		clone.fields[i] = v.Clone()
	}
	return clone
}

// fieldInfo holds details for the representation of a single field.
type fieldInfo struct {
	idx       []int
	name      string
	alias     string
	flags     FieldFlags
	scale     int
	typ       reflect.Type
	blockid   int
	override  FieldType
	indextype IndexType
}

func (f *fieldInfo) Clone() *fieldInfo {
	fi := *f
	fi.idx = make([]int, len(f.idx), len(f.idx))
	copy(fi.idx, f.idx)
	return &fi
}

func (f fieldInfo) String() string {
	return fmt.Sprintf("name=%s typ=%s idx=%v scale=%d flags=%s override=%s",
		f.name, f.typ, f.idx, f.scale, f.flags, f.override)
}

var tinfoMap = make(map[reflect.Type]*typeInfo)
var tinfoLock sync.RWMutex

var (
	textUnmarshalerType   = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	textMarshalerType     = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	binaryUnmarshalerType = reflect.TypeOf((*encoding.BinaryUnmarshaler)(nil)).Elem()
	binaryMarshalerType   = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
	stringerType          = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
	byteSliceType         = reflect.TypeOf([]byte(nil))
)

func canMarshalBinary(v reflect.Value) bool {
	return v.CanInterface() &&
		v.Type().Implements(binaryMarshalerType) &&
		reflect.PointerTo(v.Type()).Implements(binaryUnmarshalerType)
}

func canMarshalText(v reflect.Value) bool {
	return v.CanInterface() &&
		v.Type().Implements(textMarshalerType) &&
		reflect.PointerTo(v.Type()).Implements(textUnmarshalerType)
}

func canMarshalString(v reflect.Value) bool {
	return v.CanInterface() && v.Type().Implements(stringerType)
}

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
				for _, f := range inner.fields {
					finfo := f.Clone()
					finfo.idx = append([]int{i}, finfo.idx...)
					if err := addFieldInfo(typ, tinfo, finfo); err != nil {
						return nil, err
					}
				}
				continue
			}
		}

		finfo, err := structFieldInfo(&f)
		if err != nil {
			return nil, err
		}

		// pk field must be of type uint64
		if finfo.flags&FlagPrimary > 0 {
			switch f.Type.Kind() {
			case reflect.Uint64:
			default:
				return nil, fmt.Errorf("pack: invalid primary key type %s", f.Type)
			}
		}

		// extract long name
		if a := f.Tag.Get(tagAlias); a != "-" {
			finfo.alias, _, _ = strings.Cut(a, ",")
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
func structFieldInfo(f *reflect.StructField) (*fieldInfo, error) {
	finfo := &fieldInfo{idx: f.Index, typ: f.Type}
	tag := f.Tag.Get(tagName)
	kind := f.Type.Kind()

	// detect marshaler types
	if f.Type.Implements(binaryMarshalerType) && reflect.PointerTo(f.Type).Implements(binaryUnmarshalerType) {
		finfo.flags |= flagBinaryMarshalerType
	}
	if f.Type.Implements(textMarshalerType) && reflect.PointerTo(f.Type).Implements(textUnmarshalerType) {
		finfo.flags |= flagTextMarshalerType
	}
	if f.Type.Implements(stringerType) {
		finfo.flags |= flagStringerType
	}

	tokens := strings.Split(tag, ",")
	if len(tokens) > 1 {
		tag = tokens[0]
		for _, flag := range tokens[1:] {
			key, val, ok := strings.Cut(flag, "=")
			switch key {
			case "u8":
				finfo.override = FieldTypeUint8
			case "u16":
				finfo.override = FieldTypeUint16
			case "u32":
				finfo.override = FieldTypeUint32
			case "u64":
				finfo.override = FieldTypeUint16
			case "i8":
				finfo.override = FieldTypeInt8
			case "i16":
				finfo.override = FieldTypeInt16
			case "i32":
				finfo.override = FieldTypeInt32
			case "i64":
				finfo.override = FieldTypeInt64
			case "i128":
				finfo.override = FieldTypeInt128
			case "i256":
				finfo.override = FieldTypeInt256
			case "d32":
				finfo.override = FieldTypeDecimal32
			case "d64":
				finfo.override = FieldTypeDecimal64
			case "d128":
				finfo.override = FieldTypeDecimal128
			case "d256":
				finfo.override = FieldTypeDecimal256
			case "pk":
				finfo.flags |= FlagPrimary
			case "index":
				finfo.flags |= FlagIndexed
				switch val {
				case "", "hash":
					finfo.indextype = IndexTypeHash
				case "int":
					finfo.indextype = IndexTypeInteger
				default:
					return nil, fmt.Errorf("pack: unsupported index type %q on field '%s' (%s/%s)", val, tag, finfo.typ, kind)
				}
			case "lz4":
				finfo.flags |= FlagCompressLZ4
			case "snappy":
				finfo.flags |= FlagCompressSnappy
			case "scale":
				// only compatible with Decimal data types
				prec := 0
				switch finfo.typ.String() {
				case "decimal.Decimal32":
					prec = decimal.MaxDecimal32Precision
				case "decimal.Decimal64":
					prec = decimal.MaxDecimal64Precision
				case "decimal.Decimal128":
					prec = decimal.MaxDecimal128Precision
				case "decimal.Decimal256":
					prec = decimal.MaxDecimal256Precision
				default:
					switch finfo.override {
					case FieldTypeDecimal32:
						prec = decimal.MaxDecimal32Precision
					case FieldTypeDecimal64:
						prec = decimal.MaxDecimal64Precision
					case FieldTypeDecimal128:
						prec = decimal.MaxDecimal128Precision
					case FieldTypeDecimal256:
						prec = decimal.MaxDecimal256Precision
					default:
						return nil, fmt.Errorf("pack: invalid scale tag on non-decimal field '%s' (%s/%s)", tag, finfo.typ, kind)
					}
				}
				finfo.scale = prec
				if ok {
					scale, err := strconv.Atoi(val)
					if err != nil {
						return nil, fmt.Errorf("pack: invalid scale value %s on field '%s': %v", val, tag, err)
					}
					if scale < 0 || scale > prec {
						return nil, fmt.Errorf("pack: out of bound scale %d on field '%s', should be [0..%d]", scale, tag, prec)
					}
					finfo.scale = scale
				}
			case "bloom":
				finfo.flags |= FlagBloom
				// bloom filter factor
				// 1: 2% false positive rate (1 byte per item)
				// 2: 0.2% false positive rate (2 bytes per item)
				// 3: 0.02% false positive rate (3 bytes per item)
				// 4: 0.002% false positive rate (4 bytes per item)
				finfo.scale = 2
				if ok {
					factor, err := strconv.Atoi(val)
					if err != nil {
						return nil, fmt.Errorf("pack: invalid bloom filter factor %s on field '%s': %v", val, tag, err)
					}
					if factor < 1 || factor > 4 {
						return nil, fmt.Errorf("pack: out of bound bloom factor %d on field '%s', should be [1..4]", factor, tag)
					}
					// re-use scale to store bloom filter error probability factor
					finfo.scale = factor
				}
			default:
				return nil, fmt.Errorf("pack: unsupported struct tag '%s' on field '%s'", key, tag)
			}
			// check type override matches the Go type
			switch finfo.override {
			case FieldTypeUint8, FieldTypeUint16, FieldTypeUint32, FieldTypeUint64:
				switch kind {
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					// OK
				default:
					return nil, fmt.Errorf("pack: incompatible type tag '%s' on unsigned field '%s' (%s/%s)", key, tag, finfo.typ, kind)
				}
			case FieldTypeInt8, FieldTypeInt16, FieldTypeInt32, FieldTypeInt64, FieldTypeInt128, FieldTypeInt256:
				switch kind {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					// OK
				default:
					return nil, fmt.Errorf("pack: incompatible type tag '%s' on integer field '%s' (%s/%s)", key, tag, finfo.typ, kind)
				}
			case FieldTypeDecimal32, FieldTypeDecimal64, FieldTypeDecimal128, FieldTypeDecimal256:
				switch kind {
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					finfo.flags |= flagUintType
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					finfo.flags |= flagIntType
				case reflect.Float32, reflect.Float64:
					finfo.flags |= flagFloatType
				default:
					return nil, fmt.Errorf("pack: incompatible type tag '%s' on decimal field '%s' (%s/%s)", key, tag, finfo.typ, kind)
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
		oldf := tinfo.fields[i]
		if newf.name == oldf.name {
			conflicts = append(conflicts, i)
		}
	}

	// Return the first error.
	for _, i := range conflicts {
		oldf := tinfo.fields[i]
		f1 := typ.FieldByIndex(oldf.idx)
		f2 := typ.FieldByIndex(newf.idx)
		return fmt.Errorf("%s: %s field %q with tag %q conflicts with field %q with tag %q",
			tagName, typ, f1.Name, f1.Tag.Get(tagName), f2.Name, f2.Tag.Get(tagName))
	}

	// default block order is struct order
	newf.blockid = len(tinfo.fields)

	// Without conflicts, add the new field and return.
	tinfo.fields = append(tinfo.fields, newf)
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
