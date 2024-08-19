// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding"
	"fmt"
	"math/bits"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"blockwatch.cc/knoxdb/pkg/num"
)

const TAG_NAME = "knox"

var schemaRegistry sync.Map

func LookupSchema(typ reflect.Type) (*Schema, bool) {
	sval, ok := schemaRegistry.Load(typ)
	if ok {
		return sval.(*Schema), ok
	}
	return nil, ok
}

func GenericSchema[T any]() (*Schema, error) {
	var m T
	return SchemaOf(m)
}

func SchemaOf(m any) (*Schema, error) {
	// validate type
	val := reflect.Indirect(reflect.ValueOf(m))
	if !val.IsValid() {
		return nil, fmt.Errorf("invalid value of type %T", m)
	}

	// must be a struct
	typ := val.Type()
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("type %s (%s) is not a struct", typ, typ.Kind())
	}

	// lookup registry
	sval, ok := schemaRegistry.Load(typ)
	if ok {
		return sval.(*Schema), nil
	}

	// create new schema
	s := &Schema{
		name:        strings.ToLower(typ.Name()),
		fields:      make([]Field, 0),
		isFixedSize: true,
	}

	// use table name when type implements the Model interface
	if typ.Implements(modelType) {
		if n := val.Interface().(Model).Key(); len(n) > 0 {
			s.name = n
		}
	}

	for _, f := range reflect.VisibleFields(typ) {
		// skip private fields and embedded structs, promoted embedded fields
		// fields are still processed, only the anon struct itself is skipped
		if !f.IsExported() || f.Anonymous || f.Tag.Get(TAG_NAME) == "-" {
			continue
		}

		// analyze field
		field, err := reflectStructField(f)
		if err != nil {
			return nil, err
		}

		// catch duplicates
		if exist, ok := s.FieldByName(field.name); ok {
			return nil, fmt.Errorf("%s field %q conflicts with field %q",
				field.typ, field.name, exist.name)
		}

		// assign id starting at 1
		field.id = uint16(len(s.fields)) + 1
		s.fields = append(s.fields, field)
	}

	// compile encoder/decoder opcodes, calculate wire size
	s.Complete()

	// validate schema conformance
	if err := s.Validate(); err != nil {
		return nil, err
	}

	// register schema
	schemaRegistry.Store(typ, s)

	return s, nil
}

var (
	textUnmarshalerType   = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	textMarshalerType     = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	binaryUnmarshalerType = reflect.TypeOf((*encoding.BinaryUnmarshaler)(nil)).Elem()
	binaryMarshalerType   = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
	stringerType          = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
	byteSliceType         = reflect.TypeOf([]byte(nil))
	modelType             = reflect.TypeOf((*Model)(nil)).Elem()
)

func reflectStructField(f reflect.StructField) (field Field, err error) {
	tag := f.Tag.Get(TAG_NAME)
	field.name = f.Name

	// extract alias name
	if n, _, _ := strings.Cut(tag, ","); n != "" {
		field.name = n
	}

	// clean name
	field.name = strings.ToLower(strings.TrimSpace(field.name))

	// identify field type from Go type
	err = field.ParseType(f)
	if err != nil {
		err = fmt.Errorf("field %s: %v", field.name, err)
		return
	}

	// parse tags, allow type & fixed override
	err = field.ParseTag(tag)
	if err != nil {
		err = fmt.Errorf("field %s: %v", field.name, err)
		return
	}

	// Validate field

	// pk field must be of type uint64
	if field.flags&FieldFlagPrimary > 0 {
		switch f.Type.Kind() {
		case reflect.Uint64:
		default:
			err = fmt.Errorf("field %s: invalid primary key type %s", field.name, f.Type)
			return
		}
	}

	// fill en/decoder info
	field.path = f.Index
	field.offset = f.Offset
	field.dataSize = uint16(f.Type.Size()) // uintptr
	field.wireSize = uint16(field.WireSize())

	return
}

func (f *Field) ParseType(r reflect.StructField) error {
	var (
		iface   IfaceFlags
		typ     FieldType
		fixed   uint16
		scale   uint8
		isArray bool
	)

	// detect marshaler types
	if r.Type.Implements(binaryMarshalerType) {
		iface |= IfaceBinaryMarshaler
	}
	if reflect.PointerTo(r.Type).Implements(binaryUnmarshalerType) {
		iface |= IfaceBinaryUnmarshaler
	}
	if r.Type.Implements(textMarshalerType) {
		iface |= IfaceTextMarshaler
	}
	if reflect.PointerTo(r.Type).Implements(textUnmarshalerType) {
		iface |= IfaceTextUnmarshaler
	}
	if r.Type.Implements(stringerType) {
		iface |= IfaceStringer
	}

	// field must have supported kind
	switch r.Type.Kind() {
	case reflect.Complex64,
		reflect.Complex128,
		reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Pointer,
		reflect.UnsafePointer:
		return fmt.Errorf("unsupported kind %s", r.Type.Kind())

	case reflect.Int:
		if bits.UintSize == 64 {
			typ = FieldTypeInt64
		} else {
			typ = FieldTypeInt32
		}
	case reflect.Int64:
		typ = FieldTypeInt64
	case reflect.Int32:
		typ = FieldTypeInt32
	case reflect.Int16:
		typ = FieldTypeInt16
	case reflect.Int8:
		typ = FieldTypeInt8
	case reflect.Uint:
		if bits.UintSize == 64 {
			typ = FieldTypeUint64
		} else {
			typ = FieldTypeUint32
		}
	case reflect.Uint64:
		typ = FieldTypeUint64
	case reflect.Uint32:
		typ = FieldTypeUint32
	case reflect.Uint16:
		typ = FieldTypeUint16
	case reflect.Uint8:
		typ = FieldTypeUint8
	case reflect.Float64:
		typ = FieldTypeFloat64
	case reflect.Float32:
		typ = FieldTypeFloat32
	case reflect.String:
		typ = FieldTypeString
	case reflect.Bool:
		typ = FieldTypeBoolean
	case reflect.Map:
		switch {
		case iface.Is(IfaceBinaryMarshaler):
			typ = FieldTypeBytes
		case iface.Is(IfaceTextMarshaler) || iface.Is(IfaceStringer):
			typ = FieldTypeString
		default:
			return fmt.Errorf("unsupported map type %s, should implement BinaryMarshaler", r.Type)
		}
	case reflect.Slice:
		switch {
		case iface.Is(IfaceBinaryMarshaler):
			typ = FieldTypeBytes
		case iface.Is(IfaceTextMarshaler) || iface.Is(IfaceStringer):
			typ = FieldTypeString
		case r.Type == byteSliceType:
			typ = FieldTypeBytes
		default:
			return fmt.Errorf("unsupported slice type %s, should implement BinaryMarshaler", r.Type)
		}
	case reflect.Struct:
		// string-check is much quicker
		switch r.Type.String() {
		case "time.Time":
			typ = FieldTypeDatetime
		case "num.Decimal32":
			typ = FieldTypeDecimal32
			scale = num.MaxDecimal32Precision
		case "num.Decimal64":
			typ = FieldTypeDecimal64
			scale = num.MaxDecimal64Precision
		case "num.Decimal128":
			typ = FieldTypeDecimal128
			scale = num.MaxDecimal128Precision
		case "num.Decimal256":
			typ = FieldTypeDecimal256
			scale = num.MaxDecimal256Precision
		default:
			switch {
			case iface.Is(IfaceBinaryMarshaler):
				typ = FieldTypeBytes
			case iface.Is(IfaceTextMarshaler) || iface.Is(IfaceStringer):
				typ = FieldTypeString
			default:
				return fmt.Errorf("unsupported nested struct type %s, should implement BinaryMarshaler", r.Type)
			}
		}
	case reflect.Array:
		// string-check is much quicker
		switch r.Type.String() {
		case "num.Int128":
			typ = FieldTypeInt128
		case "num.Int256":
			typ = FieldTypeInt256
		default:
			switch {
			case iface.Is(IfaceBinaryMarshaler):
				typ = FieldTypeBytes
			case iface.Is(IfaceTextMarshaler) || iface.Is(IfaceStringer):
				typ = FieldTypeString
			case r.Type.Elem().Kind() == reflect.Uint8:
				typ = FieldTypeBytes
				fixed = uint16(r.Type.Len())
				isArray = true
			default:
				return fmt.Errorf("unsupported array type %s, should implement BinaryMarshaler", r.Type)
			}
		}
	default:
		return fmt.Errorf("unsupported type %s (%v)", r.Type, r.Type.Kind())
	}

	f.iface = iface
	f.typ = typ
	f.fixed = uint16(fixed)
	f.scale = uint8(scale)
	f.isArray = isArray

	return nil
}

func (f *Field) ParseTag(tag string) error {
	// first part is field name
	tokens := strings.Split(tag, ",")
	if len(tokens) < 2 {
		return nil
	}

	var (
		scale    uint8
		fixed    = f.fixed
		maxFixed = MAX_FIXED
		maxScale = f.scale
		flags    FieldFlags
		compress FieldCompression
		index    IndexKind
	)

	if f.isArray {
		maxFixed = f.fixed
	}

	for _, flag := range tokens[1:] {
		key, val, ok := strings.Cut(strings.TrimSpace(flag), "=")
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		switch key {
		case "pk":
			flags |= FieldFlagPrimary
		case "index":
			flags |= FieldFlagIndexed
			switch val {
			case "hash":
				index = IndexKindHash
			case "int":
				switch f.typ {
				case FieldTypeInt64, FieldTypeInt32, FieldTypeInt16, FieldTypeInt8,
					FieldTypeUint64, FieldTypeUint32, FieldTypeUint16, FieldTypeUint8:
				default:
					return fmt.Errorf("integer index unsupported on type %s", f.typ)
				}
				index = IndexKindInt
			case "bits":
				index = IndexKindBits
			case "bloom":
				index = IndexKindBloom
				scale = 2
			default:
				if val == "" || strings.HasPrefix(val, "bloom") {
					index = IndexKindBloom
					scale = 2
					// accept = and :
					val = strings.Replace(val, "=", ":", -1)
					if _, num, ok := strings.Cut(val, ":"); ok {
						// bloom filter factor
						// 1: 2% false positive rate (1 byte per item)
						// 2: 0.2% false positive rate (2 bytes per item)
						// 3: 0.02% false positive rate (3 bytes per item)
						// 4: 0.002% false positive rate (4 bytes per item)
						sc, err := parseInt(num, "bloom filter factor", 1, 4)
						if err != nil {
							return err
						}
						scale = uint8(sc)
					}
				} else {
					return fmt.Errorf("unsupported index type %q", val)
				}
			}
		case "zip":
			switch val {
			case "", "no", "none":
				compress = FieldCompressNone
			case "snappy":
				compress = FieldCompressSnappy
			case "lz4":
				compress = FieldCompressLZ4
			case "zstd":
				compress = FieldCompressZstd
			default:
				return fmt.Errorf("unsupported compression type %q", val)
			}
		case "lz4":
			compress = FieldCompressLZ4
		case "snappy":
			compress = FieldCompressSnappy
		case "zstd":
			compress = FieldCompressZstd
		case "fixed":
			switch f.typ {
			case FieldTypeBytes, FieldTypeString:
			// only compatible with:
			// - arrays: fixed length
			// - byte slices, strings: fixed length
			default:
				return fmt.Errorf("fixed tag unsupported on type %s", f.typ)
			}
			if ok {
				fx, err := parseInt(val, "fixed", 1, int(maxFixed))
				if err != nil {
					return err
				}
				fixed = uint16(fx)
			} else {
				return fmt.Errorf("missing value for fixed tag")
			}
		case "scale":
			switch f.typ {
			case FieldTypeDecimal32, FieldTypeDecimal64, FieldTypeDecimal128, FieldTypeDecimal256:
			// only compatible with:
			// - decimal types
			default:
				return fmt.Errorf("scale tag unsupported on type %s", f.typ)
			}
			if ok {
				sc, err := parseInt(val, "scale", 0, int(maxScale))
				if err != nil {
					return err
				}
				scale = uint8(sc)
			} else {
				return fmt.Errorf("missing value for scale tag")
			}
		default:
			return fmt.Errorf("unsupported struct tag '%s'", key)
		}
	}

	f.scale = scale
	f.fixed = fixed
	f.flags = flags
	f.compress = compress
	f.index = index

	return nil
}

func parseInt(val, name string, minVal, maxVal int) (int, error) {
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value %s: %v", name, val, err)
	}
	return validateInt(name, n, minVal, maxVal)
}

func validateInt(name string, n, minVal, maxVal int) (int, error) {
	if n < minVal || (maxVal > 0 && n > maxVal) {
		return 0, fmt.Errorf("%s %d out of bounds [%d..%d]", name, n, minVal, maxVal)
	}
	return n, nil
}

func compileCodecs(s *Schema) (enc []OpCode, dec []OpCode) {
	for i := range s.fields {
		f := &s.fields[i]
		var ec, dc OpCode
		switch f.typ {
		case FieldTypeDatetime:
			dc, ec = OpCodeDateTime, OpCodeDateTime

		case FieldTypeInt64:
			dc, ec = OpCodeInt64, OpCodeInt64

		case FieldTypeInt32:
			dc, ec = OpCodeInt32, OpCodeInt32

		case FieldTypeInt16:
			dc, ec = OpCodeInt16, OpCodeInt16

		case FieldTypeInt8:
			dc, ec = OpCodeInt8, OpCodeInt8

		case FieldTypeUint64:
			dc, ec = OpCodeUint64, OpCodeUint64

		case FieldTypeUint32:
			dc, ec = OpCodeUint32, OpCodeUint32

		case FieldTypeUint16:
			dc, ec = OpCodeUint16, OpCodeUint16

		case FieldTypeUint8:
			dc, ec = OpCodeUint8, OpCodeUint8

		case FieldTypeFloat64:
			dc, ec = OpCodeFloat64, OpCodeFloat64

		case FieldTypeFloat32:
			dc, ec = OpCodeFloat32, OpCodeFloat32

		case FieldTypeBoolean:
			dc, ec = OpCodeBool, OpCodeBool

		case FieldTypeString:
			// encoder side
			switch {
			case f.fixed > 0:
				ec = OpCodeFixedString
			case f.Can(IfaceTextMarshaler):
				ec = OpCodeMarshalText
			case f.Can(IfaceStringer):
				ec = OpCodeStringer
			default:
				ec = OpCodeString
			}

			// decoder side
			switch {
			case f.fixed > 0:
				dc = OpCodeFixedString
			case f.Can(IfaceTextUnmarshaler):
				dc = OpCodeUnmarshalText
			default:
				dc = OpCodeString
			}

		case FieldTypeBytes:
			// encoder side
			switch {
			case f.Can(IfaceBinaryMarshaler):
				ec = OpCodeMarshalBinary
			case f.isArray:
				ec = OpCodeFixedArray
			case f.fixed > 0:
				ec = OpCodeFixedBytes
			default:
				ec = OpCodeBytes
			}

			// decoder side
			switch {
			case f.Can(IfaceBinaryUnmarshaler):
				dc = OpCodeUnmarshalBinary
			case f.isArray:
				dc = OpCodeFixedArray
			case f.fixed > 0:
				dc = OpCodeFixedBytes
			default:
				dc = OpCodeBytes
			}

		case FieldTypeInt256:
			dc, ec = OpCodeInt256, OpCodeInt256

		case FieldTypeInt128:
			dc, ec = OpCodeInt128, OpCodeInt128

		case FieldTypeDecimal256:
			dc, ec = OpCodeDecimal256, OpCodeDecimal256

		case FieldTypeDecimal128:
			dc, ec = OpCodeDecimal128, OpCodeDecimal128

		case FieldTypeDecimal64:
			dc, ec = OpCodeDecimal64, OpCodeDecimal64

		case FieldTypeDecimal32:
			dc, ec = OpCodeDecimal32, OpCodeDecimal32
		}

		enc = append(enc, ec)
		dec = append(dec, dc)
	}
	return
}
