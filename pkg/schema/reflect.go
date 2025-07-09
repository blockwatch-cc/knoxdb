// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding"
	"fmt"
	"math/bits"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
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

func MustSchemaOf(m any) *Schema {
	s, err := SchemaOf(m)
	if err != nil {
		panic(err)
	}
	return s
}

func SchemaOf(m any) (*Schema, error) {
	return SchemaOfTag(m, TAG_NAME)
}

func SchemaOfTag(m any, tag string) (*Schema, error) {
	// interface must not be nil
	if m == nil {
		return nil, ErrNilValue
	}

	// validate type
	val := reflect.Indirect(reflect.ValueOf(m))
	if !val.IsValid() {
		return nil, fmt.Errorf("invalid value of type %T", m)
	}

	// must be a struct or slice of struct
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Struct:
		// ok
	case reflect.Slice:
		telem := typ.Elem()
		if telem.Kind() == reflect.Pointer {
			telem = telem.Elem()
		}
		if telem.Kind() != reflect.Struct {
			return nil, fmt.Errorf("slice element type %s (%s) is not a struct", telem, telem.Kind())
		}
		typ = telem
	default:
		return nil, fmt.Errorf("type %s (%s) is not a struct", typ, typ.Kind())
	}

	// lookup registry
	sval, ok := schemaRegistry.Load(typ)
	if ok {
		return sval.(*Schema), nil
	}

	// create new schema
	s := &Schema{
		name:        util.FromCamelCase(typ.Name(), "_"),
		fields:      make([]Field, 0),
		isFixedSize: true,
		version:     1,
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
		if !f.IsExported() || f.Anonymous || f.Tag.Get(tag) == "-" {
			continue
		}

		// analyze field
		field, err := reflectStructField(f, tag)
		if err != nil {
			return nil, err
		}

		// catch duplicates
		if exist, ok := s.FieldByName(field.name); ok {
			return nil, fmt.Errorf("%s field %q conflicts with field %q",
				field.typ, field.name, exist.name)
		}

		// assign id starting at 1, allow pre-assigned ids
		if field.id == 0 {
			field.id = uint16(len(s.fields)) + 1
		}
		s.fields = append(s.fields, field)
	}

	// compile encoder/decoder opcodes, calculate wire size, lookup enums
	s.Finalize()

	// validate schema conformance
	if err := s.Validate(); err != nil {
		return nil, err
	}

	// register schema
	schemaRegistry.Store(typ, s)

	return s, nil
}

// Produces a dynamic struct type only using native types like int64 for Decimal64
// [16]byte for Int128, etc.
func (s *Schema) NativeStructType() reflect.Type {
	sfields := make([]reflect.StructField, 0, len(s.fields))
	for _, f := range s.fields {
		if !f.IsVisible() {
			continue
		}
		var rtyp reflect.Type
		switch f.typ {
		case FT_TIMESTAMP, FT_TIME, FT_DATE, FT_I64, FT_D64:
			rtyp = reflect.TypeFor[int64]()
		case FT_U64:
			rtyp = reflect.TypeFor[uint64]()
		case FT_F64:
			rtyp = reflect.TypeFor[float64]()
		case FT_BOOL:
			rtyp = reflect.TypeFor[bool]()
		case FT_STRING:
			rtyp = reflect.TypeFor[string]()
		case FT_BYTES, FT_BIGINT:
			if f.fixed > 0 {
				rtyp = reflect.ArrayOf(int(f.fixed), reflect.TypeFor[byte]())
			} else {
				rtyp = reflect.TypeFor[[]byte]()
			}
		case FT_I32, FT_D32:
			rtyp = reflect.TypeFor[int32]()
		case FT_I16:
			rtyp = reflect.TypeFor[int16]()
		case FT_I8:
			rtyp = reflect.TypeFor[int8]()
		case FT_U32:
			rtyp = reflect.TypeFor[uint32]()
		case FT_U16:
			rtyp = reflect.TypeFor[uint16]()
		case FT_U8:
			rtyp = reflect.TypeFor[uint8]()
		case FT_F32:
			rtyp = reflect.TypeFor[float32]()
		case FT_I256, FT_D256:
			rtyp = reflect.TypeFor[[32]byte]()
		case FT_I128, FT_D128:
			rtyp = reflect.TypeFor[[16]byte]()
		default:
			continue
		}
		sfields = append(sfields, reflect.StructField{
			Name: util.ToTitle(sanitize(f.name)),
			Type: rtyp,
		})
	}
	return reflect.StructOf(sfields)
}

// Produces a dynamic struct type compatible with SchemaOf which uses custom types
// for large numeric values (num.Int128) and decimals (num.Decimal64).
func (s *Schema) StructType() reflect.Type {
	sfields := make([]reflect.StructField, 0, len(s.fields))
	for _, f := range s.fields {
		if !f.IsVisible() {
			continue
		}
		tag := fmt.Sprintf(`knox:"%s,id=%d`, f.name, f.id)
		if f.IsPrimary() {
			tag += ",pk"
		}
		if f.IsEnum() {
			tag += ",enum"
		}
		if f.IsFixedSize() {
			tag += fmt.Sprintf(",fixed=%d", f.fixed)
		}
		if f.IsIndexed() {
			tag += fmt.Sprintf(",index=%s", f.index)
		}
		if f.scale > 0 {
			if f.index == types.IndexTypeBloom {
				tag += ":"
			} else {
				tag += strconv.Itoa(int(f.scale))
			}
		}
		if f.IsCompressed() {
			tag += ",zip=" + f.compress.String()
		}
		tag += `"`
		sfields = append(sfields, reflect.StructField{
			Name: util.ToTitle(sanitize(f.name)),
			Type: f.GoType(),
			Tag:  reflect.StructTag(tag),
		})
	}
	return reflect.StructOf(sfields)
}

var rx = regexp.MustCompile("[^a-zA-Z0-9]+")

func sanitize(s string) string {
	if len(s) == 0 {
		return s
	}

	// Prefix internal field names
	if s[0] == '$' {
		s = "X" + s[1:]
	}

	// Replace invalid characters
	s = rx.ReplaceAllString(s, "_")

	// Replace multiple __ with single _
	s = strings.ReplaceAll(s, "__", "_")

	return s
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

func reflectStructField(f reflect.StructField, tagName string) (field Field, err error) {
	tag := f.Tag.Get(tagName)
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
	if field.flags&F_PRIMARY > 0 {
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
	field.wireSize = uint16(field.WireSize())

	return
}

func (f *Field) ParseType(r reflect.StructField) error {
	var (
		iface types.IfaceFlags
		typ   types.FieldType
		flags types.FieldFlags
		fixed uint16
		scale uint8
	)

	// detect marshaler types
	if r.Type.Implements(binaryMarshalerType) {
		iface |= types.IfaceBinaryMarshaler
	}
	if reflect.PointerTo(r.Type).Implements(binaryUnmarshalerType) {
		iface |= types.IfaceBinaryUnmarshaler
	}
	if r.Type.Implements(textMarshalerType) {
		iface |= types.IfaceTextMarshaler
	}
	if reflect.PointerTo(r.Type).Implements(textUnmarshalerType) {
		iface |= types.IfaceTextUnmarshaler
	}
	if r.Type.Implements(stringerType) {
		iface |= types.IfaceStringer
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
			typ = FT_I64
		} else {
			typ = FT_I32
		}
	case reflect.Int64:
		typ = FT_I64
	case reflect.Int32:
		typ = FT_I32
	case reflect.Int16:
		typ = FT_I16
	case reflect.Int8:
		typ = FT_I8
	case reflect.Uint:
		if bits.UintSize == 64 {
			typ = FT_U64
		} else {
			typ = FT_U32
		}
	case reflect.Uint64:
		typ = FT_U64
	case reflect.Uint32:
		typ = FT_U32
	case reflect.Uint16:
		typ = FT_U16
	case reflect.Uint8:
		typ = FT_U8
	case reflect.Float64:
		typ = FT_F64
	case reflect.Float32:
		typ = FT_F32
	case reflect.String:
		if r.Type.String() == "schema.Enum" {
			typ = FT_U16
			flags = F_ENUM
		} else {
			typ = FT_STRING
		}
	case reflect.Bool:
		typ = FT_BOOL
	case reflect.Map:
		switch {
		case iface.Is(types.IfaceBinaryMarshaler):
			typ = FT_BYTES
		case iface.Is(types.IfaceTextMarshaler) || iface.Is(types.IfaceStringer):
			typ = FT_STRING
		default:
			return fmt.Errorf("unsupported map type %s, should implement BinaryMarshaler", r.Type)
		}
	case reflect.Slice:
		switch {
		case iface.Is(types.IfaceBinaryMarshaler):
			typ = FT_BYTES
		case iface.Is(types.IfaceTextMarshaler) || iface.Is(types.IfaceStringer):
			typ = FT_STRING
		case r.Type == byteSliceType:
			typ = FT_BYTES
		default:
			return fmt.Errorf("unsupported slice type %s, should implement BinaryMarshaler", r.Type)
		}
	case reflect.Struct:
		// string-check is much quicker
		switch r.Type.String() {
		case "time.Time":
			typ = FT_TIMESTAMP
			scale = TIME_SCALE_NANO.AsUint()
		case "num.Decimal32":
			typ = FT_D32
			scale = num.MaxDecimal32Precision
		case "num.Decimal64":
			typ = FT_D64
			scale = num.MaxDecimal64Precision
		case "num.Decimal128":
			typ = FT_D128
			scale = num.MaxDecimal128Precision
		case "num.Decimal256":
			typ = FT_D256
			scale = num.MaxDecimal256Precision
		case "num.Big":
			typ = FT_BIGINT
		default:
			switch {
			case iface.Is(types.IfaceBinaryMarshaler):
				typ = FT_BYTES
			case iface.Is(types.IfaceTextMarshaler) || iface.Is(types.IfaceStringer):
				typ = FT_STRING
			default:
				return fmt.Errorf("unsupported nested struct type %s, should implement BinaryMarshaler", r.Type)
			}
		}
	case reflect.Array:
		// string-check is much quicker
		switch r.Type.String() {
		case "num.Int128":
			typ = FT_I128
		case "num.Int256":
			typ = FT_I256
		default:
			switch {
			case iface.Is(types.IfaceBinaryMarshaler):
				typ = FT_BYTES
			case iface.Is(types.IfaceTextMarshaler) || iface.Is(types.IfaceStringer):
				typ = FT_STRING
			case r.Type.Elem().Kind() == reflect.Uint8:
				typ = FT_BYTES
				fixed = uint16(r.Type.Len())
			default:
				return fmt.Errorf("unsupported array type %s, should implement BinaryMarshaler", r.Type)
			}
		}
	default:
		return fmt.Errorf("unsupported type %s (%v)", r.Type, r.Type.Kind())
	}

	f.iface = iface
	f.typ = typ
	f.flags = flags
	f.fixed = fixed
	f.scale = scale

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
		flags    types.FieldFlags
		compress types.BlockCompression
		index    types.IndexType
	)

	for _, flag := range tokens[1:] {
		key, val, ok := strings.Cut(strings.TrimSpace(flag), "=")
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		switch key {
		case "pk":
			flags |= F_PRIMARY | F_INDEXED
			index = I_PK
		case "index":
			flags |= F_INDEXED
			switch val {
			case "hash":
				index = I_HASH
			case "int":
				switch f.typ {
				case FT_I64, FT_I32, FT_I16, FT_I8, FT_U64, FT_U32, FT_U16, FT_U8:
				default:
					return fmt.Errorf("integer index unsupported on type %s", f.typ)
				}
				index = I_INT
			case "pk":
				if f.typ != FT_U64 || !f.IsPrimary() {
					return fmt.Errorf("pk index on invalid field %s type %s", f.name, f.typ)
				}
				index = I_PK
			case "bits":
				index = I_BITS
			case "bloom":
				index = I_BLOOM
				scale = 2
			case "bfuse":
				index = I_BFUSE
			default:
				if val == "" || strings.HasPrefix(val, "bloom") {
					index = I_BLOOM
					scale = 2
					// accept = and :
					val = strings.ReplaceAll(val, "=", ":")
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
				compress = types.BlockCompressNone
			case "snappy":
				compress = types.BlockCompressSnappy
			case "lz4":
				compress = types.BlockCompressLZ4
			case "zstd":
				compress = types.BlockCompressZstd
			default:
				return fmt.Errorf("unsupported compression type %q", val)
			}
		case "fixed":
			// only compatible with strings, bytes must use [n]byte arrays):
			if f.typ != FT_STRING {
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
			// only compatible with:
			// - decimal types
			// - datetime
			switch f.typ {
			case FT_D32, FT_D64, FT_D128, FT_D256:
				if ok {
					sc, err := parseInt(val, "scale", 0, int(maxScale))
					if err != nil {
						return err
					}
					scale = uint8(sc)
				} else {
					return fmt.Errorf("missing value for scale tag")
				}
			case FT_TIMESTAMP, FT_TIME:
				s, ok := ParseTimeScale(val)
				if !ok {
					return fmt.Errorf("invalid time scale value %q", val)
				}
				scale = s.AsUint()
			default:
				return fmt.Errorf("scale tag unsupported on type %s", f.typ)
			}
		case "enum":
			switch f.typ {
			case FT_STRING, FT_U16:
				// ok
				flags |= F_ENUM
				f.typ = FT_U16
			default:
				return fmt.Errorf("unsupported enum type %s", f.typ)
			}
		case "internal":
			flags |= F_INTERNAL
		case "id":
			num, err := strconv.ParseUint(val, 0, 16)
			if err != nil {
				return fmt.Errorf("invalid field id %q: %v", val, err)
			}
			f.id = uint16(num)
		case "null":
			flags |= F_NULLABLE
		case "notnull":
			flags &^= F_NULLABLE
		case "timestamp":
			f.typ = FT_TIMESTAMP
			scale = TIME_SCALE_NANO.AsUint()
		case "date":
			f.typ = FT_DATE
			scale = TIME_SCALE_DAY.AsUint()
		case "time":
			f.typ = FT_TIME
			scale = TIME_SCALE_SECOND.AsUint()
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
		ec, dc := OpCodeSkip, OpCodeSkip
		switch f.typ {
		case FT_TIMESTAMP:
			dc, ec = OpCodeTimestamp, OpCodeTimestamp

		case FT_DATE:
			dc, ec = OpCodeDate, OpCodeDate

		case FT_TIME:
			dc, ec = OpCodeTime, OpCodeTime

		case FT_I64:
			dc, ec = OpCodeInt64, OpCodeInt64

		case FT_I32:
			dc, ec = OpCodeInt32, OpCodeInt32

		case FT_I16:
			dc, ec = OpCodeInt16, OpCodeInt16

		case FT_I8:
			dc, ec = OpCodeInt8, OpCodeInt8

		case FT_U64:
			dc, ec = OpCodeUint64, OpCodeUint64

		case FT_U32:
			dc, ec = OpCodeUint32, OpCodeUint32

		case FT_U16:
			if f.flags.Is(types.FieldFlagEnum) {
				dc, ec = OpCodeEnum, OpCodeEnum
			} else {
				dc, ec = OpCodeUint16, OpCodeUint16
			}

		case FT_U8:
			dc, ec = OpCodeUint8, OpCodeUint8

		case FT_F64:
			dc, ec = OpCodeFloat64, OpCodeFloat64

		case FT_F32:
			dc, ec = OpCodeFloat32, OpCodeFloat32

		case FT_BOOL:
			dc, ec = OpCodeBool, OpCodeBool

		case FT_STRING:
			// encoder side
			switch {
			case f.fixed > 0:
				ec = OpCodeFixedString
			case f.Can(types.IfaceTextMarshaler):
				ec = OpCodeMarshalText
			case f.Can(types.IfaceStringer):
				ec = OpCodeStringer
			default:
				ec = OpCodeString
			}

			// decoder side
			switch {
			case f.fixed > 0:
				dc = OpCodeFixedString
			case f.Can(types.IfaceTextUnmarshaler):
				dc = OpCodeUnmarshalText
			default:
				dc = OpCodeString
			}

		case FT_BYTES:
			// encoder side
			switch {
			case f.Can(types.IfaceBinaryMarshaler):
				ec = OpCodeMarshalBinary
			case f.fixed > 0:
				ec = OpCodeFixedBytes
			default:
				ec = OpCodeBytes
			}

			// decoder side
			switch {
			case f.Can(types.IfaceBinaryUnmarshaler):
				dc = OpCodeUnmarshalBinary
			case f.fixed > 0:
				dc = OpCodeFixedBytes
			default:
				dc = OpCodeBytes
			}

		case FT_I256:
			dc, ec = OpCodeInt256, OpCodeInt256

		case FT_I128:
			dc, ec = OpCodeInt128, OpCodeInt128

		case FT_D256:
			dc, ec = OpCodeDecimal256, OpCodeDecimal256

		case FT_D128:
			dc, ec = OpCodeDecimal128, OpCodeDecimal128

		case FT_D64:
			dc, ec = OpCodeDecimal64, OpCodeDecimal64

		case FT_D32:
			dc, ec = OpCodeDecimal32, OpCodeDecimal32

		case FT_BIGINT:
			dc, ec = OpCodeBigInt, OpCodeBigInt
		}

		if !f.IsVisible() {
			ec, dc = OpCodeSkip, OpCodeSkip
		}

		enc = append(enc, ec)
		dec = append(dec, dc)
	}
	return
}
