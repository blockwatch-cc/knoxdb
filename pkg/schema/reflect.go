// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
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
		Name:        util.FromCamelCase(typ.Name(), "_"),
		Fields:      make([]*Field, 0),
		IsFixedSize: true,
		Version:     1,
	}

	// use table name when type implements the Model interface
	if typ.Implements(modelType) {
		if n := val.Interface().(Model).Key(); len(n) > 0 {
			s.Name = n
		}
	}

	for _, f := range reflect.VisibleFields(typ) {
		// skip private fields and embedded structs, promoted embedded fields
		// fields are still processed, only the anon struct itself is skipped
		if !f.IsExported() || f.Anonymous || f.Tag.Get(tag) == "-" {
			continue
		}

		// skip empty structs (used to define composite indexes)
		if f.Type == emptyType {
			continue
		}

		// analyze field
		field, err := reflectStructField(f, tag)
		if err != nil {
			return nil, err
		}

		// catch duplicates
		if exist, ok := s.FieldByName(field.Name); ok {
			return nil, fmt.Errorf("%s field %q conflicts with field %q",
				field.Type, field.Name, exist.Name)
		}

		// assign id starting at 1, allow pre-assigned ids
		if field.Id == 0 {
			field.Id = uint16(len(s.Fields)) + 1
		}
		s.Fields = append(s.Fields, field)
	}

	// detect indexes
	idxs, err := IndexesOfTag(reflect.New(typ).Interface(), tag, s)
	if err != nil {
		return nil, err
	}
	s.Indexes = idxs

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
	sfields := make([]reflect.StructField, 0, len(s.Fields))
	for _, f := range s.Fields {
		if !f.IsVisible() {
			continue
		}
		var rtyp reflect.Type
		switch f.Type {
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
			if f.Fixed > 0 {
				rtyp = reflect.ArrayOf(int(f.Fixed), reflect.TypeFor[byte]())
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
			Name: util.ToTitle(sanitize(f.Name)),
			Type: rtyp,
		})
	}
	return reflect.StructOf(sfields)
}

// Produces a dynamic struct type compatible with SchemaOf which uses custom types
// for large numeric values (num.Int128) and decimals (num.Decimal64).
func (s *Schema) StructType() reflect.Type {
	sfields := make([]reflect.StructField, 0, len(s.Fields))
	for _, f := range s.Fields {
		if !f.IsVisible() {
			continue
		}
		tag := fmt.Sprintf(`knox:"%s,id=%d`, f.Name, f.Id)
		if f.IsPrimary() {
			tag += ",pk"
		}
		if f.IsEnum() {
			tag += ",enum"
		}
		if f.IsFixedSize() && f.Fixed > 0 {
			tag += fmt.Sprintf(",fixed=%d", f.Fixed)
		}
		// if f.IsIndexed() {
		// 	tag += fmt.Sprintf(",index=%s", f.Index.Type)
		// }
		if f.Scale > 0 {
			tag += fmt.Sprintf(",scale=%d", f.Scale)
		}
		if f.IsCompressed() {
			tag += ",zip=" + f.Compress.String()
		}
		tag += `"`
		sfields = append(sfields, reflect.StructField{
			Name: util.ToTitle(sanitize(f.Name)),
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
	emptyType     = reflect.TypeOf(struct{}{})
	uint8Type     = reflect.TypeOf(uint8(0))
	byteSliceType = reflect.TypeOf([]byte(nil))
	modelType     = reflect.TypeOf((*Model)(nil)).Elem()
)

func reflectStructField(f reflect.StructField, tagName string) (field *Field, err error) {
	tag := f.Tag.Get(tagName)
	field = &Field{
		Name: f.Name,
	}
	// extract alias name
	if n, _, _ := strings.Cut(tag, ","); n != "" {
		field.Name = n
	}

	// clean name
	field.Name = strings.ToLower(strings.TrimSpace(field.Name))

	// identify field type from Go type
	err = field.ParseType(f)
	if err != nil {
		err = fmt.Errorf("field %s: %v", field.Name, err)
		return
	}

	// parse tags, allow type & fixed override
	err = field.ParseTag(tag)
	if err != nil {
		err = fmt.Errorf("field %s: %v", field.Name, err)
		return
	}

	// Validate field

	// pk field must be of type uint64
	if field.Flags&F_PRIMARY > 0 {
		switch f.Type.Kind() {
		case reflect.Uint64:
		default:
			err = fmt.Errorf("field %s: invalid primary key type %s", field.Name, f.Type)
			return
		}
	}

	// fill en/decoder info
	field.Path = f.Index
	field.Offset = f.Offset
	field.Size = uint16(field.Type.Size())

	return
}

func (f *Field) ParseType(r reflect.StructField) error {
	var (
		typ   types.FieldType
		flags types.FieldFlags
		fixed uint16
		scale uint8
	)

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
		return fmt.Errorf("unsupported map type %s", r.Type)
	case reflect.Slice:
		if r.Type == byteSliceType {
			typ = FT_BYTES
		} else {
			return fmt.Errorf("unsupported slice type %s", r.Type)
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
			return fmt.Errorf("unsupported nested struct type %s", r.Type)
		}
	case reflect.Array:
		// string-check is much quicker
		switch r.Type.String() {
		case "num.Int128":
			typ = FT_I128
		case "num.Int256":
			typ = FT_I256
		default:
			if r.Type.Elem() == uint8Type {
				typ = FT_BYTES
				fixed = uint16(r.Type.Len())
			} else {
				return fmt.Errorf("unsupported array type %s", r.Type)
			}
		}
	default:
		return fmt.Errorf("unsupported type %s (%v)", r.Type, r.Type.Kind())
	}

	f.Type = typ
	f.Flags = flags
	f.Fixed = fixed
	f.Scale = scale

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
		fixed    = f.Fixed
		maxFixed = MAX_FIXED
		maxScale = f.Scale
		flags    types.FieldFlags
		compress types.BlockCompression
		filter   types.FilterType
	)

	for _, flag := range tokens[1:] {
		key, val, ok := strings.Cut(strings.TrimSpace(flag), "=")
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		switch key {
		case "index", "fields", "extra":
			// skip here
		case "pk":
			flags |= F_PRIMARY
		case "filter":
			switch val {
			case "bits":
				filter = FL_BITS
			case "bloom2b":
				filter = FL_BLOOM2B
			case "bloom3b":
				filter = FL_BLOOM3B
			case "bloom4b":
				filter = FL_BLOOM4B
			case "bloom5b":
				filter = FL_BLOOM5B
			case "bfuse8":
				filter = FL_BFUSE8
			case "bfuse16":
				filter = FL_BFUSE16
			default:
				return fmt.Errorf("unsupported filter type %q", val)
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
			if f.Type != FT_STRING {
				return fmt.Errorf("fixed tag unsupported on type %s", f.Type)
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
			switch f.Type {
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
				return fmt.Errorf("scale tag unsupported on type %s", f.Type)
			}
		case "enum":
			switch f.Type {
			case FT_STRING, FT_U16:
				// ok
				flags |= F_ENUM
				f.Type = FT_U16
			default:
				return fmt.Errorf("unsupported enum type %s", f.Type)
			}
		case "metadata":
			flags |= F_METADATA
		case "id":
			num, err := strconv.ParseUint(val, 0, 16)
			if err != nil {
				return fmt.Errorf("invalid field id %q: %v", val, err)
			}
			f.Id = uint16(num)
		case "null":
			flags |= F_NULLABLE
		case "notnull":
			flags &^= F_NULLABLE
		case "timestamp":
			f.Type = FT_TIMESTAMP
			scale = TIME_SCALE_NANO.AsUint()
		case "date":
			f.Type = FT_DATE
			scale = TIME_SCALE_DAY.AsUint()
		case "time":
			f.Type = FT_TIME
			scale = TIME_SCALE_SECOND.AsUint()
		case "timebase":
			flags |= F_TIMEBASE
		default:
			return fmt.Errorf("unsupported struct tag '%s'", key)
		}
	}

	f.Scale = scale
	f.Fixed = fixed
	f.Flags = flags
	f.Compress = compress
	f.Filter = filter

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
	enc = make([]OpCode, len(s.Fields))
	dec = make([]OpCode, len(s.Fields))
	for i, f := range s.Fields {
		ec, dc := OpCodeSkip, OpCodeSkip
		switch f.Type {
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
			if f.Flags.Is(types.FieldFlagEnum) {
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
			if f.Fixed > 0 {
				ec = OpCodeFixedString
				dc = OpCodeFixedString
			} else {
				ec = OpCodeString
				dc = OpCodeString
			}

		case FT_BYTES:
			if f.Fixed > 0 {
				ec = OpCodeFixedBytes
				dc = OpCodeFixedBytes
			} else {
				ec = OpCodeBytes
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

		enc[i], dec[i] = ec, dc
	}
	return
}
