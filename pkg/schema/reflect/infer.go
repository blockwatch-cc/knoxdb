// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reflect

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var (
	emptyType       = reflect.TypeFor[struct{}]()
	typeOfTime      = reflect.TypeFor[time.Time]()
	typeOfDuration  = reflect.TypeFor[time.Duration]()
	typeOfInt256    = reflect.TypeFor[num.Int256]()
	typeOfInt128    = reflect.TypeFor[num.Int128]()
	typeOfDec32     = reflect.TypeFor[num.Decimal32]()
	typeOfDec64     = reflect.TypeFor[num.Decimal64]()
	typeOfDec128    = reflect.TypeFor[num.Decimal128]()
	typeOfDec256    = reflect.TypeFor[num.Decimal256]()
	typeOfBigInt    = reflect.TypeFor[num.Big]()
	typeOfUnion     = reflect.TypeFor[schema.UnionValue]()
	typeOfByteSlice = reflect.TypeFor[[]byte]()
	typeOfInt8      = reflect.TypeFor[int8]()
	typeOfInt16     = reflect.TypeFor[int16]()
	typeOfInt32     = reflect.TypeFor[int32]()
	typeOfInt64     = reflect.TypeFor[int64]()
	typeOfUint8     = reflect.TypeFor[uint8]()
	typeOfUint16    = reflect.TypeFor[uint16]()
	typeOfUint32    = reflect.TypeFor[uint32]()
	typeOfUint64    = reflect.TypeFor[uint64]()
	typeOfFloat32   = reflect.TypeFor[float32]()
	typeOfFloat64   = reflect.TypeFor[float64]()
	typeOfBool      = reflect.TypeFor[bool]()
	typeOfString    = reflect.TypeFor[string]()
)

type builder struct {
	tag            string
	schema         *schema.Schema
	prefix         string
	allowStruct    bool
	disallowNested bool
}

func newBuilder(typ reflect.Type, tag string) *builder {
	s := &schema.Schema{
		Name:    fromCamelCase(typ.Name(), sep),
		Fields:  make([]*schema.Field, 0),
		Version: 1,
	}

	return &builder{
		tag:    tag,
		schema: s,
	}
}

func (b *builder) nextId() uint16 {
	return uint16(len(b.schema.Fields) + 1)
}

func (b *builder) inferStruct(typ reflect.Type) (*schema.Schema, error) {
	// each struct inference produces a struct schema
	s := &schema.Schema{
		Name:    fromCamelCase(typ.Name(), sep),
		Fields:  make([]*schema.Field, 0),
		Version: b.schema.Version,
	}

	for _, f := range reflect.VisibleFields(typ) {
		// skip private fields and embedded structs, promoted embedded fields
		// are still processed, only the anon struct itself is skipped
		if !f.IsExported() || f.Tag.Get(b.tag) == "-" {
			continue
		}

		// skip empty structs (used to define composite indexes)
		if f.Type == emptyType {
			continue
		}

		// fail on anonymous embedded structs (we only support direct members)
		if f.Anonymous {
			return nil, fmt.Errorf("field[%s]: invalid embedded type %s", b.prefix+f.Name, f.Type)
		}

		// analyze field
		field, err := b.inferStructField(f)
		if err != nil {
			return nil, err
		}

		// collect direct struct field
		s.Fields = append(s.Fields, field)
	}

	return s.Finalize(), nil
}

func (b *builder) inferStructField(structField reflect.StructField) (*schema.Field, error) {
	// prepare field, assign continuous field ids starting at 1
	// including for nested types in list & maps
	field := &schema.Field{
		Id:   b.nextId(),
		Name: b.prefix + fromCamelCase(structField.Name, sep),
	}

	// extract alias name
	tag := structField.Tag.Get(b.tag)
	if n, _, _ := strings.Cut(tag, ","); n != "" {
		field.Name = b.prefix + strings.ToLower(strings.TrimSpace(n))
	}

	// add to builder schema field list in case we detect embedded fields later
	b.schema.Fields = append(b.schema.Fields, field)

	// identify field type from Go type
	err := b.inferFieldType(field, structField.Type)
	if err != nil {
		return nil, fmt.Errorf("field[%s]: %w", b.prefix+field.Name, err)
	}

	// add metadata for union type
	if field.Type == schema.Union {
		field.Child = schema.UnionType.Clone()
		for _, cf := range field.Child.Fields {
			cf.Id = b.nextId()
			cf.ParentId = field.Id
			cf.Name = field.Name + "." + cf.Name
			b.schema.Fields = append(b.schema.Fields, cf)
		}
		field.Child.Finalize()
	}

	// parse tags, allow feature override
	err = b.parseFieldTag(field, tag)
	if err != nil {
		return nil, fmt.Errorf("field[%s]: %w", b.prefix+field.Name, err)
	}

	return field, nil
}

func (b *builder) inferFieldType(f *schema.Field, t reflect.Type) error {
	// unwrap pointer and mark type nullable
	if t.Kind() == reflect.Pointer {
		f.Flags |= schema.FlagNullable
		t = t.Elem()
	}

	// force check for []byte here to preempt misdetection as slice
	if t == typeOfByteSlice {
		f.Type = schema.Bytes
		f.Flags = schema.FlagNullable
		return nil
	}

	// handle different Go type kinds
	switch t.Kind() {
	case reflect.Array:
		return b.inferArrayFieldType(f, t)
	case reflect.Slice:
		return b.inferListFieldType(f, t)
	case reflect.Map:
		return b.inferMapFieldType(f, t)
	case reflect.Struct:
		return b.inferStructFieldType(f, t)
	default:
		return b.inferPrimitiveFieldType(f, t)
	}
}

// inferArrayFieldType accepts supported arrays (Int128, Int256, [N]byte)
// and rejects all other array types
func (b *builder) inferArrayFieldType(f *schema.Field, t reflect.Type) error {
	switch t {
	case typeOfInt256:
		f.Type = schema.Int256
	case typeOfInt128:
		f.Type = schema.Int128
	default:
		if t.Elem() != typeOfUint8 {
			return fmt.Errorf("go array type %v: %w", t, schema.ErrUnsupportedType)
		}
		if t.Len() > schema.MAX_ARRAY {
			f.Type = schema.Binary
		} else {
			f.Type = schema.Bytes
			f.Scale = uint8(t.Len())
		}
	}
	return nil
}

// inferStructFieldType accepts supported structs (time.Time, num.DecimalX,
// num.Big) but rejects struct field in struct fields unless a struct
// appears as child of a LIST or MAP type
func (b *builder) inferStructFieldType(f *schema.Field, t reflect.Type) error {
	switch t {
	case typeOfTime:
		f.Type = schema.Timestamp
		f.Scale = schema.TIME_SCALE_NANO.AsUint()
	case typeOfDec32:
		f.Type = schema.Decimal32
		f.Scale = num.MaxDecimal32Precision
	case typeOfDec64:
		f.Type = schema.Decimal64
		f.Scale = num.MaxDecimal64Precision
	case typeOfDec128:
		f.Type = schema.Decimal128
		f.Scale = num.MaxDecimal128Precision
	case typeOfDec256:
		f.Type = schema.Decimal256
		f.Scale = num.MaxDecimal256Precision
	case typeOfBigInt:
		f.Type = schema.Bigint
	case typeOfUnion:
		f.Type = schema.Union
		f.Flags |= schema.FlagNullable
	default:
		if b.allowStruct {
			// allow nested struct in lists and maps
			s, err := b.inferStruct(t)
			if err != nil {
				return err
			}

			// on success use as child schema
			f.Child = s.Finalize()
			return nil
		} else {
			// disallow struct in struct
			return fmt.Errorf("nested Go struct type %v: %w", t, schema.ErrUnsupportedType)
		}
	}
	return nil
}

func (b *builder) inferListFieldType(f *schema.Field, t reflect.Type) error {
	// reject when the incoming type is a MAP key
	if b.disallowNested {
		return fmt.Errorf("invalid type %s", t)
	}

	// parse as LIST type
	// - if t.Elem is a supported primitive or array
	// - if t.Elem is a nested list type (add intermediate list schema)
	// - if t.Elem is a struct type (read full nested schema)
	if f.Type == schema.List || f.Type == schema.Map {
		// when the incoming type is a list or map, add as nested list
		child := &schema.Field{
			Id:    b.nextId(),
			Name:  f.Name,
			Type:  schema.List,
			Flags: schema.FlagNullable,
		}
		// fmt.Printf("List-in-List %s > id=%d\n", f.Name, child.Id)

		// add simple schema for primitive type
		f.Child = &schema.Schema{
			Name:    f.Name,
			Fields:  []*schema.Field{child},
			Version: b.schema.Version,
		}
		f.Child.Finalize()

		// add it to the schema list
		b.schema.Fields = append(b.schema.Fields, child)

		f = child
	} else {
		// set detected type
		f.Type = schema.List
		f.Flags = schema.FlagNullable
	}

	// fill a new field (use list type to detect recursive nested
	// lists during downstream calls)
	child := &schema.Field{
		Id:   b.nextId(),
		Name: f.Name + "." + schema.ElementName,
		Type: schema.List,
	}

	// extend field name prefix for nesting and restore once we
	// leave this recursion level
	prefix := b.prefix
	b.prefix = child.Name + "."
	defer func() {
		b.prefix = prefix
	}()
	// fmt.Printf("List %s > child=%s id=%d\n", f.Name, child.Name, child.Id)

	// infer the lists element type (this may add more nested fields
	// and assign proper ids through builder)
	b.allowStruct = true
	if err := b.inferFieldType(child, t.Elem()); err != nil {
		return fmt.Errorf("go slice type %v: %w", t, err)
	}
	b.allowStruct = false

	// when a nested child was added this means we either processed
	// a nested list or a nested struct, in both cases lift the child
	// and link all embedded fields as children even if they are not
	// from the next nesting level but deeper
	if child.Child != nil {
		// if child.Type == schema.List || child.Type == schema.Map {
		// 	fmt.Printf("List elem is nested %s %s\n", child.Type, child.Child)
		// } else {
		// 	fmt.Printf("List elem is %s %s\n", child.Type, child.Child)
		// }

		// use a nested schema produced by a struct; in this case
		// all nested child fields have already been added to the
		// top-level builder schema during nested inference and
		// all fields have proper ids assigned
		f.Child = child.Child

		// link from the current list nesting level to all nested
		// child fields (doing this recursively bubbles nested field
		// references across all levels up to the top-most list type)
		for _, cf := range f.Child.Fields {
			if cf.Child == nil {
				continue
			}
			f.Child.Fields = append(f.Child.Fields, cf.Child.Fields...)
		}

	} else {
		// use a simple primitive type which the inference algo
		// placed into the child field directly (without extra
		// child.Child schema)

		// add the nested type to the top-level builder schema;
		// for primitive types this is not done recursively so
		// we need to do this here
		b.schema.Fields = append(b.schema.Fields, child)

		// to capture the nesting relation add a simple one-field
		// schema for the primitive type to the current field
		f.Child = &schema.Schema{
			Version: b.schema.Version,
			Fields:  []*schema.Field{child},
		}

		// add metadata for union type
		if child.Type == schema.Union {
			child.Child = schema.UnionType.Clone()
			for _, cf := range child.Child.Fields {
				cf.Id = b.nextId()
				cf.ParentId = child.Id
				cf.Name = child.Name + "." + cf.Name
				b.schema.Fields = append(b.schema.Fields, cf)
				f.Child.Fields = append(f.Child.Fields, cf)
			}
			child.Child.Finalize()
		}
	}

	// finalize to re-calculate nested schema hashes and sizes
	f.Child.Finalize()
	return nil
}

func (b *builder) inferMapFieldType(f *schema.Field, t reflect.Type) error {
	// reject when the incoming type is a MAP key
	if b.disallowNested {
		return fmt.Errorf("invalid type %s", t)
	}

	if f.Type == schema.List || f.Type == schema.Map {
		// when the incoming type is a list or map, add as nested map
		child := &schema.Field{
			Id:    b.nextId(),
			Name:  f.Name,
			Type:  schema.Map,
			Flags: schema.FlagNullable,
		}
		// fmt.Printf("Map-in-Map %s > id=%d\n", f.Name, child.Id)

		// add simple schema for primitive type
		f.Child = &schema.Schema{
			Name:    f.Name,
			Fields:  []*schema.Field{child},
			Version: b.schema.Version,
		}
		f.Child.Finalize()

		// add it to the schema list
		b.schema.Fields = append(b.schema.Fields, child)

		f = child
	} else {
		// set detected type
		f.Type = schema.Map
		f.Flags = schema.FlagNullable
	}

	// infer map key type (must be primitive)

	// prepare the key type field (use map type to reject nested types on key)
	keyT := &schema.Field{
		Id:   b.nextId(),
		Name: f.Name + "." + schema.KeyName,
		Type: schema.Map,
	}
	// fmt.Printf("Map key %s > child=%s id=%d\n", f.Name, keyT.Name, keyT.Id)

	// infer the map key type (must be primitive)
	b.disallowNested = true
	if err := b.inferFieldType(keyT, t.Key()); err != nil {
		return fmt.Errorf("go map type %v: %w", t, err)
	}
	b.disallowNested = false

	// fmt.Printf("Map key is %s\n", keyT.TypeName())

	// add the key type to the top-level builder schema
	b.schema.Fields = append(b.schema.Fields, keyT)

	// prepare the map type's child schema, start with adding key type
	f.Child = &schema.Schema{
		Name:    schema.EntriesName,
		Version: b.schema.Version,
		Fields:  []*schema.Field{keyT},
	}

	// infer map value type (may be primitive or nested)

	// prepare the map value type field (leave type empty to allow any)
	valT := &schema.Field{
		Id:   b.nextId(),
		Name: f.Name + "." + schema.ValueName,
		Type: schema.Map,
	}

	// update field name prefix for nesting and restore on return
	prefix := b.prefix
	b.prefix = valT.Name + "."
	defer func() {
		b.prefix = prefix
	}()

	// infer the map value type
	b.allowStruct = true
	if err := b.inferFieldType(valT, t.Elem()); err != nil {
		return fmt.Errorf("go map type %v: %w", t, err)
	}
	b.allowStruct = false

	// merge value inference result into map child type
	if valT.Child != nil {
		// if valT.Type == schema.List || valT.Type == schema.Map {
		// 	fmt.Printf("Map val is nested %s %s\n", valT.Type, valT.Child)
		// } else {
		// 	fmt.Printf("Map val is %s %s\n", valT.Type, valT.Child)
		// }

		// for struct or nested types extract the detected
		// child schema fields from valT and append to map schema
		f.Child.Fields = append(f.Child.Fields, valT.Child.Fields...)

		// link from the current nesting level to all nested
		// child fields (doing this recursively bubbles nested field
		// references across all levels up to the top-most map type)
		for _, cf := range f.Child.Fields {
			if cf.Child == nil {
				continue
			}
			f.Child.Fields = append(f.Child.Fields, cf.Child.Fields...)
		}

	} else {
		// fmt.Printf("Map val is %s\n", valT.TypeName())

		// for primitive types, use the valT field directly, note
		// it has not been appended to the top-level builder schema
		// yet, so let's do that first
		b.schema.Fields = append(b.schema.Fields, valT)

		// append to map child schema
		f.Child.Fields = append(f.Child.Fields, valT)

		// add metadata for union type
		if valT.Type == schema.Union {
			valT.Child = schema.UnionType.Clone()
			for _, cf := range valT.Child.Fields {
				cf.Id = b.nextId()
				cf.ParentId = valT.Id
				cf.Name = valT.Name + "." + cf.Name
				b.schema.Fields = append(b.schema.Fields, cf)
				f.Child.Fields = append(f.Child.Fields, cf)
			}
			valT.Child.Finalize()
		}
	}

	// finalize map child
	f.Child.Finalize()
	return nil
}

func (b *builder) inferPrimitiveFieldType(f *schema.Field, t reflect.Type) error {
	switch t {
	case typeOfDuration:
		f.Type = schema.Duration
	case typeOfInt64:
		f.Type = schema.Int64
	case typeOfInt32:
		f.Type = schema.Int32
	case typeOfInt16:
		f.Type = schema.Int16
	case typeOfInt8:
		f.Type = schema.Int8
	case typeOfUint64:
		f.Type = schema.Uint64
	case typeOfUint32:
		f.Type = schema.Uint32
	case typeOfUint16:
		f.Type = schema.Uint16
	case typeOfUint8:
		f.Type = schema.Uint8
	case typeOfFloat64:
		f.Type = schema.Float64
	case typeOfFloat32:
		f.Type = schema.Float32
	case typeOfString:
		f.Type = schema.String
	case typeOfBool:
		f.Type = schema.Boolean
	default:
		return b.inferPrimitiveFieldTypeAlias(f, t)
	}
	return nil
}

func (b *builder) inferPrimitiveFieldTypeAlias(f *schema.Field, t reflect.Type) error {
	switch t.Kind() {
	case reflect.Int64:
		f.Type = schema.Int64
	case reflect.Int32:
		f.Type = schema.Int32
	case reflect.Int16:
		f.Type = schema.Int16
	case reflect.Int8:
		f.Type = schema.Int8
	case reflect.Uint64:
		f.Type = schema.Uint64
	case reflect.Uint32:
		f.Type = schema.Uint32
	case reflect.Uint16:
		f.Type = schema.Uint16
	case reflect.Uint8:
		f.Type = schema.Uint8
	case reflect.Float64:
		f.Type = schema.Float64
	case reflect.Float32:
		f.Type = schema.Float32
	case reflect.String:
		f.Type = schema.String
	case reflect.Bool:
		f.Type = schema.Boolean
	default:
		return fmt.Errorf("go type %v: %w", t, schema.ErrUnsupportedType)
	}
	return nil
}

func (b *builder) parseFieldTag(field *schema.Field, tag string) error {
	// skip field name part
	tokens := strings.Split(tag, ",")
	if len(tokens) < 2 {
		return nil
	}

	// apply tags to field until we encounter an `element=`
	// (LIST only) or either `key=` or `value=` (MAP only)
	// where we will switch to the relevant child field
	var (
		f            = field
		haveElemTags bool
	)

	for _, flag := range tokens[1:] {
		key, val, ok := strings.Cut(strings.TrimSpace(flag), "=")
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)

		// switch to embedded field after the elem/key/val keyword
		switch key {
		case "element":
			if field.Type == schema.List {
				f = field.Child.Fields[0]
				key, val, ok = strings.Cut(strings.TrimSpace(val), "=")
				haveElemTags = true
			}
		case "key":
			if field.Type == schema.Map {
				f = field.Child.Fields[0]
				key, val, ok = strings.Cut(strings.TrimSpace(val), "=")
				haveElemTags = true
			}
		case "value":
			if field.Type == schema.Map {
				f = field.Child.Fields[1]
				key, val, ok = strings.Cut(strings.TrimSpace(val), "=")
				haveElemTags = true
			}
		}

		// process the flag
		switch key {
		case "index", "fields", "extra":
			// skip index related tags here, will process later
		case "pk", "primary":
			if f.Type == schema.Uint64 {
				f.Flags |= schema.FlagPrimary
			} else {
				return fmt.Errorf("pk tag unsupported on field type %s", f.Type)
			}
		case "filter":
			switch val {
			case "bits":
				f.Filter = schema.BitsFilter
			case "bloom2b":
				f.Filter = schema.BloomFilter2b
			case "bloom3b":
				f.Filter = schema.BloomFilter3b
			case "bloom4b":
				f.Filter = schema.BloomFilter4b
			case "bloom5b":
				f.Filter = schema.BloomFilter5b
			case "bfuse8":
				f.Filter = schema.BinaryFuseFilter8
			case "bfuse16":
				f.Filter = schema.BinaryFuseFilter16
			default:
				return fmt.Errorf("unsupported filter type %q", val)
			}
		case "zip":
			switch val {
			case "", "no", "none":
				f.Compress = schema.Uncompressed
			case "snappy":
				f.Compress = schema.Snappy
			case "lz4":
				f.Compress = schema.LZ4
			case "zstd":
				f.Compress = schema.Zstd
			default:
				return fmt.Errorf("unsupported compression type %q", val)
			}
		case "array":
			// only compatible with strings, bytes must use [n]byte arrays):
			if f.Type != schema.String {
				return fmt.Errorf("array tag unsupported on type %s", f.Type)
			}
			if ok {
				fx, err := parseInt(val, "array", 1, schema.MAX_ARRAY)
				if err != nil {
					return err
				}
				f.Scale = uint8(fx)
			} else {
				return fmt.Errorf("missing value for array tag")
			}
		case "scale":
			// only compatible with:
			// - decimal types
			// - datetime
			switch f.Type {
			case schema.Decimal32, schema.Decimal64, schema.Decimal128, schema.Decimal256:
				if ok {
					sc, err := parseInt(val, "scale", 0, int(f.Scale))
					if err != nil {
						return err
					}
					f.Scale = uint8(sc)
				} else {
					return fmt.Errorf("missing value for scale tag")
				}
			case schema.Timestamp, schema.Time, schema.Duration:
				s, ok := schema.ParseTimeScale(val)
				if !ok {
					return fmt.Errorf("invalid time scale value %q", val)
				}
				f.Scale = s.AsUint()
			default:
				return fmt.Errorf("scale tag unsupported on type %s", f.Type)
			}
		case "enum":
			if f.Type == schema.String {
				f.Type = schema.Enum
			} else {
				return fmt.Errorf("unsupported enum type %s", f.Type)
			}
		case "metadata":
			f.Flags |= schema.FlagMetadata
		case "deleted":
			f.Flags |= schema.FlagDeleted
		case "id":
			num, err := strconv.ParseUint(val, 0, 16)
			if err != nil {
				return fmt.Errorf("invalid field id %q: %v", val, err)
			}
			f.Id = uint16(num)
		case "null", "nullable":
			f.Flags |= schema.FlagNullable
		case "notnull":
			f.Flags &^= schema.FlagNullable
		case "timestamp":
			f.Type = schema.Timestamp
			f.Scale = schema.TIME_SCALE_NANO.AsUint()
		case "date":
			f.Type = schema.Date
			f.Scale = schema.TIME_SCALE_DAY.AsUint()
		case "time":
			f.Type = schema.Time
			f.Scale = schema.TIME_SCALE_SECOND.AsUint()
		case "timebase":
			f.Flags |= schema.FlagTimebase
		case "text":
			if f.Type != schema.String {
				return fmt.Errorf("text tag unsupported on type %s", f.Type)
			}
			f.Type = schema.Text
			f.Scale = 0
		case "binary":
			if f.Type != schema.Bytes {
				return fmt.Errorf("binary tag unsupported on type %s", f.Type)
			}
			f.Type = schema.Binary
			f.Scale = 0
		default:
			return fmt.Errorf("unsupported struct tag '%s'", key)
		}
	}

	// re-calculate child schema hash in case nested element flags
	// have changed
	if haveElemTags {
		if field.Child != nil {
			field.Child.Finalize()
		}
	}

	return nil
}
