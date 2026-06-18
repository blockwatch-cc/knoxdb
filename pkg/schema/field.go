// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
)

const (
	// defaultVarFieldSize is an estimation for variable sized
	// bytes slices and strings used as hint for buffer allocs
	defaultVarFieldSize = 64
)

type Field struct {
	Name       string           // field name
	Id         uint16           // unique lifetime id
	ParentId   uint16           // parent reference (nested fields only)
	CaseId     uint8            // VARIANT only: case this field belongs to
	Scale      uint8            // 0..255 fixed point scale, time scale, array len
	Level      uint8            // nesting level
	Type       FieldType        // schema field type
	Flags      FieldFlags       // schema flags
	Compress   Compression      // data compression
	Filter     FilterType       // metadata filter type
	Enum       *enum.Dictionary // enum dictionary when field is an enum
	Child      *Schema          // LIST, MAP, UNION, VARIANT flat fields
	Cases      *[]*Schema       // VARIANT cases
	UserData64 uint64           // pad to 64 byte
}

func NewField(typ FieldType, opts ...FieldOption) *Field {
	f := &Field{Type: typ}
	for _, o := range opts {
		o(f)
	}
	return f
}

func (f *Field) Basename() string {
	return basename(f.Name)
}

func (f *Field) Clone() *Field {
	clone := *f
	return &clone
}

func (f *Field) WireSize() int {
	if f.IsArray() {
		return int(f.Scale)
	}
	return f.Type.Size()
}

func (f *Field) VarSizeEstimate() int {
	if f.IsFixedSize() {
		return 0
	}
	return defaultVarFieldSize
}

func (f *Field) IsValid() bool {
	return len(f.Name) > 0 && f.Type.IsValid()
}

func (f *Field) IsNested() bool {
	switch f.Type {
	case List, Map, Variant:
		return true
	default:
		return false
	}
}

func (f *Field) ValueType() *Schema {
	switch f.Type {
	case List:
		return f.Child
	case Map:
		return (&Schema{
			Name:    f.Child.Name,
			Version: f.Child.Version,
			Fields:  f.Child.Fields[1:],
		}).Finalize()
	default:
		return nil
	}
}

func (f *Field) Is(v FieldFlags) bool {
	return f.Flags&v > 0
}

func (f *Field) IsVisible() bool {
	return f.Flags&(FlagDeleted|FlagMetadata) == 0
}

func (f *Field) IsActive() bool {
	return f.Flags&FlagDeleted == 0
}

func (f *Field) IsMeta() bool {
	return f.Flags&FlagMetadata > 0
}

func (f *Field) IsPrimary() bool {
	return f.Flags&FlagPrimary > 0
}

func (f *Field) IsTimebase() bool {
	return f.Flags&FlagTimebase > 0
}

func (f *Field) IsNullable() bool {
	return f.Flags&FlagNullable > 0
}

func (f *Field) IsArray() bool {
	return f.Scale > 0 && (f.Type == Bytes || f.Type == String)
}

func (f *Field) IsFixedSize() bool {
	switch f.Type {
	case String, Bytes:
		return f.IsArray()
	case Bigint, Text, Binary, List, Map, Union, Variant:
		return false
	default:
		return true
	}
}

func (f *Field) IsCompressed() bool {
	return f.Compress > Uncompressed
}

func (f *Field) TimeFormat() string {
	switch f.Type {
	case Timestamp, Date:
		return TimeScale(f.Scale).DateTimeFormat()
	case Time:
		return TimeScale(f.Scale).TimeOnlyFormat()
	default:
		return ""
	}
}

func (f *Field) NumCases() int {
	if f.Cases == nil {
		return 0
	}
	return len(*f.Cases)
}

func (f *Field) Case(i uint8) (*Schema, bool) {
	if f.Cases == nil || i == 0 || len(*f.Cases) < int(i) {
		return nil, false
	}
	return (*f.Cases)[i-1], true
}

func (f *Field) FindCase(n string) (uint8, *Schema) {
	for i, c := range *f.Cases {
		if c.Name == n {
			return uint8(i + 1), c
		}
	}
	return 0, nil
}

func (f *Field) EnsureCase(i uint8) *Schema {
	if i == 0 {
		panic("illegal variant field case id 0")
	}
	// alloc slice when nil
	if f.Cases == nil {
		c := make([]*Schema, i)
		f.Cases = &c
	}
	// grow slice when too small
	if cap(*f.Cases) < int(i) {
		c := make([]*Schema, i)
		copy(c, *f.Cases)
		f.Cases = &c
	}
	// resize slice for i
	if len(*f.Cases) < int(i) {
		*f.Cases = (*f.Cases)[:i]
	}
	// alloc case[i] when nil
	if (*f.Cases)[i-1] == nil {
		(*f.Cases)[i-1] = &Schema{
			Name:    f.Name + "{" + strconv.Itoa(int(i)) + "}",
			Version: f.Child.Version,
			Fields:  make([]*Field, 0),
		}
	}
	return (*f.Cases)[i-1]
}

// note: does not nest struct child schemas in list/map/variant
func (f *Field) Typename() (typ string) {
	typ = f.Type.String()
	switch f.Type {
	case Time, Timestamp, Duration:
		typ += "(" + TimeScale(f.Scale).ShortName() + ")"
	case Decimal32, Decimal64, Decimal128, Decimal256:
		typ += "(" + strconv.Itoa(int(f.Scale)) + ")"
	case String, Bytes:
		if f.IsArray() {
			typ = "[" + strconv.Itoa(int(f.Scale)) + "]" + typ
		}
	case List:
		if f.Child.NumFields() == 1 {
			typ += "[" + f.Child.Fields[0].Typename() + "]"
		} else {
			typ += "[" + f.Child.Name + "]"
		}
	case Map:
		typ += "["
		// child is a key/value struct
		if f.Child.Fields[0].Child == nil {
			// primitive key type
			typ += f.Child.Fields[0].Typename()
		} else {
			// complex key type
			typ += f.Child.Fields[0].Child.Name
		}
		typ += ","
		// if len(f.Child.Fields) == 2 {
		if f.Child.NumFields() == 2 {
			// primitive value type
			typ += f.Child.Fields[1].Typename()
		} else {
			// complex value type
			typ += ValueName
		}
		typ += "]"
	}
	return
}

// Note: does not handle list/map/variant child schemas
func ParseFieldFromTypename(typ string) (*Field, error) {
	if len(typ) == 0 {
		return nil, ErrNoType
	}
	var (
		f     *Field
		scale uint8
		flags FieldFlags
		child *Schema
	)
	switch {
	case typ[0] == '[':
		// array
		num, typstr, ok := strings.Cut(typ[1:], "]")
		if !ok {
			return nil, fmt.Errorf("invalid array type %q", typ)
		}
		n, err := strconv.Atoi(num)
		if err != nil {
			return nil, fmt.Errorf("invalid array len: %v", err)
		}
		scale = uint8(n)
		typ = typstr
	case strings.HasSuffix(typ, "]"):
		// LIST or MAP
		typstr, subtypstr, ok := strings.Cut(typ, "[")
		if ok {
			ty := ParseFieldType(typstr)
			if !ty.IsValid() || (ty != List && ty != Map) {
				return nil, fmt.Errorf("invalid field type %q", typ)
			}
			subtypstr = strings.TrimSuffix(subtypstr, "]")
			if ty == List {
				sub, err := ParseFieldFromTypename(subtypstr)
				if err != nil {
					return nil, fmt.Errorf("invalid %s type %q", ty, typ)
				}
				typ = typstr
				sub.Name = ElementName
				child = SchemaOf([]*Field{sub}).Finalize()
			} else {
				key, val, ok := strings.Cut(subtypstr, ",")
				if !ok {
					return nil, fmt.Errorf("invalid %s type %q", ty, typ)
				}
				keyT, err := ParseFieldFromTypename(key)
				if err != nil {
					return nil, fmt.Errorf("invalid %s key type %q: %v", ty, key, err)
				}
				valT, err := ParseFieldFromTypename(val)
				if err != nil {
					return nil, fmt.Errorf("invalid %s value type %q: %v", ty, val, err)
				}
				typ = typstr
				keyT.Name = KeyName
				valT.Name = ValueName
				child = SchemaOf([]*Field{keyT, valT}).Finalize()
			}
		}
	default:
		// primitive types only
		typstr, scalestr, ok := strings.Cut(typ, "(")
		if ok {
			if !strings.HasSuffix(scalestr, ")") {
				return nil, fmt.Errorf("invalid scaled type %q", typ)
			}
			scalestr = strings.TrimSuffix(scalestr, ")")
			n, err := strconv.Atoi(scalestr)
			if err == nil {
				scale = uint8(n)
			} else {
				tscale, ok := ParseTimeScale(scalestr)
				if !ok {
					return nil, fmt.Errorf("invalid scale factor %q", typ)
				}
				scale = uint8(tscale)
			}
		}
		typ = typstr
	}
	ty := ParseFieldType(typ)
	if !ty.IsValid() {
		return nil, fmt.Errorf("invalid field type %q", typ)
	}
	f = &Field{
		Type:  ty,
		Scale: scale,
		Flags: flags,
		Child: child,
	}
	return f, f.Validate()
}

func ParseFieldFlags(s string) (FieldFlags, error) {
	var flags FieldFlags
	for f := range strings.SplitSeq(s, ",") {
		ff := ParseFieldFlag(f)
		if ff == 0 {
			return 0, fmt.Errorf("invalid field flag %q", f)
		}
		flags |= ff
	}
	return flags, nil
}

func (f *Field) Validate(withNested ...bool) error {
	// require name between 1..255 bytes length
	if l := len(f.Name); l > MAX_NAME {
		return fmt.Errorf("field[%d:%s]: name %q too long, max %d chars",
			f.Id, f.Type, f.Name, MAX_NAME)
	} else if l < 1 {
		return fmt.Errorf("field[%d:%s]: missing name", f.Id, f.Type)
	}

	// require scale on decimal fields only
	if f.Scale != 0 {
		var minScale, maxScale uint8
		switch f.Type {
		case Decimal32:
			maxScale = num.MaxDecimal32Precision
		case Decimal64:
			maxScale = num.MaxDecimal64Precision
		case Decimal128:
			maxScale = num.MaxDecimal128Precision
		case Decimal256:
			maxScale = num.MaxDecimal256Precision
		case Timestamp, Time, Duration:
			maxScale = uint8(TIME_SCALE_SECOND)
		case Date:
			minScale = uint8(TIME_SCALE_DAY)
			maxScale = uint8(TIME_SCALE_DAY)
		case String, Bytes:
			minScale = 1
			maxScale = MAX_ARRAY
		default:
			return fmt.Errorf("field[%s]: scale unsupported on type %s", f.Name, f.Type)
		}
		if err := validateInt("scale", int(f.Scale), int(minScale), int(maxScale)); err != nil {
			return fmt.Errorf("field[%s]: %v", f.Name, err)
		}
	}

	// require valid filter types
	if f.Filter > 0 {
		if !f.Filter.IsValid() {
			return fmt.Errorf("field[%s]: invalid filter type %d", f.Name, f.Filter)
		}
	}

	// require array on string/byte fields only
	if f.IsArray() {
		if err := validateInt("array", int(f.Scale), 1, MAX_ARRAY); err != nil {
			return fmt.Errorf("field[%s]: %v", f.Name, err)
		}
		switch f.Type {
		case Bytes, String:
			// ok
		default:
			return fmt.Errorf("field[%s]: array unsupported on type %s", f.Name, f.Type)
		}
	}

	// require enum dict for enum types
	if f.Type == Enum && f.Enum == nil {
		return fmt.Errorf("field[%s]: nil enum registry", f.Name)
	}

	// allow timebase flag only on 64bit fields
	if f.IsTimebase() && f.Type.Size() != 8 {
		return fmt.Errorf("field[%s]: invalid use of timebase flag on type %s", f.Name, f.Type)
	}

	// primary key field is limited to uint64 (TODO: relax)
	if f.IsPrimary() && f.Type != Uint64 {
		return fmt.Errorf("field[%s]: invalid primary key type %s", f.Name, f.Type)
	}

	// check nested types only if requested
	if len(withNested) > 0 && withNested[0] && f.IsNested() {
		// require nested schema for LIST, MAP, VARIANT types
		if f.Child == nil {
			return fmt.Errorf("field[%s]: missing %s child schema", f.Name, f.Type)
		}
		if err := f.Child.Validate(); err != nil {
			return fmt.Errorf("field[%s]: invalid %s child schema: %w", f.Name, f.Type, err)
		}
		id := f.Id + 1
		for _, c := range f.Child.Fields {
			// child field must have shared prefix
			if !strings.HasPrefix(c.Name, f.Name) {
				return fmt.Errorf("field[%s]: invalid child name %s", f.Name, c.Name)
			}
			// child field must have sorted ids
			if c.Id < id {
				return fmt.Errorf("field[%s]: invalid child %s id %d (want >= %d)", f.Name, c.Name, c.Id, id)
			}
			id = c.Id + 1
			// child field must have higher level
			if c.Level <= f.Level {
				return fmt.Errorf("field[%s]: invalid child %s level %d (want > %d)", f.Name, c.Name, c.Level, f.Level)
			}
			// child field must have parent id defined
			if c.ParentId == 0 {
				return fmt.Errorf("field[%s]: missing parent id on child %s", f.Name, c.Name)
			}
			// parent id must be < child field id (no circular dependenices)
			if c.ParentId > c.Id {
				return fmt.Errorf("field[%s]: invalid parent id %d on child %s/%d", f.Name, c.ParentId, c.Name, c.Id)
			}
		}

		// special checks for map fields
		// - must contain at least two child fields (key + value)
		// - first (key) field must be a short primitive
		//   (not allowed: Bytes, Binary, Text, List, Map, Union, Variant)
		// - key type must not have nullable flag
		// - may contain more than one value field of any type including List, Map
		if f.Type == Map {
			if len(f.Child.Fields) < 2 {
				return fmt.Errorf("field[%s]: map needs at least two child fields", f.Name)
			}
			switch f.Child.Fields[0].Type {
			case Binary, Text, List, Map, Union, Variant:
				return fmt.Errorf("field[%s]: invalid map key type %s", f.Name, f.Child.Fields[0].Typename())
			case Bytes:
				// must be array
				if !f.Child.Fields[0].IsArray() {
					return fmt.Errorf("field[%s]: invalid map key type %s", f.Name, f.Child.Fields[0].Typename())
				}
			}
			if f.Child.Fields[0].IsNullable() {
				return fmt.Errorf("field[%s]: map key must not be nullable", f.Name)
			}
		}

		// special checks for variant fields
		if f.Type == Variant {
			// must have at least two cases
			if f.Cases == nil || len(*f.Cases) < 2 {
				return fmt.Errorf("field[%s]: missing cases on %s field", f.Name, f.Type)
			}

			// non-metadata child fields must have case id set
			for _, cf := range f.Child.Fields[2:] {
				if cf.CaseId == 0 {
					return fmt.Errorf("field[%s]: zero case id on case field %s", f.Name, cf.Name)
				}
			}

			// case schemas must validate
			for i, cf := range *f.Cases {
				if cf == nil {
					return fmt.Errorf("field[%s]: nil %s case %d", f.Name, f.Type, i+1)
				}
				if err := cf.Validate(); err != nil {
					return fmt.Errorf("field[%s]: invalid %s case %d schema: %w", f.Name, f.Type, i+1, err)
				}
				// all case fields must be listed in child schema and re-linked
				for _, ccf := range cf.Fields {
					if _, ok := f.Child.FindId(ccf.Id); !ok {
						return fmt.Errorf("field[%s]: missing child field %s (%d) in variant case %d/%s ", f.Name, ccf.Name, ccf.Id, i+1, cf.Name)
					}
				}
			}
		}
	}

	return nil
}

func (f *Field) WriteTo(w *bytes.Buffer) error {
	// id: u16
	binary.Write(w, LE, f.Id)

	// parent id: u16
	binary.Write(w, LE, f.ParentId)

	// name: 1 byte len, string
	w.Write([]byte{byte(len(f.Name))})
	w.WriteString(f.Name)

	// typ, flags, compression, filter, scale, level, caseid: byte
	w.Write([]byte{
		byte(f.Type),
		byte(f.Flags),
		byte(f.Compress),
		byte(f.Filter),
		f.Scale,
		f.Level,
		f.CaseId,
	})

	return nil
}

func (f *Field) ReadFrom(buf *bytes.Buffer) (err error) {
	if buf.Len() < 8 {
		return io.ErrShortBuffer
	}

	// id: u16
	err = binary.Read(buf, LE, &f.Id)
	if err != nil {
		return
	}

	// parent id: u16
	err = binary.Read(buf, LE, &f.ParentId)
	if err != nil {
		return
	}

	// name: string
	l := int(buf.Next(1)[0])
	f.Name = string(buf.Next(l))
	if len(f.Name) != l {
		return io.ErrShortBuffer
	}

	// typ, flags, compression, filter, scale, level, caseid: byte
	if buf.Len() < 7 {
		return io.ErrShortBuffer
	}
	f.Type = FieldType(buf.Next(1)[0])
	f.Flags = FieldFlags(buf.Next(1)[0])
	f.Compress = Compression(buf.Next(1)[0])
	f.Filter = FilterType(buf.Next(1)[0])
	f.Scale = buf.Next(1)[0]
	f.Level = buf.Next(1)[0]
	f.CaseId = buf.Next(1)[0]

	// alloc empty enum dict to satisfy field validity
	if f.Type == Enum {
		f.Enum = enum.NewDictionary(f.Name)
	}

	return f.Validate()
}
