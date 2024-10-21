// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

type Field struct {
	// schema values for CREATE TABLE
	name     string                 // field name from struct tag or variable name
	id       uint16                 // unique lifetime id of the field
	typ      types.FieldType        // schema field type from struct tag or Go type
	flags    types.FieldFlags       // schema flags from struct tag
	compress types.FieldCompression // data compression from struct tag
	index    types.IndexType        // index type: none, hash, int, bloom
	fixed    uint16                 // 0..65535 fixed size array/bytes/string length
	scale    uint8                  // 0..255 fixed point scale, bloom error probability 1/x (1..4)

	// encoder values for INSERT, UPDATE, QUERY
	isArray  bool             // field is a fixed size array
	path     []int            // reflect struct nested positions
	offset   uintptr          // struct field offset from reflect
	wireSize uint16           // wire encoding field size in bytes, min size for []byte & string
	iface    types.IfaceFlags // Go encoder default interfaces
	enum     *EnumDictionary  // dynamic enum data
}

// ExportedField is a performance improved version of Field
// containing exported fields for direct access in other packages
type ExportedField struct {
	Name      string
	Id        uint16
	Type      types.FieldType
	Flags     types.FieldFlags
	Compress  types.FieldCompression
	Index     types.IndexType
	IsVisible bool
	IsArray   bool
	Iface     types.IfaceFlags
	Scale     uint8
	Fixed     uint16
	Offset    uintptr
	path      []int
	_         [4]byte // padding
}

func NewField(typ types.FieldType) Field {
	return Field{
		typ:      typ,
		wireSize: uint16(typ.Size()),
	}
}

func (f *Field) Name() string {
	return f.name
}

func (f *Field) Id() uint16 {
	return f.id
}

func (f *Field) Type() types.FieldType {
	return f.typ
}

func (f *Field) Path() []int {
	return f.path
}

func (f *Field) Flags() types.FieldFlags {
	return f.flags
}

func (f *Field) Is(v types.FieldFlags) bool {
	return f.flags.Is(v)
}

func (f *Field) Can(v types.IfaceFlags) bool {
	return f.iface.Is(v)
}

func (f *Field) Compress() types.FieldCompression {
	return f.compress
}

func (f *Field) Index() types.IndexType {
	return f.index
}

func (f *Field) Offset() uintptr {
	return f.offset
}

func (f *Field) Scale() uint8 {
	return f.scale
}

func (f *Field) Fixed() uint16 {
	return f.fixed
}

func (f *Field) Enum() *EnumDictionary {
	return f.enum
}

func (f *Field) IsValid() bool {
	return len(f.name) > 0 && f.typ.IsValid()
}

func (f *Field) IsVisible() bool {
	return f.flags&(types.FieldFlagDeleted|types.FieldFlagInternal) == 0
}

func (f *Field) IsFixedSize() bool {
	switch f.typ {
	case types.FieldTypeString, types.FieldTypeBytes:
		return f.fixed > 0
	default:
		return true
	}
}

func (f *Field) IsInterface() bool {
	return f.iface != 0
}

func (f *Field) IsArray() bool {
	return f.isArray
}

func (f *Field) WireSize() int {
	switch f.typ {
	case types.FieldTypeString, types.FieldTypeBytes:
		if f.fixed > 0 {
			return int(f.fixed)
		}
		return 4 // uint32 for size on the wire
	default:
		return f.typ.Size()
	}
}

// WithXXX methods do not use pointer receivers and always return a
// changed copy of the field.
func (f Field) WithName(n string) Field {
	f.name = n
	return f
}

func (f Field) WithFlags(v types.FieldFlags) Field {
	f.flags = v
	return f
}

func (f Field) WithCompression(c types.FieldCompression) Field {
	f.compress = c
	return f
}

func (f Field) WithFixed(n uint16) Field {
	f.fixed = n
	if f.typ == types.FieldTypeString || f.typ == types.FieldTypeBytes {
		f.wireSize = n
	}
	return f
}

func (f Field) WithScale(n uint8) Field {
	f.scale = n
	return f
}

func (f Field) WithEnum(d *EnumDictionary) Field {
	f.enum = d
	return f
}

func (f Field) WithIndex(kind types.IndexType) Field {
	f.index = kind
	if kind != types.IndexTypeNone {
		f.flags |= types.FieldFlagIndexed
	} else {
		f.flags &^= types.FieldFlagIndexed
	}
	return f
}

func (f Field) WithGoType(typ reflect.Type, path []int, ofs uintptr) Field {
	var iface types.IfaceFlags
	// detect marshaler types
	if typ.Implements(binaryMarshalerType) {
		iface |= types.IfaceBinaryMarshaler
	}
	if reflect.PointerTo(typ).Implements(binaryUnmarshalerType) {
		iface |= types.IfaceBinaryUnmarshaler
	}
	if typ.Implements(textMarshalerType) {
		iface |= types.IfaceTextMarshaler
	}
	if reflect.PointerTo(typ).Implements(textUnmarshalerType) {
		iface |= types.IfaceTextUnmarshaler
	}
	if typ.Implements(stringerType) {
		iface |= types.IfaceStringer
	}
	f.wireSize = uint16(typ.Size())
	if typ.Kind() == reflect.Array && typ.Elem().Kind() == reflect.Uint8 {
		f.isArray = true
		f.wireSize = uint16(typ.Len())
	}
	if f.flags.Is(types.FieldFlagEnum) {
		f.wireSize = 2
	} else if f.typ == types.FieldTypeString || f.typ == types.FieldTypeBytes {
		if f.fixed > 0 {
			f.wireSize = f.fixed
		}
	}
	f.path = path
	f.offset = ofs
	f.iface = iface
	return f
}

func (f *Field) Validate() error {
	// require scale on decimal fields only
	if f.scale != 0 {
		var minScale, maxScale uint8
		switch f.typ {
		case types.FieldTypeDecimal32:
			maxScale = num.MaxDecimal32Precision
		case types.FieldTypeDecimal64:
			maxScale = num.MaxDecimal64Precision
		case types.FieldTypeDecimal128:
			maxScale = num.MaxDecimal128Precision
		case types.FieldTypeDecimal256:
			maxScale = num.MaxDecimal256Precision
		default:
			if f.index == types.IndexTypeBloom {
				minScale, maxScale = 1, 4
			} else {
				return fmt.Errorf("scale unsupported on type %s", f.typ)
			}
		}
		if _, err := validateInt("scale", int(f.scale), int(minScale), int(maxScale)); err != nil {
			return err
		}
	}

	// require fixed on string/byte fields only
	if f.fixed != 0 {
		if _, err := validateInt("fixed", int(f.fixed), 1, int(MAX_FIXED)); err != nil {
			return err
		}
		switch f.typ {
		case types.FieldTypeBytes, types.FieldTypeString:
			// ok
		default:
			return fmt.Errorf("fixed unsupported on type %s", f.typ)
		}
	}

	// require index kind in range
	if f.index < 0 || f.index > types.IndexTypeBloom {
		return fmt.Errorf("invalid index kind %d", f.index)
	}

	// require index flag when index is != none
	if f.index > 0 && f.flags&types.FieldFlagIndexed == 0 {
		return fmt.Errorf("missing indexed flag with index kind set")
	}

	// require integer index on int fields only
	if f.index == types.IndexTypeInt {
		switch f.typ {
		case types.FieldTypeInt64, types.FieldTypeInt32,
			types.FieldTypeInt16, types.FieldTypeInt8,
			types.FieldTypeUint64, types.FieldTypeUint32,
			types.FieldTypeUint16, types.FieldTypeUint8:
		default:
			return fmt.Errorf("unsupported integer index on type %s", f.typ)
		}
	}

	// require bloom scale 1..4
	if f.index == types.IndexTypeBloom {
		if _, err := validateInt("bloom factor", int(f.scale), 1, 4); err != nil {
			return err
		}
	}

	// require uint16 for enum types
	if f.flags.Is(types.FieldFlagEnum) && f.typ != types.FieldTypeUint16 {
		return fmt.Errorf("invalid type %s for enum, requires uint16", f.typ)
	}

	if f.typ == types.FieldTypeString || f.typ == types.FieldTypeBytes {
		if f.fixed > 0 && f.wireSize != f.fixed {
			return NewFieldError(f.name, f.typ.String(), 
				fmt.Errorf("%w: wireSize %d != fixed %d", ErrFixedSizeMismatch, f.wireSize, f.fixed))
		}
	}

	if f.isArray && f.typ != types.FieldTypeBytes {
		return NewFieldError(f.name, f.typ.String(), ErrUnsupportedArray)
	}

	return nil
}

func (f *Field) Codec() OpCode {
	switch f.typ {
	case types.FieldTypeDatetime:
		return OpCodeDateTime

	case types.FieldTypeInt64:
		return OpCodeInt64

	case types.FieldTypeInt32:
		return OpCodeInt32

	case types.FieldTypeInt16:
		return OpCodeInt16

	case types.FieldTypeInt8:
		return OpCodeInt8

	case types.FieldTypeUint64:
		return OpCodeUint64

	case types.FieldTypeUint32:
		return OpCodeUint32

	case types.FieldTypeUint16:
		if f.flags.Is(types.FieldFlagEnum) {
			return OpCodeEnum
		}
		return OpCodeUint16

	case types.FieldTypeUint8:
		return OpCodeUint8

	case types.FieldTypeFloat64:
		return OpCodeFloat64

	case types.FieldTypeFloat32:
		return OpCodeFloat32

	case types.FieldTypeBoolean:
		return OpCodeBool

	case types.FieldTypeString:
		switch {
		case f.fixed > 0:
			return OpCodeFixedString
		case f.Can(types.IfaceTextMarshaler):
			return OpCodeMarshalText
		case f.Can(types.IfaceStringer):
			return OpCodeStringer
		default:
			return OpCodeString
		}

	case types.FieldTypeBytes:
		switch {
		case f.Can(types.IfaceBinaryMarshaler):
			return OpCodeMarshalBinary
		case f.isArray:
			return OpCodeFixedArray
		case f.fixed > 0:
			return OpCodeFixedBytes
		default:
			return OpCodeBytes
		}

	case types.FieldTypeInt256:
		return OpCodeInt256

	case types.FieldTypeInt128:
		return OpCodeInt128

	case types.FieldTypeDecimal256:
		return OpCodeDecimal256

	case types.FieldTypeDecimal128:
		return OpCodeDecimal128

	case types.FieldTypeDecimal64:
		return OpCodeDecimal64

	case types.FieldTypeDecimal32:
		return OpCodeDecimal32

	default:
		return OpCodeInvalid
	}
}

// Add this helper function at the top of the file
func isOverflow(value, min, max int64) bool {
	return value < min || value > max
}

// Simple per field encoder used to wire-encode individual typed values
// found in query conditions.
func (f *Field) Encode(w io.Writer, val any) error {
	if f.isArray {
		return f.encodeArray(w, val)
	}
	return f.encodeValue(w, val)
}

func (f *Field) encodeArray(w io.Writer, val any) error {
	v := reflect.ValueOf(val)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return fmt.Errorf("expected slice or array, got %T", val)
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(v.Len())); err != nil {
		return err
	}
	for i := 0; i < v.Len(); i++ {
		if err := f.encodeValue(w, v.Index(i).Interface()); err != nil {
			return err
		}
	}
	return nil
}

func (f *Field) encodeValue(w io.Writer, val any) error {
	switch f.typ {
	case types.FieldTypeInt8:
		v, err := convertToInt(val, 8)
		if err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, int8(v))
	case types.FieldTypeInt16:
		v, err := convertToInt(val, 16)
		if err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, int16(v))
	case types.FieldTypeInt32:
		v, err := convertToInt(val, 32)
		if err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, int32(v))
	case types.FieldTypeInt64:
		v, err := convertToInt(val, 64)
		if err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, v)
	case types.FieldTypeUint8:
		v, err := convertToUint(val, 8)
		if err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, uint8(v))
	case types.FieldTypeUint16:
		v, err := convertToUint(val, 16)
		if err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, uint16(v))
	case types.FieldTypeUint32:
		v, err := convertToUint(val, 32)
		if err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, uint32(v))
	case types.FieldTypeUint64:
		v, err := convertToUint(val, 64)
		if err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, v)
	case types.FieldTypeFloat32:
		return binary.Write(w, binary.LittleEndian, val.(float32))
	case types.FieldTypeFloat64:
		return binary.Write(w, binary.LittleEndian, val.(float64))
	case types.FieldTypeBoolean:
		return binary.Write(w, binary.LittleEndian, val.(bool))
	case types.FieldTypeString:
		s := val.(string)
		if f.fixed > 0 {
			if len(s) > int(f.fixed) {
				s = s[:f.fixed]
			} else {
				s = s + strings.Repeat("\x00", int(f.fixed)-len(s))
			}
			_, err := w.Write([]byte(s))
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(s))); err != nil {
			return err
		}
		_, err := w.Write([]byte(s))
		return err
	case types.FieldTypeBytes:
		b := val.([]byte)
		if f.fixed > 0 {
			if len(b) > int(f.fixed) {
				b = b[:f.fixed]
			} else {
				b = append(b, make([]byte, int(f.fixed)-len(b))...)
			}
			_, err := w.Write(b)
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(b))); err != nil {
			return err
		}
		_, err := w.Write(b)
		return err
	case types.FieldTypeDatetime:
		t, ok := val.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", val)
		}
		// Convert to UTC before encoding
		return binary.Write(w, binary.LittleEndian, t.UTC().UnixNano())
	}
	return fmt.Errorf("unsupported type for encoding: %v", f.typ)
}

// Helper functions for type conversion and overflow checking
func convertToInt(val interface{}, bitSize int) (int64, error) {
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i := v.Int()
		if bitSize < 64 && (i < math.MinInt64>>uint(64-bitSize) || i > math.MaxInt64>>uint(64-bitSize)) {
			return 0, fmt.Errorf("value %d out of range for int%d", i, bitSize)
		}
		return i, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u := v.Uint()
		if u > uint64(math.MaxInt64>>uint(64-bitSize)) {
			return 0, fmt.Errorf("value %d out of range for int%d", u, bitSize)
		}
		return int64(u), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int%d", val, bitSize)
	}
}

func convertToUint(val interface{}, bitSize int) (uint64, error) {
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i := v.Int()
		if i < 0 || uint64(i) > math.MaxUint64>>uint(64-bitSize) {
			return 0, fmt.Errorf("value %d out of range for uint%d", i, bitSize)
		}
		return uint64(i), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u := v.Uint()
		if u > math.MaxUint64>>uint(64-bitSize) {
			return 0, fmt.Errorf("value %d out of range for uint%d", u, bitSize)
		}
		return u, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to uint%d", val, bitSize)
	}
}

// Simple per field decoder used to wire-decode individual typed values
// found in query conditions.
func (f *Field) Decode(r io.Reader) (any, error) {
	if f.isArray {
		return f.decodeArray(r)
	}
	return f.decodeValue(r)
}

func (f *Field) decodeArray(r io.Reader) (any, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	sliceType := f.sliceType()
	slice := reflect.MakeSlice(sliceType, int(length), int(length))

	for i := 0; i < int(length); i++ {
		val, err := f.decodeValue(r)
		if err != nil {
			return nil, err
		}
		slice.Index(i).Set(reflect.ValueOf(val))
	}

	return slice.Interface(), nil
}

func (f *Field) sliceType() reflect.Type {
	var elemType reflect.Type
	switch f.typ {
	case types.FieldTypeInt8:
		elemType = reflect.TypeOf(int8(0))
	case types.FieldTypeInt16:
		elemType = reflect.TypeOf(int16(0))
	case types.FieldTypeInt32:
		elemType = reflect.TypeOf(int32(0))
	case types.FieldTypeInt64:
		elemType = reflect.TypeOf(int64(0))
	case types.FieldTypeUint8:
		elemType = reflect.TypeOf(uint8(0))
	case types.FieldTypeUint16:
		elemType = reflect.TypeOf(uint16(0))
	case types.FieldTypeUint32:
		elemType = reflect.TypeOf(uint32(0))
	case types.FieldTypeUint64:
		elemType = reflect.TypeOf(uint64(0))
	case types.FieldTypeFloat32:
		elemType = reflect.TypeOf(float32(0))
	case types.FieldTypeFloat64:
		elemType = reflect.TypeOf(float64(0))
	case types.FieldTypeBoolean:
		elemType = reflect.TypeOf(bool(false))
	case types.FieldTypeString:
		elemType = reflect.TypeOf("")
	case types.FieldTypeBytes:
		elemType = reflect.TypeOf([]byte{})
	case types.FieldTypeDatetime:
		elemType = reflect.TypeOf(time.Time{})
	default:
		panic(fmt.Sprintf("unsupported type for array: %v", f.typ))
	}
	return reflect.SliceOf(elemType)
}

func (f *Field) decodeValue(r io.Reader) (any, error) {
	switch f.typ {
	case types.FieldTypeInt8:
		var v int8
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.FieldTypeInt16:
		var v int16
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.FieldTypeInt32:
		var v int32
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.FieldTypeInt64:
		var v int64
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.FieldTypeUint8:
		var v uint8
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.FieldTypeUint16:
		var v uint16
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.FieldTypeUint32:
		var v uint32
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.FieldTypeUint64:
		var v uint64
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.FieldTypeFloat32:
		var v float32
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.FieldTypeFloat64:
		var v float64
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.FieldTypeBoolean:
		var v bool
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.FieldTypeString:
		if f.fixed > 0 {
			b := make([]byte, f.fixed)
			if _, err := io.ReadFull(r, b); err != nil {
				return nil, err
			}
			return strings.TrimRight(string(b), "\x00"), nil
		}
		var length uint32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			return nil, err
		}
		b := make([]byte, length)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		return string(b), nil
	case types.FieldTypeBytes:
		if f.fixed > 0 {
			b := make([]byte, f.fixed)
			if _, err := io.ReadFull(r, b); err != nil {
				return nil, err
			}
			return b, nil
		}
		var length uint32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			return nil, err
		}
		b := make([]byte, length)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		return b, nil
	case types.FieldTypeDatetime:
		var nanos int64
		err := binary.Read(r, binary.LittleEndian, &nanos)
		if err != nil {
			return nil, err
		}
		// Return the time in UTC
		return time.Unix(0, nanos).UTC(), nil
	}
	return nil, fmt.Errorf("unsupported type for decoding: %v", f.typ)
}

// StructValue resolves a struct field from a struct. When the field
// is a pointer it allocates the target type and dereferences it
// so that the return value can consistently be used for interface calls.
func (f *Field) StructValue(rval reflect.Value) reflect.Value {
	dst := rval.FieldByIndex(f.path)
	if dst.Kind() == reflect.Ptr {
		if dst.IsNil() && dst.CanSet() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}
	return dst
}

func (f *ExportedField) StructValue(rval reflect.Value) reflect.Value {
	dst := rval.FieldByIndex(f.path)
	if dst.Kind() == reflect.Ptr {
		if dst.IsNil() && dst.CanSet() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}
	return dst
}

func (f Field) WriteTo(w *bytes.Buffer) error {
	// id: u16
	binary.Write(w, LE, f.id)

	// name: string
	binary.Write(w, LE, uint16(len(f.name)))
	w.WriteString(f.name)

	// typ, flags, compression, index: byte
	binary.Write(w, LE, []byte{
		byte(f.typ),
		byte(f.flags),
		byte(f.compress),
		byte(f.index),
	})

	// fixed: u16
	binary.Write(w, LE, f.fixed)

	// scale: u8
	binary.Write(w, LE, f.scale)

	// wireSize: u16
	binary.Write(w, LE, f.wireSize)

	return nil
}

func (f *Field) ReadFrom(buf *bytes.Buffer) (err error) {
	if buf.Len() < 11 {
		return io.ErrShortBuffer
	}

	// id: u16
	err = binary.Read(buf, LE, &f.id)
	if err != nil {
		return
	}

	// name: string
	var l uint16
	err = binary.Read(buf, LE, &l)
	if err != nil {
		return
	}
	f.name = string(buf.Next(int(l)))
	if len(f.name) != int(l) {
		return io.ErrShortBuffer
	}

	// typ, flags, compression, index: byte
	if buf.Len() < 7 {
		return io.ErrShortBuffer
	}
	f.typ = types.FieldType(buf.Next(1)[0])
	f.flags = types.FieldFlags(buf.Next(1)[0])
	f.compress = types.FieldCompression(buf.Next(1)[0])
	f.index = types.IndexType(buf.Next(1)[0])

	// fixed: u16
	binary.Read(buf, LE, &f.fixed)

	// scale: u8
	binary.Read(buf, LE, &f.scale)

	// wireSize: u16
	err = binary.Read(buf, LE, &f.wireSize)
	if err != nil {
		return
	}

	return f.Validate()
}

func (f Field) DataSize() int {
	switch f.typ {
	case types.FieldTypeString, types.FieldTypeBytes:
		return 16
	default:
		return f.typ.Size()
	}
}

