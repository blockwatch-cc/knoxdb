// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/filter/bloom"
	"blockwatch.cc/knoxdb/filter/loglogbeta"

	"blockwatch.cc/knoxdb/util"

	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/encoding/decimal"
	. "blockwatch.cc/knoxdb/vec"
)

type FieldFlags int

const (
	FlagPrimary FieldFlags = 1 << iota
	FlagIndexed
	FlagCompressSnappy
	FlagCompressLZ4
	FlagBloom

	// internal type conversion flags used when a struct field's Go type
	// does not directly match the requested field type
	flagFloatType
	flagIntType
	flagUintType
	flagStringerType
	flagBinaryMarshalerType
	flagTextMarshalerType
)

func (f FieldFlags) Compression() block.Compression {
	if f&FlagCompressSnappy > 0 {
		return block.SnappyCompression
	}
	if f&FlagCompressLZ4 > 0 {
		return block.LZ4Compression
	}
	return 0
}

func (f FieldFlags) Contains(i FieldFlags) bool {
	return f&i > 0
}

func (f FieldFlags) String() string {
	s := make([]string, 0)
	for i := FlagPrimary; i <= flagUintType; i <<= 1 {
		if f&i > 0 {
			switch i {
			case FlagPrimary:
				s = append(s, "primary")
			case FlagIndexed:
				s = append(s, "indexed")
			case FlagCompressSnappy:
				s = append(s, "snappy")
			case FlagBloom:
				s = append(s, "bloom")
			case FlagCompressLZ4:
				s = append(s, "lz4")
			case flagFloatType:
				s = append(s, "as_float")
			case flagIntType:
				s = append(s, "as_int")
			case flagUintType:
				s = append(s, "as_uint")
			}
		}
	}
	return strings.Join(s, ",")
}

type FieldType byte

const (
	FieldTypeUndefined  FieldType = iota
	FieldTypeDatetime             // BlockTime
	FieldTypeInt64                // BlockInt64
	FieldTypeUint64               // BlockUint64
	FieldTypeFloat64              // BlockFloat64
	FieldTypeBoolean              // BlockBool
	FieldTypeString               // BlockString
	FieldTypeBytes                // BlockBytes
	FieldTypeInt32                // BlockInt32
	FieldTypeInt16                // BlockInt16
	FieldTypeInt8                 // BlockInt8
	FieldTypeUint32               // BlockUint32
	FieldTypeUint16               // BlockUint16
	FieldTypeUint8                // BlockUint8
	FieldTypeFloat32              // BlockFloat32
	FieldTypeInt256               // BlockInt256
	FieldTypeInt128               // BlockInt128
	FieldTypeDecimal256           // BlockDecimal256
	FieldTypeDecimal128           // BlockDecimal128
	FieldTypeDecimal64            // BlockDecimal64
	FieldTypeDecimal32            // BlockDecimal32

	// TODO: extend pack encoders and block types
	// FieldTypeDate                   = "date" // BlockDate (unix second / 24*3600)
)

type Field struct {
	Index int        `json:"index"`
	Name  string     `json:"name"`
	Alias string     `json:"alias"`
	Type  FieldType  `json:"type"`
	Flags FieldFlags `json:"flags"` // primary, indexed, compression
	Scale int        `json:"scale"` // 0..127 fixed point scale, bloom error probability 1/x
	IType IndexType  `json:"itype"` // index type: hash, integer
}

func (f Field) IsValid() bool {
	return f.Index >= 0 && f.Type.IsValid()
}

func (f Field) NewBlock(sz int) block.Block {
	return block.NewBlock(f.Type.BlockType(), f.Flags.Compression(), sz)
}

type FieldList []*Field

func (l FieldList) Sort() FieldList {
	sort.Slice(l, func(i, j int) bool { return l[i].Index < l[j].Index })
	return l
}

func (l FieldList) MaskString(m FieldList) string {
	return hex.EncodeToString(l.Mask(m))
}

func (l FieldList) Mask(m FieldList) []byte {
	b := make([]byte, (len(l)+7)>>3)
	for i, v := range l {
		if m.Contains(v.Name) {
			b[i>>3] |= byte(1 << uint(i&0x7))
		}
	}
	return b
}

func (l FieldList) Names() []string {
	s := make([]string, len(l))
	for i, v := range l {
		s[i] = v.Name
	}
	return s
}

func (l FieldList) Aliases() []string {
	s := make([]string, len(l))
	for i, v := range l {
		s[i] = v.Alias
	}
	return s
}

func (l FieldList) Find(name string) *Field {
	for _, v := range l {
		if v.Name == name || v.Alias == name {
			return v
		}
	}
	return &Field{Index: -1, Name: name, Alias: name}
}

func (l FieldList) Select(names ...string) FieldList {
	res := make(FieldList, 0)
	for _, v := range names {
		if f := l.Find(v); f.IsValid() {
			res = append(res, f)
		}
	}
	return res
}

func (l FieldList) Add(field *Field) FieldList {
	return append(l, field)
}

func (l FieldList) AddUnique(fields ...*Field) FieldList {
	res := make(FieldList, len(l))
	copy(res, l)
	for _, f := range fields {
		if !res.Contains(f.Name) {
			res = res.Add(f)
		}
	}
	return res
}

func (l FieldList) Indexed() FieldList {
	res := make(FieldList, 0)
	for _, v := range l {
		if v.Flags&FlagIndexed > 0 {
			res = append(res, v)
		}
	}
	return res
}

func (l FieldList) Pk() *Field {
	for _, v := range l {
		if v.Flags&FlagPrimary > 0 {
			return v
		}
	}
	return &Field{Index: -1}
}

func (l FieldList) PkIndex() int {
	for _, v := range l {
		if v.Flags&FlagPrimary > 0 {
			return v.Index
		}
	}
	return -1
}

func (l FieldList) Contains(name string) bool {
	for _, v := range l {
		if v.Name == name || v.Alias == name {
			return true
		}
	}
	return false
}

func (l FieldList) MergeUnique(fields ...*Field) FieldList {
	for _, v := range fields {
		if !v.IsValid() {
			continue
		}
		if !l.Contains(v.Name) {
			l = append(l, v)
		}
	}
	return l
}

// long -> short name map
func (l FieldList) NameMapReverse() map[string]string {
	m := make(map[string]string, len(l))
	for _, v := range l {
		m[v.Alias] = v.Name
	}
	return m
}

// short -> long name map
func (l FieldList) NameMap() map[string]string {
	m := make(map[string]string, len(l))
	for _, v := range l {
		m[v.Name] = v.Alias
	}
	return m
}

func mustParseFields(proto interface{}) FieldList {
	fields, err := Fields(proto)
	if err != nil {
		panic(err)
	}
	return fields
}

func Fields(proto interface{}) (FieldList, error) {
	tinfo, err := getTypeInfo(proto)
	if err != nil {
		return nil, err
	}
	val := reflect.Indirect(reflect.ValueOf(proto))
	fields := make(FieldList, len(tinfo.fields))
	for i, finfo := range tinfo.fields {
		f := finfo.value(val)
		fields[i] = &Field{
			Name:  finfo.name,
			Alias: finfo.alias,
			Index: i,
			Flags: finfo.flags,
			IType: finfo.indextype,
		}
		switch f.Kind() {
		case reflect.Int, reflect.Int64:
			fields[i].Type = FieldTypeInt64
		case reflect.Int32:
			fields[i].Type = FieldTypeInt32
		case reflect.Int16:
			fields[i].Type = FieldTypeInt16
		case reflect.Int8:
			fields[i].Type = FieldTypeInt8
		case reflect.Uint, reflect.Uint64:
			fields[i].Type = FieldTypeUint64
		case reflect.Uint32:
			fields[i].Type = FieldTypeUint32
		case reflect.Uint16:
			fields[i].Type = FieldTypeUint16
		case reflect.Uint8:
			fields[i].Type = FieldTypeUint8
		case reflect.Float64:
			fields[i].Type = FieldTypeFloat64
		case reflect.Float32:
			fields[i].Type = FieldTypeFloat32
		case reflect.String:
			fields[i].Type = FieldTypeString
		case reflect.Slice:
			// check if type implements BinaryMarshaler -> BlockBytes
			if canMarshalBinary(f) {
				fields[i].Type = FieldTypeBytes
				break
			}
			// check if type implements TextMarshaler -> BlockString
			if canMarshalText(f) {
				fields[i].Type = FieldTypeString
				break
			}
			// check if type implements fmt.Stringer -> BlockString
			if canMarshalString(f) {
				fields[i].Type = FieldTypeString
				break
			}
			if f.Type() != byteSliceType {
				return nil, fmt.Errorf("pack: unsupported slice type %s", f.Type().String())
			}
			fields[i].Type = FieldTypeBytes
		case reflect.Bool:
			fields[i].Type = FieldTypeBoolean
		case reflect.Struct:
			// string-check is much quicker
			switch f.Type().String() {
			case "time.Time":
				fields[i].Type = FieldTypeDatetime
			case "decimal.Decimal32":
				fields[i].Type = FieldTypeDecimal32
				fields[i].Scale = finfo.scale
			case "decimal.Decimal64":
				fields[i].Type = FieldTypeDecimal64
				fields[i].Scale = finfo.scale
			case "decimal.Decimal128":
				fields[i].Type = FieldTypeDecimal128
				fields[i].Scale = finfo.scale
			case "decimal.Decimal256":
				fields[i].Type = FieldTypeDecimal256
				fields[i].Scale = finfo.scale
			default:
				if canMarshalBinary(f) {
					fields[i].Type = FieldTypeBytes
				} else {
					return nil, fmt.Errorf("pack: unsupported embedded struct type %s", f.Type().String())
				}
			}
		case reflect.Array:
			// string-check is much quicker
			switch f.Type().String() {
			case "vec.Int128":
				fields[i].Type = FieldTypeInt128
			case "vec.Int256":
				fields[i].Type = FieldTypeInt256
			default:
				// check if type implements BinaryMarshaler -> BlockBytes
				if canMarshalBinary(f) {
					fields[i].Type = FieldTypeBytes
				} else {
					return nil, fmt.Errorf("pack: unsupported array type %s", f.Type().String())
				}
			}
		default:
			return nil, fmt.Errorf("pack: unsupported type %s (%v) for field %s",
				f.Type().String(), f.Kind(), finfo.name)
		}
		// allow the user to specify a different type in struct tags
		if finfo.override.IsValid() {
			fields[i].Type = finfo.override
			fields[i].Scale = finfo.scale
		}
		if finfo.flags.Contains(FlagBloom) {
			fields[i].Scale = finfo.scale
		}
	}
	return fields, nil
}

func (t FieldType) String() string {
	switch t {
	case FieldTypeBytes:
		return "bytes"
	case FieldTypeString:
		return "string"
	case FieldTypeDatetime:
		return "datetime"
	case FieldTypeBoolean:
		return "boolean"
	case FieldTypeFloat64:
		return "float64"
	case FieldTypeFloat32:
		return "float32"
	case FieldTypeInt256:
		return "int256"
	case FieldTypeInt128:
		return "int128"
	case FieldTypeInt64:
		return "int64"
	case FieldTypeInt32:
		return "int32"
	case FieldTypeInt16:
		return "int16"
	case FieldTypeInt8:
		return "int8"
	case FieldTypeUint64:
		return "uint64"
	case FieldTypeUint32:
		return "uint32"
	case FieldTypeUint16:
		return "uint16"
	case FieldTypeUint8:
		return "uint8"
	case FieldTypeDecimal256:
		return "decimal256"
	case FieldTypeDecimal128:
		return "decimal128"
	case FieldTypeDecimal64:
		return "decimal64"
	case FieldTypeDecimal32:
		return "decimal32"
	default:
		return ""
	}
}

func ParseFieldType(s string) FieldType {
	switch strings.ToLower(s) {
	case "bytes":
		return FieldTypeBytes
	case "string":
		return FieldTypeString
	case "datetime":
		return FieldTypeDatetime
	case "bool", "boolean":
		return FieldTypeBoolean
	case "int256":
		return FieldTypeInt256
	case "int128":
		return FieldTypeInt128
	case "integer", "int", "int64":
		return FieldTypeInt64
	case "int32":
		return FieldTypeInt32
	case "int16":
		return FieldTypeInt16
	case "int8":
		return FieldTypeInt8
	case "unsigned", "uint", "uint64":
		return FieldTypeUint64
	case "uint32":
		return FieldTypeUint32
	case "uint16":
		return FieldTypeUint16
	case "uint8":
		return FieldTypeUint8
	case "float", "float64":
		return FieldTypeFloat64
	case "float32":
		return FieldTypeFloat32
	case "decimal256":
		return FieldTypeDecimal256
	case "decimal128":
		return FieldTypeDecimal128
	case "decimal64":
		return FieldTypeDecimal64
	case "decimal32":
		return FieldTypeDecimal32
	default:
		return FieldTypeUndefined
	}
}

func (t FieldType) BlockType() block.BlockType {
	switch t {
	case FieldTypeUint64:
		return block.BlockTypeUint64
	case FieldTypeInt64, FieldTypeDecimal64:
		return block.BlockTypeInt64
	case FieldTypeUint32:
		return block.BlockTypeUint32
	case FieldTypeInt32, FieldTypeDecimal32:
		return block.BlockTypeInt32
	case FieldTypeUint16:
		return block.BlockTypeUint16
	case FieldTypeInt16:
		return block.BlockTypeInt16
	case FieldTypeUint8:
		return block.BlockTypeUint8
	case FieldTypeInt8:
		return block.BlockTypeInt8
	case FieldTypeBoolean:
		return block.BlockTypeBool
	case FieldTypeDatetime:
		return block.BlockTypeTime
	case FieldTypeFloat64:
		return block.BlockTypeFloat64
	case FieldTypeFloat32:
		return block.BlockTypeFloat32
	case FieldTypeBytes:
		return block.BlockTypeBytes
	case FieldTypeString:
		return block.BlockTypeString
	case FieldTypeInt128, FieldTypeDecimal128:
		return block.BlockTypeInt128
	case FieldTypeInt256, FieldTypeDecimal256:
		return block.BlockTypeInt256
	default:
		return block.BlockTypeBytes
	}
}

func (t FieldType) IsValid() bool {
	return t != FieldTypeUndefined
}

func (r FieldType) MarshalText() ([]byte, error) {
	return []byte(r.String()), nil
}

func (t *FieldType) UnmarshalText(data []byte) error {
	typ := ParseFieldType(string(data))
	if !typ.IsValid() {
		return fmt.Errorf("pack: invalid field type '%s'", string(data))
	}
	*t = typ
	return nil
}

func (t FieldType) ParseAs(s string, f *Field) (interface{}, error) {
	switch t {
	case FieldTypeBytes:
		return []byte(s), nil
	case FieldTypeString:
		return s, nil
	case FieldTypeDatetime:
		tm, err := util.ParseTime(s)
		if err != nil {
			return nil, err
		}
		return tm.Time(), nil
	case FieldTypeBoolean:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return nil, err
		}
		return b, nil
	case FieldTypeFloat64:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		return f, nil
	case FieldTypeFloat32:
		f, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return nil, err
		}
		return float32(f), nil
	case FieldTypeInt256:
		i, err := ParseInt256(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	case FieldTypeInt128:
		i, err := ParseInt128(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	case FieldTypeInt64:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return i, nil
	case FieldTypeInt32:
		i, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(i), nil
	case FieldTypeInt16:
		i, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return nil, err
		}
		return int16(i), nil
	case FieldTypeInt8:
		i, err := strconv.ParseInt(s, 10, 8)
		if err != nil {
			return nil, err
		}
		return int8(i), nil
	case FieldTypeUint64:
		i, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return i, nil
	case FieldTypeUint32:
		i, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return nil, err
		}
		return uint32(i), nil
	case FieldTypeUint16:
		i, err := strconv.ParseUint(s, 10, 16)
		if err != nil {
			return nil, err
		}
		return uint16(i), nil
	case FieldTypeUint8:
		i, err := strconv.ParseUint(s, 10, 8)
		if err != nil {
			return nil, err
		}
		return uint8(i), nil
	case FieldTypeDecimal32:
		d, err := decimal.ParseDecimal32(s)
		if err != nil {
			return nil, err
		}
		return d.Quantize(f.Scale), nil
	case FieldTypeDecimal64:
		d, err := decimal.ParseDecimal64(s)
		if err != nil {
			return nil, err
		}
		return d.Quantize(f.Scale), nil
	case FieldTypeDecimal128:
		d, err := decimal.ParseDecimal128(s)
		if err != nil {
			return nil, err
		}
		return d.Quantize(f.Scale), nil
	case FieldTypeDecimal256:
		d, err := decimal.ParseDecimal256(s)
		if err != nil {
			return nil, err
		}
		return d.Quantize(f.Scale), nil
	default:
		return nil, fmt.Errorf("unsupported field type '%s'", t)
	}
}

func (t FieldType) ParseSliceAs(s string, f *Field) (interface{}, error) {
	vv := strings.Split(s, ",")
	switch t {
	case FieldTypeBytes:
		slice := make([][]byte, len(vv))
		for i, v := range vv {
			slice[i] = []byte(v)
		}
		return slice, nil
	case FieldTypeString:
		return vv, nil
	case FieldTypeDatetime:
		slice := make([]time.Time, len(vv))
		for i, v := range vv {
			tm, err := util.ParseTime(v)
			if err != nil {
				return nil, err
			}
			slice[i] = tm.Time()
		}
		return slice, nil
	case FieldTypeBoolean:
		slice := make([]bool, len(vv))
		for i, v := range vv {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, err
			}
			slice[i] = b
		}
		return slice, nil
	case FieldTypeFloat64:
		slice := make([]float64, len(vv))
		for i, v := range vv {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, err
			}
			slice[i] = f
		}
		return slice, nil
	case FieldTypeFloat32:
		slice := make([]float32, len(vv))
		for i, v := range vv {
			f, err := strconv.ParseFloat(v, 32)
			if err != nil {
				return nil, err
			}
			slice[i] = float32(f)
		}
		return slice, nil
	case FieldTypeInt256:
		slice := make([]Int256, len(vv))
		for i, v := range vv {
			j, err := ParseInt256(v)
			if err != nil {
				return nil, err
			}
			slice[i] = j
		}
		return slice, nil
	case FieldTypeInt128:
		slice := make([]Int128, len(vv))
		for i, v := range vv {
			j, err := ParseInt128(v)
			if err != nil {
				return nil, err
			}
			slice[i] = j
		}
		return slice, nil
	case FieldTypeInt64:
		slice := make([]int64, len(vv))
		for i, v := range vv {
			j, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, err
			}
			slice[i] = j
		}
		return slice, nil
	case FieldTypeInt32:
		slice := make([]int32, len(vv))
		for i, v := range vv {
			j, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return nil, err
			}
			slice[i] = int32(j)
		}
		return slice, nil
	case FieldTypeInt16:
		slice := make([]int16, len(vv))
		for i, v := range vv {
			j, err := strconv.ParseInt(v, 10, 16)
			if err != nil {
				return nil, err
			}
			slice[i] = int16(j)
		}
		return slice, nil
	case FieldTypeInt8:
		slice := make([]int8, len(vv))
		for i, v := range vv {
			j, err := strconv.ParseInt(v, 10, 8)
			if err != nil {
				return nil, err
			}
			slice[i] = int8(j)
		}
		return slice, nil
	case FieldTypeUint64:
		slice := make([]uint64, len(vv))
		for i, v := range vv {
			j, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, err
			}
			slice[i] = j
		}
		return slice, nil
	case FieldTypeUint32:
		slice := make([]uint32, len(vv))
		for i, v := range vv {
			j, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return nil, err
			}
			slice[i] = uint32(j)
		}
		return slice, nil
	case FieldTypeUint16:
		slice := make([]uint16, len(vv))
		for i, v := range vv {
			j, err := strconv.ParseUint(v, 10, 16)
			if err != nil {
				return nil, err
			}
			slice[i] = uint16(j)
		}
		return slice, nil
	case FieldTypeUint8:
		slice := make([]uint8, len(vv))
		for i, v := range vv {
			j, err := strconv.ParseUint(v, 10, 8)
			if err != nil {
				return nil, err
			}
			slice[i] = uint8(j)
		}
		return slice, nil
	case FieldTypeDecimal32:
		slice := make([]decimal.Decimal32, len(vv))
		for i, v := range vv {
			d, err := decimal.ParseDecimal32(v)
			if err != nil {
				return nil, err
			}
			slice[i] = d.Quantize(f.Scale)
		}
		return slice, nil
	case FieldTypeDecimal64:
		slice := make([]decimal.Decimal64, len(vv))
		for i, v := range vv {
			d, err := decimal.ParseDecimal64(v)
			if err != nil {
				return nil, err
			}
			slice[i] = d.Quantize(f.Scale)
		}
		return slice, nil
	case FieldTypeDecimal128:
		slice := make([]decimal.Decimal128, len(vv))
		for i, v := range vv {
			d, err := decimal.ParseDecimal128(v)
			if err != nil {
				return nil, err
			}
			slice[i] = d.Quantize(f.Scale)
		}
		return slice, nil
	case FieldTypeDecimal256:
		slice := make([]decimal.Decimal256, len(vv))
		for i, v := range vv {
			d, err := decimal.ParseDecimal256(v)
			if err != nil {
				return nil, err
			}
			slice[i] = d.Quantize(f.Scale)
		}
		return slice, nil
	default:
		return nil, fmt.Errorf("unsupported field type '%s'", t)
	}
}

func (t FieldType) SliceToString(val interface{}, f *Field) string {
	ss := make([]string, 0)
	switch t {
	case FieldTypeBytes:
		if v, ok := val.([][]byte); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeString:
		if v, ok := val.([]string); ok {
			ss = v
		}
	case FieldTypeDatetime:
		if v, ok := val.([]time.Time); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeBoolean:
		if v, ok := val.([]bool); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeInt64:
		if v, ok := val.([]int64); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeInt32:
		if v, ok := val.([]int32); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeInt16:
		if v, ok := val.([]int16); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeInt8:
		if v, ok := val.([]int8); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeUint64:
		if v, ok := val.([]uint64); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeUint32:
		if v, ok := val.([]uint32); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeUint16:
		if v, ok := val.([]uint16); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeUint8:
		if v, ok := val.([]uint8); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeFloat64:
		if v, ok := val.([]float64); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeFloat32:
		if v, ok := val.([]float32); ok {
			for _, vv := range v {
				ss = append(ss, util.ToString(vv))
			}
		}
	case FieldTypeDecimal32:
		if v, ok := val.([]int32); ok {
			var d decimal.Decimal32
			for _, vv := range v {
				_ = d.SetInt64(int64(vv), f.Scale)
				ss = append(ss, d.String())
			}
		} else if v, ok := val.([]decimal.Decimal32); ok {
			for _, vv := range v {
				ss = append(ss, vv.String())
			}
		}
	case FieldTypeDecimal64:
		if v, ok := val.([]int64); ok {
			var d decimal.Decimal64
			for _, vv := range v {
				_ = d.SetInt64(vv, f.Scale)
				ss = append(ss, d.String())
			}
		} else if v, ok := val.([]decimal.Decimal64); ok {
			for _, vv := range v {
				ss = append(ss, vv.String())
			}
		}
	case FieldTypeDecimal128:
		if v, ok := val.([]Int128); ok {
			var d decimal.Decimal128
			for _, vv := range v {
				_ = d.SetInt128(vv, f.Scale)
				ss = append(ss, d.String())
			}
		} else if v, ok := val.([]decimal.Decimal128); ok {
			for _, vv := range v {
				ss = append(ss, vv.String())
			}
		}
	case FieldTypeDecimal256:
		if v, ok := val.([]Int256); ok {
			var d decimal.Decimal256
			for _, vv := range v {
				_ = d.SetInt256(vv, f.Scale)
				ss = append(ss, d.String())
			}
		} else if v, ok := val.([]decimal.Decimal256); ok {
			for _, vv := range v {
				ss = append(ss, vv.String())
			}
		}
	}
	if len(ss) > 0 {
		return strings.Join(ss, ", ")
	}
	return util.ToString(val)
}

// always called with concrete types, used in BinaryCondition only
func (t FieldType) Equal(xa, xb interface{}) bool {
	switch t {
	case FieldTypeBytes:
		return bytes.Equal(xa.([]byte), xb.([]byte))
	case FieldTypeString:
		return xa.(string) == xb.(string)
	case FieldTypeDatetime:
		return xa.(time.Time).Equal(xb.(time.Time))
	case FieldTypeBoolean:
		return xa.(bool) == xb.(bool)
	case FieldTypeInt256:
		return xa.(Int256).Eq(xb.(Int256))
	case FieldTypeInt128:
		return xa.(Int128).Eq(xb.(Int128))
	case FieldTypeInt64:
		return xa.(int64) == xb.(int64)
	case FieldTypeInt32:
		return xa.(int32) == xb.(int32)
	case FieldTypeInt16:
		return xa.(int16) == xb.(int16)
	case FieldTypeInt8:
		return xa.(int8) == xb.(int8)
	case FieldTypeUint64:
		return xa.(uint64) == xb.(uint64)
	case FieldTypeUint32:
		return xa.(uint32) == xb.(uint32)
	case FieldTypeUint16:
		return xa.(uint16) == xb.(uint16)
	case FieldTypeUint8:
		return xa.(uint8) == xb.(uint8)
	case FieldTypeFloat64:
		return xa.(float64) == xb.(float64)
	case FieldTypeFloat32:
		return xa.(float32) == xb.(float32)
	case FieldTypeDecimal32:
		return xa.(decimal.Decimal32).Eq(xb.(decimal.Decimal32))
	case FieldTypeDecimal64:
		return xa.(decimal.Decimal64).Eq(xb.(decimal.Decimal64))
	case FieldTypeDecimal128:
		return xa.(decimal.Decimal128).Eq(xb.(decimal.Decimal128))
	case FieldTypeDecimal256:
		return xa.(decimal.Decimal256).Eq(xb.(decimal.Decimal256))
	default:
		return false
	}
}

func (t FieldType) EqualAt(pkg *Package, index, pos int, val interface{}) bool {
	switch t {
	case FieldTypeBytes:
		a, _ := pkg.BytesAt(index, pos)
		return bytes.Equal(a, val.([]byte))
	case FieldTypeString:
		a, _ := pkg.StringAt(index, pos)
		return a == val.(string)
	case FieldTypeDatetime:
		a, _ := pkg.TimeAt(index, pos)
		return a.Equal(val.(time.Time))
	case FieldTypeBoolean:
		a, _ := pkg.BoolAt(index, pos)
		return a == val.(bool)
	case FieldTypeInt256:
		a, _ := pkg.Int256At(index, pos)
		return a.Eq(val.(Int256))
	case FieldTypeInt128:
		a, _ := pkg.Int128At(index, pos)
		return a.Eq(val.(Int128))
	case FieldTypeInt64:
		a, _ := pkg.Int64At(index, pos)
		return a == val.(int64)
	case FieldTypeInt32:
		a, _ := pkg.Int32At(index, pos)
		return a == val.(int32)
	case FieldTypeInt16:
		a, _ := pkg.Int16At(index, pos)
		return a == val.(int16)
	case FieldTypeInt8:
		a, _ := pkg.Int8At(index, pos)
		return a == val.(int8)
	case FieldTypeUint64:
		a, _ := pkg.Uint64At(index, pos)
		return a == val.(uint64)
	case FieldTypeUint32:
		a, _ := pkg.Uint32At(index, pos)
		return a == val.(uint32)
	case FieldTypeUint16:
		a, _ := pkg.Uint16At(index, pos)
		return a == val.(uint16)
	case FieldTypeUint8:
		a, _ := pkg.Uint8At(index, pos)
		return a == val.(uint8)
	case FieldTypeFloat64:
		a, _ := pkg.Float64At(index, pos)
		return a == val.(float64)
	case FieldTypeFloat32:
		a, _ := pkg.Float32At(index, pos)
		return a == val.(float32)
	case FieldTypeDecimal32:
		a, _ := pkg.Decimal32At(index, pos)
		return a.Eq(val.(decimal.Decimal32))
	case FieldTypeDecimal64:
		a, _ := pkg.Decimal64At(index, pos)
		return a.Eq(val.(decimal.Decimal64))
	case FieldTypeDecimal128:
		a, _ := pkg.Decimal128At(index, pos)
		return a.Eq(val.(decimal.Decimal128))
	case FieldTypeDecimal256:
		a, _ := pkg.Decimal256At(index, pos)
		return a.Eq(val.(decimal.Decimal256))
	default:
		return false
	}
}

func (t FieldType) EqualBlock(b block.Block, val interface{}, bits, mask *Bitset) *Bitset {
	switch t {
	case FieldTypeBytes:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeString:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeDatetime:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeBoolean:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeInt256, FieldTypeDecimal256:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeInt128, FieldTypeDecimal128:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeInt64, FieldTypeDecimal64:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeInt32, FieldTypeDecimal32:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeInt16:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeInt8:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeUint64:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeUint32:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeUint16:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeUint8:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeFloat64:
		return b.MatchEqual(val, bits, mask)
	case FieldTypeFloat32:
		return b.MatchEqual(val, bits, mask)
	default:
		return bits
	}
}

func (t FieldType) NotEqualBlock(b block.Block, val interface{}, bits, mask *Bitset) *Bitset {
	switch t {
	case FieldTypeBytes:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeString:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeDatetime:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeBoolean:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeInt256, FieldTypeDecimal256:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeInt128, FieldTypeDecimal128:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeInt64, FieldTypeDecimal64:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeInt32, FieldTypeDecimal32:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeInt16:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeInt8:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeUint64:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeUint32:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeUint16:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeUint8:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeFloat64:
		return b.MatchNotEqual(val, bits, mask)
	case FieldTypeFloat32:
		return b.MatchNotEqual(val, bits, mask)
	default:
		return bits
	}
}

func (t FieldType) Regexp(v interface{}, re string) bool {
	switch t {
	case FieldTypeBytes,
		FieldTypeBoolean,
		FieldTypeInt256,
		FieldTypeInt128,
		FieldTypeInt64,
		FieldTypeInt32,
		FieldTypeInt16,
		FieldTypeInt8,
		FieldTypeUint64,
		FieldTypeUint32,
		FieldTypeUint16,
		FieldTypeUint8,
		FieldTypeFloat32,
		FieldTypeFloat64,
		FieldTypeDecimal32,
		FieldTypeDecimal64,
		FieldTypeDecimal128,
		FieldTypeDecimal256:
		return false
	case FieldTypeString:
		val := v.(string)
		match, _ := regexp.MatchString(strings.Replace(re, "*", ".*", -1), val)
		return match
	case FieldTypeDatetime:
		val := v.(time.Time).Format(time.RFC3339)
		match, _ := regexp.MatchString(strings.Replace(re, "*", ".*", -1), val)
		return match
	default:
		return false
	}
}

func (t FieldType) RegexpAt(pkg *Package, index, pos int, re string) bool {
	switch t {
	case FieldTypeBytes,
		FieldTypeBoolean,
		FieldTypeInt256,
		FieldTypeInt128,
		FieldTypeInt64,
		FieldTypeInt32,
		FieldTypeInt16,
		FieldTypeInt8,
		FieldTypeUint64,
		FieldTypeUint32,
		FieldTypeUint16,
		FieldTypeUint8,
		FieldTypeFloat32,
		FieldTypeFloat64,
		FieldTypeDecimal32,
		FieldTypeDecimal64,
		FieldTypeDecimal128,
		FieldTypeDecimal256:
		return false
	case FieldTypeString:
		val, _ := pkg.StringAt(index, pos)
		match, _ := regexp.MatchString(strings.Replace(re, "*", ".*", -1), val)
		return match
	case FieldTypeDatetime:
		val, _ := pkg.TimeAt(index, pos)
		match, _ := regexp.MatchString(
			strings.Replace(re, "*", ".*", -1),
			val.Format(time.RFC3339),
		)
		return match
	default:
		return false
	}
}

func (t FieldType) RegexpBlock(b block.Block, re string, bits, mask *Bitset) *Bitset {
	switch t {
	case FieldTypeBytes,
		FieldTypeBoolean,
		FieldTypeInt256,
		FieldTypeInt128,
		FieldTypeInt64,
		FieldTypeInt32,
		FieldTypeInt16,
		FieldTypeInt8,
		FieldTypeUint64,
		FieldTypeUint32,
		FieldTypeUint16,
		FieldTypeUint8,
		FieldTypeFloat32,
		FieldTypeFloat64,
		FieldTypeDecimal32,
		FieldTypeDecimal64,
		FieldTypeDecimal128,
		FieldTypeDecimal256:
		return bits
	case FieldTypeString:
		rematch := strings.Replace(re, "*", ".*", -1)
		for i, v := range b.Slice().([][]byte) {
			// skip masked values
			if mask != nil && !mask.IsSet(i) {
				continue
			}
			if match, _ := regexp.Match(rematch, v); match {
				bits.Set(i)
			}
		}
		return bits
	case FieldTypeDatetime:
		rematch := strings.Replace(re, "*", ".*", -1)
		for i, v := range b.Slice().([]int64) {
			// skip masked values
			if mask != nil && !mask.IsSet(i) {
				continue
			}
			val := time.Unix(0, v).Format(time.RFC3339)
			if match, _ := regexp.MatchString(rematch, val); match {
				bits.Set(i)
			}
		}
		return bits
	default:
		return bits
	}
}

func (t FieldType) Gt(xa, xb interface{}) bool {
	switch t {
	case FieldTypeBytes:
		return bytes.Compare(xa.([]byte), xb.([]byte)) > 0
	case FieldTypeString:
		return xa.(string) > xb.(string)
	case FieldTypeDatetime:
		return xa.(time.Time).After(xb.(time.Time))
	case FieldTypeBoolean:
		return xa.(bool) != xb.(bool)
	case FieldTypeInt256:
		return xa.(Int256).Gt(xb.(Int256))
	case FieldTypeInt128:
		return xa.(Int128).Gt(xb.(Int128))
	case FieldTypeInt64:
		return xa.(int64) > xb.(int64)
	case FieldTypeInt32:
		return xa.(int32) > xb.(int32)
	case FieldTypeInt16:
		return xa.(int16) > xb.(int16)
	case FieldTypeInt8:
		return xa.(int8) > xb.(int8)
	case FieldTypeUint64:
		return xa.(uint64) > xb.(uint64)
	case FieldTypeUint32:
		return xa.(uint32) > xb.(uint32)
	case FieldTypeUint16:
		return xa.(uint16) > xb.(uint16)
	case FieldTypeUint8:
		return xa.(uint8) > xb.(uint8)
	case FieldTypeFloat64:
		return xa.(float64) > xb.(float64)
	case FieldTypeFloat32:
		return xa.(float32) > xb.(float32)
	case FieldTypeDecimal32:
		return xa.(decimal.Decimal32).Gt(xb.(decimal.Decimal32))
	case FieldTypeDecimal64:
		return xa.(decimal.Decimal64).Gt(xb.(decimal.Decimal64))
	case FieldTypeDecimal128:
		return xa.(decimal.Decimal128).Gt(xb.(decimal.Decimal128))
	case FieldTypeDecimal256:
		return xa.(decimal.Decimal256).Gt(xb.(decimal.Decimal256))
	default:
		return false
	}
}

func (t FieldType) GtAt(pkg *Package, index, pos int, val interface{}) bool {
	switch t {
	case FieldTypeBytes:
		a, _ := pkg.BytesAt(index, pos)
		return bytes.Compare(a, val.([]byte)) > 0
	case FieldTypeString:
		a, _ := pkg.StringAt(index, pos)
		return a > val.(string)
	case FieldTypeDatetime:
		a, _ := pkg.TimeAt(index, pos)
		return a.After(val.(time.Time))
	case FieldTypeBoolean:
		a, _ := pkg.BoolAt(index, pos)
		return a != val.(bool)
	case FieldTypeInt256:
		a, _ := pkg.Int256At(index, pos)
		return a.Gt(val.(Int256))
	case FieldTypeInt128:
		a, _ := pkg.Int128At(index, pos)
		return a.Gt(val.(Int128))
	case FieldTypeInt64:
		a, _ := pkg.Int64At(index, pos)
		return a > val.(int64)
	case FieldTypeInt32:
		a, _ := pkg.Int32At(index, pos)
		return a > val.(int32)
	case FieldTypeInt16:
		a, _ := pkg.Int16At(index, pos)
		return a > val.(int16)
	case FieldTypeInt8:
		a, _ := pkg.Int8At(index, pos)
		return a > val.(int8)
	case FieldTypeUint64:
		a, _ := pkg.Uint64At(index, pos)
		return a > val.(uint64)
	case FieldTypeUint32:
		a, _ := pkg.Uint32At(index, pos)
		return a > val.(uint32)
	case FieldTypeUint16:
		a, _ := pkg.Uint16At(index, pos)
		return a > val.(uint16)
	case FieldTypeUint8:
		a, _ := pkg.Uint8At(index, pos)
		return a > val.(uint8)
	case FieldTypeFloat64:
		a, _ := pkg.Float64At(index, pos)
		return a > val.(float64)
	case FieldTypeFloat32:
		a, _ := pkg.Float32At(index, pos)
		return a > val.(float32)
	case FieldTypeDecimal32:
		a, _ := pkg.Decimal32At(index, pos)
		return a.Gt(val.(decimal.Decimal32))
	case FieldTypeDecimal64:
		a, _ := pkg.Decimal64At(index, pos)
		return a.Gt(val.(decimal.Decimal64))
	case FieldTypeDecimal128:
		a, _ := pkg.Decimal128At(index, pos)
		return a.Gt(val.(decimal.Decimal128))
	case FieldTypeDecimal256:
		a, _ := pkg.Decimal256At(index, pos)
		return a.Gt(val.(decimal.Decimal256))
	default:
		return false
	}
}

func (t FieldType) GtBlock(b block.Block, val interface{}, bits, mask *Bitset) *Bitset {
	switch t {
	case FieldTypeBytes:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeString:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeDatetime:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeBoolean:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeInt256, FieldTypeDecimal256:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeInt128, FieldTypeDecimal128:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeInt64, FieldTypeDecimal64:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeInt32, FieldTypeDecimal32:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeInt16:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeInt8:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeUint64:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeUint32:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeUint16:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeUint8:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeFloat64:
		return b.MatchGreaterThan(val, bits, mask)
	case FieldTypeFloat32:
		return b.MatchGreaterThan(val, bits, mask)
	default:
		return bits
	}
}

func (t FieldType) Gte(xa, xb interface{}) bool {
	switch t {
	case FieldTypeBytes:
		return bytes.Compare(xa.([]byte), xb.([]byte)) >= 0
	case FieldTypeString:
		return xa.(string) >= xb.(string)
	case FieldTypeDatetime:
		return !xa.(time.Time).Before(xb.(time.Time))
	case FieldTypeBoolean:
		return true
	case FieldTypeInt256:
		return xa.(Int256).Gte(xb.(Int256))
	case FieldTypeInt128:
		return xa.(Int128).Gte(xb.(Int128))
	case FieldTypeInt64:
		return xa.(int64) >= xb.(int64)
	case FieldTypeInt32:
		return xa.(int32) >= xb.(int32)
	case FieldTypeInt16:
		return xa.(int16) >= xb.(int16)
	case FieldTypeInt8:
		return xa.(int8) >= xb.(int8)
	case FieldTypeUint64:
		return xa.(uint64) >= xb.(uint64)
	case FieldTypeUint32:
		return xa.(uint32) >= xb.(uint32)
	case FieldTypeUint16:
		return xa.(uint16) >= xb.(uint16)
	case FieldTypeUint8:
		return xa.(uint8) >= xb.(uint8)
	case FieldTypeFloat64:
		return xa.(float64) >= xb.(float64)
	case FieldTypeFloat32:
		return xa.(float32) >= xb.(float32)
	case FieldTypeDecimal32:
		return xa.(decimal.Decimal32).Gte(xb.(decimal.Decimal32))
	case FieldTypeDecimal64:
		return xa.(decimal.Decimal64).Gte(xb.(decimal.Decimal64))
	case FieldTypeDecimal128:
		return xa.(decimal.Decimal128).Gte(xb.(decimal.Decimal128))
	case FieldTypeDecimal256:
		return xa.(decimal.Decimal256).Gte(xb.(decimal.Decimal256))
	default:
		return false
	}
}

func (t FieldType) GteAt(pkg *Package, index, pos int, val interface{}) bool {
	switch t {
	case FieldTypeBytes:
		a, _ := pkg.BytesAt(index, pos)
		return bytes.Compare(a, val.([]byte)) >= 0
	case FieldTypeString:
		a, _ := pkg.StringAt(index, pos)
		return a >= val.(string)
	case FieldTypeDatetime:
		a, _ := pkg.TimeAt(index, pos)
		return !a.Before(val.(time.Time))
	case FieldTypeBoolean:
		return true
	case FieldTypeInt256:
		a, _ := pkg.Int256At(index, pos)
		return a.Gte(val.(Int256))
	case FieldTypeInt128:
		a, _ := pkg.Int128At(index, pos)
		return a.Gte(val.(Int128))
	case FieldTypeInt64:
		a, _ := pkg.Int64At(index, pos)
		return a >= val.(int64)
	case FieldTypeInt32:
		a, _ := pkg.Int32At(index, pos)
		return a >= val.(int32)
	case FieldTypeInt16:
		a, _ := pkg.Int16At(index, pos)
		return a >= val.(int16)
	case FieldTypeInt8:
		a, _ := pkg.Int8At(index, pos)
		return a >= val.(int8)
	case FieldTypeUint64:
		a, _ := pkg.Uint64At(index, pos)
		return a >= val.(uint64)
	case FieldTypeUint32:
		a, _ := pkg.Uint32At(index, pos)
		return a >= val.(uint32)
	case FieldTypeUint16:
		a, _ := pkg.Uint16At(index, pos)
		return a >= val.(uint16)
	case FieldTypeUint8:
		a, _ := pkg.Uint8At(index, pos)
		return a >= val.(uint8)
	case FieldTypeFloat64:
		a, _ := pkg.Float64At(index, pos)
		return a >= val.(float64)
	case FieldTypeFloat32:
		a, _ := pkg.Float32At(index, pos)
		return a >= val.(float32)
	case FieldTypeDecimal32:
		a, _ := pkg.Decimal32At(index, pos)
		return a.Gte(val.(decimal.Decimal32))
	case FieldTypeDecimal64:
		a, _ := pkg.Decimal64At(index, pos)
		return a.Gte(val.(decimal.Decimal64))
	case FieldTypeDecimal128:
		a, _ := pkg.Decimal128At(index, pos)
		return a.Gte(val.(decimal.Decimal128))
	case FieldTypeDecimal256:
		a, _ := pkg.Decimal256At(index, pos)
		return a.Gte(val.(decimal.Decimal256))
	default:
		return false
	}
}

func (t FieldType) GteBlock(b block.Block, val interface{}, bits, mask *Bitset) *Bitset {
	switch t {
	case FieldTypeBytes:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeString:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeDatetime:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeBoolean:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeInt256, FieldTypeDecimal256:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeInt128, FieldTypeDecimal128:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeInt64, FieldTypeDecimal64:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeInt32, FieldTypeDecimal32:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeInt16:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeInt8:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeUint64:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeUint32:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeUint16:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeUint8:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeFloat64:
		return b.MatchGreaterThanEqual(val, bits, mask)
	case FieldTypeFloat32:
		return b.MatchGreaterThanEqual(val, bits, mask)
	default:
		return bits
	}
}

func (t FieldType) Lt(xa, xb interface{}) bool {
	switch t {
	case FieldTypeBytes:
		return bytes.Compare(xa.([]byte), xb.([]byte)) < 0
	case FieldTypeString:
		return xa.(string) < xb.(string)
	case FieldTypeDatetime:
		return xa.(time.Time).Before(xb.(time.Time))
	case FieldTypeBoolean:
		return xa.(bool) != xb.(bool)
	case FieldTypeInt256:
		return xa.(Int256).Lt(xb.(Int256))
	case FieldTypeInt128:
		return xa.(Int128).Lt(xb.(Int128))
	case FieldTypeInt64:
		return xa.(int64) < xb.(int64)
	case FieldTypeInt32:
		return xa.(int32) < xb.(int32)
	case FieldTypeInt16:
		return xa.(int16) < xb.(int16)
	case FieldTypeInt8:
		return xa.(int8) < xb.(int8)
	case FieldTypeUint64:
		return xa.(uint64) < xb.(uint64)
	case FieldTypeUint32:
		return xa.(uint32) < xb.(uint32)
	case FieldTypeUint16:
		return xa.(uint16) < xb.(uint16)
	case FieldTypeUint8:
		return xa.(uint8) < xb.(uint8)
	case FieldTypeFloat64:
		return xa.(float64) < xb.(float64)
	case FieldTypeFloat32:
		return xa.(float32) < xb.(float32)
	case FieldTypeDecimal32:
		return xa.(decimal.Decimal32).Lt(xb.(decimal.Decimal32))
	case FieldTypeDecimal64:
		return xa.(decimal.Decimal64).Lt(xb.(decimal.Decimal64))
	case FieldTypeDecimal128:
		return xa.(decimal.Decimal128).Lt(xb.(decimal.Decimal128))
	case FieldTypeDecimal256:
		return xa.(decimal.Decimal256).Lt(xb.(decimal.Decimal256))
	default:
		return false
	}
}

func (t FieldType) LtAt(pkg *Package, index, pos int, val interface{}) bool {
	switch t {
	case FieldTypeBytes:
		a, _ := pkg.BytesAt(index, pos)
		return bytes.Compare(a, val.([]byte)) < 0
	case FieldTypeString:
		a, _ := pkg.StringAt(index, pos)
		return a < val.(string)
	case FieldTypeDatetime:
		a, _ := pkg.TimeAt(index, pos)
		return a.Before(val.(time.Time))
	case FieldTypeBoolean:
		a, _ := pkg.BoolAt(index, pos)
		return a != val.(bool)
	case FieldTypeInt256:
		a, _ := pkg.Int256At(index, pos)
		return a.Lt(val.(Int256))
	case FieldTypeInt128:
		a, _ := pkg.Int128At(index, pos)
		return a.Lt(val.(Int128))
	case FieldTypeInt64:
		a, _ := pkg.Int64At(index, pos)
		return a < val.(int64)
	case FieldTypeInt32:
		a, _ := pkg.Int32At(index, pos)
		return a < val.(int32)
	case FieldTypeInt16:
		a, _ := pkg.Int16At(index, pos)
		return a < val.(int16)
	case FieldTypeInt8:
		a, _ := pkg.Int8At(index, pos)
		return a < val.(int8)
	case FieldTypeUint64:
		a, _ := pkg.Uint64At(index, pos)
		return a < val.(uint64)
	case FieldTypeUint32:
		a, _ := pkg.Uint32At(index, pos)
		return a < val.(uint32)
	case FieldTypeUint16:
		a, _ := pkg.Uint16At(index, pos)
		return a < val.(uint16)
	case FieldTypeUint8:
		a, _ := pkg.Uint8At(index, pos)
		return a < val.(uint8)
	case FieldTypeFloat64:
		a, _ := pkg.Float64At(index, pos)
		return a < val.(float64)
	case FieldTypeFloat32:
		a, _ := pkg.Float32At(index, pos)
		return a < val.(float32)
	case FieldTypeDecimal32:
		a, _ := pkg.Decimal32At(index, pos)
		return a.Lt(val.(decimal.Decimal32))
	case FieldTypeDecimal64:
		a, _ := pkg.Decimal64At(index, pos)
		return a.Lt(val.(decimal.Decimal64))
	case FieldTypeDecimal128:
		a, _ := pkg.Decimal128At(index, pos)
		return a.Lt(val.(decimal.Decimal128))
	case FieldTypeDecimal256:
		a, _ := pkg.Decimal256At(index, pos)
		return a.Lt(val.(decimal.Decimal256))
	default:
		return false
	}
}

func (t FieldType) LtBlock(b block.Block, val interface{}, bits, mask *Bitset) *Bitset {
	switch t {
	case FieldTypeBytes:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeString:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeDatetime:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeBoolean:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeInt256, FieldTypeDecimal256:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeInt128, FieldTypeDecimal128:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeInt64, FieldTypeDecimal64:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeInt32, FieldTypeDecimal32:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeInt16:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeInt8:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeUint64:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeUint32:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeUint16:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeUint8:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeFloat64:
		return b.MatchLessThan(val, bits, mask)
	case FieldTypeFloat32:
		return b.MatchLessThan(val, bits, mask)
	default:
		return bits
	}
}

func (t FieldType) Lte(xa, xb interface{}) bool {
	switch t {
	case FieldTypeBytes:
		return bytes.Compare(xa.([]byte), xb.([]byte)) <= 0
	case FieldTypeString:
		return xa.(string) <= xb.(string)
	case FieldTypeDatetime:
		return !xa.(time.Time).After(xb.(time.Time))
	case FieldTypeBoolean:
		return xb.(bool) || xa.(bool) == xb.(bool)
	case FieldTypeInt256:
		return xa.(Int256).Lte(xb.(Int256))
	case FieldTypeInt128:
		return xa.(Int128).Lte(xb.(Int128))
	case FieldTypeInt64:
		return xa.(int64) <= xb.(int64)
	case FieldTypeInt32:
		return xa.(int32) <= xb.(int32)
	case FieldTypeInt16:
		return xa.(int16) <= xb.(int16)
	case FieldTypeInt8:
		return xa.(int8) <= xb.(int8)
	case FieldTypeUint64:
		return xa.(uint64) <= xb.(uint64)
	case FieldTypeUint32:
		return xa.(uint32) <= xb.(uint32)
	case FieldTypeUint16:
		return xa.(uint16) <= xb.(uint16)
	case FieldTypeUint8:
		return xa.(uint8) <= xb.(uint8)
	case FieldTypeFloat64:
		return xa.(float64) <= xb.(float64)
	case FieldTypeFloat32:
		return xa.(float32) <= xb.(float32)
	case FieldTypeDecimal32:
		return xa.(decimal.Decimal32).Lte(xb.(decimal.Decimal32))
	case FieldTypeDecimal64:
		return xa.(decimal.Decimal64).Lte(xb.(decimal.Decimal64))
	case FieldTypeDecimal128:
		return xa.(decimal.Decimal128).Lte(xb.(decimal.Decimal128))
	case FieldTypeDecimal256:
		return xa.(decimal.Decimal256).Lte(xb.(decimal.Decimal256))
	default:
		return false
	}
}

func (t FieldType) LteAt(pkg *Package, index, pos int, val interface{}) bool {
	switch t {
	case FieldTypeBytes:
		a, _ := pkg.BytesAt(index, pos)
		return bytes.Compare(a, val.([]byte)) <= 0
	case FieldTypeString:
		a, _ := pkg.StringAt(index, pos)
		return a <= val.(string)
	case FieldTypeDatetime:
		a, _ := pkg.TimeAt(index, pos)
		return !a.After(val.(time.Time))
	case FieldTypeBoolean:
		a, _ := pkg.BoolAt(index, pos)
		return val.(bool) || a == val.(bool)
	case FieldTypeInt256:
		a, _ := pkg.Int256At(index, pos)
		return a.Lte(val.(Int256))
	case FieldTypeInt128:
		a, _ := pkg.Int128At(index, pos)
		return a.Lte(val.(Int128))
	case FieldTypeInt64:
		a, _ := pkg.Int64At(index, pos)
		return a <= val.(int64)
	case FieldTypeInt32:
		a, _ := pkg.Int32At(index, pos)
		return a <= val.(int32)
	case FieldTypeInt16:
		a, _ := pkg.Int16At(index, pos)
		return a <= val.(int16)
	case FieldTypeInt8:
		a, _ := pkg.Int8At(index, pos)
		return a <= val.(int8)
	case FieldTypeUint64:
		a, _ := pkg.Uint64At(index, pos)
		return a <= val.(uint64)
	case FieldTypeUint32:
		a, _ := pkg.Uint32At(index, pos)
		return a <= val.(uint32)
	case FieldTypeUint16:
		a, _ := pkg.Uint16At(index, pos)
		return a <= val.(uint16)
	case FieldTypeUint8:
		a, _ := pkg.Uint8At(index, pos)
		return a <= val.(uint8)
	case FieldTypeFloat64:
		a, _ := pkg.Float64At(index, pos)
		return a <= val.(float64)
	case FieldTypeFloat32:
		a, _ := pkg.Float32At(index, pos)
		return a <= val.(float32)
	case FieldTypeDecimal32:
		a, _ := pkg.Decimal32At(index, pos)
		return a.Lte(val.(decimal.Decimal32))
	case FieldTypeDecimal64:
		a, _ := pkg.Decimal64At(index, pos)
		return a.Lte(val.(decimal.Decimal64))
	case FieldTypeDecimal128:
		a, _ := pkg.Decimal128At(index, pos)
		return a.Lte(val.(decimal.Decimal128))
	case FieldTypeDecimal256:
		a, _ := pkg.Decimal256At(index, pos)
		return a.Lte(val.(decimal.Decimal256))
	default:
		return false
	}
}

func (t FieldType) LteBlock(b block.Block, val interface{}, bits, mask *Bitset) *Bitset {
	switch t {
	case FieldTypeBytes:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeString:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeDatetime:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeBoolean:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeInt256, FieldTypeDecimal256:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeInt128, FieldTypeDecimal128:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeInt64, FieldTypeDecimal64:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeInt32, FieldTypeDecimal32:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeInt16:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeInt8:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeUint64:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeUint32:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeUint16:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeUint8:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeFloat64:
		return b.MatchLessThanEqual(val, bits, mask)
	case FieldTypeFloat32:
		return b.MatchLessThanEqual(val, bits, mask)
	default:
		return bits
	}
}

// first arg is value to compare, second is slice of value types from condition
func (t FieldType) In(v, in interface{}) bool {
	switch t {
	case FieldTypeBytes:
		val, list := v.([]byte), in.([][]byte)
		return Bytes.Contains(list, val)
	case FieldTypeString:
		val, list := v.(string), in.([]string)
		return Strings.Contains(list, val)
	case FieldTypeDatetime:
		val, list := v.(time.Time), in.([]time.Time)
		return Times.Contains(list, val)
	case FieldTypeBoolean:
		val, list := v.(bool), in.([]bool)
		return Booleans.Contains(list, val)
	case FieldTypeInt256:
		val, list := v.(Int256), in.([]Int256)
		return Int256Slice(list).Contains(val)
	case FieldTypeInt128:
		val, list := v.(Int128), in.([]Int128)
		return Int128Slice(list).Contains(val)
	case FieldTypeInt64:
		val, list := v.(int64), in.([]int64)
		return Int64.Contains(list, val)
	case FieldTypeInt32:
		val, list := v.(int32), in.([]int32)
		return Int32.Contains(list, val)
	case FieldTypeInt16:
		val, list := v.(int16), in.([]int16)
		return Int16.Contains(list, val)
	case FieldTypeInt8:
		val, list := v.(int8), in.([]int8)
		return Int8.Contains(list, val)
	case FieldTypeUint64:
		val, list := v.(uint64), in.([]uint64)
		return Uint64.Contains(list, val)
	case FieldTypeUint32:
		val, list := v.(uint32), in.([]uint32)
		return Uint32.Contains(list, val)
	case FieldTypeUint16:
		val, list := v.(uint16), in.([]uint16)
		return Uint16.Contains(list, val)
	case FieldTypeUint8:
		val, list := v.(uint8), in.([]uint8)
		return Uint8.Contains(list, val)
	case FieldTypeFloat64:
		val, list := v.(float64), in.([]float64)
		return Float64.Contains(list, val)
	case FieldTypeFloat32:
		val, list := v.(float32), in.([]float32)
		return Float32.Contains(list, val)
	case FieldTypeDecimal32:
		val, list := v.(decimal.Decimal32).Int32(), in.([]int32)
		return Int32.Contains(list, val)
	case FieldTypeDecimal64:
		val, list := v.(decimal.Decimal64).Int64(), in.([]int64)
		return Int64.Contains(list, val)
	case FieldTypeDecimal128:
		val, list := v.(decimal.Decimal128).Int128(), in.([]Int128)
		return Int128Slice(list).Contains(val)
	case FieldTypeDecimal256:
		val, list := v.(decimal.Decimal256).Int256(), in.([]Int256)
		return Int256Slice(list).Contains(val)
	}
	return false
}

// assumes `in` is sorted
func (t FieldType) InAt(pkg *Package, index, pos int, in interface{}) bool {
	switch t {
	case FieldTypeBytes:
		val, _ := pkg.BytesAt(index, pos)
		list := in.([][]byte)
		return Bytes.Contains(list, val)
	case FieldTypeString:
		val, _ := pkg.StringAt(index, pos)
		list := in.([]string)
		return Strings.Contains(list, val)
	case FieldTypeDatetime:
		val, _ := pkg.TimeAt(index, pos)
		list := in.([]time.Time)
		return Times.Contains(list, val)
	case FieldTypeBoolean:
		val, _ := pkg.BoolAt(index, pos)
		list := in.([]bool)
		return Booleans.Contains(list, val)
	case FieldTypeInt256:
		val, _ := pkg.Int256At(index, pos)
		list := in.([]Int256)
		return Int256Slice(list).Contains(val)
	case FieldTypeInt128:
		val, _ := pkg.Int128At(index, pos)
		list := in.([]Int128)
		return Int128Slice(list).Contains(val)
	case FieldTypeInt64:
		val, _ := pkg.Int64At(index, pos)
		list := in.([]int64)
		return Int64.Contains(list, val)
	case FieldTypeInt32:
		val, _ := pkg.Int32At(index, pos)
		list := in.([]int32)
		return Int32.Contains(list, val)
	case FieldTypeInt16:
		val, _ := pkg.Int16At(index, pos)
		list := in.([]int16)
		return Int16.Contains(list, val)
	case FieldTypeInt8:
		val, _ := pkg.Int8At(index, pos)
		list := in.([]int8)
		return Int8.Contains(list, val)
	case FieldTypeUint64:
		val, _ := pkg.Uint64At(index, pos)
		list := in.([]uint64)
		return Uint64.Contains(list, val)
	case FieldTypeUint32:
		val, _ := pkg.Uint32At(index, pos)
		list := in.([]uint32)
		return Uint32.Contains(list, val)
	case FieldTypeUint16:
		val, _ := pkg.Uint16At(index, pos)
		list := in.([]uint16)
		return Uint16.Contains(list, val)
	case FieldTypeUint8:
		val, _ := pkg.Uint8At(index, pos)
		list := in.([]uint8)
		return Uint8.Contains(list, val)
	case FieldTypeFloat64:
		val, _ := pkg.Float64At(index, pos)
		list := in.([]float64)
		return Float64.Contains(list, val)
	case FieldTypeFloat32:
		val, _ := pkg.Float32At(index, pos)
		list := in.([]float32)
		return Float32.Contains(list, val)
	case FieldTypeDecimal32:
		val, _ := pkg.Decimal32At(index, pos)
		list := in.([]int32)
		return Int32.Contains(list, val.Int32())
	case FieldTypeDecimal64:
		val, _ := pkg.Decimal64At(index, pos)
		list := in.([]int64)
		return Int64.Contains(list, val.Int64())
	case FieldTypeDecimal128:
		val, _ := pkg.Decimal128At(index, pos)
		list := in.([]Int128)
		return Int128Slice(list).Contains(val.Int128())
	case FieldTypeDecimal256:
		val, _ := pkg.Decimal256At(index, pos)
		list := in.([]Int256)
		return Int256Slice(list).Contains(val.Int256())
	}
	return false
}

// assumes from <= to
func (t FieldType) Between(val, from, to interface{}) bool {
	switch t {
	case FieldTypeBytes:
		v := val.([]byte)
		fromMatch := bytes.Compare(v, from.([]byte))
		if fromMatch == 0 || len(from.([]byte)) == 0 {
			return true
		}
		if fromMatch < 0 {
			return false
		}
		toMatch := bytes.Compare(v, to.([]byte))
		if toMatch > 0 {
			return false
		}
		return true

	case FieldTypeString:
		v := val.(string)
		fromMatch := strings.Compare(v, from.(string))
		if fromMatch == 0 || len(from.(string)) == 0 {
			return true
		}
		if fromMatch < 0 {
			return false
		}
		toMatch := strings.Compare(v, to.(string))
		if toMatch > 0 {
			return false
		}
		return true

	case FieldTypeDatetime:
		v := val.(time.Time)
		if v.Before(from.(time.Time)) {
			return false
		}
		if v.After(to.(time.Time)) {
			return false
		}
		return true

	case FieldTypeBoolean:
		switch true {
		case from.(bool) != to.(bool):
			return true
		case from.(bool) == val.(bool):
			return true
		case to.(bool) == val.(bool):
			return true
		}

	case FieldTypeInt256:
		v := val.(Int256)
		return !(v.Lt(from.(Int256)) || v.Gt(to.(Int256)))

	case FieldTypeInt128:
		v := val.(Int128)
		return !(v.Lt(from.(Int128)) || v.Gt(to.(Int128)))

	case FieldTypeInt64:
		v := val.(int64)
		return !(v < from.(int64) || v > to.(int64))

	case FieldTypeInt32:
		v := val.(int32)
		return !(v < from.(int32) || v > to.(int32))

	case FieldTypeInt16:
		v := val.(int16)
		return !(v < from.(int16) || v > to.(int16))

	case FieldTypeInt8:
		v := val.(int8)
		return !(v < from.(int8) || v > to.(int8))

	case FieldTypeUint64:
		v := val.(uint64)
		return !(v < from.(uint64) || v > to.(uint64))

	case FieldTypeUint32:
		v := val.(uint32)
		return !(v < from.(uint32) || v > to.(uint32))

	case FieldTypeUint16:
		v := val.(uint16)
		return !(v < from.(uint16) || v > to.(uint16))

	case FieldTypeUint8:
		v := val.(uint8)
		return !(v < from.(uint8) || v > to.(uint8))

	case FieldTypeFloat64:
		v := val.(float64)
		return !(v < from.(float64) || v > to.(float64))

	case FieldTypeFloat32:
		v := val.(float32)
		return !(v < from.(float32) || v > to.(float32))

	case FieldTypeDecimal32:
		v := val.(decimal.Decimal32)
		return !(v.Lt(from.(decimal.Decimal32)) || v.Gt(to.(decimal.Decimal32)))

	case FieldTypeDecimal64:
		v := val.(decimal.Decimal64)
		return !(v.Lt(from.(decimal.Decimal64)) || v.Gt(to.(decimal.Decimal64)))

	case FieldTypeDecimal128:
		v := val.(decimal.Decimal128)
		return !(v.Lt(from.(decimal.Decimal128)) || v.Gt(to.(decimal.Decimal128)))

	case FieldTypeDecimal256:
		v := val.(decimal.Decimal256)
		return !(v.Lt(from.(decimal.Decimal256)) || v.Gt(to.(decimal.Decimal256)))
	}
	return false
}

// assumes from <= to
func (t FieldType) BetweenAt(pkg *Package, index, pos int, from, to interface{}) bool {
	switch t {
	case FieldTypeBytes:
		val, _ := pkg.BytesAt(index, pos)
		fromMatch := bytes.Compare(val, from.([]byte))
		if fromMatch == 0 || len(from.([]byte)) == 0 {
			return true
		}
		if fromMatch < 0 {
			return false
		}
		toMatch := bytes.Compare(val, to.([]byte))
		if toMatch > 0 {
			return false
		}
		return true

	case FieldTypeString:
		val, _ := pkg.StringAt(index, pos)
		fromMatch := strings.Compare(val, from.(string))
		if fromMatch == 0 || len(from.(string)) == 0 {
			return true
		}
		if fromMatch < 0 {
			return false
		}
		toMatch := strings.Compare(val, to.(string))
		if toMatch > 0 {
			return false
		}
		return true

	case FieldTypeDatetime:
		val, _ := pkg.TimeAt(index, pos)
		if val.Before(from.(time.Time)) {
			return false
		}
		if val.After(to.(time.Time)) {
			return false
		}
		return true

	case FieldTypeBoolean:
		val, _ := pkg.BoolAt(index, pos)
		switch true {
		case from.(bool) != to.(bool):
			return true
		case from.(bool) == val:
			return true
		case to.(bool) == val:
			return true
		}

	case FieldTypeInt256:
		val, _ := pkg.Int256At(index, pos)
		return !(val.Lt(from.(Int256)) || val.Gt(to.(Int256)))

	case FieldTypeInt128:
		val, _ := pkg.Int128At(index, pos)
		return !(val.Lt(from.(Int128)) || val.Gt(to.(Int128)))

	case FieldTypeInt64:
		val, _ := pkg.Int64At(index, pos)
		return !(val < from.(int64) || val > to.(int64))

	case FieldTypeInt32:
		val, _ := pkg.Int32At(index, pos)
		return !(val < from.(int32) || val > to.(int32))

	case FieldTypeInt16:
		val, _ := pkg.Int16At(index, pos)
		return !(val < from.(int16) || val > to.(int16))

	case FieldTypeInt8:
		val, _ := pkg.Int8At(index, pos)
		return !(val < from.(int8) || val > to.(int8))

	case FieldTypeUint64:
		val, _ := pkg.Uint64At(index, pos)
		return !(val < from.(uint64) || val > to.(uint64))

	case FieldTypeUint32:
		val, _ := pkg.Uint32At(index, pos)
		return !(val < from.(uint32) || val > to.(uint32))

	case FieldTypeUint16:
		val, _ := pkg.Uint16At(index, pos)
		return !(val < from.(uint16) || val > to.(uint16))

	case FieldTypeUint8:
		val, _ := pkg.Uint8At(index, pos)
		return !(val < from.(uint8) || val > to.(uint8))

	case FieldTypeFloat64:
		val, _ := pkg.Float64At(index, pos)
		return !(val < from.(float64) || val > to.(float64))

	case FieldTypeFloat32:
		val, _ := pkg.Float32At(index, pos)
		return !(val < from.(float32) || val > to.(float32))

	case FieldTypeDecimal32:
		val, _ := pkg.Decimal32At(index, pos)
		return !(val.Lt(from.(decimal.Decimal32)) || val.Gt(to.(decimal.Decimal32)))

	case FieldTypeDecimal64:
		val, _ := pkg.Decimal64At(index, pos)
		return !(val.Lt(from.(decimal.Decimal64)) || val.Gt(to.(decimal.Decimal64)))

	case FieldTypeDecimal128:
		val, _ := pkg.Decimal128At(index, pos)
		return !(val.Lt(from.(decimal.Decimal128)) || val.Gt(to.(decimal.Decimal128)))

	case FieldTypeDecimal256:
		val, _ := pkg.Decimal256At(index, pos)
		return !(val.Lt(from.(decimal.Decimal256)) || val.Gt(to.(decimal.Decimal256)))

	}
	return false
}

// assumes from <= to
func (t FieldType) BetweenBlock(b block.Block, from, to interface{}, bits, mask *Bitset) *Bitset {
	switch t {
	case FieldTypeBytes:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeString:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeDatetime:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeBoolean:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeInt256, FieldTypeDecimal256:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeInt128, FieldTypeDecimal128:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeInt64, FieldTypeDecimal64:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeInt32, FieldTypeDecimal32:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeInt16:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeInt8:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeUint64:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeUint32:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeUint16:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeUint8:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeFloat64:
		return b.MatchBetween(from, to, bits, mask)
	case FieldTypeFloat32:
		return b.MatchBetween(from, to, bits, mask)
	default:
		return bits
	}
}

// using binary search to find if slice contains values in interval [from, to]
// Note: there's no *At func because this function works on slices already
// assumes from <= to
func (t FieldType) InBetween(slice, from, to interface{}) bool {
	switch t {
	case FieldTypeBytes:
		return Bytes.ContainsRange(slice.([][]byte), from.([]byte), to.([]byte))

	case FieldTypeString:
		return Strings.ContainsRange(slice.([]string), from.(string), to.(string))

	case FieldTypeDatetime:
		return Times.ContainsRange(slice.([]time.Time), from.(time.Time), to.(time.Time))

	case FieldTypeBoolean:
		return Booleans.ContainsRange(slice.([]bool), from.(bool), to.(bool))

	case FieldTypeInt256:
		return Int256Slice(slice.([]Int256)).ContainsRange(from.(Int256), to.(Int256))

	case FieldTypeInt128:
		return Int128Slice(slice.([]Int128)).ContainsRange(from.(Int128), to.(Int128))

	case FieldTypeInt64:
		return Int64.ContainsRange(slice.([]int64), from.(int64), to.(int64))

	case FieldTypeInt32:
		return Int32.ContainsRange(slice.([]int32), from.(int32), to.(int32))

	case FieldTypeInt16:
		return Int16.ContainsRange(slice.([]int16), from.(int16), to.(int16))

	case FieldTypeInt8:
		return Int8.ContainsRange(slice.([]int8), from.(int8), to.(int8))

	case FieldTypeUint64:
		return Uint64.ContainsRange(slice.([]uint64), from.(uint64), to.(uint64))

	case FieldTypeUint32:
		return Uint32.ContainsRange(slice.([]uint32), from.(uint32), to.(uint32))

	case FieldTypeUint16:
		return Uint16.ContainsRange(slice.([]uint16), from.(uint16), to.(uint16))

	case FieldTypeUint8:
		return Uint8.ContainsRange(slice.([]uint8), from.(uint8), to.(uint8))

	case FieldTypeFloat64:
		return Float64.ContainsRange(slice.([]float64), from.(float64), to.(float64))

	case FieldTypeFloat32:
		return Float32.ContainsRange(slice.([]float32), from.(float32), to.(float32))

	case FieldTypeDecimal256:
		return Int256Slice(slice.(decimal.Decimal256Slice).Int256).ContainsRange(
			from.(decimal.Decimal256).Int256(),
			to.(decimal.Decimal256).Int256(),
		)

	case FieldTypeDecimal128:
		return Int128Slice(slice.(decimal.Decimal128Slice).Int128).ContainsRange(
			from.(decimal.Decimal128).Int128(),
			to.(decimal.Decimal128).Int128(),
		)

	case FieldTypeDecimal64:
		return Int64.ContainsRange(
			slice.(decimal.Decimal64Slice).Int64,
			from.(decimal.Decimal64).Int64(),
			to.(decimal.Decimal64).Int64(),
		)

	case FieldTypeDecimal32:
		return Int32.ContainsRange(
			slice.(decimal.Decimal32Slice).Int32,
			from.(decimal.Decimal32).Int32(),
			to.(decimal.Decimal32).Int32(),
		)

	}
	return false
}

// used in table indexes
func (t FieldType) isZero(val interface{}) bool {
	switch t {
	case FieldTypeBytes:
		return len(val.([]byte)) == 0
	case FieldTypeString:
		return len(val.(string)) == 0
	case FieldTypeDatetime:
		return val.(time.Time).IsZero()
	case FieldTypeBoolean,
		FieldTypeInt256,
		FieldTypeInt128,
		FieldTypeInt64,
		FieldTypeInt32,
		FieldTypeInt16,
		FieldTypeInt8,
		FieldTypeUint64,
		FieldTypeUint32,
		FieldTypeUint16,
		FieldTypeUint8,
		FieldTypeDecimal32,
		FieldTypeDecimal64,
		FieldTypeDecimal128,
		FieldTypeDecimal256:
		// Note: zero is undefined here since 0 is also a valid value
		return false
	case FieldTypeFloat64:
		v := val.(float64)
		return math.IsNaN(v) || math.IsInf(v, 0)
	case FieldTypeFloat32:
		v := val.(float32)
		return math.IsNaN(float64(v)) || math.IsInf(float64(v), 0)
	}
	return true
}

func (t FieldType) Less(xa, xb interface{}) bool {
	switch t {
	case FieldTypeBytes:
		return bytes.Compare(xa.([]byte), xb.([]byte)) < 0
	case FieldTypeString:
		return xa.(string) < xb.(string)
	case FieldTypeDatetime:
		return xa.(time.Time).Before(xb.(time.Time))
	case FieldTypeBoolean:
		return xa.(bool) != xb.(bool)
	case FieldTypeInt256:
		return xa.(Int256).Lt(xb.(Int256))
	case FieldTypeInt128:
		return xa.(Int128).Lt(xb.(Int128))
	case FieldTypeInt64:
		return xa.(int64) < xb.(int64)
	case FieldTypeInt32:
		return xa.(int32) < xb.(int32)
	case FieldTypeInt16:
		return xa.(int16) < xb.(int16)
	case FieldTypeInt8:
		return xa.(int8) < xb.(int8)
	case FieldTypeUint64:
		return xa.(uint64) < xb.(uint64)
	case FieldTypeUint32:
		return xa.(uint32) < xb.(uint32)
	case FieldTypeUint16:
		return xa.(uint16) < xb.(uint16)
	case FieldTypeUint8:
		return xa.(uint8) < xb.(uint8)
	case FieldTypeFloat64:
		return xa.(float64) < xb.(float64)
	case FieldTypeFloat32:
		return xa.(float32) < xb.(float32)
	case FieldTypeDecimal32:
		return xa.(decimal.Decimal32).Lt(xb.(decimal.Decimal32))
	case FieldTypeDecimal64:
		return xa.(decimal.Decimal64).Lt(xb.(decimal.Decimal64))
	case FieldTypeDecimal128:
		return xa.(decimal.Decimal128).Lt(xb.(decimal.Decimal128))
	case FieldTypeDecimal256:
		return xa.(decimal.Decimal256).Lt(xb.(decimal.Decimal256))
	default:
		return false
	}
}

func (t FieldType) Compare(xa, xb interface{}) int {
	switch t {
	case FieldTypeBytes:
		return bytes.Compare(xa.([]byte), xb.([]byte))
	case FieldTypeString:
		return strings.Compare(xa.(string), xb.(string))
	case FieldTypeDatetime:
		ta, tb := xa.(time.Time), xb.(time.Time)
		switch true {
		case ta.After(tb):
			return 1
		case ta.Equal(tb):
			return 0
		default:
			return -1
		}
	case FieldTypeBoolean:
		ba, bb := xa.(bool), xb.(bool)
		switch true {
		case ba == bb:
			return 0
		case !ba && bb:
			return -1
		default:
			return 1
		}
	case FieldTypeInt256:
		return xa.(Int256).Cmp(xb.(Int256))
	case FieldTypeInt128:
		return xa.(Int128).Cmp(xb.(Int128))
	case FieldTypeInt64:
		ia, ib := xa.(int64), xb.(int64)
		switch true {
		case ia < ib:
			return -1
		case ia == ib:
			return 0
		default:
			return 1
		}
	case FieldTypeInt32:
		ia, ib := xa.(int32), xb.(int32)
		switch true {
		case ia < ib:
			return -1
		case ia == ib:
			return 0
		default:
			return 1
		}
	case FieldTypeInt16:
		ia, ib := xa.(int16), xb.(int16)
		switch true {
		case ia < ib:
			return -1
		case ia == ib:
			return 0
		default:
			return 1
		}
	case FieldTypeInt8:
		ia, ib := xa.(int8), xb.(int8)
		switch true {
		case ia < ib:
			return -1
		case ia == ib:
			return 0
		default:
			return 1
		}
	case FieldTypeUint64:
		ua, ub := xa.(uint64), xb.(uint64)
		switch true {
		case ua < ub:
			return -1
		case ua == ub:
			return 0
		default:
			return 1
		}
	case FieldTypeUint32:
		ua, ub := xa.(uint32), xb.(uint32)
		switch true {
		case ua < ub:
			return -1
		case ua == ub:
			return 0
		default:
			return 1
		}
	case FieldTypeUint16:
		ua, ub := xa.(uint16), xb.(uint16)
		switch true {
		case ua < ub:
			return -1
		case ua == ub:
			return 0
		default:
			return 1
		}
	case FieldTypeUint8:
		ua, ub := xa.(uint8), xb.(uint8)
		switch true {
		case ua < ub:
			return -1
		case ua == ub:
			return 0
		default:
			return 1
		}
	case FieldTypeFloat64:
		fa, fb := xa.(float64), xb.(float64)
		switch true {
		case fa < fb:
			return -1
		case fa == fb:
			return 0
		default:
			return 1
		}
	case FieldTypeFloat32:
		fa, fb := xa.(float32), xb.(float32)
		switch true {
		case fa < fb:
			return -1
		case fa == fb:
			return 0
		default:
			return 1
		}
	case FieldTypeDecimal32:
		return xa.(decimal.Decimal32).Cmp(xb.(decimal.Decimal32))

	case FieldTypeDecimal64:
		return xa.(decimal.Decimal64).Cmp(xb.(decimal.Decimal64))

	case FieldTypeDecimal128:
		return xa.(decimal.Decimal128).Cmp(xb.(decimal.Decimal128))

	case FieldTypeDecimal256:
		return xa.(decimal.Decimal256).Cmp(xb.(decimal.Decimal256))

	default:
		return -1
	}
}

func (t FieldType) EqualPacksAt(p1 *Package, i1, n1 int, p2 *Package, i2, n2 int) bool {
	switch t {
	case FieldTypeBytes:
		v1, _ := p1.BytesAt(i1, n1)
		v2, _ := p2.BytesAt(i2, n2)
		return bytes.Equal(v1, v2)
	case FieldTypeString:
		v1, _ := p1.StringAt(i1, n1)
		v2, _ := p2.StringAt(i2, n2)
		return v1 == v2
	case FieldTypeDatetime:
		v1, _ := p1.TimeAt(i1, n1)
		v2, _ := p2.TimeAt(i2, n2)
		return v1.Equal(v2)
	case FieldTypeBoolean:
		v1, _ := p1.BoolAt(i1, n1)
		v2, _ := p2.BoolAt(i2, n2)
		return v1 == v2
	case FieldTypeInt256:
		v1, _ := p1.Int256At(i1, n1)
		v2, _ := p2.Int256At(i2, n2)
		return v1.Eq(v2)
	case FieldTypeInt128:
		v1, _ := p1.Int128At(i1, n1)
		v2, _ := p2.Int128At(i2, n2)
		return v1.Eq(v2)
	case FieldTypeInt64:
		v1, _ := p1.Int64At(i1, n1)
		v2, _ := p2.Int64At(i2, n2)
		return v1 == v2
	case FieldTypeInt32:
		v1, _ := p1.Int32At(i1, n1)
		v2, _ := p2.Int32At(i2, n2)
		return v1 == v2
	case FieldTypeInt16:
		v1, _ := p1.Int16At(i1, n1)
		v2, _ := p2.Int16At(i2, n2)
		return v1 == v2
	case FieldTypeInt8:
		v1, _ := p1.Int8At(i1, n1)
		v2, _ := p2.Int8At(i2, n2)
		return v1 == v2
	case FieldTypeUint64:
		v1, _ := p1.Uint64At(i1, n1)
		v2, _ := p2.Uint64At(i2, n2)
		return v1 == v2
	case FieldTypeUint32:
		v1, _ := p1.Uint32At(i1, n1)
		v2, _ := p2.Uint32At(i2, n2)
		return v1 == v2
	case FieldTypeUint16:
		v1, _ := p1.Uint16At(i1, n1)
		v2, _ := p2.Uint16At(i2, n2)
		return v1 == v2
	case FieldTypeUint8:
		v1, _ := p1.Uint8At(i1, n1)
		v2, _ := p2.Uint8At(i2, n2)
		return v1 == v2
	case FieldTypeFloat64:
		v1, _ := p1.Float64At(i1, n1)
		v2, _ := p2.Float64At(i2, n2)
		return v1 == v2
	case FieldTypeFloat32:
		v1, _ := p1.Float32At(i1, n1)
		v2, _ := p2.Float32At(i2, n2)
		return v1 == v2
	case FieldTypeDecimal32:
		// Note: assumes both packs have same scale factor
		v1, _ := p1.Decimal32At(i1, n1)
		v2, _ := p2.Decimal32At(i2, n2)
		return v1.Eq(v2)
	case FieldTypeDecimal64:
		// Note: assumes both packs have same scale factor
		v1, _ := p1.Decimal64At(i1, n1)
		v2, _ := p2.Decimal64At(i2, n2)
		return v1.Eq(v2)
	case FieldTypeDecimal128:
		// Note: assumes both packs have same scale factor
		v1, _ := p1.Decimal128At(i1, n1)
		v2, _ := p2.Decimal128At(i2, n2)
		return v1.Eq(v2)
	case FieldTypeDecimal256:
		// Note: assumes both packs have same scale factor
		v1, _ := p1.Decimal256At(i1, n1)
		v2, _ := p2.Decimal256At(i2, n2)
		return v1.Eq(v2)
	default:
		return false
	}
}

func (t FieldType) BuildBloomFilter(b block.Block, cardinality uint32, factor int) *bloom.Filter {
	if cardinality <= 0 || factor <= 0 {
		return nil
	}
	m := int(cardinality) * factor * 8 // unit is bits
	flt := bloom.NewFilter(m)
	switch t {
	case FieldTypeBytes, FieldTypeString:
		for i := 0; i < b.Len(); i++ {
			flt.Add(b.Elem(i).([]byte))
		}
	case FieldTypeDatetime:
		flt.AddManyInt64(b.Slice().([]int64))
	case FieldTypeBoolean:
		var (
			count int
			last  bool
		)
		for _, v := range b.Slice().([]bool) {
			if count == 2 {
				break
			}
			if v {
				flt.Add([]byte{1})
				if count == 0 || !last {
					count++
				}
			} else {
				flt.Add([]byte{0})
				if count == 0 || last {
					count++
				}
			}
		}
	case FieldTypeInt256, FieldTypeDecimal256:
		for i := 0; i < b.Len(); i++ {
			buf := b.Elem(i).(Int256).Bytes32()
			flt.Add(buf[:])
		}
	case FieldTypeInt128, FieldTypeDecimal128:
		for i := 0; i < b.Len(); i++ {
			buf := b.Elem(i).(Int128).Bytes16()
			flt.Add(buf[:])
		}
	case FieldTypeInt64, FieldTypeDecimal64:
		flt.AddManyInt64(b.Slice().([]int64))
	case FieldTypeInt32, FieldTypeDecimal32:
		flt.AddManyInt32(b.Slice().([]int32))
	case FieldTypeInt16:
		flt.AddManyInt16(b.Slice().([]int16))
	case FieldTypeInt8:
		for _, v := range b.Slice().([]int8) {
			flt.Add([]byte{byte(v)})
		}
	case FieldTypeUint64:
		flt.AddManyUint64(b.Slice().([]uint64))
	case FieldTypeUint32:
		flt.AddManyUint32(b.Slice().([]uint32))
	case FieldTypeUint16:
		flt.AddManyUint16(b.Slice().([]uint16))
	case FieldTypeUint8:
		for _, v := range b.Slice().([]uint8) {
			flt.Add([]byte{v})
		}
	case FieldTypeFloat64:
		flt.AddManyFloat64(b.Slice().([]float64))
	case FieldTypeFloat32:
		flt.AddManyFloat32(b.Slice().([]float32))
	default:
		return nil
	}
	return flt
}

// Hash produces a hash value compatible with bloom filters.
func (t FieldType) Hash(val interface{}) [2]uint32 {
	if val == nil {
		return [2]uint32{}
	}
	switch t {
	case FieldTypeBytes:
		return bloom.Hash(val.([]byte))
	case FieldTypeString:
		if s, ok := val.(string); ok {
			return bloom.Hash(compress.UnsafeGetBytes(s))
		}
		return bloom.Hash(val.([]byte))
	case FieldTypeDatetime:
		if i, ok := val.(int64); ok {
			return bloom.HashInt64(i)
		} else {
			return bloom.HashInt64(val.(time.Time).UnixNano())
		}
	case FieldTypeBoolean:
		if v := val.(bool); v {
			return bloom.Hash([]byte{1})
		} else {
			return bloom.Hash([]byte{0})
		}
	case FieldTypeInt256:
		buf := val.(Int256).Bytes32()
		return bloom.Hash(buf[:])
	case FieldTypeDecimal256:
		if i, ok := val.(Int256); ok {
			buf := i.Bytes32()
			return bloom.Hash(buf[:])
		} else {
			buf := val.(decimal.Decimal256).Int256().Bytes32()
			return bloom.Hash(buf[:])
		}
	case FieldTypeInt128:
		buf := val.(Int128).Bytes16()
		return bloom.Hash(buf[:])
	case FieldTypeDecimal128:
		if i, ok := val.(Int128); ok {
			buf := i.Bytes16()
			return bloom.Hash(buf[:])
		} else {
			buf := val.(decimal.Decimal128).Int128().Bytes16()
			return bloom.Hash(buf[:])
		}
	case FieldTypeInt64:
		return bloom.HashInt64(val.(int64))
	case FieldTypeDecimal64:
		if i, ok := val.(int64); ok {
			return bloom.HashInt64(i)
		} else {
			return bloom.HashInt64(val.(decimal.Decimal64).Int64())
		}
	case FieldTypeInt32:
		return bloom.HashInt32(val.(int32))
	case FieldTypeDecimal32:
		if i, ok := val.(int32); ok {
			return bloom.HashInt32(i)
		} else {
			return bloom.HashInt32(val.(decimal.Decimal32).Int32())
		}
	case FieldTypeInt16:
		return bloom.HashInt16(val.(int16))
	case FieldTypeInt8:
		return bloom.Hash([]byte{byte(val.(int8))})
	case FieldTypeUint64:
		return bloom.HashUint64(val.(uint64))
	case FieldTypeUint32:
		return bloom.HashUint32(val.(uint32))
	case FieldTypeUint16:
		return bloom.HashUint16(val.(uint16))
	case FieldTypeUint8:
		return bloom.Hash([]byte{byte(val.(uint8))})
	case FieldTypeFloat64:
		return bloom.HashFloat64(val.(float64))
	case FieldTypeFloat32:
		return bloom.HashFloat32(val.(float32))
	default:
		return [2]uint32{}
	}
}

func (t FieldType) EstimateCardinality(b block.Block, precision uint) uint32 {
	// shortcut for empty and very small packs
	switch b.Len() {
	case 0:
		return 0
	case 1:
		return 1
	case 2:
		min, max := b.MinMax()
		if t.Equal(min, max) {
			return 1
		}
		return 2
	}

	filter := loglogbeta.NewFilterWithPrecision(uint32(precision))
	var buf [8]byte
	switch t {
	case FieldTypeBytes:
		for i := 0; i < b.Len(); i++ {
			filter.Add(b.Elem(i).([]byte))
		}
	case FieldTypeString:
		for i := 0; i < b.Len(); i++ {
			filter.Add(compress.UnsafeGetBytes(b.Elem(i).(string)))
		}
	case FieldTypeDatetime:
		filter.AddManyInt64(b.Slice().([]int64))
	case FieldTypeBoolean:
		var (
			count int
			last  bool
		)
		for _, v := range b.Slice().([]bool) {
			if count == 2 {
				break
			}
			if v {
				filter.Add([]byte{1})
				if count == 0 || !last {
					count++
				}
			} else {
				filter.Add([]byte{0})
				if count == 0 || last {
					count++
				}
			}
		}
	case FieldTypeInt256, FieldTypeDecimal256:
		for i := 0; i < b.Len(); i++ {
			buf := b.Elem(i).(Int256).Bytes32()
			filter.Add(buf[:])
		}
	case FieldTypeInt128, FieldTypeDecimal128:
		for i := 0; i < b.Len(); i++ {
			buf := b.Elem(i).(Int128).Bytes16()
			filter.Add(buf[:])
		}
	case FieldTypeInt64, FieldTypeDecimal64:
		filter.AddManyInt64(b.Slice().([]int64))
	case FieldTypeInt32, FieldTypeDecimal32:
		filter.AddManyInt32(b.Slice().([]int32))
	case FieldTypeInt16:
		for _, v := range b.Slice().([]int16) {
			bigEndian.PutUint16(buf[:], uint16(v))
			filter.Add(buf[:2])
		}
	case FieldTypeInt8:
		for _, v := range b.Slice().([]int8) {
			filter.Add([]byte{byte(v)})
		}
	case FieldTypeUint64:
		filter.AddManyUint64(b.Slice().([]uint64))
	case FieldTypeUint32:
		filter.AddManyUint32(b.Slice().([]uint32))
	case FieldTypeUint16:
		for _, v := range b.Slice().([]uint16) {
			bigEndian.PutUint16(buf[:], v)
			filter.Add(buf[:2])
		}
	case FieldTypeUint8:
		for _, v := range b.Slice().([]uint8) {
			filter.Add([]byte{byte(v)})
		}
	case FieldTypeFloat64:
		for _, v := range b.Slice().([]float64) {
			bigEndian.PutUint64(buf[:], math.Float64bits(v))
			filter.Add(buf[:])
		}
	case FieldTypeFloat32:
		for _, v := range b.Slice().([]float32) {
			bigEndian.PutUint32(buf[:], math.Float32bits(v))
			filter.Add(buf[:4])
		}
	}
	return util.MinU32(uint32(b.Len()), uint32(filter.Cardinality()))
}
