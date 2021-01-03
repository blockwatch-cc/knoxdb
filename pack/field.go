// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
package pack

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/util"

	. "blockwatch.cc/knoxdb/encoding/decimal"
	. "blockwatch.cc/knoxdb/vec"
)

type FieldFlags int

const (
	FlagPrimary FieldFlags = 1 << iota
	FlagIndexed
	FlagCompressSnappy
	FlagCompressLZ4
	FlagMode = FlagPrimary | FlagIndexed | FlagCompressSnappy | FlagCompressLZ4
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

func (f FieldFlags) String() string {
	s := make([]string, 0)
	for i := FlagPrimary; i <= FlagCompressLZ4; i <<= 1 {
		if f&i > 0 {
			switch i {
			case FlagPrimary:
				s = append(s, "primary")
			case FlagIndexed:
				s = append(s, "indexed")
			case FlagCompressSnappy:
				s = append(s, "snappy")
			case FlagCompressLZ4:
				s = append(s, "lz4")
			}
		}
	}
	return strings.Join(s, ",")
}

type FieldType string

const (
	FieldTypeUndefined  FieldType = ""
	FieldTypeBytes      FieldType = "bytes"      // BlockBytes
	FieldTypeString     FieldType = "string"     // BlockString
	FieldTypeDatetime   FieldType = "datetime"   // BlockTime
	FieldTypeBoolean    FieldType = "boolean"    // BlockBool
	FieldTypeFloat64    FieldType = "float64"    // BlockFloat64
	FieldTypeFloat32    FieldType = "float32"    // BlockFloat32
	FieldTypeInt256     FieldType = "int256"     // BlockInt256
	FieldTypeInt128     FieldType = "int128"     // BlockInt128
	FieldTypeInt64      FieldType = "int64"      // BlockInt64
	FieldTypeInt32      FieldType = "int32"      // BlockInt32
	FieldTypeInt16      FieldType = "int16"      // BlockInt16
	FieldTypeInt8       FieldType = "int8"       // BlockInt8
	FieldTypeUint64     FieldType = "uint64"     // BlockUint64
	FieldTypeUint32     FieldType = "uint32"     // BlockUint32
	FieldTypeUint16     FieldType = "uint16"     // BlockUint16
	FieldTypeUint8      FieldType = "uint8"      // BlockUint8
	FieldTypeDecimal256 FieldType = "decimal256" // BlockDecimal256
	FieldTypeDecimal128 FieldType = "decimal128" // BlockDecimal128
	FieldTypeDecimal64  FieldType = "decimal64"  // BlockDecimal64
	FieldTypeDecimal32  FieldType = "decimal32"  // BlockDecimal32

	// TODO: extend pack encoders and block types
	// FieldTypeDate                   = "date" // BlockDate (unix second / 24*3600)
)

type Field struct {
	Index int        `json:"index"`
	Name  string     `json:"name"`
	Alias string     `json:"alias"`
	Type  FieldType  `json:"type"`
	Flags FieldFlags `json:"flags"` // primary, indexed, compression
	Scale int        `json:"scale"` // fixed point scale
}

func (f Field) IsValid() bool {
	return f.Index >= 0 && f.Type.IsValid()
}

func (f Field) NewBlock(sz int) *block.Block {
	return block.NewBlock(f.Type.BlockType(), f.Flags.Compression(), sz)
}

type FieldList []Field

func (l FieldList) Key() string {
	s := make([]string, len(l))
	for i, v := range l {
		s[i] = v.Name
	}
	return strings.Join(s, "")
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

func (l FieldList) Find(name string) Field {
	for _, v := range l {
		if v.Name == name {
			return v
		}
	}
	return Field{Index: -1}
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

func (l FieldList) Add(field Field) FieldList {
	return append(l, field)
}

func (l FieldList) AddUnique(fields ...Field) FieldList {
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

func (l FieldList) Pk() Field {
	for _, v := range l {
		if v.Flags&FlagPrimary > 0 {
			return v
		}
	}
	return Field{Index: -1}
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
		if v.Name == name {
			return true
		}
	}
	return false
}

func (l FieldList) MergeUnique(fields ...Field) FieldList {
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
		fields[i].Name = finfo.name
		fields[i].Alias = finfo.alias
		fields[i].Index = i
		fields[i].Flags = finfo.flags
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
			if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				// log.Debugf("Slice type field %s type %s implements binary marshaler", finfo.name, f.Type().String())
				fields[i].Type = FieldTypeBytes
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
				if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
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
				if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
					log.Debugf("Array type field %s type %s implements binary marshaler", finfo.name, f.Type().String())
					fields[i].Type = FieldTypeBytes
				} else {
					return nil, fmt.Errorf("pack: unsupported array type %s", f.Type().String())
				}
			}
		default:
			return nil, fmt.Errorf("pack: unsupported type %s (%v) for field %s",
				f.Type().String(), f.Kind(), finfo.name)
		}
	}
	return fields, nil
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
	case FieldTypeBytes:
		return block.BlockBytes
	case FieldTypeString:
		return block.BlockString
	case FieldTypeDatetime:
		return block.BlockTime
	case FieldTypeBoolean:
		return block.BlockBool
	case FieldTypeFloat64:
		return block.BlockFloat64
	case FieldTypeFloat32:
		return block.BlockFloat32
	case FieldTypeInt128, FieldTypeDecimal128:
		return block.BlockInt128
	case FieldTypeInt256, FieldTypeDecimal256:
		return block.BlockInt256
	case FieldTypeInt64, FieldTypeDecimal64:
		return block.BlockInt64
	case FieldTypeInt32, FieldTypeDecimal32:
		return block.BlockInt32
	case FieldTypeInt16:
		return block.BlockInt16
	case FieldTypeInt8:
		return block.BlockInt8
	case FieldTypeUint64:
		return block.BlockUint64
	case FieldTypeUint32:
		return block.BlockUint32
	case FieldTypeUint16:
		return block.BlockUint16
	case FieldTypeUint8:
		return block.BlockUint8
	default:
		return block.BlockBytes
	}
}

func (t FieldType) IsValid() bool {
	return t != FieldTypeUndefined
}

func (r FieldType) MarshalText() ([]byte, error) {
	return []byte(r), nil
}

func (t *FieldType) UnmarshalText(data []byte) error {
	typ := ParseFieldType(string(data))
	if !typ.IsValid() {
		return fmt.Errorf("pack: invalid field type '%s'", string(data))
	}
	*t = typ
	return nil
}

func (t FieldType) ParseAs(s string, f Field) (interface{}, error) {
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
		d, err := ParseDecimal32(s)
		if err != nil {
			return nil, err
		}
		return d.Quantize(f.Scale), nil
	case FieldTypeDecimal64:
		d, err := ParseDecimal64(s)
		if err != nil {
			return nil, err
		}
		return d.Quantize(f.Scale), nil
	case FieldTypeDecimal128:
		d, err := ParseDecimal128(s)
		if err != nil {
			return nil, err
		}
		return d.Quantize(f.Scale), nil
	case FieldTypeDecimal256:
		d, err := ParseDecimal256(s)
		if err != nil {
			return nil, err
		}
		return d.Quantize(f.Scale), nil
	default:
		return nil, fmt.Errorf("unsupported field type '%s'", t)
	}
}

func (t FieldType) ParseSliceAs(s string, f Field) (interface{}, error) {
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
		slice := make([]Decimal32, len(vv))
		for i, v := range vv {
			d, err := ParseDecimal32(v)
			if err != nil {
				return nil, err
			}
			slice[i] = d.Quantize(f.Scale)
		}
		return slice, nil
	case FieldTypeDecimal64:
		slice := make([]Decimal64, len(vv))
		for i, v := range vv {
			d, err := ParseDecimal64(v)
			if err != nil {
				return nil, err
			}
			slice[i] = d.Quantize(f.Scale)
		}
		return slice, nil
	case FieldTypeDecimal128:
		slice := make([]Decimal128, len(vv))
		for i, v := range vv {
			d, err := ParseDecimal128(v)
			if err != nil {
				return nil, err
			}
			slice[i] = d.Quantize(f.Scale)
		}
		return slice, nil
	case FieldTypeDecimal256:
		slice := make([]Decimal256, len(vv))
		for i, v := range vv {
			d, err := ParseDecimal256(v)
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

func (t FieldType) ToString(val interface{}, f Field) string {
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
			var d Decimal32
			for _, vv := range v {
				_ = d.SetInt64(int64(vv), f.Scale)
				ss = append(ss, d.String())
			}
		} else if v, ok := val.([]Decimal32); ok {
			for _, vv := range v {
				ss = append(ss, vv.String())
			}
		}
	case FieldTypeDecimal64:
		if v, ok := val.([]int64); ok {
			var d Decimal64
			for _, vv := range v {
				_ = d.SetInt64(vv, f.Scale)
				ss = append(ss, d.String())
			}
		} else if v, ok := val.([]Decimal64); ok {
			for _, vv := range v {
				ss = append(ss, vv.String())
			}
		}
	case FieldTypeDecimal128:
		if v, ok := val.([]Int128); ok {
			var d Decimal128
			for _, vv := range v {
				_ = d.SetInt128(vv, f.Scale)
				ss = append(ss, d.String())
			}
		} else if v, ok := val.([]Decimal128); ok {
			for _, vv := range v {
				ss = append(ss, vv.String())
			}
		}
	case FieldTypeDecimal256:
		if v, ok := val.([]Int256); ok {
			var d Decimal256
			for _, vv := range v {
				_ = d.SetInt256(vv, f.Scale)
				ss = append(ss, d.String())
			}
		} else if v, ok := val.([]Decimal256); ok {
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
		return bytes.Compare(xa.([]byte), xb.([]byte)) == 0
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
		return xa.(Decimal32).Eq(xb.(Decimal32))
	case FieldTypeDecimal64:
		return xa.(Decimal64).Eq(xb.(Decimal64))
	case FieldTypeDecimal128:
		return xa.(Decimal128).Eq(xb.(Decimal128))
	case FieldTypeDecimal256:
		return xa.(Decimal256).Eq(xb.(Decimal256))
	default:
		return false
	}
}

func (t FieldType) EqualAt(pkg *Package, index, pos int, val interface{}) bool {
	switch t {
	case FieldTypeBytes:
		a, _ := pkg.BytesAt(index, pos)
		return bytes.Compare(a, val.([]byte)) == 0
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
		return a.Eq(val.(Decimal32))
	case FieldTypeDecimal64:
		a, _ := pkg.Decimal64At(index, pos)
		return a.Eq(val.(Decimal64))
	case FieldTypeDecimal128:
		a, _ := pkg.Decimal128At(index, pos)
		return a.Eq(val.(Decimal128))
	case FieldTypeDecimal256:
		a, _ := pkg.Decimal256At(index, pos)
		return a.Eq(val.(Decimal256))
	default:
		return false
	}
}

func (t FieldType) EqualSlice(slice, val interface{}, bits, mask *BitSet) *BitSet {
	switch t {
	case FieldTypeBytes:
		return MatchBytesEqual(slice.([][]byte), val.([]byte), bits, mask)
	case FieldTypeString:
		return MatchStringsEqual(slice.([]string), val.(string), bits, mask)
	case FieldTypeDatetime:
		return MatchInt64Equal(slice.([]int64), val.(time.Time).UnixNano(), bits, mask)
	case FieldTypeBoolean:
		return MatchBoolEqual(slice.([]bool), val.(bool), bits, mask)
	case FieldTypeInt256:
		return MatchInt256Equal(slice.([]Int256), val.(Int256), bits, mask)
	case FieldTypeInt128:
		return MatchInt128Equal(slice.([]Int128), val.(Int128), bits, mask)
	case FieldTypeInt64:
		return MatchInt64Equal(slice.([]int64), val.(int64), bits, mask)
	case FieldTypeInt32:
		return MatchInt32Equal(slice.([]int32), val.(int32), bits, mask)
	case FieldTypeInt16:
		return MatchInt16Equal(slice.([]int16), val.(int16), bits, mask)
	case FieldTypeInt8:
		return MatchInt8Equal(slice.([]int8), val.(int8), bits, mask)
	case FieldTypeUint64:
		return MatchUint64Equal(slice.([]uint64), val.(uint64), bits, mask)
	case FieldTypeUint32:
		return MatchUint32Equal(slice.([]uint32), val.(uint32), bits, mask)
	case FieldTypeUint16:
		return MatchUint16Equal(slice.([]uint16), val.(uint16), bits, mask)
	case FieldTypeUint8:
		return MatchUint8Equal(slice.([]uint8), val.(uint8), bits, mask)
	case FieldTypeFloat64:
		return MatchFloat64Equal(slice.([]float64), val.(float64), bits, mask)
	case FieldTypeFloat32:
		return MatchFloat32Equal(slice.([]float32), val.(float32), bits, mask)
	case FieldTypeDecimal256:
		return MatchInt256Equal(slice.(Decimal256Slice).Int256, val.(Decimal256).Int256(), bits, mask)
	case FieldTypeDecimal128:
		return MatchInt128Equal(slice.(Decimal128Slice).Int128, val.(Decimal128).Int128(), bits, mask)
	case FieldTypeDecimal64:
		return MatchInt64Equal(slice.(Decimal64Slice).Int64, val.(Decimal64).Int64(), bits, mask)
	case FieldTypeDecimal32:
		return MatchInt32Equal(slice.(Decimal32Slice).Int32, val.(Decimal32).Int32(), bits, mask)
	default:
		return bits
	}
}

func (t FieldType) EqualPacksAt(p1 *Package, i1, n1 int, p2 *Package, i2, n2 int) bool {
	switch t {
	case FieldTypeBytes:
		v1, _ := p1.BytesAt(i1, n1)
		v2, _ := p2.BytesAt(i2, n2)
		return bytes.Compare(v1, v2) == 0
	case FieldTypeString:
		v1, _ := p1.StringAt(i1, n1)
		v2, _ := p2.StringAt(i2, n2)
		return strings.Compare(v1, v2) == 0
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

func (t FieldType) NotEqualSlice(slice, val interface{}, bits, mask *BitSet) *BitSet {
	switch t {
	case FieldTypeBytes:
		return MatchBytesNotEqual(slice.([][]byte), val.([]byte), bits, mask)
	case FieldTypeString:
		return MatchStringsNotEqual(slice.([]string), val.(string), bits, mask)
	case FieldTypeDatetime:
		return MatchInt64NotEqual(slice.([]int64), val.(time.Time).UnixNano(), bits, mask)
	case FieldTypeBoolean:
		return MatchBoolNotEqual(slice.([]bool), val.(bool), bits, mask)
	case FieldTypeInt256:
		return MatchInt256NotEqual(slice.([]Int256), val.(Int256), bits, mask)
	case FieldTypeInt128:
		return MatchInt128NotEqual(slice.([]Int128), val.(Int128), bits, mask)
	case FieldTypeInt64:
		return MatchInt64NotEqual(slice.([]int64), val.(int64), bits, mask)
	case FieldTypeInt32:
		return MatchInt32NotEqual(slice.([]int32), val.(int32), bits, mask)
	case FieldTypeInt16:
		return MatchInt16NotEqual(slice.([]int16), val.(int16), bits, mask)
	case FieldTypeInt8:
		return MatchInt8NotEqual(slice.([]int8), val.(int8), bits, mask)
	case FieldTypeUint64:
		return MatchUint64NotEqual(slice.([]uint64), val.(uint64), bits, mask)
	case FieldTypeUint32:
		return MatchUint32NotEqual(slice.([]uint32), val.(uint32), bits, mask)
	case FieldTypeUint16:
		return MatchUint16NotEqual(slice.([]uint16), val.(uint16), bits, mask)
	case FieldTypeUint8:
		return MatchUint8NotEqual(slice.([]uint8), val.(uint8), bits, mask)
	case FieldTypeFloat64:
		return MatchFloat64NotEqual(slice.([]float64), val.(float64), bits, mask)
	case FieldTypeFloat32:
		return MatchFloat32NotEqual(slice.([]float32), val.(float32), bits, mask)
	case FieldTypeDecimal256:
		return MatchInt256NotEqual(slice.(Decimal256Slice).Int256, val.(Decimal256).Int256(), bits, mask)
	case FieldTypeDecimal128:
		return MatchInt128NotEqual(slice.(Decimal128Slice).Int128, val.(Decimal128).Int128(), bits, mask)
	case FieldTypeDecimal64:
		return MatchInt64NotEqual(slice.(Decimal64Slice).Int64, val.(Decimal64).Int64(), bits, mask)
	case FieldTypeDecimal32:
		return MatchInt32NotEqual(slice.(Decimal32Slice).Int32, val.(Decimal32).Int32(), bits, mask)
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

func (t FieldType) RegexpSlice(slice interface{}, re string, bits, mask *BitSet) *BitSet {
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
		for i, v := range slice.([]string) {
			// skip masked values
			if mask != nil && !mask.IsSet(i) {
				continue
			}
			if match, _ := regexp.MatchString(rematch, v); match {
				bits.Set(i)
			}
		}
		return bits
	case FieldTypeDatetime:
		rematch := strings.Replace(re, "*", ".*", -1)
		for i, v := range slice.([]int64) {
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
		return xa.(Decimal32).Gt(xb.(Decimal32))
	case FieldTypeDecimal64:
		return xa.(Decimal64).Gt(xb.(Decimal64))
	case FieldTypeDecimal128:
		return xa.(Decimal128).Gt(xb.(Decimal128))
	case FieldTypeDecimal256:
		return xa.(Decimal256).Gt(xb.(Decimal256))
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
		return a.Gt(val.(Decimal32))
	case FieldTypeDecimal64:
		a, _ := pkg.Decimal64At(index, pos)
		return a.Gt(val.(Decimal64))
	case FieldTypeDecimal128:
		a, _ := pkg.Decimal128At(index, pos)
		return a.Gt(val.(Decimal128))
	case FieldTypeDecimal256:
		a, _ := pkg.Decimal256At(index, pos)
		return a.Gt(val.(Decimal256))
	default:
		return false
	}
}

func (t FieldType) GtSlice(slice, val interface{}, bits, mask *BitSet) *BitSet {
	switch t {
	case FieldTypeBytes:
		return MatchBytesGreaterThan(slice.([][]byte), val.([]byte), bits, mask)
	case FieldTypeString:
		return MatchStringsGreaterThan(slice.([]string), val.(string), bits, mask)
	case FieldTypeDatetime:
		return MatchInt64GreaterThan(slice.([]int64), val.(time.Time).UnixNano(), bits, mask)
	case FieldTypeBoolean:
		return MatchBoolGreaterThan(slice.([]bool), val.(bool), bits, mask)
	case FieldTypeInt256:
		return MatchInt256GreaterThan(slice.([]Int256), val.(Int256), bits, mask)
	case FieldTypeInt128:
		return MatchInt128GreaterThan(slice.([]Int128), val.(Int128), bits, mask)
	case FieldTypeInt64:
		return MatchInt64GreaterThan(slice.([]int64), val.(int64), bits, mask)
	case FieldTypeInt32:
		return MatchInt32GreaterThan(slice.([]int32), val.(int32), bits, mask)
	case FieldTypeInt16:
		return MatchInt16GreaterThan(slice.([]int16), val.(int16), bits, mask)
	case FieldTypeInt8:
		return MatchInt8GreaterThan(slice.([]int8), val.(int8), bits, mask)
	case FieldTypeUint64:
		return MatchUint64GreaterThan(slice.([]uint64), val.(uint64), bits, mask)
	case FieldTypeUint32:
		return MatchUint32GreaterThan(slice.([]uint32), val.(uint32), bits, mask)
	case FieldTypeUint16:
		return MatchUint16GreaterThan(slice.([]uint16), val.(uint16), bits, mask)
	case FieldTypeUint8:
		return MatchUint8GreaterThan(slice.([]uint8), val.(uint8), bits, mask)
	case FieldTypeFloat64:
		return MatchFloat64GreaterThan(slice.([]float64), val.(float64), bits, mask)
	case FieldTypeFloat32:
		return MatchFloat32GreaterThan(slice.([]float32), val.(float32), bits, mask)
	case FieldTypeDecimal32:
		return MatchInt32GreaterThan(slice.(Decimal32Slice).Int32, val.(Decimal32).Int32(), bits, mask)
	case FieldTypeDecimal64:
		return MatchInt64GreaterThan(slice.(Decimal64Slice).Int64, val.(Decimal64).Int64(), bits, mask)
	case FieldTypeDecimal128:
		return MatchInt128GreaterThan(slice.(Decimal128Slice).Int128, val.(Decimal128).Int128(), bits, mask)
	case FieldTypeDecimal256:
		return MatchInt256GreaterThan(slice.(Decimal256Slice).Int256, val.(Decimal256).Int256(), bits, mask)
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
		return xa.(Decimal32).Gte(xb.(Decimal32))
	case FieldTypeDecimal64:
		return xa.(Decimal64).Gte(xb.(Decimal64))
	case FieldTypeDecimal128:
		return xa.(Decimal128).Gte(xb.(Decimal128))
	case FieldTypeDecimal256:
		return xa.(Decimal256).Gte(xb.(Decimal256))
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
		return a.Gte(val.(Decimal32))
	case FieldTypeDecimal64:
		a, _ := pkg.Decimal64At(index, pos)
		return a.Gte(val.(Decimal64))
	case FieldTypeDecimal128:
		a, _ := pkg.Decimal128At(index, pos)
		return a.Gte(val.(Decimal128))
	case FieldTypeDecimal256:
		a, _ := pkg.Decimal256At(index, pos)
		return a.Gte(val.(Decimal256))
	default:
		return false
	}
}

func (t FieldType) GteSlice(slice, val interface{}, bits, mask *BitSet) *BitSet {
	switch t {
	case FieldTypeBytes:
		return MatchBytesGreaterThanEqual(slice.([][]byte), val.([]byte), bits, mask)
	case FieldTypeString:
		return MatchStringsGreaterThanEqual(slice.([]string), val.(string), bits, mask)
	case FieldTypeDatetime:
		return MatchInt64GreaterThanEqual(slice.([]int64), val.(time.Time).UnixNano(), bits, mask)
	case FieldTypeBoolean:
		return MatchBoolGreaterThanEqual(slice.([]bool), val.(bool), bits, mask)
	case FieldTypeInt256:
		return MatchInt256GreaterThanEqual(slice.([]Int256), val.(Int256), bits, mask)
	case FieldTypeInt128:
		return MatchInt128GreaterThanEqual(slice.([]Int128), val.(Int128), bits, mask)
	case FieldTypeInt64:
		return MatchInt64GreaterThanEqual(slice.([]int64), val.(int64), bits, mask)
	case FieldTypeInt32:
		return MatchInt32GreaterThanEqual(slice.([]int32), val.(int32), bits, mask)
	case FieldTypeInt16:
		return MatchInt16GreaterThanEqual(slice.([]int16), val.(int16), bits, mask)
	case FieldTypeInt8:
		return MatchInt8GreaterThanEqual(slice.([]int8), val.(int8), bits, mask)
	case FieldTypeUint64:
		return MatchUint64GreaterThanEqual(slice.([]uint64), val.(uint64), bits, mask)
	case FieldTypeUint32:
		return MatchUint32GreaterThanEqual(slice.([]uint32), val.(uint32), bits, mask)
	case FieldTypeUint16:
		return MatchUint16GreaterThanEqual(slice.([]uint16), val.(uint16), bits, mask)
	case FieldTypeUint8:
		return MatchUint8GreaterThanEqual(slice.([]uint8), val.(uint8), bits, mask)
	case FieldTypeFloat64:
		return MatchFloat64GreaterThanEqual(slice.([]float64), val.(float64), bits, mask)
	case FieldTypeFloat32:
		return MatchFloat32GreaterThanEqual(slice.([]float32), val.(float32), bits, mask)
	case FieldTypeDecimal32:
		return MatchInt32GreaterThanEqual(slice.(Decimal32Slice).Int32, val.(Decimal32).Int32(), bits, mask)
	case FieldTypeDecimal64:
		return MatchInt64GreaterThanEqual(slice.(Decimal64Slice).Int64, val.(Decimal64).Int64(), bits, mask)
	case FieldTypeDecimal128:
		return MatchInt128GreaterThanEqual(slice.(Decimal128Slice).Int128, val.(Decimal128).Int128(), bits, mask)
	case FieldTypeDecimal256:
		return MatchInt256GreaterThanEqual(slice.(Decimal256Slice).Int256, val.(Decimal256).Int256(), bits, mask)
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
		return xa.(Decimal32).Lt(xb.(Decimal32))
	case FieldTypeDecimal64:
		return xa.(Decimal64).Lt(xb.(Decimal64))
	case FieldTypeDecimal128:
		return xa.(Decimal128).Lt(xb.(Decimal128))
	case FieldTypeDecimal256:
		return xa.(Decimal256).Lt(xb.(Decimal256))
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
		return a.Lt(val.(Decimal32))
	case FieldTypeDecimal64:
		a, _ := pkg.Decimal64At(index, pos)
		return a.Lt(val.(Decimal64))
	case FieldTypeDecimal128:
		a, _ := pkg.Decimal128At(index, pos)
		return a.Lt(val.(Decimal128))
	case FieldTypeDecimal256:
		a, _ := pkg.Decimal256At(index, pos)
		return a.Lt(val.(Decimal256))
	default:
		return false
	}
}

func (t FieldType) LtSlice(slice, val interface{}, bits, mask *BitSet) *BitSet {
	switch t {
	case FieldTypeBytes:
		return MatchBytesLessThan(slice.([][]byte), val.([]byte), bits, mask)
	case FieldTypeString:
		return MatchStringsLessThan(slice.([]string), val.(string), bits, mask)
	case FieldTypeDatetime:
		return MatchInt64LessThan(slice.([]int64), val.(time.Time).UnixNano(), bits, mask)
	case FieldTypeBoolean:
		return MatchBoolLessThan(slice.([]bool), val.(bool), bits, mask)
	case FieldTypeInt256:
		return MatchInt256LessThan(slice.([]Int256), val.(Int256), bits, mask)
	case FieldTypeInt128:
		return MatchInt128LessThan(slice.([]Int128), val.(Int128), bits, mask)
	case FieldTypeInt64:
		return MatchInt64LessThan(slice.([]int64), val.(int64), bits, mask)
	case FieldTypeInt32:
		return MatchInt32LessThan(slice.([]int32), val.(int32), bits, mask)
	case FieldTypeInt16:
		return MatchInt16LessThan(slice.([]int16), val.(int16), bits, mask)
	case FieldTypeInt8:
		return MatchInt8LessThan(slice.([]int8), val.(int8), bits, mask)
	case FieldTypeUint64:
		return MatchUint64LessThan(slice.([]uint64), val.(uint64), bits, mask)
	case FieldTypeUint32:
		return MatchUint32LessThan(slice.([]uint32), val.(uint32), bits, mask)
	case FieldTypeUint16:
		return MatchUint16LessThan(slice.([]uint16), val.(uint16), bits, mask)
	case FieldTypeUint8:
		return MatchUint8LessThan(slice.([]uint8), val.(uint8), bits, mask)
	case FieldTypeFloat64:
		return MatchFloat64LessThan(slice.([]float64), val.(float64), bits, mask)
	case FieldTypeFloat32:
		return MatchFloat32LessThan(slice.([]float32), val.(float32), bits, mask)
	case FieldTypeDecimal32:
		return MatchInt32LessThan(slice.(Decimal32Slice).Int32, val.(Decimal32).Int32(), bits, mask)
	case FieldTypeDecimal64:
		return MatchInt64LessThan(slice.(Decimal64Slice).Int64, val.(Decimal64).Int64(), bits, mask)
	case FieldTypeDecimal128:
		return MatchInt128LessThan(slice.(Decimal128Slice).Int128, val.(Decimal128).Int128(), bits, mask)
	case FieldTypeDecimal256:
		return MatchInt256LessThan(slice.(Decimal256Slice).Int256, val.(Decimal256).Int256(), bits, mask)
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
		return xa.(Decimal32).Lte(xb.(Decimal32))
	case FieldTypeDecimal64:
		return xa.(Decimal64).Lte(xb.(Decimal64))
	case FieldTypeDecimal128:
		return xa.(Decimal128).Lte(xb.(Decimal128))
	case FieldTypeDecimal256:
		return xa.(Decimal256).Lte(xb.(Decimal256))
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
		return a.Lte(val.(Decimal32))
	case FieldTypeDecimal64:
		a, _ := pkg.Decimal64At(index, pos)
		return a.Lte(val.(Decimal64))
	case FieldTypeDecimal128:
		a, _ := pkg.Decimal128At(index, pos)
		return a.Lte(val.(Decimal128))
	case FieldTypeDecimal256:
		a, _ := pkg.Decimal256At(index, pos)
		return a.Lte(val.(Decimal256))
	default:
		return false
	}
}

func (t FieldType) LteSlice(slice, val interface{}, bits, mask *BitSet) *BitSet {
	switch t {
	case FieldTypeBytes:
		return MatchBytesLessThanEqual(slice.([][]byte), val.([]byte), bits, mask)
	case FieldTypeString:
		return MatchStringsLessThanEqual(slice.([]string), val.(string), bits, mask)
	case FieldTypeDatetime:
		return MatchInt64LessThanEqual(slice.([]int64), val.(time.Time).UnixNano(), bits, mask)
	case FieldTypeBoolean:
		return MatchBoolLessThanEqual(slice.([]bool), val.(bool), bits, mask)
	case FieldTypeInt256:
		return MatchInt256LessThanEqual(slice.([]Int256), val.(Int256), bits, mask)
	case FieldTypeInt128:
		return MatchInt128LessThanEqual(slice.([]Int128), val.(Int128), bits, mask)
	case FieldTypeInt64:
		return MatchInt64LessThanEqual(slice.([]int64), val.(int64), bits, mask)
	case FieldTypeInt32:
		return MatchInt32LessThanEqual(slice.([]int32), val.(int32), bits, mask)
	case FieldTypeInt16:
		return MatchInt16LessThanEqual(slice.([]int16), val.(int16), bits, mask)
	case FieldTypeInt8:
		return MatchInt8LessThanEqual(slice.([]int8), val.(int8), bits, mask)
	case FieldTypeUint64:
		return MatchUint64LessThanEqual(slice.([]uint64), val.(uint64), bits, mask)
	case FieldTypeUint32:
		return MatchUint32LessThanEqual(slice.([]uint32), val.(uint32), bits, mask)
	case FieldTypeUint16:
		return MatchUint16LessThanEqual(slice.([]uint16), val.(uint16), bits, mask)
	case FieldTypeUint8:
		return MatchUint8LessThanEqual(slice.([]uint8), val.(uint8), bits, mask)
	case FieldTypeFloat64:
		return MatchFloat64LessThanEqual(slice.([]float64), val.(float64), bits, mask)
	case FieldTypeFloat32:
		return MatchFloat32LessThanEqual(slice.([]float32), val.(float32), bits, mask)
	case FieldTypeDecimal32:
		return MatchInt32LessThanEqual(slice.(Decimal32Slice).Int32, val.(Decimal32).Int32(), bits, mask)
	case FieldTypeDecimal64:
		return MatchInt64LessThanEqual(slice.(Decimal64Slice).Int64, val.(Decimal64).Int64(), bits, mask)
	case FieldTypeDecimal128:
		return MatchInt128LessThanEqual(slice.(Decimal128Slice).Int128, val.(Decimal128).Int128(), bits, mask)
	case FieldTypeDecimal256:
		return MatchInt256LessThanEqual(slice.(Decimal256Slice).Int256, val.(Decimal256).Int256(), bits, mask)
	default:
		return bits
	}
}

// first arg is value to compare, second is slice of value types from condition
func (t FieldType) In(v, in interface{}) bool {
	switch t {
	case FieldTypeBytes:
		val, list := v.([]byte), in.([][]byte)
		return ByteSlice(list).Contains(val)
	case FieldTypeString:
		val, list := v.(string), in.([]string)
		return StringSlice(list).Contains(val)
	case FieldTypeDatetime:
		val, list := v.(time.Time), in.([]time.Time)
		return TimeSlice(list).Contains(val)
	case FieldTypeBoolean:
		val, list := v.(bool), in.([]bool)
		return BoolSlice(list).Contains(val)
	case FieldTypeInt256:
		val, list := v.(Int256), in.([]Int256)
		return Int256Slice(list).Contains(val)
	case FieldTypeInt128:
		val, list := v.(Int128), in.([]Int128)
		return Int128Slice(list).Contains(val)
	case FieldTypeInt64:
		val, list := v.(int64), in.([]int64)
		return Int64Slice(list).Contains(val)
	case FieldTypeInt32:
		val, list := v.(int32), in.([]int32)
		return Int32Slice(list).Contains(val)
	case FieldTypeInt16:
		val, list := v.(int16), in.([]int16)
		return Int16Slice(list).Contains(val)
	case FieldTypeInt8:
		val, list := v.(int8), in.([]int8)
		return Int8Slice(list).Contains(val)
	case FieldTypeUint64:
		val, list := v.(uint64), in.([]uint64)
		return Uint64Slice(list).Contains(val)
	case FieldTypeUint32:
		val, list := v.(uint32), in.([]uint32)
		return Uint32Slice(list).Contains(val)
	case FieldTypeUint16:
		val, list := v.(uint16), in.([]uint16)
		return Uint16Slice(list).Contains(val)
	case FieldTypeUint8:
		val, list := v.(uint8), in.([]uint8)
		return Uint8Slice(list).Contains(val)
	case FieldTypeFloat64:
		val, list := v.(float64), in.([]float64)
		return Float64Slice(list).Contains(val)
	case FieldTypeFloat32:
		val, list := v.(float32), in.([]float32)
		return Float32Slice(list).Contains(val)
	case FieldTypeDecimal32:
		val, list := v.(Decimal32).Int32(), in.([]int32)
		return Int32Slice(list).Contains(val)
	case FieldTypeDecimal64:
		val, list := v.(Decimal64).Int64(), in.([]int64)
		return Int64Slice(list).Contains(val)
	case FieldTypeDecimal128:
		val, list := v.(Decimal128).Int128(), in.([]Int128)
		return Int128Slice(list).Contains(val)
	case FieldTypeDecimal256:
		val, list := v.(Decimal256).Int256(), in.([]Int256)
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
		return ByteSlice(list).Contains(val)
	case FieldTypeString:
		val, _ := pkg.StringAt(index, pos)
		list := in.([]string)
		return StringSlice(list).Contains(val)
	case FieldTypeDatetime:
		val, _ := pkg.TimeAt(index, pos)
		list := in.([]time.Time)
		return TimeSlice(list).Contains(val)
	case FieldTypeBoolean:
		val, _ := pkg.BoolAt(index, pos)
		list := in.([]bool)
		return BoolSlice(list).Contains(val)
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
		return Int64Slice(list).Contains(val)
	case FieldTypeInt32:
		val, _ := pkg.Int32At(index, pos)
		list := in.([]int32)
		return Int32Slice(list).Contains(val)
	case FieldTypeInt16:
		val, _ := pkg.Int16At(index, pos)
		list := in.([]int16)
		return Int16Slice(list).Contains(val)
	case FieldTypeInt8:
		val, _ := pkg.Int8At(index, pos)
		list := in.([]int8)
		return Int8Slice(list).Contains(val)
	case FieldTypeUint64:
		val, _ := pkg.Uint64At(index, pos)
		list := in.([]uint64)
		return Uint64Slice(list).Contains(val)
	case FieldTypeUint32:
		val, _ := pkg.Uint32At(index, pos)
		list := in.([]uint32)
		return Uint32Slice(list).Contains(val)
	case FieldTypeUint16:
		val, _ := pkg.Uint16At(index, pos)
		list := in.([]uint16)
		return Uint16Slice(list).Contains(val)
	case FieldTypeUint8:
		val, _ := pkg.Uint8At(index, pos)
		list := in.([]uint8)
		return Uint8Slice(list).Contains(val)
	case FieldTypeFloat64:
		val, _ := pkg.Float64At(index, pos)
		list := in.([]float64)
		return Float64Slice(list).Contains(val)
	case FieldTypeFloat32:
		val, _ := pkg.Float32At(index, pos)
		list := in.([]float32)
		return Float32Slice(list).Contains(val)
	case FieldTypeDecimal32:
		val, _ := pkg.Decimal32At(index, pos)
		list := in.([]int32)
		return Int32Slice(list).Contains(val.Int32())
	case FieldTypeDecimal64:
		val, _ := pkg.Decimal64At(index, pos)
		list := in.([]int64)
		return Int64Slice(list).Contains(val.Int64())
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
		if fromMatch == 0 {
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
		if fromMatch == 0 {
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
		v := val.(Decimal32)
		return !(v.Lt(from.(Decimal32)) || v.Gt(to.(Decimal32)))

	case FieldTypeDecimal64:
		v := val.(Decimal64)
		return !(v.Lt(from.(Decimal64)) || v.Gt(to.(Decimal64)))

	case FieldTypeDecimal128:
		v := val.(Decimal128)
		return !(v.Lt(from.(Decimal128)) || v.Gt(to.(Decimal128)))

	case FieldTypeDecimal256:
		v := val.(Decimal256)
		return !(v.Lt(from.(Decimal256)) || v.Gt(to.(Decimal256)))
	}
	return false
}

// assumes from <= to
func (t FieldType) BetweenAt(pkg *Package, index, pos int, from, to interface{}) bool {
	switch t {
	case FieldTypeBytes:
		val, _ := pkg.BytesAt(index, pos)
		fromMatch := bytes.Compare(val, from.([]byte))
		if fromMatch == 0 {
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
		if fromMatch == 0 {
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
		return !(val.Lt(from.(Decimal32)) || val.Gt(to.(Decimal32)))

	case FieldTypeDecimal64:
		val, _ := pkg.Decimal64At(index, pos)
		return !(val.Lt(from.(Decimal64)) || val.Gt(to.(Decimal64)))

	case FieldTypeDecimal128:
		val, _ := pkg.Decimal128At(index, pos)
		return !(val.Lt(from.(Decimal128)) || val.Gt(to.(Decimal128)))

	case FieldTypeDecimal256:
		val, _ := pkg.Decimal256At(index, pos)
		return !(val.Lt(from.(Decimal256)) || val.Gt(to.(Decimal256)))

	}
	return false
}

// assumes from <= to
func (t FieldType) BetweenSlice(slice, from, to interface{}, bits, mask *BitSet) *BitSet {
	switch t {
	case FieldTypeBytes:
		return MatchBytesBetween(
			slice.([][]byte),
			from.([]byte),
			to.([]byte),
			bits, mask)
	case FieldTypeString:
		return MatchStringsBetween(
			slice.([]string),
			from.(string),
			to.(string),
			bits, mask)
	case FieldTypeDatetime:
		return MatchInt64Between(
			slice.([]int64),
			from.(time.Time).UnixNano(),
			to.(time.Time).UnixNano(),
			bits, mask)
	case FieldTypeBoolean:
		return MatchBoolBetween(
			slice.([]bool),
			from.(bool),
			to.(bool),
			bits, mask)
	case FieldTypeInt256:
		return MatchInt256Between(
			slice.([]Int256),
			from.(Int256),
			to.(Int256),
			bits, mask)
	case FieldTypeInt128:
		return MatchInt128Between(
			slice.([]Int128),
			from.(Int128),
			to.(Int128),
			bits, mask)
	case FieldTypeInt64:
		return MatchInt64Between(
			slice.([]int64),
			from.(int64),
			to.(int64),
			bits, mask)
	case FieldTypeInt32:
		return MatchInt32Between(
			slice.([]int32),
			from.(int32),
			to.(int32),
			bits, mask)
	case FieldTypeInt16:
		return MatchInt16Between(
			slice.([]int16),
			from.(int16),
			to.(int16),
			bits, mask)
	case FieldTypeInt8:
		return MatchInt8Between(
			slice.([]int8),
			from.(int8),
			to.(int8),
			bits, mask)
	case FieldTypeUint64:
		return MatchUint64Between(
			slice.([]uint64),
			from.(uint64),
			to.(uint64),
			bits, mask)
	case FieldTypeUint32:
		return MatchUint32Between(
			slice.([]uint32),
			from.(uint32),
			to.(uint32),
			bits, mask)
	case FieldTypeUint16:
		return MatchUint16Between(
			slice.([]uint16),
			from.(uint16),
			to.(uint16),
			bits, mask)
	case FieldTypeUint8:
		return MatchUint8Between(
			slice.([]uint8),
			from.(uint8),
			to.(uint8),
			bits, mask)
	case FieldTypeFloat64:
		return MatchFloat64Between(
			slice.([]float64),
			from.(float64),
			to.(float64),
			bits, mask)
	case FieldTypeFloat32:
		return MatchFloat32Between(
			slice.([]float32),
			from.(float32),
			to.(float32),
			bits, mask)
	case FieldTypeDecimal32:
		return MatchInt32Between(
			slice.(Decimal32Slice).Int32,
			from.(Decimal32).Int32(),
			to.(Decimal32).Int32(),
			bits, mask)
	case FieldTypeDecimal64:
		return MatchInt64Between(
			slice.(Decimal64Slice).Int64,
			from.(Decimal64).Int64(),
			to.(Decimal64).Int64(),
			bits, mask)
	case FieldTypeDecimal128:
		return MatchInt128Between(
			slice.(Decimal128Slice).Int128,
			from.(Decimal128).Int128(),
			to.(Decimal128).Int128(),
			bits, mask)
	case FieldTypeDecimal256:
		return MatchInt256Between(
			slice.(Decimal256Slice).Int256,
			from.(Decimal256).Int256(),
			to.(Decimal256).Int256(),
			bits, mask)
	default:
		return bits
	}
}

// using binary search to find if slice contains values in interval [from, to]
// Note: there's no *At func because this function already on slices only
// assumes from <= to
func (t FieldType) InBetween(slice, from, to interface{}) bool {
	switch t {
	case FieldTypeBytes:
		return ByteSlice(slice.([][]byte)).ContainsRange(from.([]byte), to.([]byte))

	case FieldTypeString:
		return StringSlice(slice.([]string)).ContainsRange(from.(string), to.(string))

	case FieldTypeDatetime:
		return TimeSlice(slice.([]time.Time)).ContainsRange(from.(time.Time), to.(time.Time))

	case FieldTypeBoolean:
		return BoolSlice(slice.([]bool)).ContainsRange(from.(bool), to.(bool))

	case FieldTypeInt256:
		return Int256Slice(slice.([]Int256)).ContainsRange(from.(Int256), to.(Int256))

	case FieldTypeInt128:
		return Int128Slice(slice.([]Int128)).ContainsRange(from.(Int128), to.(Int128))

	case FieldTypeInt64:
		return Int64Slice(slice.([]int64)).ContainsRange(from.(int64), to.(int64))

	case FieldTypeInt32:
		return Int32Slice(slice.([]int32)).ContainsRange(from.(int32), to.(int32))

	case FieldTypeInt16:
		return Int16Slice(slice.([]int16)).ContainsRange(from.(int16), to.(int16))

	case FieldTypeInt8:
		return Int8Slice(slice.([]int8)).ContainsRange(from.(int8), to.(int8))

	case FieldTypeUint64:
		return Uint64Slice(slice.([]uint64)).ContainsRange(from.(uint64), to.(uint64))

	case FieldTypeUint32:
		return Uint32Slice(slice.([]uint32)).ContainsRange(from.(uint32), to.(uint32))

	case FieldTypeUint16:
		return Uint16Slice(slice.([]uint16)).ContainsRange(from.(uint16), to.(uint16))

	case FieldTypeUint8:
		return Uint8Slice(slice.([]uint8)).ContainsRange(from.(uint8), to.(uint8))

	case FieldTypeFloat64:
		return Float64Slice(slice.([]float64)).ContainsRange(from.(float64), to.(float64))

	case FieldTypeFloat32:
		return Float32Slice(slice.([]float32)).ContainsRange(from.(float32), to.(float32))

	case FieldTypeDecimal256:
		return Int256Slice(slice.(Decimal256Slice).Int256).ContainsRange(from.(Decimal256).Int256(), to.(Decimal256).Int256())

	case FieldTypeDecimal128:
		return Int128Slice(slice.(Decimal128Slice).Int128).ContainsRange(from.(Decimal128).Int128(), to.(Decimal128).Int128())

	case FieldTypeDecimal64:
		return Int64Slice(slice.(Decimal64Slice).Int64).ContainsRange(from.(Decimal64).Int64(), to.(Decimal64).Int64())

	case FieldTypeDecimal32:
		return Int32Slice(slice.(Decimal32Slice).Int32).ContainsRange(from.(Decimal32).Int32(), to.(Decimal32).Int32())

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
		return xa.(Decimal32).Lt(xb.(Decimal32))
	case FieldTypeDecimal64:
		return xa.(Decimal64).Lt(xb.(Decimal64))
	case FieldTypeDecimal128:
		return xa.(Decimal128).Lt(xb.(Decimal128))
	case FieldTypeDecimal256:
		return xa.(Decimal256).Lt(xb.(Decimal256))
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
		return xa.(Decimal32).Cmp(xb.(Decimal32))

	case FieldTypeDecimal64:
		return xa.(Decimal64).Cmp(xb.(Decimal64))

	case FieldTypeDecimal128:
		return xa.(Decimal128).Cmp(xb.(Decimal128))

	case FieldTypeDecimal256:
		return xa.(Decimal256).Cmp(xb.(Decimal256))

	default:
		return -1
	}
}
