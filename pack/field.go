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
	"blockwatch.cc/knoxdb/vec"
)

type FieldFlags int

const (
	FlagPrimary FieldFlags = 1 << iota
	FlagIndexed
	FlagConvert
	FlagCompressSnappy
	FlagCompressLZ4
	FlagMode = FlagPrimary | FlagIndexed | FlagConvert | FlagCompressSnappy | FlagCompressLZ4
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

type FieldType string

const (
	FieldTypeUndefined FieldType = ""
	FieldTypeBytes     FieldType = "bytes"    // BlockBytes
	FieldTypeString    FieldType = "string"   // BlockString
	FieldTypeDatetime  FieldType = "datetime" // BlockTime
	FieldTypeBoolean   FieldType = "boolean"  // BlockBool
	FieldTypeFloat64   FieldType = "float64"  // BlockFloat
	FieldTypeInt64     FieldType = "int64"    // BlockInt
	FieldTypeInt32     FieldType = "int32"    // BlockInt
	FieldTypeUint64    FieldType = "uint64"   // BlockUnsigned

	// TODO: extend pack encoders and block types
	// FieldTypeInt8
	// FieldTypeUint8
	// FieldTypeInt16
	// FieldTypeUint16
	// FieldTypeInt32
	// FieldTypeUint32
	// FieldTypeDate                   = "date" // BlockDate (unix second / 24*3600)
	// FieldTypeDecimal36_8            = "decimal_36_8" // bigint
	// FieldTypeDecimal36_10           = "decimal_36_10"// bigint
	// FieldTypeDecimal36_12           = "decimal_36_12"// bigint
)

type Field struct {
	Index     int        `json:"index"`
	Name      string     `json:"name"`
	Alias     string     `json:"alias"`
	Type      FieldType  `json:"type"`
	Flags     FieldFlags `json:"flags"` // primary, indexed, convert, compression
	Precision int        `json:"prec"`  // floating point precision
}

func (f Field) IsValid() bool {
	return f.Index >= 0 && f.Type.IsValid()
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
		fields[i].Precision = finfo.precision
		switch f.Kind() {
		case reflect.Int32:
			fields[i].Type = FieldTypeInt32
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int64:
			fields[i].Type = FieldTypeInt64
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fields[i].Type = FieldTypeUint64
		case reflect.Float32, reflect.Float64:
			if finfo.flags&FlagConvert > 0 {
				fields[i].Type = FieldTypeUint64
			} else {
				fields[i].Type = FieldTypeFloat64
			}
		case reflect.String:
			fields[i].Type = FieldTypeString
		case reflect.Slice:
			// check if type implements BinaryMarshaler -> BlockBytes
			if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				log.Debugf("Slice type field %s type %s implements binary marshaler", finfo.name, f.Type().String())
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
			// check string is much quicker
			if f.Type().String() == "time.Time" {
				fields[i].Type = FieldTypeDatetime
			} else if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				fields[i].Type = FieldTypeBytes
			} else {
				return nil, fmt.Errorf("pack: unsupported embedded struct type %s", f.Type().String())
			}
		case reflect.Array:
			// check if type implements BinaryMarshaler -> BlockBytes
			if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				log.Debugf("Array type field %s type %s implements binary marshaler", finfo.name, f.Type().String())
				fields[i].Type = FieldTypeBytes
				break
			}
			return nil, fmt.Errorf("pack: unsupported array type %s", f.Type().String())
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
	case "integer", "int", "int64":
		return FieldTypeInt64
	case "int32":
		return FieldTypeInt32
	case "unsigned", "uint", "uint64":
		return FieldTypeUint64
	case "float", "float64":
		return FieldTypeFloat64
	default:
		return FieldTypeUndefined
	}
}

func FieldTypeFromBlock(b block.BlockType) FieldType {
	switch b {
	case block.BlockBytes:
		return FieldTypeBytes
	case block.BlockString:
		return FieldTypeString
	case block.BlockTime:
		return FieldTypeDatetime
	case block.BlockBool:
		return FieldTypeBoolean
	case block.BlockFloat64:
		return FieldTypeFloat64
	case block.BlockInt64:
		return FieldTypeInt64
	case block.BlockUint64:
		return FieldTypeUint64
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
	case FieldTypeInt64:
		return block.BlockInt64
	case FieldTypeUint64:
		return block.BlockUint64
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

func (t FieldType) ParseAs(s string) (interface{}, error) {
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
	case FieldTypeInt64:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return i, nil
	case FieldTypeUint64:
		i, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return i, nil
	default:
		return nil, fmt.Errorf("unsupported field type '%s'", t)
	}
}

func (t FieldType) ParseSliceAs(s string) (interface{}, error) {
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
	default:
		return nil, fmt.Errorf("unsupported field type '%s'", t)
	}
}

func (t FieldType) ToString(val interface{}) string {
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
	case FieldTypeUint64:
		if v, ok := val.([]uint64); ok {
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
	}
	if len(ss) > 0 {
		return strings.Join(ss, ", ")
	}
	return util.ToString(val)
}

func (t FieldType) CheckType(val interface{}) error {
	var ok bool
	switch t {
	case FieldTypeBytes:
		_, ok = val.([]byte)
	case FieldTypeString:
		_, ok = val.(string)
	case FieldTypeDatetime:
		_, ok = val.(time.Time)
	case FieldTypeBoolean:
		_, ok = val.(bool)
	case FieldTypeInt64:
		// FIXME: allow int, int8, int16, int32, int64
		// switch val.(type) {
		// case int:
		// 	ok = true
		// case int8:
		// 	ok = true
		// case int16:
		// 	ok = true
		// case int32:
		// 	ok = true
		// case int64:
		// 	ok = true
		// }
		_, ok = val.(int64)
	case FieldTypeUint64:
		_, ok = val.(uint64)
	case FieldTypeFloat64:
		_, ok = val.(float64)
	}
	if !ok {
		return fmt.Errorf("pack: unexpected value type %T for %s condition", val, t)
	}
	return nil
}

func (t FieldType) CheckSliceType(val interface{}) error {
	var ok bool
	switch t {
	case FieldTypeBytes:
		_, ok = val.([][]byte)
	case FieldTypeString:
		_, ok = val.([]string)
	case FieldTypeDatetime:
		_, ok = val.([]time.Time)
	case FieldTypeBoolean:
		_, ok = val.([]bool)
	case FieldTypeInt64:
		_, ok = val.([]int64)
	case FieldTypeUint64:
		_, ok = val.([]uint64)
	case FieldTypeFloat64:
		_, ok = val.([]float64)
	}
	if !ok {
		return fmt.Errorf("pack: unexpected value type %T for %s slice condition", val, t)
	}
	return nil
}

func (t FieldType) CopySliceType(val interface{}) (interface{}, error) {
	switch t {
	case FieldTypeBytes:
		if slice, ok := val.([][]byte); ok {
			cp := make([][]byte, len(slice))
			for i, v := range slice {
				buf := make([]byte, len(v))
				copy(buf, v)
				cp[i] = buf
			}
			return cp, nil
		}
	case FieldTypeString:
		if slice, ok := val.([]string); ok {
			cp := make([]string, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeDatetime:
		if slice, ok := val.([]time.Time); ok {
			cp := make([]time.Time, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeBoolean:
		if slice, ok := val.([]time.Time); ok {
			cp := make([]time.Time, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeInt64:
		if slice, ok := val.([]int64); ok {
			cp := make([]int64, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeUint64:
		if slice, ok := val.([]uint64); ok {
			cp := make([]uint64, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeFloat64:
		if slice, ok := val.([]float64); ok {
			cp := make([]float64, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	default:
		return nil, fmt.Errorf("pack: slice copy on unsupported field type %s", t)
	}
	return nil, fmt.Errorf("pack: slice copy mismatched value type %T for %s field", val, t)
}

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
	case FieldTypeInt64:
		return xa.(int64) == xb.(int64)
	case FieldTypeUint64:
		return xa.(uint64) == xb.(uint64)
	case FieldTypeFloat64:
		return xa.(float64) == xb.(float64)
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
	case FieldTypeInt64:
		a, _ := pkg.Int64At(index, pos)
		return a == val.(int64)
	case FieldTypeUint64:
		a, _ := pkg.Uint64At(index, pos)
		return a == val.(uint64)
	case FieldTypeFloat64:
		a, _ := pkg.Float64At(index, pos)
		return a == val.(float64)
	default:
		return false
	}
}

func (t FieldType) EqualSlice(slice, val interface{}, bits *vec.BitSet) *vec.BitSet {
	switch t {
	case FieldTypeBytes:
		return vec.MatchBytesEqual(slice.([][]byte), val.([]byte), bits)
	case FieldTypeString:
		return vec.MatchStringsEqual(slice.([]string), val.(string), bits)
	case FieldTypeDatetime:
		return vec.MatchInt64Equal(slice.([]int64), val.(time.Time).UnixNano(), bits)
	case FieldTypeBoolean:
		return vec.MatchBoolEqual(slice.([]bool), val.(bool), bits)
	case FieldTypeInt64:
		return vec.MatchInt64Equal(slice.([]int64), val.(int64), bits)
	case FieldTypeUint64:
		return vec.MatchUint64Equal(slice.([]uint64), val.(uint64), bits)
	case FieldTypeFloat64:
		return vec.MatchFloat64Equal(slice.([]float64), val.(float64), bits)
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
	case FieldTypeInt64:
		v1, _ := p1.Int64At(i1, n1)
		v2, _ := p2.Int64At(i2, n2)
		return v1 == v2
	case FieldTypeUint64:
		v1, _ := p1.Uint64At(i1, n1)
		v2, _ := p2.Uint64At(i2, n2)
		return v1 == v2
	case FieldTypeFloat64:
		v1, _ := p1.Float64At(i1, n1)
		v2, _ := p2.Float64At(i2, n2)
		return v1 == v2
	default:
		return false
	}
}

func (t FieldType) EqualUint64At(p1 *Package, i1, n1 int, p2 *Package, i2, n2 int) bool {
	v1, _ := p1.Uint64At(i1, n1)
	v2, _ := p2.Uint64At(i2, n2)
	return v1 == v2
}

func (t FieldType) NotEqualSlice(slice, val interface{}, bits *vec.BitSet) *vec.BitSet {
	switch t {
	case FieldTypeBytes:
		return vec.MatchBytesNotEqual(slice.([][]byte), val.([]byte), bits)
	case FieldTypeString:
		return vec.MatchStringsNotEqual(slice.([]string), val.(string), bits)
	case FieldTypeDatetime:
		return vec.MatchInt64NotEqual(slice.([]int64), val.(time.Time).UnixNano(), bits)
	case FieldTypeBoolean:
		return vec.MatchBoolNotEqual(slice.([]bool), val.(bool), bits)
	case FieldTypeInt64:
		return vec.MatchInt64NotEqual(slice.([]int64), val.(int64), bits)
	case FieldTypeUint64:
		return vec.MatchUint64NotEqual(slice.([]uint64), val.(uint64), bits)
	case FieldTypeFloat64:
		return vec.MatchFloat64NotEqual(slice.([]float64), val.(float64), bits)
	default:
		return bits
	}
}

func (t FieldType) Regexp(v interface{}, re string) bool {
	switch t {
	case FieldTypeBytes,
		FieldTypeBoolean,
		FieldTypeInt64,
		FieldTypeUint64,
		FieldTypeFloat64:
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
		FieldTypeInt64,
		FieldTypeUint64,
		FieldTypeFloat64:
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

func (t FieldType) RegexpSlice(slice interface{}, re string, bits *vec.BitSet) *vec.BitSet {
	switch t {
	case FieldTypeBytes,
		FieldTypeBoolean,
		FieldTypeInt64,
		FieldTypeUint64,
		FieldTypeFloat64:
		return bits
	case FieldTypeString:
		rematch := strings.Replace(re, "*", ".*", -1)
		for i, v := range slice.([]string) {
			if match, _ := regexp.MatchString(rematch, v); match {
				bits.Set(i)
			}
		}
		return bits
	case FieldTypeDatetime:
		rematch := strings.Replace(re, "*", ".*", -1)
		for i, v := range slice.([]int64) {
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
	case FieldTypeInt64:
		return xa.(int64) > xb.(int64)
	case FieldTypeUint64:
		return xa.(uint64) > xb.(uint64)
	case FieldTypeFloat64:
		return xa.(float64) > xb.(float64)
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
	case FieldTypeInt64:
		a, _ := pkg.Int64At(index, pos)
		return a > val.(int64)
	case FieldTypeUint64:
		a, _ := pkg.Uint64At(index, pos)
		return a > val.(uint64)
	case FieldTypeFloat64:
		a, _ := pkg.Float64At(index, pos)
		return a > val.(float64)
	default:
		return false
	}
}

func (t FieldType) GtSlice(slice, val interface{}, bits *vec.BitSet) *vec.BitSet {
	switch t {
	case FieldTypeBytes:
		return vec.MatchBytesGreaterThan(slice.([][]byte), val.([]byte), bits)
	case FieldTypeString:
		return vec.MatchStringsGreaterThan(slice.([]string), val.(string), bits)
	case FieldTypeDatetime:
		return vec.MatchInt64GreaterThan(slice.([]int64), val.(time.Time).UnixNano(), bits)
	case FieldTypeBoolean:
		return vec.MatchBoolGreaterThan(slice.([]bool), val.(bool), bits)
	case FieldTypeInt64:
		return vec.MatchInt64GreaterThan(slice.([]int64), val.(int64), bits)
	case FieldTypeUint64:
		return vec.MatchUint64GreaterThan(slice.([]uint64), val.(uint64), bits)
	case FieldTypeFloat64:
		return vec.MatchFloat64GreaterThan(slice.([]float64), val.(float64), bits)
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
	case FieldTypeInt64:
		return xa.(int64) >= xb.(int64)
	case FieldTypeUint64:
		return xa.(uint64) >= xb.(uint64)
	case FieldTypeFloat64:
		return xa.(float64) >= xb.(float64)
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
	case FieldTypeInt64:
		a, _ := pkg.Int64At(index, pos)
		return a >= val.(int64)
	case FieldTypeUint64:
		a, _ := pkg.Uint64At(index, pos)
		return a >= val.(uint64)
	case FieldTypeFloat64:
		a, _ := pkg.Float64At(index, pos)
		return a >= val.(float64)
	default:
		return false
	}
}

func (t FieldType) GteSlice(slice, val interface{}, bits *vec.BitSet) *vec.BitSet {
	switch t {
	case FieldTypeBytes:
		return vec.MatchBytesGreaterThanEqual(slice.([][]byte), val.([]byte), bits)
	case FieldTypeString:
		return vec.MatchStringsGreaterThanEqual(slice.([]string), val.(string), bits)
	case FieldTypeDatetime:
		return vec.MatchInt64GreaterThanEqual(slice.([]int64), val.(time.Time).UnixNano(), bits)
	case FieldTypeBoolean:
		return vec.MatchBoolGreaterThanEqual(slice.([]bool), val.(bool), bits)
	case FieldTypeInt64:
		return vec.MatchInt64GreaterThanEqual(slice.([]int64), val.(int64), bits)
	case FieldTypeUint64:
		return vec.MatchUint64GreaterThanEqual(slice.([]uint64), val.(uint64), bits)
	case FieldTypeFloat64:
		return vec.MatchFloat64GreaterThanEqual(slice.([]float64), val.(float64), bits)
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
	case FieldTypeInt64:
		return xa.(int64) < xb.(int64)
	case FieldTypeUint64:
		return xa.(uint64) < xb.(uint64)
	case FieldTypeFloat64:
		return xa.(float64) < xb.(float64)
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
	case FieldTypeInt64:
		a, _ := pkg.Int64At(index, pos)
		return a < val.(int64)
	case FieldTypeUint64:
		a, _ := pkg.Uint64At(index, pos)
		return a < val.(uint64)
	case FieldTypeFloat64:
		a, _ := pkg.Float64At(index, pos)
		return a < val.(float64)
	default:
		return false
	}
}

func (t FieldType) LtSlice(slice, val interface{}, bits *vec.BitSet) *vec.BitSet {
	switch t {
	case FieldTypeBytes:
		return vec.MatchBytesLessThan(slice.([][]byte), val.([]byte), bits)
	case FieldTypeString:
		return vec.MatchStringsLessThan(slice.([]string), val.(string), bits)
	case FieldTypeDatetime:
		return vec.MatchInt64LessThan(slice.([]int64), val.(time.Time).UnixNano(), bits)
	case FieldTypeBoolean:
		return vec.MatchBoolLessThan(slice.([]bool), val.(bool), bits)
	case FieldTypeInt64:
		return vec.MatchInt64LessThan(slice.([]int64), val.(int64), bits)
	case FieldTypeUint64:
		return vec.MatchUint64LessThan(slice.([]uint64), val.(uint64), bits)
	case FieldTypeFloat64:
		return vec.MatchFloat64LessThan(slice.([]float64), val.(float64), bits)
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
	case FieldTypeInt64:
		return xa.(int64) <= xb.(int64)
	case FieldTypeUint64:
		return xa.(uint64) <= xb.(uint64)
	case FieldTypeFloat64:
		return xa.(float64) <= xb.(float64)
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
	case FieldTypeInt64:
		a, _ := pkg.Int64At(index, pos)
		return a <= val.(int64)
	case FieldTypeUint64:
		a, _ := pkg.Uint64At(index, pos)
		return a <= val.(uint64)
	case FieldTypeFloat64:
		a, _ := pkg.Float64At(index, pos)
		return a <= val.(float64)
	default:
		return false
	}
}

func (t FieldType) LteSlice(slice, val interface{}, bits *vec.BitSet) *vec.BitSet {
	switch t {
	case FieldTypeBytes:
		return vec.MatchBytesLessThanEqual(slice.([][]byte), val.([]byte), bits)
	case FieldTypeString:
		return vec.MatchStringsLessThanEqual(slice.([]string), val.(string), bits)
	case FieldTypeDatetime:
		return vec.MatchInt64LessThanEqual(slice.([]int64), val.(time.Time).UnixNano(), bits)
	case FieldTypeBoolean:
		return vec.MatchBoolLessThanEqual(slice.([]bool), val.(bool), bits)
	case FieldTypeInt64:
		return vec.MatchInt64LessThanEqual(slice.([]int64), val.(int64), bits)
	case FieldTypeUint64:
		return vec.MatchUint64LessThanEqual(slice.([]uint64), val.(uint64), bits)
	case FieldTypeFloat64:
		return vec.MatchFloat64LessThanEqual(slice.([]float64), val.(float64), bits)
	default:
		return bits
	}
}

// first arg is value to compare, second is slice of value types from condition
func (t FieldType) In(v, in interface{}) bool {
	switch t {
	case FieldTypeBytes:
		val, list := v.([]byte), in.([][]byte)
		return vec.ByteSlice(list).Contains(val)
	case FieldTypeString:
		val, list := v.(string), in.([]string)
		return vec.StringSlice(list).Contains(val)
	case FieldTypeDatetime:
		val, list := v.(time.Time), in.([]time.Time)
		return vec.TimeSlice(list).Contains(val)
	case FieldTypeBoolean:
		val, list := v.(bool), in.([]bool)
		return vec.BoolSlice(list).Contains(val)
	case FieldTypeInt64:
		val, list := v.(int64), in.([]int64)
		return vec.Int64Slice(list).Contains(val)
	case FieldTypeUint64:
		val, list := v.(uint64), in.([]uint64)
		return vec.Uint64Slice(list).Contains(val)
	case FieldTypeFloat64:
		val, list := v.(float64), in.([]float64)
		return vec.Float64Slice(list).Contains(val)
	}
	return false
}

// assumes `in` is sorted
func (t FieldType) InAt(pkg *Package, index, pos int, in interface{}) bool {
	switch t {
	case FieldTypeBytes:
		val, _ := pkg.BytesAt(index, pos)
		list := in.([][]byte)
		return vec.ByteSlice(list).Contains(val)
	case FieldTypeString:
		val, _ := pkg.StringAt(index, pos)
		list := in.([]string)
		return vec.StringSlice(list).Contains(val)
	case FieldTypeDatetime:
		val, _ := pkg.TimeAt(index, pos)
		list := in.([]time.Time)
		return vec.TimeSlice(list).Contains(val)
	case FieldTypeBoolean:
		val, _ := pkg.BoolAt(index, pos)
		list := in.([]bool)
		return vec.BoolSlice(list).Contains(val)
	case FieldTypeInt64:
		val, _ := pkg.Int64At(index, pos)
		list := in.([]int64)
		return vec.Int64Slice(list).Contains(val)
	case FieldTypeUint64:
		val, _ := pkg.Uint64At(index, pos)
		list := in.([]uint64)
		return vec.Uint64Slice(list).Contains(val)
	case FieldTypeFloat64:
		val, _ := pkg.Float64At(index, pos)
		list := in.([]float64)
		return vec.Float64Slice(list).Contains(val)
	}
	return false
}

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

	case FieldTypeInt64:
		v := val.(int64)
		return !(v < from.(int64) || v > to.(int64))

	case FieldTypeUint64:
		v := val.(uint64)
		return !(v < from.(uint64) || v > to.(uint64))

	case FieldTypeFloat64:
		v := val.(float64)
		return !(v < from.(float64) || v > to.(float64))

	}
	return false
}

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

	case FieldTypeInt64:
		val, _ := pkg.Int64At(index, pos)
		return !(val < from.(int64) || val > to.(int64))

	case FieldTypeUint64:
		val, _ := pkg.Uint64At(index, pos)
		return !(val < from.(uint64) || val > to.(uint64))

	case FieldTypeFloat64:
		val, _ := pkg.Float64At(index, pos)
		return !(val < from.(float64) || val > to.(float64))
	}
	return false
}

func (t FieldType) BetweenSlice(slice, from, to interface{}, bits *vec.BitSet) *vec.BitSet {
	switch t {
	case FieldTypeBytes:
		return vec.MatchBytesBetween(
			slice.([][]byte),
			from.([]byte),
			to.([]byte),
			bits)
	case FieldTypeString:
		return vec.MatchStringsBetween(
			slice.([]string),
			from.(string),
			to.(string),
			bits)
	case FieldTypeDatetime:
		return vec.MatchInt64Between(
			slice.([]int64),
			from.(time.Time).UnixNano(),
			to.(time.Time).UnixNano(),
			bits)
	case FieldTypeBoolean:
		return vec.MatchBoolBetween(
			slice.([]bool),
			from.(bool),
			to.(bool),
			bits)
	case FieldTypeInt64:
		return vec.MatchInt64Between(
			slice.([]int64),
			from.(int64),
			to.(int64),
			bits)
	case FieldTypeUint64:
		return vec.MatchUint64Between(
			slice.([]uint64),
			from.(uint64),
			to.(uint64),
			bits)
	case FieldTypeFloat64:
		return vec.MatchFloat64Between(
			slice.([]float64),
			from.(float64),
			to.(float64),
			bits)
	default:
		return bits
	}
}

// using binary search to find if slice contains values in interval [from, to]
// Note: there's no *At func because this function already works on slices
func (t FieldType) InBetween(slice, from, to interface{}) bool {
	switch t {
	case FieldTypeBytes:
		return vec.ByteSlice(slice.([][]byte)).ContainsRange(from.([]byte), to.([]byte))

	case FieldTypeString:
		return vec.StringSlice(slice.([]string)).ContainsRange(from.(string), to.(string))

	case FieldTypeDatetime:
		return vec.TimeSlice(slice.([]time.Time)).ContainsRange(from.(time.Time), to.(time.Time))

	case FieldTypeBoolean:
		return vec.BoolSlice(slice.([]bool)).ContainsRange(from.(bool), to.(bool))

	case FieldTypeInt64:
		return vec.Int64Slice(slice.([]int64)).ContainsRange(from.(int64), to.(int64))

	case FieldTypeUint64:
		return vec.Uint64Slice(slice.([]uint64)).ContainsRange(from.(uint64), to.(uint64))

	case FieldTypeFloat64:
		return vec.Float64Slice(slice.([]float64)).ContainsRange(from.(float64), to.(float64))
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
		FieldTypeInt64,
		FieldTypeUint64:
		// Note: zero is undefined here since 0 is also a valid value
		return false
	case FieldTypeFloat64:
		v := val.(float64)
		return math.IsNaN(v) || math.IsInf(v, 0)
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
	case FieldTypeInt64:
		return xa.(int64) < xb.(int64)
	case FieldTypeUint64:
		return xa.(uint64) < xb.(uint64)
	case FieldTypeFloat64:
		return xa.(float64) < xb.(float64)
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
	default:
		return -1
	}
}
