// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema_tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math/bits"
	"strings"
	"testing"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// create and register enums
	RegisterEnums()

	// init schema and link enums (will lookup myEnum and link to field)
	reflect.MustSchemaFor[AllTypes](schema.Enums(enums))

	m.Run()
}

type schemaTest struct {
	name    string
	infer   func(...schema.Option) (*schema.Schema, error)
	build   func(...schema.Option) *schema.Schema
	fields  string
	typs    []schema.FieldType
	flags   []schema.FieldFlags
	filters []schema.FilterType
	scales  []uint8
	fixed   []uint8
	isFixed bool
	iserr   bool
}

var (
	// arch dependent, only used for tests
	FT_INT  = [2]schema.FieldType{schema.Int32, schema.Int64}[bits.UintSize/32-1]
	FT_UINT = [2]schema.FieldType{schema.Uint32, schema.Uint64}[bits.UintSize/32-1]
)

type NativeTypes struct {
	Int  int  `knox:"int"`
	Uint uint `knox:"uint"`
}

type ArrayTypes struct {
	Id          uint64   `knox:"id,pk"`
	ByteArray   [20]byte `knox:"byte_array"`
	StringArray string   `knox:"string_array,array=20"`
}

func NewArrayTypes(i int64) *ArrayTypes {
	b := binary.LittleEndian.AppendUint64(nil, uint64(i))
	buf := bytes.Repeat(b, 3)[:20]
	return &ArrayTypes{
		Id:          uint64(i),
		ByteArray:   [20]byte(buf),
		StringArray: hex.EncodeToString(buf[:10]),
	}
}

// not supported, used for error checks only
type Stringer []string

func (s Stringer) String() string {
	return strings.Join(s, ",")
}

func (s Stringer) MarshalText() ([]byte, error) {
	return []byte(strings.Join(s, ",")), nil
}

func (s *Stringer) UnmarshalText(b []byte) error {
	*s = strings.Split(string(b), ",")
	return nil
}

// not supported, used for error checks only
type Byter [][]byte

func (b Byter) MarshalBinary() ([]byte, error) {
	return bytes.Join(b, []byte{0}), nil
}

func (b *Byter) UnmarshalBinary(buf []byte) error {
	*b = bytes.Split(buf, []byte{0})
	return nil
}

// not supported, used for error checks only
type StringerStruct struct{}

func (s StringerStruct) MarshalText() ([]byte, error) {
	return []byte{}, nil
}

func (s *StringerStruct) UnmarshalText(b []byte) error {
	return nil
}

// not supported, used for error checks only
type ByterStruct struct{}

func (s ByterStruct) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

func (s *ByterStruct) UnmarshalBinary(b []byte) error {
	return nil
}

// not supported, used for error checks only
type MapType map[int]int

func (MapType) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

func (*MapType) UnmarshalBinary(_ []byte) error {
	return nil
}

type NoModelNoTag struct {
	Id uint64
}

type NoModelTag struct {
	Id uint64 `knox:",pk"`
}

type InvalidPkType struct {
	Id int64 `knox:",pk"`
}

type LargeArrayToBlob struct {
	Id uint64 `knox:"id,pk"`
	F  [256]byte
}

type MarshalerTypes struct {
	Stringer Stringer `knox:"stringer"`
	Byter    Byter    `knox:"byter"`
}

type MarshalerStructTypes struct {
	Stringer StringerStruct `knox:"stringer"`
	Byter    ByterStruct    `knox:"byter"`
}

type MarshalerMapTypes struct {
	Map MapType `knox:"map"`
}

type NoMarshalerTypes struct {
	Embed MarshalerStructTypes `knox:"no_marshalers"`
}

type NoMarshalerSliceTypes struct {
	Slice []int64 `knox:"no_marshalers"`
}

type NoMarshalerMapTypes struct {
	Map map[int]int `knox:"no_map"`
}

type InvalidPointerType struct {
	Ptr *int `knox:"ptr"`
}

type InvalidDuplicateName struct {
	Id  uint64 `knox:"id"`
	Val uint64 `knox:"id"`
}

type InvalidDuplicatePkType struct {
	Id  uint64 `knox:"id,pk"`
	Val uint64 `knox:"val,pk"`
}

type InvalidNativeTypes struct {
	Int  int  `knox:"int"`
	Uint uint `knox:"uint"`
}

type InvalidArrayType struct {
	F int64 `knox:",array=1"`
}

type InvalidArrayMissing struct {
	F []byte `knox:",array"`
}

type InvalidArrayNaN struct {
	F []byte `knox:",array=x"`
}

type InvalidArrayZero struct {
	F []byte `knox:",array=0"`
}

type InvalidArrayNeg struct {
	F []byte `knox:",array=-1"`
}

type InvalidArraySizeMismatch struct {
	F [20]byte `knox:",array=21"`
}

type InvalidScaleType struct {
	F int64 `knox:",scale=1"`
}

type InvalidScaleMissing struct {
	D num.Decimal32 `knox:",scale"`
}

type InvalidScaleNaN struct {
	D num.Decimal32 `knox:",scale=x"`
}

type InvalidScaleNeg struct {
	D num.Decimal32 `knox:",scale=-1"`
}

type InvalidScaleTooLarge struct {
	D num.Decimal32 `knox:",scale=36"`
}

type BloomFilter struct {
	Id  uint64 `knox:"id,pk"`
	Int int64  `knox:"i64,filter=bloom3b"`
}

type MetaFields struct {
	Id  uint64 `knox:"id,pk"`
	I64 int64  `knox:"i64,metadata"`
	U64 uint64 `knox:"u64"`
}

// Testcase Definition
// -------------------
//
//	{
//	    name:    "",
//	    build:   reflect.SchemaFor[T],
//	    fields:  "",
//	    typs:    []FieldType{},
//	    flags:   []FieldFlags{},
//	    scales:  []uint8{},
//	    fixed:   []uint8{},
//	    isFixed: true,
//	    err:     false,
//	},
var schemaTestCases = []schemaTest{
	//
	// Schema name tests
	// -----------------

	// schema name from Go type
	{
		name:  "no_model_tag",
		infer: reflect.SchemaFor[NoModelTag],
		build: func(opts ...schema.Option) *schema.Schema {
			return schema.SchemaOf([]*schema.Field{
				schema.FieldOf(schema.Uint64,
					schema.WithName("id"),
					schema.WithFlags(schema.FlagPrimary),
				),
			}, append(opts, schema.Name("no_model_tag"))...)
		},
		fields:  "id",
		typs:    []schema.FieldType{schema.Uint64},
		flags:   []schema.FieldFlags{schema.FlagPrimary},
		scales:  []uint8{0},
		fixed:   []uint8{0},
		isFixed: true,
		// encode:  []OpCode{OC_U64},
		// decode:  []OpCode{OC_U64},
	},

	//
	// Field name tests
	// -----------------

	// error: non-struct type
	{
		name:  "no struct type",
		infer: reflect.SchemaFor[[]string],
		iserr: true,
	},

	//
	// Field type tests
	// -----------------

	// all supported types
	{
		name:  "all_types",
		infer: reflect.SchemaFor[AllTypes],
		build: func(opts ...schema.Option) *schema.Schema {
			return schema.SchemaOf([]*schema.Field{
				schema.FieldOf(schema.Uint64, schema.WithName("id"), schema.WithFlags(schema.FlagPrimary)),
				schema.FieldOf(schema.Int64, schema.WithName("i64")),
				schema.FieldOf(schema.Int32, schema.WithName("i32")),
				schema.FieldOf(schema.Int16, schema.WithName("i16")),
				schema.FieldOf(schema.Int8, schema.WithName("i8")),
				schema.FieldOf(schema.Uint64, schema.WithName("u64")),
				schema.FieldOf(schema.Uint32, schema.WithName("u32")),
				schema.FieldOf(schema.Uint16, schema.WithName("u16")),
				schema.FieldOf(schema.Uint8, schema.WithName("u8")),
				schema.FieldOf(schema.Float64, schema.WithName("f64")),
				schema.FieldOf(schema.Float32, schema.WithName("f32")),
				schema.FieldOf(schema.Decimal32, schema.WithName("d32"), schema.WithScale(5)),
				schema.FieldOf(schema.Decimal64, schema.WithName("d64"), schema.WithScale(15)),
				schema.FieldOf(schema.Decimal128, schema.WithName("d128"), schema.WithScale(18)),
				schema.FieldOf(schema.Decimal256, schema.WithName("d256"), schema.WithScale(24)),
				schema.FieldOf(schema.Int128, schema.WithName("i128")),
				schema.FieldOf(schema.Int256, schema.WithName("i256")),
				schema.FieldOf(schema.Boolean, schema.WithName("bool")),
				schema.FieldOf(schema.Timestamp, schema.WithName("time")),
				schema.FieldOf(schema.Bytes, schema.WithName("bytes")),
				schema.ArrayOf(schema.Bytes, 2, schema.WithName("array[2]")),
				schema.FieldOf(schema.String, schema.WithName("string")),
				schema.FieldOf(schema.Enum, schema.WithName("my_enum"), schema.WithEnum(myEnum)),
				schema.FieldOf(schema.Bigint, schema.WithName("big")),
				schema.FieldOf(schema.Duration, schema.WithName("duration")),
				schema.FieldOf(schema.Union, schema.WithName("union")),
			}, append(opts, schema.Name("all_types"))...)
		},
		fields:  "id,i64,i32,i16,i8,u64,u32,u16,u8,f64,f32,d32,d64,d128,d256,i128,i256,bool,time,bytes,array[2],string,my_enum,big,duration,union,union.utag,union.unum,union.uval",
		typs:    []schema.FieldType{schema.Uint64, schema.Int64, schema.Int32, schema.Int16, schema.Int8, schema.Uint64, schema.Uint32, schema.Uint16, schema.Uint8, schema.Float64, schema.Float32, schema.Decimal32, schema.Decimal64, schema.Decimal128, schema.Decimal256, schema.Int128, schema.Int256, schema.Boolean, schema.Timestamp, schema.Bytes, schema.Bytes, schema.String, schema.Enum, schema.Bigint, schema.Duration, schema.Union, schema.Uint8, schema.Uint64, schema.Bytes},
		flags:   []schema.FieldFlags{schema.FlagPrimary, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, schema.FlagNullable, 0, 0, 0, 0, 0, schema.FlagNullable, schema.FlagMetadata, schema.FlagMetadata, schema.FlagMetadata},
		scales:  []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 15, 18, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		fixed:   []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0},
		isFixed: false,
		// encode:  []OpCode{OC_U64, OC_I64, OC_I32, OC_I16, OC_I8, OC_U64, OC_U32, OC_U16, OC_U8, OC_F64, OC_F32, OC_D32, OC_D64, OC_D128, OC_D256, OC_I128, OC_I256, OC_BOOL, OC_TIMESTAMP, OC_BYTES, OC_FIXBYTES, OC_STRING, OC_ENUM, OC_BIGINT, OC_DURATION, OC_UNION},
		// decode:  []OpCode{OC_U64, OC_I64, OC_I32, OC_I16, OC_I8, OC_U64, OC_U32, OC_U16, OC_U8, OC_F64, OC_F32, OC_D32, OC_D64, OC_D128, OC_D256, OC_I128, OC_I256, OC_BOOL, OC_TIMESTAMP, OC_BYTES, OC_FIXBYTES, OC_STRING, OC_ENUM, OC_BIGINT, OC_DURATION, OC_UNION},
	},

	// fixed size array bytes and string
	{
		name:  "array_types",
		infer: reflect.SchemaFor[ArrayTypes],
		build: func(opts ...schema.Option) *schema.Schema {
			return schema.SchemaOf([]*schema.Field{
				schema.FieldOf(schema.Uint64, schema.WithName("id"), schema.WithFlags(schema.FlagPrimary)),
				schema.ArrayOf(schema.Bytes, 20, schema.WithName("byte_array")),
				schema.FieldOf(schema.String, schema.WithName("string_array"), schema.WithArray(20)),
			}, append(opts, schema.Name("array_types"))...)
		},
		fields:  "id,byte_array,string_array",
		typs:    []schema.FieldType{schema.Uint64, schema.Bytes, schema.String},
		flags:   []schema.FieldFlags{schema.FlagPrimary, 0, 0},
		scales:  []uint8{0, 0, 0},
		fixed:   []uint8{0, 20, 20},
		isFixed: true,
		// encode:  []OpCode{OC_U64, OC_FIXBYTES, OC_FIXSTRING},
		// decode:  []OpCode{OC_U64, OC_FIXBYTES, OC_FIXSTRING},
	},

	// date/time/timestamp
	{
		name:  "time_types",
		infer: reflect.SchemaFor[TimeTypes],
		build: func(opts ...schema.Option) *schema.Schema {
			return schema.SchemaOf([]*schema.Field{
				schema.FieldOf(schema.Timestamp, schema.WithName("tsn"), schema.WithScale(schema.TIME_SCALE_NANO)),
				schema.FieldOf(schema.Timestamp, schema.WithName("tsu"), schema.WithScale(schema.TIME_SCALE_MICRO)),
				schema.FieldOf(schema.Timestamp, schema.WithName("tsm"), schema.WithScale(schema.TIME_SCALE_MILLI)),
				schema.FieldOf(schema.Timestamp, schema.WithName("tss"), schema.WithScale(schema.TIME_SCALE_SECOND)),
				schema.FieldOf(schema.Time, schema.WithName("tmn"), schema.WithScale(schema.TIME_SCALE_NANO)),
				schema.FieldOf(schema.Time, schema.WithName("tmu"), schema.WithScale(schema.TIME_SCALE_MICRO)),
				schema.FieldOf(schema.Time, schema.WithName("tmm"), schema.WithScale(schema.TIME_SCALE_MILLI)),
				schema.FieldOf(schema.Time, schema.WithName("tms"), schema.WithScale(schema.TIME_SCALE_SECOND)),
				schema.FieldOf(schema.Date, schema.WithName("dt")),
				schema.FieldOf(schema.Duration, schema.WithName("dns"), schema.WithScale(schema.TIME_SCALE_NANO)),
				schema.FieldOf(schema.Duration, schema.WithName("dus"), schema.WithScale(schema.TIME_SCALE_MICRO)),
				schema.FieldOf(schema.Duration, schema.WithName("dms"), schema.WithScale(schema.TIME_SCALE_MILLI)),
				schema.FieldOf(schema.Duration, schema.WithName("ds"), schema.WithScale(schema.TIME_SCALE_SECOND)),
			}, append(opts, schema.Name("time_types"))...)
		},
		fields:  "tsn,tsu,tsm,tss,tmn,tmu,tmm,tms,dt,dns,dus,dms,ds",
		typs:    []schema.FieldType{schema.Timestamp, schema.Timestamp, schema.Timestamp, schema.Timestamp, schema.Time, schema.Time, schema.Time, schema.Time, schema.Date, schema.Duration, schema.Duration, schema.Duration, schema.Duration},
		flags:   []schema.FieldFlags{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		scales:  []uint8{0, 1, 2, 3, 0, 1, 2, 3, 4, 0, 1, 2, 3},
		fixed:   []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		isFixed: true,
		// encode:  []OpCode{OC_TIMESTAMP, OC_TIMESTAMP, OC_TIMESTAMP, OC_TIMESTAMP, OC_TIME, OC_TIME, OC_TIME, OC_TIME, OC_DATE, OC_DURATION, OC_DURATION, OC_DURATION, OC_DURATION},
		// decode:  []OpCode{OC_TIMESTAMP, OC_TIMESTAMP, OC_TIMESTAMP, OC_TIMESTAMP, OC_TIME, OC_TIME, OC_TIME, OC_TIME, OC_DATE, OC_DURATION, OC_DURATION, OC_DURATION, OC_DURATION},
	},

	// array > max array size
	{
		name:  "large_array_to_blob",
		infer: reflect.SchemaFor[LargeArrayToBlob],
		build: func(opts ...schema.Option) *schema.Schema {
			return schema.SchemaOf([]*schema.Field{
				schema.FieldOf(schema.Uint64, schema.WithName("id"), schema.WithFlags(schema.FlagPrimary)),
				schema.FieldOf(schema.Binary, schema.WithName("f"), schema.WithNullable(false)),
			}, append(opts, schema.Name("large_array_to_blob"))...)
		},
		fields:  "id,f",
		typs:    []schema.FieldType{schema.Uint64, schema.Binary},
		flags:   []schema.FieldFlags{schema.FlagPrimary, 0},
		scales:  []uint8{0, 0},
		fixed:   []uint8{0, 0},
		isFixed: false,
	},

	// list fields
	{
		name:  "list_fields",
		infer: reflect.SchemaFor[ListFields],
		build: func(opts ...schema.Option) *schema.Schema {
			return schema.SchemaOf([]*schema.Field{
				schema.FieldOf(schema.Int64, schema.WithName("int64a")),
				schema.ListOf(schema.Uint64, schema.WithName("u64_list")),
				schema.ListOf(schema.Date, schema.WithName("time_list")),
				schema.ListFor(
					schema.SchemaOf([]*schema.Field{
						schema.FieldOf(schema.Int64, schema.WithName("k64")),
						schema.FieldOf(schema.Int64, schema.WithName("v64")),
					}, schema.Name("pair")),
					schema.WithName("pair_list"),
				),
				schema.ListOf(schema.Bytes, schema.WithName("byte_list"), schema.WithNullable(false)),
				schema.ListFor(
					schema.SchemaOf([]*schema.Field{
						schema.ArrayOf(schema.Bytes, 2),
					}),
					schema.WithName("arr_list"),
					schema.WithNullable(false),
				),
				schema.ListFor(
					schema.SchemaOf([]*schema.Field{
						schema.FieldOf(schema.Decimal32, schema.WithScale(4)),
					}),
					schema.WithName("dec_list"),
					schema.WithNullable(false),
				),
				schema.FieldOf(schema.Int64, schema.WithName("int64b")),
			}, append(opts, schema.Name("list_fields"))...)
		},
		fields:  "int64a,u64_list,u64_list.element,time_list,time_list.element,pair_list,pair_list.element.k64,pair_list.element.v64,byte_list,byte_list.element,arr_list,arr_list.element,dec_list,dec_list.element,int64b",
		typs:    []schema.FieldType{schema.Int64, schema.List, schema.Uint64, schema.List, schema.Date, schema.List, schema.Int64, schema.Int64, schema.List, schema.Bytes, schema.List, schema.Bytes, schema.List, schema.Decimal32, schema.Int64},
		flags:   []schema.FieldFlags{0, schema.FlagNullable, 0, schema.FlagNullable, 0, schema.FlagNullable, 0, 0, 0, schema.FlagNullable, 0, 0, 0, 0, 0},
		scales:  []uint8{0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0},
		fixed:   []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0},
		isFixed: false,
		// encode:  []OpCode{},
		// decode:  []OpCode{},
	},

	// map fields
	{
		name:  "map_fields",
		infer: reflect.SchemaFor[MapFields],
		build: func(opts ...schema.Option) *schema.Schema {
			return schema.SchemaOf([]*schema.Field{
				schema.FieldOf(schema.Int64, schema.WithName("int64a")),
				schema.MapOf(schema.Uint64, schema.Uint64, schema.WithName("u64_map")),
				schema.MapOf(schema.Uint32, schema.Date, schema.WithName("date_map")),
				schema.MapFor(
					schema.String,
					schema.SchemaOf([]*schema.Field{
						schema.FieldOf(schema.Int64, schema.WithName("k64")),
						schema.FieldOf(schema.Int64, schema.WithName("v64")),
					}, schema.Name("pair")),
					schema.WithName("pair_map"),
				),
				schema.MapFor(
					schema.String,
					schema.SchemaOf([]*schema.Field{
						schema.FieldOf(schema.Bytes, schema.WithNullable(false)),
					}),
					schema.WithName("byte_map"), schema.WithNullable(false),
				),
				schema.MapFor(
					schema.String,
					schema.SchemaOf([]*schema.Field{
						schema.ArrayOf(schema.Bytes, 2),
					}),
					schema.WithName("arr_map"),
					schema.WithNullable(false),
				),
				schema.MapFor(
					schema.String,
					schema.SchemaOf([]*schema.Field{
						schema.FieldOf(schema.Decimal32, schema.WithScale(4)),
					}),
					schema.WithName("dec_map"),
					schema.WithNullable(false),
				),
				schema.FieldOf(schema.Int64, schema.WithName("int64b")),
			}, append(opts, schema.Name("map_fields"))...)
		},
		fields:  "int64a,u64_map,u64_map.key,u64_map.value,date_map,date_map.key,date_map.value,pair_map,pair_map.key,pair_map.value.k64,pair_map.value.v64,byte_map,byte_map.key,byte_map.value,arr_map,arr_map.key,arr_map.value,dec_map,dec_map.key,dec_map.value,int64b",
		typs:    []schema.FieldType{schema.Int64, schema.Map, schema.Uint64, schema.Uint64, schema.Map, schema.Uint32, schema.Date, schema.Map, schema.String, schema.Int64, schema.Int64, schema.Map, schema.String, schema.Bytes, schema.Map, schema.String, schema.Bytes, schema.Map, schema.String, schema.Decimal32, schema.Int64},
		flags:   []schema.FieldFlags{0, schema.FlagNullable, 0, 0, schema.FlagNullable, 0, 0, schema.FlagNullable, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		scales:  []uint8{0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0},
		fixed:   []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0},
		isFixed: false,
		// encode:  []OpCode{},
		// decode:  []OpCode{},
	},

	// error: native int/uint
	{
		name:  "invalid_native_types",
		infer: reflect.SchemaFor[InvalidNativeTypes],
		iserr: true,
	},

	// error: unsupported struct binary & text (un)marshaler
	{
		name:  "struct (un)marshaler",
		infer: reflect.SchemaFor[MarshalerStructTypes],
		iserr: true,
	},

	// error: unsupported map binary & text (un)marshaler
	{
		name:  "struct (un)marshaler",
		infer: reflect.SchemaFor[MarshalerMapTypes],
		iserr: true,
	},

	// error: unsupported slice type without marshaler
	{
		name:  "no map marshaler",
		infer: reflect.SchemaFor[NoMarshalerMapTypes],
		iserr: true,
	},

	// error: unsupported ptr type (TODO: may use for null)
	{
		name:  "invalid pointer",
		infer: reflect.SchemaFor[InvalidPointerType],
		iserr: true,
	},

	// error: using array on illegal type
	{
		name:  "invalid array type",
		infer: reflect.SchemaFor[InvalidArrayType],
		iserr: true,
	},

	// error: array value missing
	{
		name:  "invalid array missing",
		infer: reflect.SchemaFor[InvalidArrayMissing],
		iserr: true,
	},

	// error: array NaN
	{
		name:  "invalid array NaN",
		infer: reflect.SchemaFor[InvalidArrayNaN],
		iserr: true,
	},

	// error: array = 0
	{
		name:  "invalid array=0",
		infer: reflect.SchemaFor[InvalidArrayZero],
		iserr: true,
	},

	// error: array < 0
	{
		name:  "invalid array<0",
		infer: reflect.SchemaFor[InvalidArrayNeg],
		iserr: true,
	},

	// error: array size mismatch
	{
		name:  "invalid array size mismatch",
		infer: reflect.SchemaFor[InvalidArraySizeMismatch],
		iserr: true,
	},

	// error: using scale on illegal type
	{
		name:  "invalid scale type",
		infer: reflect.SchemaFor[InvalidScaleType],
		iserr: true,
	},

	// error: scale value missing
	{
		name:  "invalid scale missing",
		infer: reflect.SchemaFor[InvalidScaleMissing],
		iserr: true,
	},

	// error: scale NaN
	{
		name:  "invalid scale NaN",
		infer: reflect.SchemaFor[InvalidScaleNaN],
		iserr: true,
	},

	// error: scale < 0
	{
		name:  "invalid scale<0",
		infer: reflect.SchemaFor[InvalidScaleNeg],
		iserr: true,
	},

	// error: decimal out of range
	{
		name:  "invalid scale too large",
		infer: reflect.SchemaFor[InvalidScaleTooLarge],
		iserr: true,
	},

	//
	// Primary key tests
	// -----------------

	// error: pk type != uint64
	{
		name:  "no_uint64_pk",
		infer: reflect.SchemaFor[InvalidPkType],
		iserr: true,
	},

	// error: duplicate pk field
	{
		name:  "duplicate_pk",
		infer: reflect.SchemaFor[InvalidDuplicatePkType],
		iserr: true,
	},

	// error: duplicate field name
	{
		name:  "duplicate_name",
		infer: reflect.SchemaFor[InvalidDuplicateName],
		iserr: true,
	},

	//
	// Other tests
	// -----------------

	// bloom filter
	{
		name:  "bloom_filter",
		infer: reflect.SchemaFor[BloomFilter],
		build: func(opts ...schema.Option) *schema.Schema {
			return schema.SchemaOf([]*schema.Field{
				schema.FieldOf(schema.Uint64, schema.WithName("id"), schema.WithFlags(schema.FlagPrimary)),
				schema.FieldOf(schema.Int64, schema.WithName("i64"), schema.WithFilter(schema.BloomFilter3b)),
			}, append(opts, schema.Name("bloom_filter"))...)
		},
		fields:  "id,i64",
		typs:    []schema.FieldType{schema.Uint64, schema.Int64},
		flags:   []schema.FieldFlags{schema.FlagPrimary, 0},
		filters: []schema.FilterType{0, schema.BloomFilter3b},
		scales:  []uint8{0, 0},
		fixed:   []uint8{0, 0},
		isFixed: true,
		// encode:    []OpCode{OC_U64, OC_I64},
		// decode:    []OpCode{OC_U64, OC_I64},
	},

	//
	// Metadata tests
	// -----------------
	{
		name:  "meta_fields",
		infer: reflect.SchemaFor[MetaFields],
		build: func(opts ...schema.Option) *schema.Schema {
			return schema.SchemaOf([]*schema.Field{
				schema.FieldOf(schema.Uint64, schema.WithName("id"), schema.WithFlags(schema.FlagPrimary)),
				schema.FieldOf(schema.Int64, schema.WithName("i64"), schema.WithFlags(schema.FlagMetadata)),
				schema.FieldOf(schema.Uint64, schema.WithName("u64")),
			}, append(opts, schema.Name("meta_fields"))...)
		},
		fields:  "id,i64,u64",
		typs:    []schema.FieldType{schema.Uint64, schema.Int64, schema.Uint64},
		flags:   []schema.FieldFlags{schema.FlagPrimary, schema.FlagMetadata, 0},
		scales:  []uint8{0, 0, 0},
		fixed:   []uint8{0, 0, 0},
		isFixed: true,
		// encode:  []OpCode{OC_U64, OC_SKIP, OC_U64},
		// decode:  []OpCode{OC_U64, OC_SKIP, OC_U64},
	},
}

func TestSchemaDetect(t *testing.T) {
	for _, c := range schemaTestCases {
		t.Run(c.name, func(t *testing.T) {
			// check test data consistency
			require.NotNil(t, c.infer, "must define SchemaFor[T] function in testcase")
			numFields := len(strings.Split(c.fields, ","))
			if len(c.fields) == 0 {
				numFields = 0
			}
			require.Len(t, c.typs, numFields, "types")
			require.Len(t, c.flags, numFields, "flags")
			require.Len(t, c.scales, numFields, "scales")
			require.Len(t, c.fixed, numFields, "fixed")

			checkSchema := func(s *schema.Schema) {
				// schema name
				require.Equal(t, c.name, s.Name, "schema name")

				// field names
				require.ElementsMatch(t, strings.Split(c.fields, ","), s.Names(), "field names")

				// field types
				for i, f := range s.Fields {
					require.Equal(t, c.typs[i], f.Type, "field types for "+f.Name)
				}

				// field flags
				for i, f := range s.Fields {
					require.Equal(t, c.flags[i], f.Flags, "field flags for "+f.Name)
				}

				// filters
				if len(c.filters) > 0 {
					for i, f := range s.Fields {
						require.Equal(t, c.filters[i], f.Filter, "field filter for "+f.Name)
					}
				}

				// scale values
				for i, f := range s.Fields {
					if !f.IsArray() {
						require.Equal(t, c.scales[i], f.Scale, "scale for "+f.Name)
					}
				}

				// fixed values
				for i, f := range s.Fields {
					if f.IsArray() {
						require.Equal(t, c.fixed[i], f.Scale, "fixed for "+f.Name)
					}
				}

				// is fixed
				require.Equal(t, c.isFixed, s.IsFixedSize, "is_fixed")
			}

			var inferenceHash uint64
			t.Run("inference", func(t *testing.T) {
				// schema inferance from Go type
				s, err := c.infer()
				if c.iserr {
					require.Error(t, err)
					t.Log(err)
					return
				} else {
					require.NoError(t, err)
					require.NoError(t, s.Validate())
				}
				t.Log(s.String())
				checkSchema(s)
				inferenceHash = s.Hash
			})

			if c.build != nil {
				t.Run("builder", func(t *testing.T) {
					// schema inferance from Go type
					s := c.build()
					require.NoError(t, s.Validate())
					t.Log(s.String())
					checkSchema(s)
					require.Equal(t, inferenceHash, s.Hash)
				})
			}
		})
	}
}

func TestSchemaMarshal(t *testing.T) {
	for _, s := range []*schema.Schema{
		reflect.MustSchemaFor[AllTypes](schema.Enums(enums)),
		reflect.MustSchemaFor[ArrayTypes](),
		reflect.MustSchemaFor[TimeTypes](),
		reflect.MustSchemaFor[ListFields](),
		reflect.MustSchemaFor[MapFields](),
		reflect.MustSchemaFor[MetaFields](),
		listFieldsT,
		listInListT,
		listInStructInListT,
		customerT,
	} {
		t.Run(s.Name, func(t *testing.T) {
			buf, err := s.MarshalBinary()
			require.NoError(t, err)
			require.NotNil(t, buf)

			r := &schema.Schema{}
			err = r.UnmarshalBinary(buf)
			require.NoError(t, err)

			assert.True(t, s.Equal(r))
			assert.Equal(t, s.Hash, r.Hash)
			assert.Equal(t, s.Version, r.Version)
			assert.Equal(t, s.Name, r.Name)
			assert.Equal(t, s.IsFixedSize, r.IsFixedSize)
			assert.Equal(t, s.MinWireSize, r.MinWireSize)
			assert.Equal(t, s.NumFields(), r.NumFields())
			assert.Equal(t, s.NumActive(), r.NumActive())
			assert.Equal(t, s.NumVisible(), r.NumVisible())
			assert.Equal(t, s.Names(), r.Names())
			assert.Equal(t, s.Ids(), r.Ids())
			assert.Equal(t, s.PkId(), r.PkId())
			assert.Equal(t, s.PkIndex(), r.PkIndex())
		})
	}
}

// TestSchemaIsValid checks if the Schema.IsValid() method correctly identifies
// valid and invalid schema configurations.
func TestSchemaIsValid(t *testing.T) {
	s := schema.SchemaOf(nil)
	require.False(t, s.IsValid())

	s = schema.SchemaOf(nil, schema.Name("test"))
	require.False(t, s.IsValid())

	s = schema.SchemaOf([]*schema.Field{
		schema.FieldOf(schema.Int64, schema.WithName("field1")),
	})
	require.True(t, s.IsValid())
}

// TestSchemaNewBuffer verifies that Schema.NewBuffer() creates a buffer with
// the correct capacity based on the schema's maxWireSize.
func TestSchemaNewBuffer(t *testing.T) {
	s := schema.SchemaOf(
		[]*schema.Field{schema.FieldOf(schema.Int64, schema.WithName("field1"))},
		schema.Name("test"),
	)

	buf := s.NewBuffer(10)
	require.NotNil(t, buf)
	require.Equal(t, 10*s.EstWireSize, buf.Cap())
}

// TestSchemaNumFields ensures that Schema.NumFields() returns the correct
// number of fields in the schema.
func TestSchemaNumFields(t *testing.T) {
	s := schema.SchemaOf(
		[]*schema.Field{
			schema.FieldOf(schema.Int64, schema.WithName("field1")),
			schema.FieldOf(schema.String, schema.WithName("field2")),
			schema.ListFor(
				schema.SchemaOf([]*schema.Field{
					schema.FieldOf(schema.Int64, schema.WithName("field3")),
					schema.FieldOf(schema.Int64, schema.WithName("field4")),
				}),
			),
		},
		schema.Name("test"),
	)

	require.Equal(t, 3, s.NumFields())
	require.Equal(t, 2, s.Fields[2].Child.NumFields())
}

// TestSchemaFieldVisibility tests correct handling of internal/deleted
// flags and whether returned field info is in correct order.
func TestSchemaFieldVisibility(t *testing.T) {
	s := schema.SchemaOf(
		[]*schema.Field{
			schema.FieldOf(schema.Int64, schema.WithName("field1")),
			schema.FieldOf(schema.String, schema.WithName("field2"), schema.WithFlags(schema.FlagMetadata)),
			schema.FieldOf(schema.Uint64, schema.WithName("field3"), schema.WithFlags(schema.FlagDeleted)),
			schema.FieldOf(schema.Uint64, schema.WithName("field4"), schema.WithFlags(schema.FlagDeleted|schema.FlagMetadata)),
		},
		schema.Name("test"),
	)

	// counts
	require.Equal(t, 4, s.NumFields())
	require.Equal(t, 1, s.NumVisible())
	require.Equal(t, 2, s.NumActive())
	// require.Equal(t, 1, s.NumMeta())

	// ids
	require.Equal(t, []uint16{1, 2, 3, 4}, s.Ids())
	// require.Equal(t, []uint16{1}, s.VisibleIds())
	require.Equal(t, []uint16{1, 2}, s.ActiveIds())
	// require.Equal(t, []uint16{2}, s.MetaIds())

	// names
	require.Equal(t, []string{"field1", "field2", "field3", "field4"}, s.Names())
	// require.Equal(t, []string{"field1", "field2"}, s.ActiveNames())
	// require.Equal(t, []string{"field1"}, s.VisibleNames())
	// require.Equal(t, []string{"field2"}, s.MetaNames())

	// by name should hide deleted fields
	_, ok := s.Find("field1")
	require.True(t, ok)
	_, ok = s.Find("field2")
	require.True(t, ok)
	_, ok = s.Find("field3")
	require.False(t, ok)
	_, ok = s.Find("field4")
	require.False(t, ok)

	// index by name should hide deleted fields
	_, ok = s.Index("field1")
	require.True(t, ok)
	_, ok = s.Index("field2")
	require.True(t, ok)
	_, ok = s.Index("field3")
	require.False(t, ok)
	_, ok = s.Index("field4")
	require.False(t, ok)

	// by id should show all fields
	_, ok = s.FindId(1)
	require.True(t, ok)
	_, ok = s.FindId(2)
	require.True(t, ok)
	_, ok = s.FindId(3)
	require.True(t, ok)
	_, ok = s.FindId(4)
	require.True(t, ok)
}

// TestSchemaCanMatch checks if Schema.CanMatch() correctly
// identifies when a set of field names matches the schema.
func TestSchemaCanMatch(t *testing.T) {
	s := schema.SchemaOf(
		[]*schema.Field{
			schema.FieldOf(schema.Int64, schema.WithName("field1")),
			schema.FieldOf(schema.String, schema.WithName("field2"), schema.WithFlags(schema.FlagMetadata)),
			schema.FieldOf(schema.Uint64, schema.WithName("field3"), schema.WithFlags(schema.FlagDeleted)),
			schema.FieldOf(schema.Uint64, schema.WithName("field4"), schema.WithFlags(schema.FlagDeleted|schema.FlagMetadata)),
		},
		schema.Name("test"),
	)

	require.True(t, s.CanMatch("field1", "field2"))
	require.False(t, s.CanMatch("field3"))
	require.False(t, s.CanMatch("field4"))
}

// TestSchemaCanSelect verifies that Schema.CanSelect() correctly determines
// if one schema can be selected from another.
func TestSchemaContainsSchema(t *testing.T) {
	s := schema.SchemaOf(
		[]*schema.Field{
			schema.FieldOf(schema.Int64, schema.WithName("field1")),
			schema.FieldOf(schema.String, schema.WithName("field2"), schema.WithFlags(schema.FlagMetadata)),
			schema.FieldOf(schema.Uint64, schema.WithName("field3"), schema.WithFlags(schema.FlagDeleted)),
			schema.FieldOf(schema.Uint64, schema.WithName("field4"), schema.WithFlags(schema.FlagDeleted|schema.FlagMetadata)),
		},
		schema.Name("test"),
	)

	// active field
	s1 := schema.SchemaOf(
		[]*schema.Field{schema.FieldOf(schema.Int64, schema.WithName("field1"))},
		schema.Name("test1"),
	)
	require.True(t, s.ContainsSchema(s1))

	// active internal field
	s2 := schema.SchemaOf(
		[]*schema.Field{schema.FieldOf(schema.String, schema.WithName("field2"))},
		schema.Name("test2"),
	)
	require.True(t, s.ContainsSchema(s2))

	// deleted field
	s3 := schema.SchemaOf(
		[]*schema.Field{schema.FieldOf(schema.Uint64, schema.WithName("field3"))},
		schema.Name("test3"),
	)
	require.False(t, s.ContainsSchema(s3))

	// deleted internal field
	s4 := schema.SchemaOf(
		[]*schema.Field{schema.FieldOf(schema.Uint64, schema.WithName("field4"))},
		schema.Name("test4"),
	)
	require.False(t, s.ContainsSchema(s4))

	// non existing field
	s5 := schema.SchemaOf(
		[]*schema.Field{schema.FieldOf(schema.Uint64, schema.WithName("field5"))},
		schema.Name("test5"),
	)
	require.False(t, s.ContainsSchema(s5))
}

// TestSchemaSort checks if Schema.Sort() correctly sorts the fields
// of the schema by id
func TestSchemaSort(t *testing.T) {
	s := schema.SchemaOf(
		[]*schema.Field{
			schema.FieldOf(schema.String, schema.WithName("field2")),
			schema.FieldOf(schema.Int64, schema.WithName("field1")),
		},
		schema.Name("test"),
	)

	// The fields should already be sorted by ID after Finalize()
	require.Equal(t, "field2", s.Fields[0].Name, "First field should be 'field2' (id=1)")
	require.Equal(t, "field1", s.Fields[1].Name, "Second field should be 'field1' (id=2)")

	// Calling Sort() shouldn't change the order
	s.Sort()

	require.Equal(t, "field2", s.Fields[0].Name, "First field should still be 'field2' (id=1) after sorting")
	require.Equal(t, "field1", s.Fields[1].Name, "Second field should still be 'field1' (id=2) after sorting")
}

// TestSchemaMapSchema verifies that Schema.MapSchema() correctly maps fields
// from one schema to another, even if the field order is different.
func TestSchemaMapSchema(t *testing.T) {
	s := schema.SchemaOf(
		[]*schema.Field{
			schema.FieldOf(schema.Int64, schema.WithName("field1")),
			schema.FieldOf(schema.String, schema.WithName("field2"), schema.WithFlags(schema.FlagMetadata)),
			schema.FieldOf(schema.Uint64, schema.WithName("field3"), schema.WithFlags(schema.FlagDeleted)),
			schema.FieldOf(schema.Uint64, schema.WithName("field4"), schema.WithFlags(schema.FlagDeleted|schema.FlagMetadata)),
		},
		schema.Name("test"),
	)

	// active fields
	s1 := schema.SchemaOf(
		[]*schema.Field{
			schema.FieldOf(schema.Uint64, schema.WithName("field3")),
			schema.FieldOf(schema.Int64, schema.WithName("field1")),
		},
		schema.Name("test1"),
	)

	// inactive fields are hidden
	mapping, err := s.MapSchema(s1)
	require.NoError(t, err)
	require.Equal(t, []int{-1, 0}, mapping)

	// deleted fields are ignored
	s2 := schema.SchemaOf(
		[]*schema.Field{
			schema.FieldOf(schema.String, schema.WithName("field2")),
			schema.FieldOf(schema.Uint64, schema.WithName("field4")),
			schema.FieldOf(schema.Uint64, schema.WithName("field3")),
			schema.FieldOf(schema.Int64, schema.WithName("field1")),
		},
		schema.Name("test2"),
	)

	mapping, err = s.MapSchema(s2)
	require.NoError(t, err)
	require.Equal(t, []int{-1, -1, -1, 0}, mapping)
}

func TestSchemaDeleteField(t *testing.T) {
	s, err := reflect.SchemaFor[AllTypes]()
	require.NoError(t, err)
	beforeSz := s.MinWireSize
	beforeAll := len(s.Fields)
	beforeLen := s.NumFields()
	beforeVisible := s.NumVisible()
	beforeActive := s.NumActive()
	beforeHash := s.Hash
	beforeVersion := s.Version
	beforeFieldNames := s.Names()
	beforeFieldIds := s.Ids()

	s, err = s.DeleteId(2)
	require.NoError(t, err)

	require.Equal(t, beforeAll, len(s.Fields))
	require.Equal(t, beforeLen, s.NumFields())
	require.Equal(t, beforeFieldNames, s.Names())
	require.Equal(t, beforeFieldIds, s.Ids())

	require.Equal(t, beforeVisible-1, s.NumVisible(), "num visible fields must change")
	require.Equal(t, beforeActive-1, s.NumActive(), "num active fields must change")
	require.NotEqual(t, beforeFieldIds, s.ActiveIds())

	require.Less(t, s.MinWireSize, beforeSz, "wire size must change")
	require.NotEqual(t, beforeHash, s.Hash, "hash must change")
	require.Less(t, beforeVersion, s.Version, "version must increase")

	_, ok := s.Find("i64")
	require.False(t, ok, "deleted field is no longer accessible by name")
	f, ok := s.FindId(2)
	require.True(t, ok, "deleted field still accessible by id")
	require.False(t, f.IsVisible(), "deleted field is invisibile")
	require.False(t, s.CanMatch("id", "i64"), "cannot match deleted field")
	_, err = s.SelectIds(1, 2)
	require.Error(t, err, "cannot select deleted field")
}

func TestNestedMarshalFromInference(t *testing.T) {
	for _, v := range []any{
		ListFields{},
		ListInListFields{},
		ListInStructInListFields{},
	} {
		s, err := reflect.SchemaOf(v)
		require.NoError(t, err)
		t.Log(s)
		require.NoError(t, s.Validate())
		buf, err := s.MarshalBinary()
		require.NoError(t, err)
		require.NotNil(t, buf)

		r := &schema.Schema{}
		err = r.UnmarshalBinary(buf)
		require.NoError(t, err)

		assert.True(t, s.Equal(r))
		assert.Equal(t, s.Hash, r.Hash)
		assert.Equal(t, s.Version, r.Version)
		assert.Equal(t, s.Name, r.Name)
		assert.Equal(t, s.IsFixedSize, r.IsFixedSize)
		assert.Equal(t, s.MinWireSize, r.MinWireSize)
		assert.Equal(t, s.NumFields(), r.NumFields(), "s=%s\nr=%s", s, r)
		assert.Equal(t, s.NumActive(), r.NumActive())
		assert.Equal(t, s.NumVisible(), r.NumVisible())
		assert.Equal(t, s.Names(), r.Names())
		assert.Equal(t, s.Ids(), r.Ids())
		assert.Equal(t, s.PkId(), r.PkId())
		assert.Equal(t, s.PkIndex(), r.PkIndex())
		t.Log(r)
	}
}

func TestNestedMarshalFromBuilder(t *testing.T) {
	for _, s := range []*schema.Schema{
		listFieldsT,
		listInListT,
		listInStructInListT,
		customerT,
	} {
		t.Log(s)
		require.NoError(t, s.Validate())
		buf, err := s.MarshalBinary()
		require.NoError(t, err)
		require.NotNil(t, buf)

		r := &schema.Schema{}
		err = r.UnmarshalBinary(buf)
		require.NoError(t, err)
		t.Log(r)

		assert.True(t, s.Equal(r))
		assert.Equal(t, s.Hash, r.Hash)
		assert.Equal(t, s.Version, r.Version)
		assert.Equal(t, s.Name, r.Name)
		assert.Equal(t, s.IsFixedSize, r.IsFixedSize)
		assert.Equal(t, s.MinWireSize, r.MinWireSize)
		assert.Equal(t, s.NumFields(), r.NumFields(), "s=%s\nr=%s", s, r)
		assert.Equal(t, s.NumActive(), r.NumActive())
		assert.Equal(t, s.NumVisible(), r.NumVisible())
		assert.Equal(t, s.Names(), r.Names())
		assert.Equal(t, s.Ids(), r.Ids())
		assert.Equal(t, s.PkId(), r.PkId())
		assert.Equal(t, s.PkIndex(), r.PkIndex())
	}
}

func TestSchemaRegistry(t *testing.T) {
	reg := schema.NewRegistry()
	require.True(t, reg.Register(listFieldsT))
	require.True(t, reg.Register(listInListT))
	require.True(t, reg.Register(listInStructInListT))
	require.True(t, reg.Register(customerT))
	require.True(t, reg.Register(reflect.MustSchemaFor[AllTypes](schema.Enums(enums))))

	// create a version
	listFieldsT2, err := listFieldsT.DeleteId(1)
	require.NoError(t, err)
	require.True(t, reg.Register(listFieldsT2))

	// lookup by version
	ctx := context.Background()
	s, ok := reg.LookupSchemaName(ctx, listFieldsT.Name, listFieldsT.Version)
	require.True(t, ok)
	require.NotNil(t, s)
	require.Equal(t, listFieldsT.Hash, s.Hash)

	s, ok = reg.LookupSchemaName(ctx, listFieldsT.Name, listFieldsT2.Version)
	require.True(t, ok)
	require.NotNil(t, s)
	require.Equal(t, listFieldsT2.Hash, s.Hash)

	// lookup by hash
	s, ok = reg.LookupSchemaHash(ctx, listFieldsT.Hash)
	require.True(t, ok)
	require.NotNil(t, s)
	require.Equal(t, listFieldsT.Hash, s.Hash)

	s, ok = reg.LookupSchemaHash(ctx, listFieldsT2.Hash)
	require.True(t, ok)
	require.NotNil(t, s)
	require.Equal(t, listFieldsT2.Hash, s.Hash)
}

func TestSchemaExportImport(t *testing.T) {
	for _, s := range []*schema.Schema{
		reflect.MustSchemaFor[AllTypes](schema.Enums(enums)),
		reflect.MustSchemaFor[ArrayTypes](),
		reflect.MustSchemaFor[TimeTypes](),
		reflect.MustSchemaFor[ListFields](),
		reflect.MustSchemaFor[MapFields](),
		reflect.MustSchemaFor[MetaFields](),
		listFieldsT,
		listInListT,
		listInStructInListT,
		customerT,
	} {
		t.Run(s.Name, func(t *testing.T) {
			m := s.Export()
			r := new(schema.Schema)
			require.NoError(t, r.Import(m))
			require.Equal(t, s.Hash, r.Hash, "map roundtrip failed")

			// json marshal
			buf, err := json.Marshal(m)
			require.NoError(t, err)
			m2 := make(map[string]any)
			require.NoError(t, json.Unmarshal(buf, &m2))
			u := new(schema.Schema)
			require.NoError(t, u.Import(m2))
			require.Equal(t, s.Hash, u.Hash, "json rountrip failed")
		})
	}
}
