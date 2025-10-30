// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/hex"
	"math/bits"
	"strings"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type schemaTest struct {
	name      string
	build     func() (*Schema, error)
	fields    string
	idxfields string
	idxtyps   []IndexType
	typs      []FieldType
	flags     []FieldFlags
	filters   []FilterType
	scales    []uint8
	fixed     []uint16
	isFixed   bool
	encode    []OpCode
	decode    []OpCode
	iserr     bool
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

type NoModelTagName struct {
	Id uint64 `knox:"tagid,pk"`
}

type ModelName struct {
	BaseModel // defines id as pk
}

func (ModelName) Key() string { return "model_name" }

type NoModelPrivate struct {
	NoModelTagName         // anon embed will promote fields
	_              string  // non exported
	B              string  `knox:"-"` // exported but skipped
	_              [2]byte // padding
}

type AllTypes struct {
	BaseModel
	Int64   int64          `knox:"i64"`
	Int32   int32          `knox:"i32"`
	Int16   int16          `knox:"i16"`
	Int8    int8           `knox:"i8"`
	Uint64  uint64         `knox:"u64"`
	Uint32  uint32         `knox:"u32"`
	Uint16  uint16         `knox:"u16"`
	Uint8   uint8          `knox:"u8"`
	Float64 float64        `knox:"f64"`
	Float32 float32        `knox:"f32"`
	D32     num.Decimal32  `knox:"d32,scale=5"`
	D64     num.Decimal64  `knox:"d64,scale=15"`
	D128    num.Decimal128 `knox:"d128,scale=18"`
	D256    num.Decimal256 `knox:"d256,scale=24"`
	I128    num.Int128     `knox:"i128"`
	I256    num.Int256     `knox:"i256"`
	Bool    bool           `knox:"bool"`
	Time    time.Time      `knox:"time"`
	Hash    []byte         `knox:"bytes"`
	Array   [2]byte        `knox:"array[2]"`
	String  string         `knox:"string"`
	MyEnum  MyEnum         `knox:"my_enum,enum"`
	Big     num.Big        `knox:"big"`
}

func NewAllTypes(i int64) AllTypes {
	return AllTypes{
		BaseModel: BaseModel{
			Id: uint64(i),
		},
		Int64:   i,
		Int32:   int32(i),
		Int16:   int16(i),
		Int8:    int8(i),
		Uint64:  uint64(i),
		Uint32:  uint32(i),
		Uint16:  uint16(i),
		Uint8:   uint8(i),
		Float64: float64(i),
		Float32: float32(i),
		D32:     num.NewDecimal32(int32(i), 5),
		D64:     num.NewDecimal64(i, 15),
		D128:    num.NewDecimal128(num.Int128FromInt64(i), 18),
		D256:    num.NewDecimal256(num.Int256FromInt64(i), 24),
		I128:    num.Int128FromInt64(i),
		I256:    num.Int256FromInt64(i),
		Bool:    i%2 == 1,
		Time:    time.Unix(0, i).UTC(),
		Hash:    util.U64Bytes(uint64(i)),
		Array:   [2]byte{byte(i >> 8 & 0xf), byte(i & 0xf)},
		String:  util.U64Hex(uint64(i)),
		MyEnum:  MyEnum("a"),
		Big:     num.NewBig(i),
	}
}

type FixedTypes struct {
	BaseModel
	FixedBytes  [20]byte `knox:"fixed_bytes"`
	FixedString string   `knox:"fixed_string,fixed=20"`
}

func NewFixedTypes(i int64) FixedTypes {
	buf := bytes.Repeat(util.U64Bytes(uint64(i)), 3)[:20]
	return FixedTypes{
		BaseModel: BaseModel{
			Id: uint64(i),
		},
		FixedBytes:  [20]byte(buf),
		FixedString: hex.EncodeToString(buf[:10]),
	}
}

type NativeTypes struct {
	BaseModel
	Int  int  `knox:"int"`
	Uint uint `knox:"uint"`
}

type TimeTypes struct {
	TimestampNs time.Time `knox:"tsn,timestamp,scale=ns"`
	TimestampUs time.Time `knox:"tsu,timestamp,scale=us"`
	TimestampMs time.Time `knox:"tsm,timestamp,scale=ms"`
	TimestampS  time.Time `knox:"tss,timestamp,scale=s"`
	TimeNs      time.Time `knox:"tmn,time,scale=ns"`
	TimeUs      time.Time `knox:"tmu,time,scale=us"`
	TimeMs      time.Time `knox:"tmm,time,scale=ms"`
	TimeS       time.Time `knox:"tms,time,scale=s"`
	Date        time.Time `knox:"dt,date"`
}

type MarshalerTypes struct {
	BaseModel
	Stringer Stringer `knox:"stringer"`
	Byter    Byter    `knox:"byter"`
}

type MarshalerStructTypes struct {
	BaseModel
	Stringer StringerStruct `knox:"stringer"`
	Byter    ByterStruct    `knox:"byter"`
}

type MarshalerMapTypes struct {
	BaseModel
	Map MapType `knox:"map"`
}

type NoMarshalerTypes struct {
	BaseModel
	Embed MarshalerStructTypes `knox:"no_marshalers"`
}

type NoMarshalerSliceTypes struct {
	BaseModel
	Slice []int64 `knox:"no_marshalers"`
}

type OtherStruct struct {
	Other uint64
}

type MultipleAnonStructs struct {
	NoModelTagName // Id, tag: tagid,pk
	OtherStruct    // Other
}

// Fields with the same name at the same depth
// cancel one another out. reflect.VisibleFields()
// will not return such fields and we cannot use them.
type MultipleAnonStructsWithCanceledNames struct {
	NoModelTagName // Id
	NoModelNoTag   // Id
}

type NoMarshalerMapTypes struct {
	BaseModel
	Map map[int]int `knox:"no_map"`
}

type PointerTypes struct {
	BaseModel
	Ptr *int `knox:"ptr"`
}

type DuplicatePkType struct {
	BaseModel
	Val uint64 `knox:"val,pk"`
}

type DuplicateAnonPkType struct {
	BaseModel
	NoModelTag
	NoModelNoTag
}

type DuplicateField struct {
	BaseModel
	A int64 `knox:"x"`
	B int64 `knox:"x"`
}

type InvalidFixedType struct {
	BaseModel
	F int64 `knox:",fixed=1"`
}

type InvalidFixedMissing struct {
	BaseModel
	F []byte `knox:",fixed"`
}

type InvalidFixedNaN struct {
	BaseModel
	F []byte `knox:",fixed=x"`
}

type InvalidFixedZero struct {
	BaseModel
	F []byte `knox:",fixed=0"`
}

type InvalidFixedNeg struct {
	BaseModel
	F []byte `knox:",fixed=-1"`
}

type InvalidFixedTooLarge struct {
	BaseModel
	F [20]byte `knox:",fixed=21"`
}

type InvalidScaleType struct {
	BaseModel
	F int64 `knox:",scale=1"`
}

type InvalidScaleMissing struct {
	BaseModel
	D num.Decimal32 `knox:",scale"`
}

type InvalidScaleNaN struct {
	BaseModel
	D num.Decimal32 `knox:",scale=x"`
}

type InvalidScaleNeg struct {
	BaseModel
	D num.Decimal32 `knox:",scale=-1"`
}

type InvalidScaleTooLarge struct {
	BaseModel
	D num.Decimal32 `knox:",scale=36"`
}

type HashIndex struct {
	BaseModel
	Hash [32]byte `knox:"hash,index=hash"`
}

type IntegerIndex struct {
	BaseModel
	Int int64 `knox:"i64,index=int"`
}

type BloomFilter struct {
	BaseModel
	Int int64 `knox:"i64,filter=bloom3b"`
}

type InvalidIndexType struct {
	BaseModel
	Int int64 `knox:",index=undefined"`
}

type InvalidIndexFieldType struct {
	BaseModel
	B []byte `knox:",index=int"`
}

type InvalidBloomFilter struct {
	BaseModel
	B []byte `knox:",index=bloomx"`
}

type MetaFields struct {
	BaseModel
	I64 int64  `knox:"i64,metadata"`
	U64 uint64 `knox:"u64"`
}

const (
	OC_I8        = OpCodeInt8
	OC_I16       = OpCodeInt16
	OC_I32       = OpCodeInt32
	OC_I64       = OpCodeInt64
	OC_U8        = OpCodeUint8
	OC_U16       = OpCodeUint16
	OC_U32       = OpCodeUint32
	OC_U64       = OpCodeUint64
	OC_F32       = OpCodeFloat32
	OC_F64       = OpCodeFloat64
	OC_BOOL      = OpCodeBool
	OC_FIXSTRING = OpCodeFixedString
	OC_FIXBYTES  = OpCodeFixedBytes
	OC_STRING    = OpCodeString
	OC_BYTES     = OpCodeBytes
	OC_TIMESTAMP = OpCodeTimestamp
	OC_I128      = OpCodeInt128
	OC_I256      = OpCodeInt256
	OC_D32       = OpCodeDecimal32
	OC_D64       = OpCodeDecimal64
	OC_D128      = OpCodeDecimal128
	OC_D256      = OpCodeDecimal256
	OC_ENUM      = OpCodeEnum
	OC_SKIP      = OpCodeSkip
	OC_BIGINT    = OpCodeBigInt
	OC_DATE      = OpCodeDate
	OC_TIME      = OpCodeTime
)

var (
	// arch dependent, only used for tests
	FT_INT  = [2]FieldType{FT_I32, FT_I64}[bits.UintSize/32-1]
	FT_UINT = [2]FieldType{FT_U32, FT_U64}[bits.UintSize/32-1]
	OC_INT  = [2]OpCode{OC_I32, OC_I64}[bits.UintSize/32-1]
	OC_UINT = [2]OpCode{OC_U32, OC_U64}[bits.UintSize/32-1]
)

// Testcase Definition
// -------------------
//
//	{
//	    name:    "",
//	    fields:  "",
//	    indexes: "",
//	    typs:    []FieldType{},
//	    flags:   []FieldFlags{},
//	    scales:  []uint8{},
//	    fixed:   []uint16{},
//	    isFixed: true,
//	    encode:  []OpCode{},
//	    decode:  []OpCode{},
//	    err:     false,
//	},
var schemaTestCases = []schemaTest{
	//
	// Schema name tests
	// -----------------

	// schema name from Go type
	{
		name:    "no_model_tag",
		build:   GenericSchema[NoModelTag],
		fields:  "id",
		typs:    []FieldType{FT_U64},
		flags:   []FieldFlags{F_PRIMARY},
		scales:  []uint8{0},
		fixed:   []uint16{0},
		isFixed: true,
		encode:  []OpCode{OC_U64},
		decode:  []OpCode{OC_U64},
	},

	// schema name from Model type
	{
		name:    "model_name",
		build:   GenericSchema[ModelName],
		fields:  "id",
		typs:    []FieldType{FT_U64},
		flags:   []FieldFlags{F_PRIMARY},
		scales:  []uint8{0},
		fixed:   []uint16{0},
		isFixed: true,
		encode:  []OpCode{OC_U64},
		decode:  []OpCode{OC_U64},
	},

	// error: invalid generic type
	{
		name:  "invalid_T",
		build: GenericSchema[Model],
		iserr: true,
	},

	//
	// Field name tests
	// -----------------

	// struct names only, private and anon fields
	{
		name:    "no_model_private",
		build:   GenericSchema[NoModelPrivate],
		fields:  "tagid",
		typs:    []FieldType{FT_U64},
		flags:   []FieldFlags{F_PRIMARY},
		scales:  []uint8{0},
		fixed:   []uint16{0},
		isFixed: true,
		encode:  []OpCode{OC_U64},
		decode:  []OpCode{OC_U64},
	},

	// struct tag names replace struct names
	{
		name:    "no_model_tag_name",
		build:   GenericSchema[NoModelTagName],
		fields:  "tagid",
		typs:    []FieldType{FT_U64},
		flags:   []FieldFlags{F_PRIMARY},
		scales:  []uint8{0},
		fixed:   []uint16{0},
		isFixed: true,
		encode:  []OpCode{OC_U64},
		decode:  []OpCode{OC_U64},
	},

	// multiple anon (embedded) structs
	{
		name:    "multiple_anon_structs",
		build:   GenericSchema[MultipleAnonStructs],
		fields:  "tagid,other",
		typs:    []FieldType{FT_U64, FT_U64},
		flags:   []FieldFlags{F_PRIMARY, 0},
		scales:  []uint8{0, 0},
		fixed:   []uint16{0, 0},
		isFixed: true,
		encode:  []OpCode{OC_U64, OC_U64},
		decode:  []OpCode{OC_U64, OC_U64},
	},

	// error: non-struct type
	{
		name:  "no struct type",
		build: GenericSchema[[]string],
		iserr: true,
	},

	// error: canceled field names (empty list)
	{
		name:  "all names canceled",
		build: GenericSchema[MultipleAnonStructsWithCanceledNames],
		iserr: true,
	},

	//
	// Field type tests
	// -----------------

	// all supported types
	{
		name:    "all_types",
		build:   GenericSchema[AllTypes],
		fields:  "id,i64,i32,i16,i8,u64,u32,u16,u8,f64,f32,d32,d64,d128,d256,i128,i256,bool,time,bytes,array[2],string,my_enum,big",
		typs:    []FieldType{FT_U64, FT_I64, FT_I32, FT_I16, FT_I8, FT_U64, FT_U32, FT_U16, FT_U8, FT_F64, FT_F32, FT_D32, FT_D64, FT_D128, FT_D256, FT_I128, FT_I256, FT_BOOL, FT_TIMESTAMP, FT_BYTES, FT_BYTES, FT_STRING, FT_U16, FT_BIGINT},
		flags:   []FieldFlags{F_PRIMARY, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, types.FieldFlagEnum, 0},
		scales:  []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 15, 18, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		fixed:   []uint16{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0},
		isFixed: false,
		encode:  []OpCode{OC_U64, OC_I64, OC_I32, OC_I16, OC_I8, OC_U64, OC_U32, OC_U16, OC_U8, OC_F64, OC_F32, OC_D32, OC_D64, OC_D128, OC_D256, OC_I128, OC_I256, OC_BOOL, OC_TIMESTAMP, OC_BYTES, OC_FIXBYTES, OC_STRING, OC_ENUM, OC_BIGINT},
		decode:  []OpCode{OC_U64, OC_I64, OC_I32, OC_I16, OC_I8, OC_U64, OC_U32, OC_U16, OC_U8, OC_F64, OC_F32, OC_D32, OC_D64, OC_D128, OC_D256, OC_I128, OC_I256, OC_BOOL, OC_TIMESTAMP, OC_BYTES, OC_FIXBYTES, OC_STRING, OC_ENUM, OC_BIGINT},
	},

	// fixed bytes and string
	{
		name:    "fixed_types",
		build:   GenericSchema[FixedTypes],
		fields:  "id,fixed_bytes,fixed_string",
		typs:    []FieldType{FT_U64, FT_BYTES, FT_STRING},
		flags:   []FieldFlags{F_PRIMARY, 0, 0},
		scales:  []uint8{0, 0, 0},
		fixed:   []uint16{0, 20, 20},
		isFixed: true,
		encode:  []OpCode{OC_U64, OC_FIXBYTES, OC_FIXSTRING},
		decode:  []OpCode{OC_U64, OC_FIXBYTES, OC_FIXSTRING},
	},

	// // struct with binary & text (un)marshaler
	// {
	// 	name:    "marshaler_struct_types",
	// 	build:   GenericSchema[MarshalerStructTypes],
	// 	fields:  "id,stringer,byter",
	// 	typs:    []FieldType{FT_U64, FT_STRING, FT_BYTES},
	// 	flags:   []FieldFlags{F_PRIMARY, 0, 0},
	// 	scales:  []uint8{0, 0, 0},
	// 	fixed:   []uint16{0, 0, 0},
	// 	isFixed: false,
	// 	encode:  []OpCode{OC_U64, OC_MSHTXT, OC_MSHBIN},
	// 	decode:  []OpCode{OC_U64, OC_USHTXT, OC_USHBIN},
	// },

	// // map with binary & text (un)marshaler
	// {
	// 	name:    "marshaler_map_types",
	// 	build:   GenericSchema[MarshalerMapTypes],
	// 	fields:  "id,map",
	// 	typs:    []FieldType{FT_U64, FT_BYTES},
	// 	flags:   []FieldFlags{F_PRIMARY, 0},
	// 	scales:  []uint8{0, 0},
	// 	fixed:   []uint16{0, 0},
	// 	isFixed: false,
	// 	encode:  []OpCode{OC_U64, OC_MSHBIN},
	// 	decode:  []OpCode{OC_U64, OC_USHBIN},
	// },

	// // slice with binary & text (un)marshaler
	// {
	// 	name:    "marshaler_types",
	// 	build:   GenericSchema[MarshalerTypes],
	// 	fields:  "id,stringer,byter",
	// 	typs:    []FieldType{FT_U64, FT_STRING, FT_BYTES},
	// 	flags:   []FieldFlags{F_PRIMARY | F_INDEXED, 0, 0},
	// 	scales:  []uint8{0, 0, 0},
	// 	fixed:   []uint16{0, 0, 0},
	// 	isFixed: false,
	// 	encode:  []OpCode{OC_U64, OC_MSHTXT, OC_MSHBIN},
	// 	decode:  []OpCode{OC_U64, OC_USHTXT, OC_USHBIN},
	// },

	// native int/uint
	{
		name:    "native_types",
		build:   GenericSchema[NativeTypes],
		fields:  "id,int,uint",
		typs:    []FieldType{FT_U64, FT_INT, FT_UINT},
		flags:   []FieldFlags{F_PRIMARY, 0, 0},
		scales:  []uint8{0, 0, 0},
		fixed:   []uint16{0, 0, 0},
		isFixed: true,
		encode:  []OpCode{OC_U64, OC_INT, OC_UINT},
		decode:  []OpCode{OC_U64, OC_INT, OC_UINT},
	},

	// date/time/timestamp
	{
		name:    "time_types",
		build:   GenericSchema[TimeTypes],
		fields:  "tsn,tsu,tsm,tss,tmn,tmu,tmm,tms,dt",
		typs:    []FieldType{FT_TIMESTAMP, FT_TIMESTAMP, FT_TIMESTAMP, FT_TIMESTAMP, FT_TIME, FT_TIME, FT_TIME, FT_TIME, FT_DATE},
		flags:   []FieldFlags{0, 0, 0, 0, 0, 0, 0, 0, 0},
		scales:  []uint8{0, 1, 2, 3, 0, 1, 2, 3, 4},
		fixed:   []uint16{0, 0, 0, 0, 0, 0, 0, 0, 0},
		isFixed: true,
		encode:  []OpCode{OC_TIMESTAMP, OC_TIMESTAMP, OC_TIMESTAMP, OC_TIMESTAMP, OC_TIME, OC_TIME, OC_TIME, OC_TIME, OC_DATE},
		decode:  []OpCode{OC_TIMESTAMP, OC_TIMESTAMP, OC_TIMESTAMP, OC_TIMESTAMP, OC_TIME, OC_TIME, OC_TIME, OC_TIME, OC_DATE},
	},

	// error: unsupported struct binary & text (un)marshaler
	{
		name:  "struct (un)marshaler",
		build: GenericSchema[MarshalerStructTypes],
		iserr: true,
	},

	// error: unsupported map binary & text (un)marshaler
	{
		name:  "struct (un)marshaler",
		build: GenericSchema[MarshalerMapTypes],
		iserr: true,
	},

	// error: unsupported slice binary & text (un)marshaler
	{
		name:  "slice (un)marshaler",
		build: GenericSchema[MarshalerTypes],
		iserr: true,
	},

	// error: unsupported struct type without marshaler
	{
		name:  "no struct marshaler",
		build: GenericSchema[NoMarshalerTypes],
		iserr: true,
	},

	// error: unsupported slice type without marshaler
	{
		name:  "no slice marshaler",
		build: GenericSchema[NoMarshalerSliceTypes],
		iserr: true,
	},

	// error: unsupported slice type without marshaler
	{
		name:  "no map marshaler",
		build: GenericSchema[NoMarshalerMapTypes],
		iserr: true,
	},

	// error: unsupported ptr type
	{
		name:  "invalid pointer",
		build: GenericSchema[PointerTypes],
		iserr: true,
	},

	// error: using fixed on illegal type
	{
		name:  "invalid fixed type",
		build: GenericSchema[InvalidFixedType],
		iserr: true,
	},

	// error: fixed value missing
	{
		name:  "invalid fixed missing",
		build: GenericSchema[InvalidFixedMissing],
		iserr: true,
	},

	// error: fixed NaN
	{
		name:  "invalid fixed NaN",
		build: GenericSchema[InvalidFixedNaN],
		iserr: true,
	},

	// error: fixed = 0
	{
		name:  "invalid fixed=0",
		build: GenericSchema[InvalidFixedZero],
		iserr: true,
	},

	// error: fixed < 0
	{
		name:  "invalid fixed<0",
		build: GenericSchema[InvalidFixedNeg],
		iserr: true,
	},

	// error: fixed > array bounds
	{
		name:  "invalid fixed too large",
		build: GenericSchema[InvalidFixedTooLarge],
		iserr: true,
	},

	// error: using scale on illegal type
	{
		name:  "invalid scale type",
		build: GenericSchema[InvalidScaleType],
		iserr: true,
	},

	// error: scale value missing
	{
		name:  "invalid scale missing",
		build: GenericSchema[InvalidScaleMissing],
		iserr: true,
	},

	// error: scale NaN
	{
		name:  "invalid scale NaN",
		build: GenericSchema[InvalidScaleNaN],
		iserr: true,
	},

	// error: scale < 0
	{
		name:  "invalid scale<0",
		build: GenericSchema[InvalidScaleNeg],
		iserr: true,
	},

	// error: decimal out of range
	{
		name:  "invalid scale too large",
		build: GenericSchema[InvalidScaleTooLarge],
		iserr: true,
	},

	//
	// Primary key tests
	// -----------------

	// error: missing pk field
	// {
	// 	name:  "no_model_no_tag",
	// 	build: GenericSchema[NoModelNoTag],
	// 	iserr: true,
	// },

	// error: pk type != uint64
	{
		name:  "no_uint64_pk",
		build: GenericSchema[InvalidPkType],
		iserr: true,
	},

	// error: duplicate pk field
	// {
	// 	name:  "duplicate_pk",
	// 	build: GenericSchema[DuplicatePkType],
	// 	iserr: true,
	// },

	// error: duplicate pk field in anon struct
	{
		name:  "duplicate_anon_pk",
		build: GenericSchema[DuplicateAnonPkType],
		iserr: true,
	},

	// error: duplicate field name
	{
		name:  "duplicate_field",
		build: GenericSchema[DuplicateField],
		iserr: true,
	},

	//
	// Index tests
	// -----------------

	// hash index
	{
		name:      "hash_index",
		build:     GenericSchema[HashIndex],
		fields:    "id,hash",
		typs:      []FieldType{FT_U64, FT_BYTES},
		flags:     []FieldFlags{F_PRIMARY, 0},
		idxfields: "id,hash",
		idxtyps:   []types.IndexType{I_PK, I_HASH},
		scales:    []uint8{0, 0},
		fixed:     []uint16{0, 32},
		isFixed:   true,
		encode:    []OpCode{OC_U64, OC_FIXBYTES},
		decode:    []OpCode{OC_U64, OC_FIXBYTES},
	},

	// integer index
	{
		name:      "integer_index",
		build:     GenericSchema[IntegerIndex],
		fields:    "id,i64",
		typs:      []FieldType{FT_U64, FT_I64},
		flags:     []FieldFlags{F_PRIMARY, 0},
		idxfields: "id,i64",
		idxtyps:   []types.IndexType{I_PK, I_INT},
		scales:    []uint8{0, 0},
		fixed:     []uint16{0, 0},
		isFixed:   true,
		encode:    []OpCode{OC_U64, OC_I64},
		decode:    []OpCode{OC_U64, OC_I64},
	},

	// bloom filter
	{
		name:      "bloom_filter",
		build:     GenericSchema[BloomFilter],
		fields:    "id,i64",
		typs:      []FieldType{FT_U64, FT_I64},
		flags:     []FieldFlags{F_PRIMARY, 0},
		filters:   []FilterType{0, FL_BLOOM3B},
		idxfields: "id,i64",
		idxtyps:   []types.IndexType{I_PK, 0},
		scales:    []uint8{0, 0},
		fixed:     []uint16{0, 0},
		isFixed:   true,
		encode:    []OpCode{OC_U64, OC_I64},
		decode:    []OpCode{OC_U64, OC_I64},
	},

	// error: invalid index type
	{
		name:  "invalid index type",
		build: GenericSchema[InvalidIndexType],
		iserr: true,
	},

	// error: invalid field type for index (int: only (u)int fields)
	{
		name:  "invalid index field type",
		build: GenericSchema[InvalidIndexFieldType],
		iserr: true,
	},

	// error: invalid bloom filter
	{
		name:  "invalid bloom filter name",
		build: GenericSchema[InvalidBloomFilter],
		iserr: true,
	},

	//
	// Metadata tests
	// -----------------
	{
		name:    "meta_fields",
		build:   GenericSchema[MetaFields],
		fields:  "id,i64,u64",
		typs:    []FieldType{FT_U64, FT_I64, FT_U64},
		flags:   []FieldFlags{F_PRIMARY, F_METADATA, 0},
		scales:  []uint8{0, 0, 0},
		fixed:   []uint16{0, 0, 0},
		isFixed: true,
		encode:  []OpCode{OC_U64, OC_SKIP, OC_U64},
		decode:  []OpCode{OC_U64, OC_SKIP, OC_U64},
	},
}

func TestSchemaDetect(t *testing.T) {
	for _, c := range schemaTestCases {
		t.Run(c.name, func(t *testing.T) {
			// check test data consistency
			require.NotNil(t, c.build, "must define GenericSchema[T] function in testcase")
			numFields := len(strings.Split(c.fields, ","))
			if len(c.fields) == 0 {
				numFields = 0
			}
			require.Len(t, c.typs, numFields)
			require.Len(t, c.flags, numFields)
			if len(c.idxfields) > 0 {
				require.Len(t, c.idxtyps, len(strings.Split(c.idxfields, ",")))
			}
			require.Len(t, c.scales, numFields)
			require.Len(t, c.fixed, numFields)
			require.Len(t, c.encode, numFields)
			require.Len(t, c.decode, numFields)

			s, err := c.build()
			if c.iserr {
				require.Error(t, err)
				t.Log(err)
				return
			} else {
				require.NoError(t, err)
				require.NoError(t, s.Validate())
			}
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
			if len(c.idxfields) > 0 {
				allIndexNames := strings.Split(c.idxfields, ",")
				// every index is detected
				// for _, v := range allIndexNames {
				// 	f, ok := s.Find(v)
				// 	require.True(t, ok)
				// 	require.NotNil(t, f.Index)
				// 	require.NotZero(t, f.Index.Type)
				// }

				// every detected index is expected and has correct type
				for i, idx := range s.Indexes {
					// index name is expected
					require.Contains(t, allIndexNames, idx.Fields[0].Name, "unexpected index %s on field %s", idx.Name, idx.Fields[0].Name)
					// index types
					require.Equal(t, c.idxtyps[i], idx.Type, "wrong index type for "+idx.Name)
				}
			}
			// scale values
			for i, f := range s.Fields {
				require.Equal(t, c.scales[i], f.Scale, "scale for "+f.Name)
			}

			// fixed values
			for i, f := range s.Fields {
				require.Equal(t, c.fixed[i], f.Fixed, "fixed for "+f.Name)
			}
			// is fixed
			require.Equal(t, c.isFixed, s.IsFixedSize, "is_fixed")
			// encoder opcodes
			require.ElementsMatch(t, c.encode, s.Encode, "encoders")
			// decoder opcodes
			require.ElementsMatch(t, c.decode, s.Decode, "decoders")
		})
	}
}

func TestSchemaMarshal(t *testing.T) {
	s, err := GenericSchema[AllTypes]()
	require.NoError(t, err)
	buf, err := s.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	r := &Schema{}
	err = r.UnmarshalBinary(buf)
	require.NoError(t, err)

	assert.True(t, s.Equal(r))
	assert.Equal(t, s.Hash, r.Hash)
	assert.Equal(t, s.Version, r.Version)
	assert.Equal(t, s.Name, r.Name)
	assert.Equal(t, s.IsFixedSize, r.IsFixedSize)
	assert.Equal(t, s.WireSize(), r.WireSize())
	assert.Equal(t, s.NumFields(), r.NumFields())
	assert.Equal(t, s.NumActive(), r.NumActive())
	assert.Equal(t, s.NumVisible(), r.NumVisible())
	assert.Equal(t, s.NumMeta(), r.NumMeta())
	assert.Equal(t, s.Names(), r.Names())
	assert.Equal(t, s.Ids(), r.Ids())
	assert.Equal(t, s.VisibleIds(), r.VisibleIds())
	assert.Equal(t, s.MetaIds(), r.MetaIds())
	assert.Equal(t, s.PkId(), r.PkId())
	assert.Equal(t, s.PkIndex(), r.PkIndex())
}

// TestSchemaIsValid checks if the Schema.IsValid() method correctly identifies
// valid and invalid schema configurations.
func TestSchemaIsValid(t *testing.T) {
	s := NewSchema().WithName("test")
	require.False(t, s.IsValid())

	s.WithField(&Field{Name: "field1", Type: FT_I64})
	require.False(t, s.IsValid())

	s.Finalize()
	require.True(t, s.IsValid())
}

// TestSchemaNewBuffer verifies that Schema.NewBuffer() creates a buffer with
// the correct capacity based on the schema's maxWireSize.
func TestSchemaNewBuffer(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(&Field{Name: "field1", Type: FT_I64}).
		Finalize()

	buf := s.NewBuffer(10)
	require.NotNil(t, buf)
	require.Equal(t, 10*s.MaxWireSize, buf.Cap())
}

// TestSchemaNumFields ensures that Schema.NumFields() returns the correct
// number of fields in the schema.
func TestSchemaNumFields(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(&Field{Name: "field1", Type: FT_I64}).
		WithField(&Field{Name: "field2", Type: FT_STRING}).
		Finalize()

	require.Equal(t, 2, s.NumFields())
}

// TestSchemaFieldVisibility tests correct handling of internal/deleted
// flags and whether returned field info is in correct order.
func TestSchemaFieldVisibility(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(&Field{
			Name: "field1",
			Type: FT_I64,
		}).
		WithField(&Field{
			Name:  "field2",
			Type:  FT_STRING,
			Flags: types.FieldFlagMetadata,
		}).
		WithField(&Field{
			Name:  "field3",
			Type:  FT_U64,
			Flags: types.FieldFlagDeleted,
		}).
		WithField(&Field{
			Name:  "field4",
			Type:  FT_U64,
			Flags: types.FieldFlagMetadata | types.FieldFlagDeleted,
		}).
		Finalize()

	// counts
	require.Equal(t, 4, s.NumFields())
	require.Equal(t, 1, s.NumVisible())
	require.Equal(t, 2, s.NumActive())
	require.Equal(t, 1, s.NumMeta())

	// ids
	require.Equal(t, []uint16{1, 2, 3, 4}, s.Ids())
	require.Equal(t, []uint16{1}, s.VisibleIds())
	require.Equal(t, []uint16{1, 2}, s.ActiveIds())
	require.Equal(t, []uint16{2}, s.MetaIds())

	// names
	require.Equal(t, []string{"field1", "field2", "field3", "field4"}, s.Names())
	require.Equal(t, []string{"field1", "field2"}, s.ActiveNames())
	require.Equal(t, []string{"field1"}, s.VisibleNames())
	require.Equal(t, []string{"field2"}, s.MetaNames())

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
	s := NewSchema().WithName("test").
		WithField(&Field{
			Name: "field1",
			Type: FT_I64,
		}).
		WithField(&Field{
			Name:  "field2",
			Type:  FT_STRING,
			Flags: types.FieldFlagMetadata,
		}).
		WithField(&Field{
			Name:  "field3",
			Type:  FT_U64,
			Flags: types.FieldFlagDeleted,
		}).
		WithField(&Field{
			Name:  "field4",
			Type:  FT_U64,
			Flags: types.FieldFlagMetadata | types.FieldFlagDeleted,
		}).
		Finalize()

	require.True(t, s.CanMatch("field1", "field2"))
	require.False(t, s.CanMatch("field3"))
	require.False(t, s.CanMatch("field4"))
}

// TestSchemaCanSelect verifies that Schema.CanSelect() correctly determines
// if one schema can be selected from another.
func TestSchemaContainsSchema(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(&Field{
			Name: "field1",
			Type: FT_I64,
		}).
		WithField(&Field{
			Name:  "field2",
			Type:  FT_STRING,
			Flags: types.FieldFlagMetadata,
		}).
		WithField(&Field{
			Name:  "field3",
			Type:  FT_U64,
			Flags: types.FieldFlagDeleted,
		}).
		WithField(&Field{
			Name:  "field4",
			Type:  FT_U64,
			Flags: types.FieldFlagMetadata | types.FieldFlagDeleted,
		}).
		Finalize()

	// active field
	s1 := NewSchema().WithName("test1").
		WithField(&Field{Name: "field1", Type: FT_I64}).
		Finalize()

	require.True(t, s.ContainsSchema(s1))

	// active internal field
	s2 := NewSchema().WithName("test2").
		WithField(&Field{Name: "field2", Type: FT_STRING}).
		Finalize()

	require.True(t, s.ContainsSchema(s2))

	// deleted field
	s3 := NewSchema().WithName("test3").
		WithField(&Field{Name: "field3", Type: FT_U64}).
		Finalize()

	require.False(t, s.ContainsSchema(s3))

	// deleted internal field
	s4 := NewSchema().WithName("test4").
		WithField(&Field{Name: "field4", Type: FT_U64}).
		Finalize()

	require.False(t, s.ContainsSchema(s4))

	// non existing field
	s5 := NewSchema().WithName("test5").
		WithField(&Field{Name: "field5", Type: FT_U64}).
		Finalize()

	require.False(t, s.ContainsSchema(s5))
}

// TestSchemaSort checks if Schema.Sort() correctly sorts the fields
// of the schema alphabetically by name.
func TestSchemaSort(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(&Field{Name: "field2", Type: FT_STRING}).
		WithField(&Field{Name: "field1", Type: FT_I64}).
		Finalize()

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
	s := NewSchema().WithName("test").
		WithField(&Field{
			Name: "field1",
			Type: FT_I64,
		}).
		WithField(&Field{
			Name:  "field2",
			Type:  FT_STRING,
			Flags: types.FieldFlagMetadata,
		}).
		WithField(&Field{
			Name:  "field3",
			Type:  FT_U64,
			Flags: types.FieldFlagDeleted,
		}).
		WithField(&Field{
			Name:  "field4",
			Type:  FT_U64,
			Flags: types.FieldFlagMetadata | types.FieldFlagDeleted,
		}).
		Finalize()

	// active fields
	s1 := NewSchema().WithName("test1").
		WithField(&Field{Name: "field3", Type: FT_U64}).
		WithField(&Field{Name: "field1", Type: FT_I64}).
		Finalize()

	// inactive fields are hidden
	mapping, err := s.MapSchema(s1)
	require.NoError(t, err)
	require.Equal(t, []int{-1, 0}, mapping)

	// deleted fields are ignored
	s2 := NewSchema().WithName("test2").
		WithField(&Field{Name: "field2", Type: FT_STRING}).
		WithField(&Field{Name: "field4", Type: FT_U64}).
		WithField(&Field{Name: "field3", Type: FT_U64}).
		WithField(&Field{Name: "field1", Type: FT_I64}).
		Finalize()

	mapping, err = s.MapSchema(s2)
	require.NoError(t, err)
	require.Equal(t, []int{1, -1, -1, 0}, mapping)
}

func TestSchemaDeleteField(t *testing.T) {
	s, err := GenericSchema[AllTypes]()
	require.NoError(t, err)
	beforeSz := s.WireSize()
	beforeLen := s.NumFields()
	beforeHash := s.Hash
	beforeVersion := s.Version
	beforeFieldNames := s.Names()
	beforeFieldIds := s.Ids()

	s, err = s.DeleteId(2)
	require.NoError(t, err)

	require.Len(t, s.Fields, beforeLen)
	require.Equal(t, beforeFieldNames, s.Names())
	require.Equal(t, beforeFieldIds, s.Ids())
	require.NotEqual(t, beforeFieldNames, s.ActiveNames())
	require.NotEqual(t, beforeFieldIds, s.ActiveIds())
	require.NotEqual(t, beforeFieldNames, s.VisibleNames())
	require.NotEqual(t, beforeFieldIds, s.VisibleIds())

	require.Equal(t, s.NumFields()-1, s.NumVisible(), "num visible fields must change")
	require.Equal(t, s.NumFields()-1, s.NumActive(), "num active fields must change")
	require.Less(t, s.WireSize(), beforeSz, "wire size must change")
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
