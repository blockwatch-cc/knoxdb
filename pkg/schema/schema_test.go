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
	idxtyps   []types.IndexType
	typs      []types.FieldType
	flags     []types.FieldFlags
	scales    []uint8
	fixed     []uint16
	isFixed   bool
	encode    []OpCode
	decode    []OpCode
	iserr     bool
}

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

type Byter [20]byte

func (b Byter) MarshalBinary() ([]byte, error) {
	return b[:], nil
}

func (b *Byter) UnmarshalBinary(buf []byte) error {
	copy((*b)[:], buf)
	return nil
}

type StringerStruct struct{}

func (s StringerStruct) MarshalText() ([]byte, error) {
	return []byte{}, nil
}

func (s *StringerStruct) UnmarshalText(b []byte) error {
	return nil
}

type ByterStruct struct{}

func (s ByterStruct) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

func (s *ByterStruct) UnmarshalBinary(b []byte) error {
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

func (_ ModelName) Key() string { return "model_name" }

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
		Big:     num.NewBig(int64(i)),
	}
}

type FixedTypes struct {
	BaseModel
	FixedArray  [20]byte `knox:"fixed_array"`
	FixedBytes  []byte   `knox:"fixed_bytes,fixed=20"`
	FixedString string   `knox:"fixed_string,fixed=20"`
}

func NewFixedTypes(i int64) FixedTypes {
	buf := bytes.Repeat(util.U64Bytes(uint64(i)), 3)[:20]
	return FixedTypes{
		BaseModel: BaseModel{
			Id: uint64(i),
		},
		FixedArray:  [20]byte(buf),
		FixedBytes:  buf,
		FixedString: hex.EncodeToString(buf[:10]),
	}
}

type NativeTypes struct {
	BaseModel
	Int  int  `knox:"int"`
	Uint uint `knox:"uint"`
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

type MapType map[int]int

func (_ MapType) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

func (_ *MapType) UnmarshalBinary(_ []byte) error {
	return nil
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
	Hash []byte `knox:"hash,index=hash,fixed=32"`
}

type IntegerIndex struct {
	BaseModel
	Int int64 `knox:"i64,index=int"`
}

type BloomIndex struct {
	BaseModel
	Int int64 `knox:"i64,index=bloom:3"`
}

type InvalidIndexType struct {
	BaseModel
	Int int64 `knox:",index=undefined"`
}

type InvalidIndexFieldType struct {
	BaseModel
	B []byte `knox:",index=int"`
}

type InvalidIndexBloomScaleNeg struct {
	BaseModel
	B []byte `knox:",index=bloom:-1"`
}

type InvalidIndexBloomScaleNaN struct {
	BaseModel
	B []byte `knox:",index=bloom:x"`
}

type InvalidIndexBloomScaleTooLarge struct {
	BaseModel
	B []byte `knox:",index=bloom:10"`
}

type InternalFields struct {
	BaseModel
	I64 int64  `knox:"i64,internal"`
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
	OC_FIXARRAY  = OpCodeFixedArray
	OC_FIXSTRING = OpCodeFixedString
	OC_FIXBYTES  = OpCodeFixedBytes
	OC_STRING    = OpCodeString
	OC_BYTES     = OpCodeBytes
	OC_TIME      = OpCodeDateTime
	OC_I128      = OpCodeInt128
	OC_I256      = OpCodeInt256
	OC_D32       = OpCodeDecimal32
	OC_D64       = OpCodeDecimal64
	OC_D128      = OpCodeDecimal128
	OC_D256      = OpCodeDecimal256
	OC_MSHBIN    = OpCodeMarshalBinary
	OC_MSHTXT    = OpCodeMarshalText
	OC_MSHSTR    = OpCodeStringer
	OC_USHBIN    = OpCodeUnmarshalBinary
	OC_USHTXT    = OpCodeUnmarshalText
	OC_ENUM      = OpCodeEnum
	OC_SKIP      = OpCodeSkip
	OC_BIGINT    = OpCodeBigInt
)

var (
	// arch dependent, only used for tests
	FT_INT  = [2]types.FieldType{FT_I32, FT_I64}[bits.UintSize/32-1]
	FT_UINT = [2]types.FieldType{FT_U32, FT_U64}[bits.UintSize/32-1]
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
//	    typs:    []types.FieldType{},
//	    flags:   []types.FieldFlags{},
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
		typs:    []types.FieldType{FT_U64},
		flags:   []types.FieldFlags{types.FieldFlagPrimary},
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
		typs:    []types.FieldType{FT_U64},
		flags:   []types.FieldFlags{types.FieldFlagPrimary},
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
		typs:    []types.FieldType{FT_U64},
		flags:   []types.FieldFlags{types.FieldFlagPrimary},
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
		typs:    []types.FieldType{FT_U64},
		flags:   []types.FieldFlags{types.FieldFlagPrimary},
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
		typs:    []types.FieldType{FT_U64, FT_U64},
		flags:   []types.FieldFlags{types.FieldFlagPrimary, 0},
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
		typs:    []types.FieldType{FT_U64, FT_I64, FT_I32, FT_I16, FT_I8, FT_U64, FT_U32, FT_U16, FT_U8, FT_F64, FT_F32, FT_D32, FT_D64, FT_D128, FT_D256, FT_I128, FT_I256, FT_BOOL, FT_TIME, FT_BYTES, FT_BYTES, FT_STRING, FT_U16, FT_BIGINT},
		flags:   []types.FieldFlags{types.FieldFlagPrimary, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, types.FieldFlagEnum, 0},
		scales:  []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 15, 18, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		fixed:   []uint16{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0},
		isFixed: false,
		encode:  []OpCode{OC_U64, OC_I64, OC_I32, OC_I16, OC_I8, OC_U64, OC_U32, OC_U16, OC_U8, OC_F64, OC_F32, OC_D32, OC_D64, OC_D128, OC_D256, OC_I128, OC_I256, OC_BOOL, OC_TIME, OC_BYTES, OC_FIXARRAY, OC_STRING, OC_ENUM, OC_BIGINT},
		decode:  []OpCode{OC_U64, OC_I64, OC_I32, OC_I16, OC_I8, OC_U64, OC_U32, OC_U16, OC_U8, OC_F64, OC_F32, OC_D32, OC_D64, OC_D128, OC_D256, OC_I128, OC_I256, OC_BOOL, OC_TIME, OC_BYTES, OC_FIXARRAY, OC_STRING, OC_ENUM, OC_BIGINT},
	},

	// fixed bytes and string
	{
		name:    "fixed_types",
		build:   GenericSchema[FixedTypes],
		fields:  "id,fixed_array,fixed_bytes,fixed_string",
		typs:    []types.FieldType{FT_U64, FT_BYTES, FT_BYTES, FT_STRING},
		flags:   []types.FieldFlags{types.FieldFlagPrimary, 0, 0, 0},
		scales:  []uint8{0, 0, 0, 0},
		fixed:   []uint16{0, 20, 20, 20},
		isFixed: true,
		encode:  []OpCode{OC_U64, OC_FIXARRAY, OC_FIXBYTES, OC_FIXSTRING},
		decode:  []OpCode{OC_U64, OC_FIXARRAY, OC_FIXBYTES, OC_FIXSTRING},
	},

	// struct with binary & text (un)marshaler
	{
		name:    "marshaler_struct_types",
		build:   GenericSchema[MarshalerStructTypes],
		fields:  "id,stringer,byter",
		typs:    []types.FieldType{FT_U64, FT_STRING, FT_BYTES},
		flags:   []types.FieldFlags{types.FieldFlagPrimary, 0, 0},
		scales:  []uint8{0, 0, 0},
		fixed:   []uint16{0, 0, 0},
		isFixed: false,
		encode:  []OpCode{OC_U64, OC_MSHTXT, OC_MSHBIN},
		decode:  []OpCode{OC_U64, OC_USHTXT, OC_USHBIN},
	},

	// map with binary & text (un)marshaler
	{
		name:    "marshaler_map_types",
		build:   GenericSchema[MarshalerMapTypes],
		fields:  "id,map",
		typs:    []types.FieldType{FT_U64, FT_BYTES},
		flags:   []types.FieldFlags{types.FieldFlagPrimary, 0},
		scales:  []uint8{0, 0},
		fixed:   []uint16{0, 0},
		isFixed: false,
		encode:  []OpCode{OC_U64, OC_MSHBIN},
		decode:  []OpCode{OC_U64, OC_USHBIN},
	},

	// slice with binary & text (un)marshaler
	{
		name:    "marshaler_types",
		build:   GenericSchema[MarshalerTypes],
		fields:  "id,stringer,byter",
		typs:    []types.FieldType{FT_U64, FT_STRING, FT_BYTES},
		flags:   []types.FieldFlags{types.FieldFlagPrimary, 0, 0},
		scales:  []uint8{0, 0, 0},
		fixed:   []uint16{0, 0, 0},
		isFixed: false,
		encode:  []OpCode{OC_U64, OC_MSHTXT, OC_MSHBIN},
		decode:  []OpCode{OC_U64, OC_USHTXT, OC_USHBIN},
	},

	// native int/uint
	{
		name:    "native_types",
		build:   GenericSchema[NativeTypes],
		fields:  "id,int,uint",
		typs:    []types.FieldType{FT_U64, FT_INT, FT_UINT},
		flags:   []types.FieldFlags{types.FieldFlagPrimary, 0, 0},
		scales:  []uint8{0, 0, 0},
		fixed:   []uint16{0, 0, 0},
		isFixed: true,
		encode:  []OpCode{OC_U64, OC_INT, OC_UINT},
		decode:  []OpCode{OC_U64, OC_INT, OC_UINT},
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

	// error: bloom out of range

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
		typs:      []types.FieldType{FT_U64, FT_BYTES},
		flags:     []types.FieldFlags{types.FieldFlagPrimary, types.FieldFlagIndexed},
		idxfields: "hash",
		idxtyps:   []types.IndexType{types.IndexTypeHash},
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
		typs:      []types.FieldType{FT_U64, FT_I64},
		flags:     []types.FieldFlags{types.FieldFlagPrimary, types.FieldFlagIndexed},
		idxfields: "i64",
		idxtyps:   []types.IndexType{types.IndexTypeInt},
		scales:    []uint8{0, 0},
		fixed:     []uint16{0, 0},
		isFixed:   true,
		encode:    []OpCode{OC_U64, OC_I64},
		decode:    []OpCode{OC_U64, OC_I64},
	},

	// bloom index with custom scale
	{
		name:      "bloom_index",
		build:     GenericSchema[BloomIndex],
		fields:    "id,i64",
		typs:      []types.FieldType{FT_U64, FT_I64},
		flags:     []types.FieldFlags{types.FieldFlagPrimary, types.FieldFlagIndexed},
		idxfields: "i64",
		idxtyps:   []types.IndexType{types.IndexTypeBloom},
		scales:    []uint8{0, 3},
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

	// error: invalid bloom scale param < 0
	{
		name:  "invalid bloom index scale < 0",
		build: GenericSchema[InvalidIndexBloomScaleNeg],
		iserr: true,
	},

	// error: invalid bloom scale param NaN
	{
		name:  "invalid bloom index scale NaN",
		build: GenericSchema[InvalidIndexBloomScaleNaN],
		iserr: true,
	},

	// error: invalid bloom scale param too large
	{
		name:  "invalid bloom index scale too large",
		build: GenericSchema[InvalidIndexBloomScaleTooLarge],
		iserr: true,
	},

	//
	// Internal tests
	// -----------------
	{
		name:    "internal_fields",
		build:   GenericSchema[InternalFields],
		fields:  "id,i64,u64",
		typs:    []types.FieldType{FT_U64, FT_I64, FT_U64},
		flags:   []types.FieldFlags{types.FieldFlagPrimary, types.FieldFlagInternal, 0},
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
			require.Equal(t, s.name, c.name, "schema name")
			// field names
			require.ElementsMatch(t, s.AllFieldNames(), strings.Split(c.fields, ","), "field names")
			// field types
			for i := range s.fields {
				require.Equal(t, s.fields[i].typ, c.typs[i], "field types for "+s.fields[i].name)
			}
			// field flags
			for i := range s.fields {
				require.Equal(t, s.fields[i].flags, c.flags[i], "field flags for "+s.fields[i].name)
			}
			if len(c.idxfields) > 0 {
				allIndexNames := strings.Split(c.idxfields, ",")
				// every index is detected
				for _, v := range allIndexNames {
					f, ok := s.FieldByName(v)
					require.True(t, ok)
					require.NotZero(t, f.index)
				}
				// every detected index is expected and has correct type
				for i, f := range s.Indexes() {
					// index name is expected
					require.Contains(t, allIndexNames, f.name, "index unexpected for "+f.name)
					// index types
					require.Equal(t, f.index, c.idxtyps[i], "index type for "+f.name)
				}
			}
			// scale values
			for i := range s.fields {
				require.Equal(t, s.fields[i].scale, c.scales[i], "scale for"+s.fields[i].name)
			}
			// fixed values
			for i := range s.fields {
				require.Equal(t, s.fields[i].fixed, c.fixed[i], "fixed for"+s.fields[i].name)
			}
			// is fixed
			require.Equal(t, s.isFixedSize, c.isFixed, "is_fixed")
			// encoder opcodes
			require.ElementsMatch(t, s.encode, c.encode, "encoders")
			// decoder opcodes
			require.ElementsMatch(t, s.decode, c.decode, "decoders")
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

	assert.True(t, s.EqualHash(r.Hash()))
	assert.Equal(t, s.Hash(), r.Hash())
	assert.Equal(t, s.Version(), r.Version())
	assert.Equal(t, s.Name(), r.Name())
	assert.Equal(t, s.IsFixedSize(), r.IsFixedSize())
	assert.Equal(t, s.WireSize(), r.WireSize())
	assert.Equal(t, s.NumFields(), r.NumFields())
	assert.Equal(t, s.NumActiveFields(), r.NumActiveFields())
	assert.Equal(t, s.NumVisibleFields(), r.NumVisibleFields())
	assert.Equal(t, s.NumInternalFields(), r.NumInternalFields())
	assert.Equal(t, s.AllFieldNames(), r.AllFieldNames())
	assert.Equal(t, s.AllFieldIds(), r.AllFieldIds())
	assert.Equal(t, s.VisibleFieldIds(), r.VisibleFieldIds())
	assert.Equal(t, s.InternalFieldIds(), r.InternalFieldIds())
	assert.Equal(t, s.PkId(), r.PkId())
	assert.Equal(t, s.PkIndex(), r.PkIndex())
}

// TestSchemaIsValid checks if the Schema.IsValid() method correctly identifies
// valid and invalid schema configurations.
func TestSchemaIsValid(t *testing.T) {
	s := NewSchema().WithName("test")
	require.False(t, s.IsValid())

	s.WithField(Field{name: "field1", typ: FT_I64})
	require.False(t, s.IsValid())

	s.Finalize()
	require.True(t, s.IsValid())
}

// TestSchemaNewBuffer verifies that Schema.NewBuffer() creates a buffer with
// the correct capacity based on the schema's maxWireSize.
func TestSchemaNewBuffer(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(Field{name: "field1", typ: FT_I64}).
		Finalize()

	buf := s.NewBuffer(10)
	require.NotNil(t, buf)
	require.Equal(t, 10*s.maxWireSize, buf.Cap())
}

// TestSchemaNumFields ensures that Schema.NumFields() returns the correct
// number of fields in the schema.
func TestSchemaNumFields(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(Field{name: "field1", typ: FT_I64}).
		WithField(Field{name: "field2", typ: FT_STRING}).
		Finalize()

	require.Equal(t, 2, s.NumFields())
}

// TestSchemaFieldVisibility tests correct handling of internal/deleted
// flags and whether returned field info is in correct order.
func TestSchemaFieldVisibility(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(Field{
			name: "field1",
			typ:  FT_I64,
		}).
		WithField(Field{
			name:  "field2",
			typ:   FT_STRING,
			flags: types.FieldFlagInternal,
		}).
		WithField(Field{
			name:  "field3",
			typ:   FT_U64,
			flags: types.FieldFlagDeleted,
		}).
		WithField(Field{
			name:  "field4",
			typ:   FT_U64,
			flags: types.FieldFlagInternal | types.FieldFlagDeleted,
		}).
		Finalize()

	// counts
	require.Equal(t, 4, s.NumFields())
	require.Equal(t, 1, s.NumVisibleFields())
	require.Equal(t, 2, s.NumActiveFields())
	require.Equal(t, 1, s.NumInternalFields())

	// ids
	require.Equal(t, []uint16{1, 2, 3, 4}, s.AllFieldIds())
	require.Equal(t, []uint16{1}, s.VisibleFieldIds())
	require.Equal(t, []uint16{1, 2}, s.ActiveFieldIds())
	require.Equal(t, []uint16{2}, s.InternalFieldIds())

	// names
	require.Equal(t, []string{"field1", "field2", "field3", "field4"}, s.AllFieldNames())
	require.Equal(t, []string{"field1", "field2"}, s.ActiveFieldNames())
	require.Equal(t, []string{"field1"}, s.VisibleFieldNames())
	require.Equal(t, []string{"field2"}, s.InternalFieldNames())

	// by name should hide deleted fields
	_, ok := s.FieldByName("field1")
	require.True(t, ok)
	_, ok = s.FieldByName("field2")
	require.True(t, ok)
	_, ok = s.FieldByName("field3")
	require.False(t, ok)
	_, ok = s.FieldByName("field4")
	require.False(t, ok)

	// index by name should hide deleted fields
	_, ok = s.FieldIndexByName("field1")
	require.True(t, ok)
	_, ok = s.FieldIndexByName("field2")
	require.True(t, ok)
	_, ok = s.FieldIndexByName("field3")
	require.False(t, ok)
	_, ok = s.FieldIndexByName("field4")
	require.False(t, ok)

	// by id should show all fields
	_, ok = s.FieldById(1)
	require.True(t, ok)
	_, ok = s.FieldById(2)
	require.True(t, ok)
	_, ok = s.FieldById(3)
	require.True(t, ok)
	_, ok = s.FieldById(4)
	require.True(t, ok)
}

// TestSchemaCanMatchFields checks if Schema.CanMatchFields() correctly
// identifies when a set of field names matches the schema.
func TestSchemaCanMatchFields(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(Field{
			name: "field1",
			typ:  FT_I64,
		}).
		WithField(Field{
			name:  "field2",
			typ:   FT_STRING,
			flags: types.FieldFlagInternal,
		}).
		WithField(Field{
			name:  "field3",
			typ:   FT_U64,
			flags: types.FieldFlagDeleted,
		}).
		WithField(Field{
			name:  "field4",
			typ:   FT_U64,
			flags: types.FieldFlagInternal | types.FieldFlagDeleted,
		}).
		Finalize()

	require.True(t, s.CanMatchFields("field1", "field2"))
	require.False(t, s.CanMatchFields("field3"))
	require.False(t, s.CanMatchFields("field4"))
}

// TestSchemaCanSelect verifies that Schema.CanSelect() correctly determines
// if one schema can be selected from another.
func TestSchemaCanSelect(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(Field{
			name: "field1",
			typ:  FT_I64,
		}).
		WithField(Field{
			name:  "field2",
			typ:   FT_STRING,
			flags: types.FieldFlagInternal,
		}).
		WithField(Field{
			name:  "field3",
			typ:   FT_U64,
			flags: types.FieldFlagDeleted,
		}).
		WithField(Field{
			name:  "field4",
			typ:   FT_U64,
			flags: types.FieldFlagInternal | types.FieldFlagDeleted,
		}).
		Finalize()

	// active field
	s1 := NewSchema().WithName("test1").
		WithField(Field{name: "field1", typ: FT_I64}).
		Finalize()

	require.NoError(t, s.CanSelect(s1))

	// active internal field
	s2 := NewSchema().WithName("test2").
		WithField(Field{name: "field2", typ: FT_STRING}).
		Finalize()

	require.NoError(t, s.CanSelect(s2))

	// deleted field
	s3 := NewSchema().WithName("test3").
		WithField(Field{name: "field3", typ: FT_U64}).
		Finalize()

	require.Error(t, s.CanSelect(s3))

	// deleted internal field
	s4 := NewSchema().WithName("test4").
		WithField(Field{name: "field4", typ: FT_U64}).
		Finalize()

	require.Error(t, s.CanSelect(s4))

	// non existing field
	s5 := NewSchema().WithName("test5").
		WithField(Field{name: "field5", typ: FT_U64}).
		Finalize()

	require.Error(t, s.CanSelect(s5))
}

// TestSchemaSort checks if Schema.Sort() correctly sorts the fields
// of the schema alphabetically by name.
func TestSchemaSort(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(Field{name: "field2", typ: FT_STRING}).
		WithField(Field{name: "field1", typ: FT_I64}).
		Finalize()

	// The fields should already be sorted by ID after Finalize()
	require.Equal(t, "field2", s.fields[0].name, "First field should be 'field2' (id=1)")
	require.Equal(t, "field1", s.fields[1].name, "Second field should be 'field1' (id=2)")

	// Calling Sort() shouldn't change the order
	s.Sort()

	require.Equal(t, "field2", s.fields[0].name, "First field should still be 'field2' (id=1) after sorting")
	require.Equal(t, "field1", s.fields[1].name, "Second field should still be 'field1' (id=2) after sorting")
}

// TestSchemaMapTo verifies that Schema.MapTo() correctly maps fields
// from one schema to another, even if the field order is different.
func TestSchemaMapTo(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(Field{
			name: "field1",
			typ:  FT_I64,
		}).
		WithField(Field{
			name:  "field2",
			typ:   FT_STRING,
			flags: types.FieldFlagInternal,
		}).
		WithField(Field{
			name:  "field3",
			typ:   FT_U64,
			flags: types.FieldFlagDeleted,
		}).
		WithField(Field{
			name:  "field4",
			typ:   FT_U64,
			flags: types.FieldFlagInternal | types.FieldFlagDeleted,
		}).
		Finalize()

	// active fields
	s1 := NewSchema().WithName("test1").
		WithField(Field{name: "field3", typ: FT_U64}).
		WithField(Field{name: "field1", typ: FT_I64}).
		Finalize()

	// inactive fields are hidden
	mapping, err := s.MapTo(s1)
	require.NoError(t, err)
	require.Equal(t, []int{-1, 0}, mapping)

	// deleted fields are ignored
	s2 := NewSchema().WithName("test2").
		WithField(Field{name: "field2", typ: FT_STRING}).
		WithField(Field{name: "field4", typ: FT_U64}).
		WithField(Field{name: "field3", typ: FT_U64}).
		WithField(Field{name: "field1", typ: FT_I64}).
		Finalize()

	mapping, err = s.MapTo(s2)
	require.NoError(t, err)
	require.Equal(t, []int{1, -1, -1, 0}, mapping)
}

func TestSchemaDeleteField(t *testing.T) {
	s, err := GenericSchema[AllTypes]()
	require.NoError(t, err)
	beforeSz := s.WireSize()
	beforeLen := s.NumFields()
	beforeHash := s.Hash()
	beforeVersion := s.Version()
	beforeFieldNames := s.AllFieldNames()
	beforeFieldIds := s.AllFieldIds()

	s, err = s.DeleteField(2)
	require.NoError(t, err)

	require.Len(t, s.Fields(), beforeLen)
	require.Equal(t, beforeFieldNames, s.AllFieldNames())
	require.Equal(t, beforeFieldIds, s.AllFieldIds())
	require.NotEqual(t, beforeFieldNames, s.ActiveFieldNames())
	require.NotEqual(t, beforeFieldIds, s.ActiveFieldIds())
	require.NotEqual(t, beforeFieldNames, s.VisibleFieldNames())
	require.NotEqual(t, beforeFieldIds, s.VisibleFieldIds())

	require.Equal(t, s.NumFields()-1, s.NumVisibleFields(), "num visible fields must change")
	require.Equal(t, s.NumFields()-1, s.NumActiveFields(), "num active fields must change")
	require.Less(t, s.WireSize(), beforeSz, "wire size must change")
	require.NotEqual(t, beforeHash, s.Hash(), "hash must change")
	require.Less(t, beforeVersion, s.Version(), "version must increase")

	_, ok := s.FieldByName("i64")
	require.False(t, ok, "deleted field is no longer accessible by name")
	f, ok := s.FieldById(2)
	require.True(t, ok, "deleted field still accessible by id")
	require.False(t, f.IsVisible(), "deleted field is invisibile")
	require.False(t, s.CanMatchFields("id", "i64"), "cannot match deleted field")
	_, err = s.SelectFieldIds(1, 2)
	require.Error(t, err, "cannot select deleted field")
}
