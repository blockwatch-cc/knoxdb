package schema_tests

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
)

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

// Register a global enum and dictionary for all schema tests
type MyEnum string

var (
	enums  *enum.EnumRegistry
	myEnum *enum.EnumDictionary
)

type AllTypes struct {
	Id      uint64         `knox:"id,pk"`         // 0
	Int64   int64          `knox:"i64"`           // 1
	Int32   int32          `knox:"i32"`           // 2
	Int16   int16          `knox:"i16"`           // 3
	Int8    int8           `knox:"i8"`            // 4
	Uint64  uint64         `knox:"u64"`           // 5
	Uint32  uint32         `knox:"u32"`           // 6
	Uint16  uint16         `knox:"u16"`           // 7
	Uint8   uint8          `knox:"u8"`            // 8
	Float64 float64        `knox:"f64"`           // 9
	Float32 float32        `knox:"f32"`           // 10
	D32     num.Decimal32  `knox:"d32,scale=5"`   // 11
	D64     num.Decimal64  `knox:"d64,scale=15"`  // 12
	D128    num.Decimal128 `knox:"d128,scale=18"` // 13
	D256    num.Decimal256 `knox:"d256,scale=24"` // 14
	I128    num.Int128     `knox:"i128"`          // 15
	I256    num.Int256     `knox:"i256"`          // 16
	Bool    bool           `knox:"bool"`          // 17
	Time    time.Time      `knox:"time"`          // 18
	Hash    []byte         `knox:"bytes"`         // 19
	Array   [2]byte        `knox:"array[2]"`      // 20
	String  string         `knox:"string"`        // 21
	MyEnum  MyEnum         `knox:"my_enum,enum"`  // 22
	Big     num.Big        `knox:"big"`           // 23
}

func NewAllTypes(i int64) AllTypes {
	return AllTypes{
		Id:      uint64(i),
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
		Hash:    binary.BigEndian.AppendUint64(nil, uint64(i)),
		Array:   [2]byte{byte(i >> 8 & 0xf), byte(i & 0xf)},
		String:  fmt.Sprintf("%016x", i),
		MyEnum:  MyEnum("a"),
		Big:     num.NewBig(i),
	}
}

type NativeTypes struct {
	Int  int  `knox:"int"`
	Uint uint `knox:"uint"`
}

type ArrayTypes struct {
	Id          uint64   `knox:"id,pk"`
	ByteArray   [20]byte `knox:"byte_array"`
	StringArray string   `knox:"string_array,array=20"`
}

func NewArrayTypes(i int64) ArrayTypes {
	b := binary.LittleEndian.AppendUint64(nil, uint64(i))
	buf := bytes.Repeat(b, 3)[:20]
	return ArrayTypes{
		Id:          uint64(i),
		ByteArray:   [20]byte(buf),
		StringArray: hex.EncodeToString(buf[:10]),
	}
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

type ListFields struct {
	Int64a      int64
	U64List     []uint64        `knox:"u64_list"`
	TimeList    []time.Time     `knox:"time_list,element=date"`
	PairList    []Pair          `knox:"pair_list"`
	ByteList    [][]byte        `knox:"byte_list,notnull"`
	ArrList     [][2]byte       `knox:"arr_list,notnull"`
	DecimalList []num.Decimal32 `knox:"dec_list,notnull,element=scale=4"`
	Int64b      int64
}

func NewListFields() ListFields {
	return ListFields{
		Int64a:  1,
		U64List: []uint64{2, 3},
		TimeList: []time.Time{
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 4, 4, 0, 0, 0, 0, time.UTC),
		},
		PairList: []Pair{
			{Key: 1, Val: 2},
			{Key: 3, Val: 4},
		},
		ByteList: [][]byte{
			binary.BigEndian.AppendUint64(nil, 23),
			binary.BigEndian.AppendUint64(nil, 42),
		},
		ArrList: [][2]byte{
			{1, 2},
			{3, 4},
		},
		DecimalList: []num.Decimal32{
			num.NewDecimal32(1000, 4),
			num.NewDecimal32(2000, 4),
			num.NewDecimal32(3000, 4),
		},
		Int64b: 42,
	}
}

type Pair struct {
	Key int64 `knox:"k64"`
	Val int64 `knox:"v64"`
}

type ListInListFields struct {
	Int64a      int64
	NestedUints [][]uint64
	NestedPairs [][]Pair
	Int64b      int64
}

func NewListInListFields() ListInListFields {
	return ListInListFields{
		Int64a: 1,
		NestedUints: [][]uint64{
			{2, 3},
			{4, 5},
		},
		NestedPairs: [][]Pair{
			{
				{Key: 1, Val: 2},
				{Key: 3, Val: 4},
			},
			{
				{Key: 5, Val: 6},
				{Key: 7, Val: 8},
			},
		},
		Int64b: 42,
	}
}

type OuterPairStruct struct {
	Val    uint32
	Pairs2 []Pair
}

type ListInStructInListFields struct {
	Int64a int64
	Pairs1 []OuterPairStruct
	Int64b int64
}

func NewListInStructInListFields() ListInStructInListFields {
	return ListInStructInListFields{
		Int64a: 1,
		Pairs1: []OuterPairStruct{
			{Val: 2, Pairs2: []Pair{
				{Key: 1, Val: 2},
				{Key: 3, Val: 4},
			}},
			{Val: 4, Pairs2: []Pair{
				{Key: 5, Val: 6},
				{Key: 7, Val: 8},
			}},
		},
		Int64b: 42,
	}
}
