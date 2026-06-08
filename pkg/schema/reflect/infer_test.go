// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reflect

import (
	"bytes"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/assert"
)

type MyEnum string

type AllTypes struct {
	Id          uint64                   `knox:"id,pk"`
	Int64       int64                    `knox:"i64,null"`
	Int32       int32                    `knox:"i32"`
	Int16       int16                    `knox:"i16"`
	Int8        int8                     `knox:"i8"`
	Uint64      uint64                   `knox:"u64"`
	Uint32      uint32                   `knox:"u32"`
	Uint16      uint16                   `knox:"u16"`
	Uint8       uint8                    `knox:"u8"`
	Float64     float64                  `knox:"f64"`
	Float32     float32                  `knox:"f32"`
	D32         num.Decimal32            `knox:"d32,scale=5"`
	D64         num.Decimal64            `knox:"d64,scale=15"`
	D128        num.Decimal128           `knox:"d128,scale=18"`
	D256        num.Decimal256           `knox:"d256,scale=24"`
	I128        num.Int128               `knox:"i128"`
	I256        num.Int256               `knox:"i256"`
	Bool        bool                     `knox:"bool"`
	Timestamp   time.Time                `knox:"timestamp,timebase"`
	Time        time.Time                `knox:"time,time"`
	Date        time.Time                `knox:"date,date"`
	Bytes       []byte                   `knox:"bytes"`
	BArray      [2]byte                  `knox:"array[2]"`
	String      string                   `knox:"string"`
	SArray      string                   `knox:"s_array,array=3"`
	MyEnum      MyEnum                   `knox:"my_enum,enum"`
	Big         num.Big                  `knox:"big"`
	Text        string                   `knox:"text,text"`
	Blob        []byte                   `knox:"blob,blob"`
	U64List     []uint64                 `knox:"u64_list"`
	TimeList    []time.Time              `knox:"time_list,element=date"`
	PairList    []Pair                   `knox:"pair_list"`
	ByteList    [][]byte                 `knox:"byte_list,notnull"`
	ArrList     [][2]byte                `knox:"arr_list,notnull"`
	DecimalList []num.Decimal32          `knox:"dec_list,notnull,element=scale=4"`
	U64Map      map[uint64]uint64        `knox:"u64_map"`
	DateMap     map[uint32]time.Time     `knox:"date_map,value=date"`
	PairMap     map[string]Pair          `knox:"pair_map"`
	ByteMap     map[string][]byte        `knox:"byte_map,notnull,value=notnull"`
	ArrMap      map[string][2]byte       `knox:"arr_map,notnull"`
	DecimalMap  map[string]num.Decimal32 `knox:"dec_map,notnull,value=scale=4"`
}

type Pair struct {
	Key int64 `knox:"k64"`
	Val int64 `knox:"v64"`
}

func TestFieldStructReadBasic(t *testing.T) {
	// supported field types work
	allTypeOf := reflect.TypeFor[AllTypes]()
	tests := []struct {
		name  string
		typ   schema.FieldType
		flags schema.FieldFlags
		scale uint8
	}{
		{"id", schema.Uint64, schema.FlagPrimary, 0},
		{"i64", schema.Int64, schema.FlagNullable, 0},
		{"i32", schema.Int32, 0, 0},
		{"i16", schema.Int16, 0, 0},
		{"i8", schema.Int8, 0, 0},
		{"u64", schema.Uint64, 0, 0},
		{"u32", schema.Uint32, 0, 0},
		{"u16", schema.Uint16, 0, 0},
		{"u8", schema.Uint8, 0, 0},
		{"f64", schema.Float64, 0, 0},
		{"f32", schema.Float32, 0, 0},
		{"d32", schema.Decimal32, 0, 5},
		{"d64", schema.Decimal64, 0, 15},
		{"d128", schema.Decimal128, 0, 18},
		{"d256", schema.Decimal256, 0, 24},
		{"i128", schema.Int128, 0, 0},
		{"i256", schema.Int256, 0, 0},
		{"bool", schema.Boolean, 0, 0},
		{"timestamp", schema.Timestamp, schema.FlagTimebase, 0},
		{"time", schema.Time, 0, schema.TIME_SCALE_SECOND.AsUint()},
		{"date", schema.Date, 0, schema.TIME_SCALE_DAY.AsUint()},
		{"bytes", schema.Bytes, schema.FlagNullable, 0},
		{"array[2]", schema.Bytes, schema.FlagArray, 2},
		{"string", schema.String, 0, 0},
		{"s_array", schema.String, schema.FlagArray, 3},
		{"my_enum", schema.Uint16, schema.FlagEnum, 0},
		{"big", schema.Bigint, 0, 0},
		{"text", schema.Text, 0, 0},
		{"blob", schema.Binary, schema.FlagNullable, 0},
		{"u64_list", schema.List, schema.FlagNullable, 0},
		{"time_list", schema.List, schema.FlagNullable, 0},
		{"pair_list", schema.List, schema.FlagNullable, 0},
		{"byte_list", schema.List, 0, 0},
		{"arr_list", schema.List, 0, 0},
		{"dec_list", schema.List, 0, 0},
		{"u64_map", schema.Map, schema.FlagNullable, 0},
		{"date_map", schema.Map, schema.FlagNullable, 0},
		{"pair_map", schema.Map, schema.FlagNullable, 0},
		{"byte_map", schema.Map, 0, 0},
		{"arr_map", schema.Map, 0, 0},
		{"dec_map", schema.Map, 0, 0},
	}
	for i, tt := range tests {
		sf := allTypeOf.Field(i)
		if !sf.IsExported() || sf.Anonymous || sf.Tag.Get(TAG_NAME) == "-" {
			continue
		}

		// skip empty structs (used to define composite indexes)
		if sf.Type == emptyType {
			continue
		}

		// create a new builder for each test
		b := newBuilder(allTypeOf, TAG_NAME)

		t.Run(tt.name, func(t *testing.T) {
			f, err := b.inferStructField(sf)
			assert.NoError(t, err)
			assert.Equal(t, tt.name, f.Name)
			assert.Equal(t, tt.typ, f.Type, "wrong type %s", f.Type)
			assert.Equal(t, tt.flags, f.Flags, "wrong flags %s", f.Flags)
			assert.Equal(t, tt.scale, f.Scale, "wrong scale %d", f.Scale)

			t.Logf("f[%d][%s]: type %s %s", f.Id, f.Name, f.TypeName(), f.Flags)
			if f.Child != nil {
				t.Log("child schema:", f.Child.String())
			}
		})
	}
}

type SpecialTypes struct {
	F1  int64 `knox:"f1,filter=bits"`
	F2  int64 `knox:"f2,filter=bloom2b"`
	F3  int64 `knox:"f3,filter=bloom3b"`
	F4  int64 `knox:"f4,filter=bloom4b"`
	F5  int64 `knox:"f5,filter=bloom5b"`
	F6  int64 `knox:"f6,filter=bfuse8"`
	F7  int64 `knox:"f7,filter=bfuse16"`
	F8x int64 `knox:"f8x,filter=invalid"`
	F9x int64 `knox:"f9x,filter="`

	Z1 int64 `knox:"z1,zip=snappy"`
	Z2 int64 `knox:"z2,zip=lz4"`
	Z3 int64 `knox:"z3,zip=zstd"`
	Z4 int64 `knox:"z4x,zip=invalid"`
	Z5 int64 `knox:"z5x,zi"`

	P1  uint64 `knox:"p1,pk"`
	P2  int64  `knox:"p2,id=0x42"`
	P3x int64  `knox:"p3x,pk"`
}

func TestFieldStructReadSpecial(t *testing.T) {
	sTypeOf := reflect.TypeFor[SpecialTypes]()
	tests := []struct {
		name  string
		flt   schema.FilterType
		zip   schema.Compression
		idx   schema.IndexType
		flags schema.FieldFlags
		id    uint16
		err   bool
	}{
		{"f1", schema.BitsFilter, 0, 0, 0, 1, false},
		{"f2", schema.BloomFilter2b, 0, 0, 0, 1, false},
		{"f3", schema.BloomFilter3b, 0, 0, 0, 1, false},
		{"f4", schema.BloomFilter4b, 0, 0, 0, 1, false},
		{"f5", schema.BloomFilter5b, 0, 0, 0, 1, false},
		{"f6", schema.BinaryFuseFilter8, 0, 0, 0, 1, false},
		{"f7", schema.BinaryFuseFilter16, 0, 0, 0, 1, false},
		{"f8x", 0, 0, 0, 0, 1, true},
		{"f9x", 0, 0, 0, 0, 1, true},
		{"z1", 0, schema.Snappy, 0, 0, 1, false},
		{"z2", 0, schema.LZ4, 0, 0, 1, false},
		{"z3", 0, schema.Zstd, 0, 0, 1, false},
		{"z4x", 0, 0, 0, 0, 1, true},
		{"z5x", 0, 0, 0, 0, 1, true},
		{"p1", 0, 0, 0, schema.FlagPrimary, 1, false},
		{"p2", 0, 0, 0, 0, 0x42, false},
		{"p3x", 0, 0, 0, 0, 1, true},
	}
	for i, tt := range tests {
		sf := sTypeOf.Field(i)
		if !sf.IsExported() || sf.Anonymous || sf.Tag.Get(TAG_NAME) == "-" {
			continue
		}

		// skip empty structs (used to define composite indexes)
		if sf.Type == emptyType {
			continue
		}

		// create a new builder for each test
		b := newBuilder(sTypeOf, TAG_NAME)

		t.Run(tt.name, func(t *testing.T) {
			f, err := b.inferStructField(sf)
			if tt.err {
				assert.Error(t, err)
				return
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.name, f.Name)
			assert.Equal(t, tt.flt, f.Filter)
			assert.Equal(t, tt.zip, f.Compress)
			assert.Equal(t, tt.flags, f.Flags)
			assert.Equal(t, tt.id, f.Id)
		})
	}
}

type BaseStruct struct {
	Id uint64 `knox:"id,pk"`
}

type BadEmbeddedStruct struct {
	BaseStruct
	F1 int64
}

type BadEmbeddedStructPtr struct {
	*BaseStruct
	F1 int64
}

type BadNestedStruct struct {
	F1 int64
	F2 BaseStruct
}

type BadNestedStructPtr struct {
	F1 int64
	F2 *BaseStruct
}

// not supported, used for error checks only
type MarshalerTypes struct {
	Id       uint64          `knox:"id,pk"`
	Stringer InvalidStringer `knox:"stringer"`
}

type InvalidStringer []int

func (s InvalidStringer) String() string {
	var b strings.Builder
	for _, v := range s {
		if b.Len() > 0 {
			b.WriteRune(',')
		}
		b.WriteString(strconv.Itoa(v))
	}
	return b.String()
}

func (s InvalidStringer) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s *InvalidStringer) UnmarshalText(b []byte) error {
	for v := range bytes.SplitSeq(b, []byte{','}) {
		i, err := strconv.Atoi(string(v))
		if err != nil {
			return err
		}
		*s = append(*s, i)
	}
	return nil
}

func TestBadStructEmbeds(t *testing.T) {
	_, err := SchemaFor[BadEmbeddedStruct]()
	assert.Error(t, err)
	_, err = SchemaFor[BadEmbeddedStructPtr]()
	assert.Error(t, err)
	_, err = SchemaFor[BadNestedStruct]()
	assert.Error(t, err)
	_, err = SchemaFor[BadNestedStructPtr]()
	assert.Error(t, err)
	_, err = SchemaFor[MarshalerTypes]()
	assert.Error(t, err)
}

type MapFun struct {
	MapInList []map[[2]byte]Pair          `knox:"map_in_list,notnull,element=notnull"`
	ListInMap map[[2]byte][]Pair          `knox:"list_in_map,notnull,value=notnull"`
	MapInMap  map[[2]byte]map[string]Pair `knox:"map_in_map,notnull,value=notnull"`
	TimeMap   map[time.Time]uint64        `knox:"time_map,notnull,key=date"`
}

func TestFunnyEmbeds(t *testing.T) {
	s, err := SchemaFor[MapFun]()
	assert.NoError(t, err)
	t.Log(s)
	l, err := LayoutFor[MapFun]()
	assert.NoError(t, err)
	_ = l
	// spew.Dump(l)
}
